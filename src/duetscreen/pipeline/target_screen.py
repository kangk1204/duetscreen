from __future__ import annotations

import csv
import hashlib
import json
import time
from pathlib import Path
from typing import Callable, Dict, List, Tuple

from duetscreen.config import MODELS_DIR, RESULTS_DIR
from duetscreen.data.alphafold import fetch_human_proteome_fasta
from duetscreen.data.download import archive_file
from duetscreen.data.fasta import read_fasta
from duetscreen.data.positive_controls import fetch_smiles
from duetscreen.models.drugban import load_drugban_model, predict_drugban_pairs
from duetscreen.models.graphdta import load_graphdta_model, predict_graphdta_pairs
from duetscreen.models.moltrans import load_moltrans_model, predict_moltrans_pairs


def _hash_inputs(ligand_smiles: str, proteome_path: Path, chunk_size: int) -> Dict[str, object]:
    st = proteome_path.stat()
    digest = hashlib.sha256(ligand_smiles.encode("utf-8")).hexdigest()
    return {
        "ligand_hash": digest,
        "proteome_path": str(proteome_path),
        "proteome_size": st.st_size,
        "proteome_mtime": st.st_mtime,
        "chunk_size": chunk_size,
    }


def _load_state(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _write_state(path: Path, state: dict) -> None:
    path.write_text(json.dumps(state, indent=2))


def _ensure_scores_header(path: Path) -> None:
    if path.exists() and path.stat().st_size > 0:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["protein_id", "score"])
        writer.writeheader()


def _score_proteins_stream(
    name: str,
    ligand_smiles: str,
    protein_ids: List[str],
    protein_seqs: List[str],
    score_fn: Callable[[List[str], List[str]], List[float]],
    chunk_size: int,
    out_prefix: str,
    preview: int,
    proteome_path: Path,
) -> Path:
    out_path = RESULTS_DIR / f"{out_prefix}_{name}_scores.csv"
    state_path = RESULTS_DIR / f"{out_prefix}_{name}_scores.state.json"
    signature = _hash_inputs(ligand_smiles, proteome_path, chunk_size)

    state = _load_state(state_path)
    if state and any(state.get(k) != v for k, v in signature.items()):
        if out_path.exists():
            archive_file(out_path, "params")
        archive_file(state_path, "params")
        state = None

    if state is None:
        state = {
            **signature,
            "index": 0,
            "output_rows": 0,
            "complete": False,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        _write_state(state_path, state)

    _ensure_scores_header(out_path)

    start_idx = int(state.get("index", 0))
    total = len(protein_ids)
    if start_idx >= total:
        state["complete"] = True
        _write_state(state_path, state)
        return out_path

    preview_left = preview if start_idx == 0 else 0

    with out_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["protein_id", "score"])
        for i in range(start_idx, total, chunk_size):
            chunk_ids = protein_ids[i : i + chunk_size]
            chunk_seqs = protein_seqs[i : i + chunk_size]
            smiles_list = [ligand_smiles] * len(chunk_ids)
            scores = score_fn(smiles_list, chunk_seqs)
            if len(scores) != len(chunk_ids):
                raise RuntimeError(f"{name} returned {len(scores)} scores for {len(chunk_ids)} proteins")
            for pid, score in zip(chunk_ids, scores):
                writer.writerow({"protein_id": pid, "score": score})
                if preview_left > 0:
                    print(f"[{name}] {pid}\t{score}")
                    preview_left -= 1
            state["index"] = i + len(chunk_ids)
            state["output_rows"] += len(chunk_ids)
            state["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            _write_state(state_path, state)

    state["complete"] = True
    state["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    _write_state(state_path, state)
    return out_path


def screen_ligand_against_proteome(
    ligand_name: str | None = None,
    ligand_smiles: str | None = None,
    proteome_fasta: Path | None = None,
    out_prefix: str | None = None,
    chunk_size: int = 512,
    limit: int | None = None,
    preview: int = 5,
) -> None:
    if not ligand_smiles:
        if not ligand_name:
            raise ValueError("Provide ligand_name or ligand_smiles")
        ligand_smiles = fetch_smiles(ligand_name)

    if proteome_fasta is None:
        proteome_fasta = fetch_human_proteome_fasta()

    records = read_fasta(proteome_fasta)
    if limit:
        records = records[:limit]
    protein_ids = [pid for pid, _ in records]
    protein_seqs = [seq for _, seq in records]

    out_prefix = out_prefix or (ligand_name.lower().replace(" ", "_") if ligand_name else "ligand")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    # Store the proteins used for reproducibility.
    proteome_copy = RESULTS_DIR / f"{out_prefix}_proteins.fasta"
    if not proteome_copy.exists():
        from duetscreen.data.fasta import write_fasta
        write_fasta(records, proteome_copy)

    moltrans_ckpt = MODELS_DIR / "moltrans" / "best.pt"
    drugban_ckpt = MODELS_DIR / "drugban" / "best.pth"
    graphdta_ckpt = MODELS_DIR / "graphdta" / "best.pt"

    moltrans_model = load_moltrans_model(moltrans_ckpt, batch_size=64)
    drugban_model = load_drugban_model(drugban_ckpt)
    graphdta_model = load_graphdta_model(graphdta_ckpt)

    _score_proteins_stream(
        "moltrans",
        ligand_smiles,
        protein_ids,
        protein_seqs,
        lambda smi, prot: predict_moltrans_pairs(
            smi, prot, moltrans_ckpt, batch_size=64, model=moltrans_model
        ),
        chunk_size,
        out_prefix,
        preview,
        proteome_copy,
    )

    _score_proteins_stream(
        "drugban",
        ligand_smiles,
        protein_ids,
        protein_seqs,
        lambda smi, prot: predict_drugban_pairs(
            smi, prot, drugban_ckpt, batch_size=64, model=drugban_model
        ),
        chunk_size,
        out_prefix,
        preview,
        proteome_copy,
    )

    _score_proteins_stream(
        "graphdta",
        ligand_smiles,
        protein_ids,
        protein_seqs,
        lambda smi, prot: predict_graphdta_pairs(
            smi, prot, graphdta_ckpt, batch_size=256, model=graphdta_model
        ),
        chunk_size,
        out_prefix,
        preview,
        proteome_copy,
    )
