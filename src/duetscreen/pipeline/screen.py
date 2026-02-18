from __future__ import annotations

import csv
import hashlib
import json
import time
from pathlib import Path
from typing import Dict, List, Tuple

from duetscreen.config import CONTROLS_DIR, RESULTS_DIR, ZINC_DIR, MODELS_DIR
from duetscreen.data.download import archive_file
from duetscreen.models.drugban import predict_drugban
from duetscreen.models.graphdta import predict_graphdta
from duetscreen.models.moltrans import predict_moltrans


def _read_fasta(path: Path) -> str:
    seq = []
    with path.open("r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith(">"):
                continue
            seq.append(line)
    return "".join(seq)


def _load_controls(path: Path) -> Dict[str, str]:
    controls = {}
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            controls[row["smiles"]] = row["name"]
    return controls


def _hash_controls(controls: Dict[str, str]) -> str:
    h = hashlib.sha256()
    for smi in sorted(controls.keys()):
        h.update(smi.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _count_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r") as f:
        return max(0, sum(1 for _ in f) - 1)


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
        writer = csv.DictWriter(f, fieldnames=["smiles", "score"])
        writer.writeheader()


def _score_paths(name: str, out_prefix: str | None) -> Tuple[Path, Path]:
    base = f"{name}_scores" if not out_prefix else f"{out_prefix}_{name}_scores"
    out_path = RESULTS_DIR / f"{base}.csv"
    state_path = RESULTS_DIR / f"{base}.state.json"
    return out_path, state_path


def _score_smiles_stream(
    name: str,
    smiles_path: Path,
    controls: Dict[str, str],
    score_fn,
    chunk_size: int,
    out_prefix: str | None = None,
) -> bool:
    out_path, state_path = _score_paths(name, out_prefix)

    input_stat = smiles_path.stat()
    controls_hash = _hash_controls(controls)
    signature = {
        "input_path": str(smiles_path),
        "input_size": input_stat.st_size,
        "input_mtime": input_stat.st_mtime,
        "controls_hash": controls_hash,
        "chunk_size": chunk_size,
    }

    state = _load_state(state_path)
    if state and any(state.get(k) != v for k, v in signature.items()):
        if out_path.exists():
            archive_file(out_path, "smiles")
        archive_file(state_path, "smiles")
        state = None

    if state is None:
        state = {
            **signature,
            "controls_done": False,
            "file_offset": 0,
            "output_rows": 0,
            "complete": False,
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        _write_state(state_path, state)

    _ensure_scores_header(out_path)

    controls_smiles = list(controls.keys())
    if not state.get("controls_done") and controls_smiles:
        with out_path.open("a", newline="") as f:
            for i in range(0, len(controls_smiles), chunk_size):
                chunk = controls_smiles[i : i + chunk_size]
                scores = score_fn(chunk)
                if len(scores) != len(chunk):
                    raise RuntimeError(f"{name} returned {len(scores)} scores for {len(chunk)} smiles")
                for smi, score in zip(chunk, scores):
                    f.write(f"{smi},{score}\n")
                state["output_rows"] += len(chunk)
        state["controls_done"] = True
        state["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        _write_state(state_path, state)

    controls_set = set(controls_smiles)
    buffer: List[str] = []
    with smiles_path.open("rb") as f:
        f.seek(int(state.get("file_offset", 0)))
        while True:
            line = f.readline()
            if not line:
                break
            parts = line.split()
            if not parts:
                continue
            smi = parts[0]
            if smi in controls_set:
                continue
            buffer.append(smi.decode("utf-8"))
            if len(buffer) >= chunk_size:
                scores = score_fn(buffer)
                if len(scores) != len(buffer):
                    raise RuntimeError(f"{name} returned {len(scores)} scores for {len(buffer)} smiles")
                with out_path.open("a", newline="") as out_f:
                    for smi_out, score in zip(buffer, scores):
                        out_f.write(f"{smi_out},{score}\n")
                buffer.clear()
                state["file_offset"] = f.tell()
                state["output_rows"] += len(scores)
                state["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
                _write_state(state_path, state)

        if buffer:
            scores = score_fn(buffer)
            if len(scores) != len(buffer):
                raise RuntimeError(f"{name} returned {len(scores)} scores for {len(buffer)} smiles")
            with out_path.open("a", newline="") as out_f:
                for smi_out, score in zip(buffer, scores):
                    out_f.write(f"{smi_out},{score}\n")
            state["output_rows"] += len(scores)
        state["file_offset"] = f.tell()

    state["complete"] = True
    state["updated_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    _write_state(state_path, state)
    return True


def _extract_control_scores(path: Path, controls: Dict[str, str]) -> Dict[str, float]:
    scores: Dict[str, float] = {}
    targets = set(controls.keys())
    with path.open("r") as f:
        header = f.readline()
        if not header:
            return scores
        for line in f:
            parts = line.strip().split(",", 1)
            if len(parts) != 2:
                continue
            smi, score_s = parts
            if smi in targets and smi not in scores:
                try:
                    scores[smi] = float(score_s)
                except ValueError:
                    continue
                if len(scores) >= len(targets):
                    break
    return scores


def _control_ranks(path: Path, control_scores: Dict[str, float]) -> Dict[str, int]:
    ranks = {smi: 1 for smi in control_scores}
    if not control_scores:
        return ranks
    with path.open("r") as f:
        header = f.readline()
        if not header:
            return ranks
        for line in f:
            parts = line.strip().split(",", 1)
            if len(parts) != 2:
                continue
            try:
                score = float(parts[1])
            except ValueError:
                continue
            for smi, ctrl_score in control_scores.items():
                if score > ctrl_score:
                    ranks[smi] += 1
    return ranks


def screen_all_models(
    protein_fasta: Path,
    chunk_size: int = 4096,
    zinc_path: Path | None = None,
    out_prefix: str | None = None,
) -> None:
    protein_seq = _read_fasta(protein_fasta)
    controls_path = CONTROLS_DIR / "positive_controls.csv"
    zinc_path = zinc_path or (ZINC_DIR / "purchasable_druglike.smi")
    controls = _load_controls(controls_path)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    moltrans_ckpt = MODELS_DIR / "moltrans" / "best.pt"
    drugban_ckpt = MODELS_DIR / "drugban" / "best.pth"
    graphdta_ckpt = MODELS_DIR / "graphdta" / "best.pt"

    _score_smiles_stream(
        "moltrans",
        zinc_path,
        controls,
        lambda chunk: predict_moltrans(chunk, protein_seq, moltrans_ckpt),
        chunk_size,
        out_prefix,
    )
    _score_smiles_stream(
        "drugban",
        zinc_path,
        controls,
        lambda chunk: predict_drugban(chunk, protein_seq, drugban_ckpt),
        chunk_size,
        out_prefix,
    )
    _score_smiles_stream(
        "graphdta",
        zinc_path,
        controls,
        lambda chunk: predict_graphdta(chunk, protein_seq, graphdta_ckpt),
        chunk_size,
        out_prefix,
    )

    ctrl_scores = {
        "moltrans": _extract_control_scores(_score_paths("moltrans", out_prefix)[0], controls),
        "drugban": _extract_control_scores(_score_paths("drugban", out_prefix)[0], controls),
        "graphdta": _extract_control_scores(_score_paths("graphdta", out_prefix)[0], controls),
    }
    ctrl_ranks = {
        "moltrans": _control_ranks(_score_paths("moltrans", out_prefix)[0], ctrl_scores["moltrans"]),
        "drugban": _control_ranks(_score_paths("drugban", out_prefix)[0], ctrl_scores["drugban"]),
        "graphdta": _control_ranks(_score_paths("graphdta", out_prefix)[0], ctrl_scores["graphdta"]),
    }

    ctrl_name = "controls_ranking.csv" if not out_prefix else f"{out_prefix}_controls_ranking.csv"
    ctrl_out = RESULTS_DIR / ctrl_name
    ctrl_out.parent.mkdir(parents=True, exist_ok=True)
    with ctrl_out.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "smiles",
                "name",
                "moltrans_score",
                "drugban_score",
                "graphdta_score",
                "moltrans_rank",
                "drugban_rank",
                "graphdta_rank",
                "avg_rank",
            ],
        )
        writer.writeheader()
        for smi, name in controls.items():
            m_score = ctrl_scores["moltrans"].get(smi)
            d_score = ctrl_scores["drugban"].get(smi)
            g_score = ctrl_scores["graphdta"].get(smi)
            m_rank = ctrl_ranks["moltrans"].get(smi)
            d_rank = ctrl_ranks["drugban"].get(smi)
            g_rank = ctrl_ranks["graphdta"].get(smi)
            ranks = [r for r in [m_rank, d_rank, g_rank] if r is not None]
            avg_rank = sum(ranks) / len(ranks) if ranks else None
            writer.writerow(
                {
                    "smiles": smi,
                    "name": name,
                    "moltrans_score": m_score,
                    "drugban_score": d_score,
                    "graphdta_score": g_score,
                    "moltrans_rank": m_rank,
                    "drugban_rank": d_rank,
                    "graphdta_rank": g_rank,
                    "avg_rank": avg_rank,
                }
            )
