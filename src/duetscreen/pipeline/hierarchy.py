from __future__ import annotations

import csv
import hashlib
import json
import random
import time
from collections import Counter
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple

from rdkit import Chem
from rdkit.Chem import rdMolDescriptors
from rdkit.Chem.Scaffolds import MurckoScaffold


PRIME = 4_294_967_311  # large 32-bit prime
NUM_HASHES = 4
_rng = random.Random(13)
HASH_COEFFS: List[Tuple[int, int]] = [
    (_rng.randrange(1, PRIME - 1), _rng.randrange(0, PRIME - 1)) for _ in range(NUM_HASHES)
]


def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")


def _iter_smiles(path: Path, limit: int | None = None) -> Iterator[str]:
    with path.open("r") as f:
        for line in f:
            if limit is not None and limit <= 0:
                break
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if not parts:
                continue
            yield parts[0]
            if limit is not None:
                limit -= 1


def _scaffold_smiles(mol: Chem.Mol, mode: str = "murcko") -> str:
    if mode == "none":
        return "ALL"
    try:
        scaf = MurckoScaffold.MurckoScaffoldSmiles(mol=mol, includeChirality=False)
        if not scaf:
            return "NOSCAF"
        if mode == "generic":
            scaf_mol = Chem.MolFromSmiles(scaf)
            if scaf_mol is None:
                return "NOSCAF"
            scaf_mol = MurckoScaffold.MakeScaffoldGeneric(scaf_mol)
            scaf = Chem.MolToSmiles(scaf_mol, isomericSmiles=False)
            return scaf if scaf else "NOSCAF"
        return scaf
    except Exception:
        return "NOSCAF"


def _hash_scaffold(scaffold: str) -> str:
    return hashlib.blake2b(scaffold.encode("utf-8"), digest_size=8).hexdigest()


def _minhash_signature(mol: Chem.Mol) -> int:
    feats = rdMolDescriptors.GetMorganFingerprint(mol, 2).GetNonzeroElements().keys()
    if not feats:
        return 0
    mins = [PRIME] * NUM_HASHES
    for feat in feats:
        for i, (a, b) in enumerate(HASH_COEFFS):
            h = (a * feat + b) % PRIME
            if h < mins[i]:
                mins[i] = h
    sig = 0
    for m in mins:
        sig = (sig << 16) | (m & 0xFFFF)
    return sig


def _cluster_key(scaffold_hash: str, sig: int, bits: int) -> str:
    if bits <= 0:
        return f"{scaffold_hash}:0:0"
    prefix = sig >> (64 - bits)
    return f"{scaffold_hash}:{bits}:{prefix}"


def _parse_cluster_key(key: str) -> Tuple[str, int, int]:
    parts = key.split(":")
    if len(parts) != 3:
        raise ValueError(f"Invalid cluster_key: {key}")
    scaf_hash, bits_s, prefix_s = parts
    return scaf_hash, int(bits_s), int(prefix_s)


def _parent_key(scaf_hash: str, prefix: int, bits: int, parent_bits: int) -> str:
    if parent_bits == 0:
        return f"{scaf_hash}:0:0"
    if parent_bits > bits:
        raise ValueError("parent_bits must be <= bits")
    shift = bits - parent_bits
    parent_prefix = prefix >> shift if shift > 0 else prefix
    return f"{scaf_hash}:{parent_bits}:{parent_prefix}"


def build_layer(
    smiles_path: Path,
    out_dir: Path,
    bits: int,
    rep_target: int,
    limit: int | None = None,
    write_all_counts: bool = False,
    log_every: int = 1_000_000,
    per_parent: int | None = None,
    parent_bits: int | None = None,
    parent_keys_path: Path | None = None,
    scaffold_mode: str = "murcko",
) -> Dict[str, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    counts: Dict[str, int] = {}
    total = 0
    invalid = 0

    parent_keys: set[str] | None = None
    if parent_keys_path:
        parent_keys = {line.strip() for line in parent_keys_path.read_text().splitlines() if line.strip()}

    if per_parent is not None and parent_bits is None:
        raise ValueError("parent_bits is required when per_parent is set")
    if parent_bits is not None and parent_bits > bits:
        raise ValueError("parent_bits must be <= bits")

    for smi in _iter_smiles(smiles_path, limit=limit):
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            invalid += 1
            continue
        scaf = _scaffold_smiles(mol, mode=scaffold_mode)
        scaf_hash = _hash_scaffold(scaf)
        sig = _minhash_signature(mol) if (bits > 0 or (parent_bits and parent_bits > 0)) else 0
        key = _cluster_key(scaf_hash, sig, bits)
        if parent_keys is not None:
            pbits = parent_bits or 0
            parent_key = _parent_key(scaf_hash, sig >> max(64 - bits, 0), bits, pbits)
            if parent_key not in parent_keys:
                continue
        counts[key] = counts.get(key, 0) + 1
        total += 1
        if log_every and total % log_every == 0:
            print(f"[hierarchy] processed={total} invalid={invalid} clusters={len(counts)}")

    if per_parent is None:
        top = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:rep_target]
    else:
        groups: Dict[str, List[Tuple[str, int]]] = {}
        for key, count in counts.items():
            scaf_hash, key_bits, prefix = _parse_cluster_key(key)
            parent_key = _parent_key(scaf_hash, prefix, key_bits, parent_bits or 0)
            groups.setdefault(parent_key, []).append((key, count))
        top = []
        for parent_key, items in groups.items():
            items.sort(key=lambda x: x[1], reverse=True)
            top.extend(items[:per_parent])
        top.sort(key=lambda x: x[1], reverse=True)
    top_keys = {key for key, _ in top}
    rep_smiles: Dict[str, str] = {}
    rep_scaffold: Dict[str, str] = {}
    rep_parent: Dict[str, str] = {}

    for smi in _iter_smiles(smiles_path, limit=limit):
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            continue
        scaf = _scaffold_smiles(mol, mode=scaffold_mode)
        scaf_hash = _hash_scaffold(scaf)
        sig = _minhash_signature(mol) if (bits > 0 or (parent_bits and parent_bits > 0)) else 0
        key = _cluster_key(scaf_hash, sig, bits)
        if key not in top_keys:
            continue
        if key not in rep_smiles:
            rep_smiles[key] = smi
            rep_scaffold[scaf_hash] = scaf
            if parent_bits is not None:
                rep_parent[key] = _parent_key(scaf_hash, sig >> max(64 - bits, 0), bits, parent_bits)
            if len(rep_smiles) >= len(top_keys):
                break

    rep_meta_path = out_dir / "rep_meta.tsv"
    rep_smiles_path = out_dir / "rep_smiles.smi"
    top_counts_path = out_dir / "cluster_top.tsv"
    all_counts_path = out_dir / "cluster_counts.tsv"
    state_path = out_dir / "state.json"

    with top_counts_path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["cluster_key", "count"])
        for key, count in top:
            writer.writerow([key, count])

    with rep_meta_path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        header = ["smiles", "cluster_key", "count", "scaffold_hash", "scaffold_smiles"]
        if parent_bits is not None:
            header.insert(2, "parent_key")
        writer.writerow(header)
        for key, count in top:
            smi = rep_smiles.get(key)
            if not smi:
                continue
            scaf_hash = key.split(":", 1)[0]
            row = [smi, key]
            if parent_bits is not None:
                row.append(rep_parent.get(key, ""))
            row.extend([count, scaf_hash, rep_scaffold.get(scaf_hash, "")])
            writer.writerow(row)

    with rep_smiles_path.open("w") as f:
        for key, _ in top:
            smi = rep_smiles.get(key)
            if smi:
                f.write(f"{smi}\n")

    if write_all_counts:
        with all_counts_path.open("w", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            writer.writerow(["cluster_key", "count"])
            for key, count in counts.items():
                writer.writerow([key, count])

    state = {
        "input": str(smiles_path),
        "bits": bits,
        "rep_target": rep_target,
        "per_parent": per_parent,
        "parent_bits": parent_bits,
        "scaffold_mode": scaffold_mode,
        "total": total,
        "invalid": invalid,
        "clusters": len(counts),
        "updated_at": _now(),
    }
    state_path.write_text(json.dumps(state, indent=2))

    return {
        "rep_meta": rep_meta_path,
        "rep_smiles": rep_smiles_path,
        "cluster_top": top_counts_path,
        "cluster_counts": all_counts_path if write_all_counts else top_counts_path,
        "state": state_path,
    }


def expand_layer(
    smiles_path: Path,
    selected_keys_path: Path,
    bits: int,
    out_path: Path,
    limit: int | None = None,
    log_every: int = 1_000_000,
    scaffold_mode: str = "murcko",
) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    selected = {line.strip() for line in selected_keys_path.read_text().splitlines() if line.strip()}
    if not selected:
        raise ValueError("selected_keys is empty")

    total = 0
    hits = 0
    with out_path.open("w") as out_f:
        for smi in _iter_smiles(smiles_path, limit=limit):
            mol = Chem.MolFromSmiles(smi)
            if mol is None:
                continue
            scaf = _scaffold_smiles(mol, mode=scaffold_mode)
            scaf_hash = _hash_scaffold(scaf)
            sig = _minhash_signature(mol) if bits > 0 else 0
            key = _cluster_key(scaf_hash, sig, bits)
            if key in selected:
                out_f.write(f"{smi}\n")
                hits += 1
            total += 1
            if log_every and total % log_every == 0:
                print(f"[hierarchy] scanned={total} matched={hits}")
    return out_path


def select_keys(
    scores_path: Path,
    rep_meta_path: Path,
    out_keys: Path,
    topk: int | None,
    score_column: str = "score",
    smiles_column: str = "smiles",
    ascending: bool = False,
    top_fraction: float | None = None,
    top_percent: float | None = None,
) -> Path:
    if top_fraction is not None and top_percent is not None:
        raise ValueError("Provide only one of top_fraction or top_percent")

    rep_map: Dict[str, str] = {}
    with rep_meta_path.open("r", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            smi = row.get("smiles")
            key = row.get("cluster_key")
            if smi and key:
                rep_map[smi] = key

    scored: List[Tuple[float, str]] = []
    with scores_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            smi = row.get(smiles_column)
            if not smi or smi not in rep_map:
                continue
            try:
                score = float(row.get(score_column))
            except Exception:
                continue
            scored.append((score, smi))

    scored.sort(key=lambda x: x[0], reverse=not ascending)

    if top_fraction is None and top_percent is None and topk is None:
        raise ValueError("Provide topk or top_fraction or top_percent")

    if top_percent is not None:
        top_fraction = top_percent / 100.0

    if top_fraction is not None:
        if top_fraction <= 0 or top_fraction > 1:
            raise ValueError("top_fraction must be in (0, 1]")
        topk = max(1, int(round(len(scored) * top_fraction)))

    if topk is None:
        raise ValueError("topk could not be determined")

    selected_keys: List[str] = []
    seen = set()
    for _, smi in scored[:topk]:
        key = rep_map.get(smi)
        if key and key not in seen:
            selected_keys.append(key)
            seen.add(key)

    out_keys.parent.mkdir(parents=True, exist_ok=True)
    out_keys.write_text("\n".join(selected_keys) + "\n")
    return out_keys
