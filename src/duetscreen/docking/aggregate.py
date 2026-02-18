from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


def _load_scores(path: Path, higher_is_better: bool) -> Dict[str, float]:
    scores: Dict[str, float] = {}
    if not path.exists():
        return scores
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lig_id = row.get("ligand_id") or row.get("name")
            if not lig_id:
                continue
            try:
                score = float(row.get("score") or row.get("delta_gbsa"))
            except Exception:
                continue
            if lig_id not in scores:
                scores[lig_id] = score
            else:
                if higher_is_better:
                    scores[lig_id] = max(scores[lig_id], score)
                else:
                    scores[lig_id] = min(scores[lig_id], score)
    return scores


def _rank(scores: Dict[str, float], higher_is_better: bool) -> Dict[str, int]:
    sorted_items = sorted(scores.items(), key=lambda x: x[1], reverse=higher_is_better)
    ranks = {}
    for idx, (lig_id, _) in enumerate(sorted_items, start=1):
        ranks[lig_id] = idx
    return ranks


def aggregate_rankings(
    methods: List[Tuple[str, Path, str]],
    ligand_table: Path | None,
    out_path: Path,
) -> Path:
    method_scores: Dict[str, Dict[str, float]] = {}
    method_ranks: Dict[str, Dict[str, int]] = {}
    for name, path, direction in methods:
        higher_is_better = direction == "higher"
        scores = _load_scores(path, higher_is_better)
        if not scores:
            continue
        method_scores[name] = scores
        method_ranks[name] = _rank(scores, higher_is_better=higher_is_better)

    if not method_scores:
        raise RuntimeError("No docking scores found to aggregate.")

    ligands = set()
    for scores in method_scores.values():
        ligands.update(scores.keys())

    ligand_smiles: Dict[str, str] = {}
    if ligand_table and ligand_table.exists():
        with ligand_table.open("r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                lig_id = row.get("ligand_id")
                smi = row.get("smiles")
                if lig_id and smi:
                    ligand_smiles[lig_id] = smi

    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["ligand_id", "smiles", "avg_rank"]
    for name in method_scores.keys():
        fieldnames.extend([f"{name}_score", f"{name}_rank"])

    rows = []
    for lig_id in ligands:
        rank_sum = 0.0
        rank_count = 0
        row = {"ligand_id": lig_id, "smiles": ligand_smiles.get(lig_id, "")}
        for name, scores in method_scores.items():
            score = scores.get(lig_id)
            rank = method_ranks[name].get(lig_id)
            if score is not None:
                row[f"{name}_score"] = score
            if rank is not None:
                row[f"{name}_rank"] = rank
                rank_sum += rank
                rank_count += 1
        row["avg_rank"] = (rank_sum / rank_count) if rank_count else None
        rows.append(row)

    rows.sort(key=lambda r: r["avg_rank"] if r["avg_rank"] is not None else float("inf"))

    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return out_path
