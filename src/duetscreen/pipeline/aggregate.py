from __future__ import annotations

import csv
import heapq
from pathlib import Path
from typing import Dict, List, Tuple

from duetscreen.config import RESULTS_DIR


def _top_k_scores(path: Path, k: int) -> List[Tuple[float, str]]:
    heap: List[Tuple[float, str]] = []
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                score = float(row["score"])
            except (KeyError, ValueError, TypeError):
                continue
            smi = row.get("smiles")
            if not smi:
                continue
            if len(heap) < k:
                heapq.heappush(heap, (score, smi))
            elif score > heap[0][0]:
                heapq.heapreplace(heap, (score, smi))
    return sorted(heap, key=lambda x: x[0], reverse=True)


def _rank_map(top_scores: List[Tuple[float, str]]) -> Tuple[Dict[str, int], Dict[str, float]]:
    ranks: Dict[str, int] = {}
    scores: Dict[str, float] = {}
    for idx, (score, smi) in enumerate(top_scores, start=1):
        ranks[smi] = idx
        scores[smi] = score
    return ranks, scores


def _scores_path(name: str, prefix: str | None) -> Path:
    base = f"{name}_scores.csv" if not prefix else f"{prefix}_{name}_scores.csv"
    return RESULTS_DIR / base


def aggregate_topk(topk: int = 10000, per_model_k: int = 100000, prefix: str | None = None) -> Path:
    moltrans_path = _scores_path("moltrans", prefix)
    drugban_path = _scores_path("drugban", prefix)
    graphdta_path = _scores_path("graphdta", prefix)

    top_m = _top_k_scores(moltrans_path, per_model_k)
    top_d = _top_k_scores(drugban_path, per_model_k)
    top_g = _top_k_scores(graphdta_path, per_model_k)

    ranks_m, scores_m = _rank_map(top_m)
    ranks_d, scores_d = _rank_map(top_d)
    ranks_g, scores_g = _rank_map(top_g)

    inter = set(ranks_m) & set(ranks_d) & set(ranks_g)
    if len(inter) >= topk:
        candidates = inter
    else:
        candidates = set(ranks_m) | set(ranks_d) | set(ranks_g)

    rows = []
    missing_rank = per_model_k + 1
    for smi in candidates:
        rm = ranks_m.get(smi, missing_rank)
        rd = ranks_d.get(smi, missing_rank)
        rg = ranks_g.get(smi, missing_rank)
        avg_rank = (rm + rd + rg) / 3
        rows.append(
            {
                "smiles": smi,
                "moltrans_score": scores_m.get(smi),
                "drugban_score": scores_d.get(smi),
                "graphdta_score": scores_g.get(smi),
                "moltrans_rank": rm,
                "drugban_rank": rd,
                "graphdta_rank": rg,
                "avg_rank": avg_rank,
            }
        )

    rows.sort(key=lambda r: r["avg_rank"])
    final = rows[:topk]

    out_name = f"top_intersection_{topk}.csv" if not prefix else f"{prefix}_top_intersection_{topk}.csv"
    out_path = RESULTS_DIR / out_name
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "smiles",
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
        for row in final:
            writer.writerow(row)
    return out_path


def _load_scores_by_id(path: Path, id_field: str = "protein_id") -> Dict[str, float]:
    scores: Dict[str, float] = {}
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = row.get(id_field)
            if not pid:
                continue
            try:
                score = float(row["score"])
            except (KeyError, ValueError, TypeError):
                continue
            scores[pid] = score
    return scores


def _rank_all(scores: Dict[str, float]) -> Dict[str, int]:
    ordered = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    ranks: Dict[str, int] = {}
    for idx, (pid, _) in enumerate(ordered, start=1):
        ranks[pid] = idx
    return ranks


def aggregate_protein_topk(prefix: str, topk: int = 1000) -> Path:
    moltrans_path = RESULTS_DIR / f"{prefix}_moltrans_scores.csv"
    drugban_path = RESULTS_DIR / f"{prefix}_drugban_scores.csv"
    graphdta_path = RESULTS_DIR / f"{prefix}_graphdta_scores.csv"

    scores_m = _load_scores_by_id(moltrans_path)
    scores_d = _load_scores_by_id(drugban_path)
    scores_g = _load_scores_by_id(graphdta_path)

    common = set(scores_m) & set(scores_d) & set(scores_g)
    if not common:
        raise RuntimeError("No common protein IDs across models.")

    ranks_m = _rank_all({k: scores_m[k] for k in common})
    ranks_d = _rank_all({k: scores_d[k] for k in common})
    ranks_g = _rank_all({k: scores_g[k] for k in common})

    rows = []
    for pid in common:
        rm = ranks_m[pid]
        rd = ranks_d[pid]
        rg = ranks_g[pid]
        avg_rank = (rm + rd + rg) / 3
        rows.append(
            {
                "protein_id": pid,
                "moltrans_score": scores_m[pid],
                "drugban_score": scores_d[pid],
                "graphdta_score": scores_g[pid],
                "moltrans_rank": rm,
                "drugban_rank": rd,
                "graphdta_rank": rg,
                "avg_rank": avg_rank,
            }
        )

    rows.sort(key=lambda r: r["avg_rank"])
    final = rows[:topk]

    out_path = RESULTS_DIR / f"{prefix}_top_{topk}.csv"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "protein_id",
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
        for row in final:
            writer.writerow(row)
    return out_path
