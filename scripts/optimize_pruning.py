#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import itertools
import math
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from rdkit import Chem

from duetscreen.pipeline import hierarchy as hier


def _load_rep_meta(path: Path, parent_bits: int | None = None) -> List[dict]:
    rows = []
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            if not row.get("smiles") or not row.get("cluster_key"):
                continue
            if "parent_key" not in row and parent_bits is not None:
                scaf_hash, bits, prefix = hier._parse_cluster_key(row["cluster_key"])
                row["parent_key"] = hier._parent_key(scaf_hash, prefix, bits, parent_bits)
            rows.append(row)
    return rows


def _collect_scores(score_path: Path, targets: set[str]) -> Dict[str, float]:
    scores: Dict[str, float] = {}
    with score_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            smi = row.get("smiles")
            if not smi or smi not in targets:
                continue
            try:
                scores[smi] = float(row.get("score"))
            except Exception:
                continue
    return scores


def _rank_reps(reps: List[dict], model_scores: List[Dict[str, float]]) -> List[dict]:
    # Build per-model rank maps.
    rep_smiles = [r["smiles"] for r in reps]
    rank_maps: List[Dict[str, int]] = []
    for scores in model_scores:
        items = [(scores.get(smi), smi) for smi in rep_smiles]
        items = [(score, smi) for score, smi in items if score is not None]
        items.sort(key=lambda x: x[0], reverse=True)
        ranks = {smi: idx + 1 for idx, (_, smi) in enumerate(items)}
        rank_maps.append(ranks)

    ranked = []
    for rep in reps:
        smi = rep["smiles"]
        ranks = [rm.get(smi) for rm in rank_maps if rm.get(smi) is not None]
        if not ranks:
            continue
        avg_rank = sum(ranks) / len(ranks)
        ranked.append({**rep, "avg_rank": avg_rank})
    ranked.sort(key=lambda r: r["avg_rank"])
    return ranked


def _select_fraction(ranked: List[dict], fraction: float, parents: set[str] | None = None) -> List[dict]:
    if fraction <= 0 or fraction > 1:
        raise ValueError("fraction must be in (0,1]")
    filtered = ranked if parents is None else [r for r in ranked if r.get("parent_key") in parents]
    if not filtered:
        return []
    k = max(1, int(round(len(filtered) * fraction)))
    return filtered[:k]


def _cluster_key_for_smiles(smi: str, bits_list: List[int]) -> List[str]:
    mol = Chem.MolFromSmiles(smi)
    if mol is None:
        return []
    scaf = hier._scaffold_smiles(mol)
    scaf_hash = hier._hash_scaffold(scaf)
    sig = hier._minhash_signature(mol) if max(bits_list) > 0 else 0
    keys = []
    for bits in bits_list:
        keys.append(hier._cluster_key(scaf_hash, sig, bits))
    return keys


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer-dir", action="append", required=True,
                    help="Layer directory containing rep_meta.tsv (repeat per layer)")
    ap.add_argument("--bits", type=int, action="append", required=True,
                    help="Bits per layer (repeat per layer)")
    ap.add_argument("--baseline", type=Path, default=Path("data/results/top_intersection_10000.csv"))
    ap.add_argument("--baseline-topk", type=int, default=10000)
    ap.add_argument("--model-score", action="append", default=None,
                    help="Model score CSV (repeat). Defaults to moltrans/drugban/graphdta scores.")
    ap.add_argument("--p1", type=str, default="10,20,30,40,50")
    ap.add_argument("--p2", type=str, default="10,20,30,40,50")
    ap.add_argument("--p3", type=str, default=None)
    ap.add_argument("--out", type=Path, default=Path("data/results/pruning_optimization.csv"))
    args = ap.parse_args()

    layer_dirs = [Path(p) for p in args.layer_dir]
    bits_list = args.bits
    if len(layer_dirs) != len(bits_list):
        raise ValueError("layer-dir and bits lengths must match")

    model_scores_paths = args.model_score or [
        "data/results/moltrans_scores.csv",
        "data/results/drugban_scores.csv",
        "data/results/graphdta_scores.csv",
    ]
    model_scores_paths = [Path(p) for p in model_scores_paths]

    # Load reps.
    layers: List[List[dict]] = []
    parent_bits = [None] + bits_list[:-1]
    for layer_dir, pbits in zip(layer_dirs, parent_bits):
        rep_meta = layer_dir / "rep_meta.tsv"
        if not rep_meta.exists():
            raise FileNotFoundError(f"rep_meta.tsv not found: {rep_meta}")
        layers.append(_load_rep_meta(rep_meta, parent_bits=pbits))

    # Collect rep smiles for score lookup.
    rep_smiles = set()
    for layer in layers:
        rep_smiles.update(r["smiles"] for r in layer)

    # Read model scores for reps.
    model_scores = []
    for path in model_scores_paths:
        model_scores.append(_collect_scores(path, rep_smiles))

    # Rank reps per layer.
    ranked_layers = [_rank_reps(layer, model_scores) for layer in layers]

    # Baseline top-k hits.
    baseline_hits: List[str] = []
    with args.baseline.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if len(baseline_hits) >= args.baseline_topk:
                break
            smi = row.get("smiles")
            if smi:
                baseline_hits.append(smi)

    # Compute cluster keys for baseline hits.
    hit_keys = []
    for smi in baseline_hits:
        keys = _cluster_key_for_smiles(smi, bits_list)
        if keys:
            hit_keys.append(keys)

    def pct_list(s: str | None) -> List[float]:
        if not s:
            return [1.0]
        return [float(x) / 100.0 for x in s.split(",") if x.strip()]

    p_lists = [pct_list(args.p1), pct_list(args.p2)]
    if len(bits_list) > 2:
        p_lists.append(pct_list(args.p3))

    combos = list(itertools.product(*p_lists))

    out_rows = []
    for combo in combos:
        selected_keys: List[set[str]] = []
        # Layer 1 selection (no parent filter)
        l1 = _select_fraction(ranked_layers[0], combo[0])
        l1_keys = {r["cluster_key"] for r in l1}
        selected_keys.append(l1_keys)

        total_screened = len(ranked_layers[0])

        # Subsequent layers
        for li in range(1, len(ranked_layers)):
            frac = combo[li] if li < len(combo) else 1.0
            parents = selected_keys[li - 1]
            selected = _select_fraction(ranked_layers[li], frac, parents=parents)
            total_screened += len(selected)
            selected_keys.append({r["cluster_key"] for r in selected})

        # Recall on baseline hits.
        kept = 0
        for keys in hit_keys:
            ok = True
            for li, key in enumerate(keys):
                if li >= len(selected_keys):
                    break
                if key not in selected_keys[li]:
                    ok = False
                    break
            if ok:
                kept += 1
        recall = kept / max(1, len(hit_keys))
        efficiency = kept / max(1.0, total_screened / 1_000_000)

        out_rows.append({
            "p1": combo[0],
            "p2": combo[1] if len(combo) > 1 else 1.0,
            "p3": combo[2] if len(combo) > 2 else 1.0,
            "total_screened": total_screened,
            "baseline_hits": len(hit_keys),
            "hits_recovered": kept,
            "recall": recall,
            "hits_per_million": efficiency,
        })

    out_rows.sort(key=lambda r: (-r["recall"], -r["hits_per_million"]))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "p1",
                "p2",
                "p3",
                "total_screened",
                "baseline_hits",
                "hits_recovered",
                "recall",
                "hits_per_million",
            ],
        )
        writer.writeheader()
        for row in out_rows:
            writer.writerow(row)

    best = out_rows[0] if out_rows else None
    if best:
        print("[best]", best)


if __name__ == "__main__":
    main()
