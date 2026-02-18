#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import itertools
import math
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
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


def _rank_by_avg_rank(reps: List[dict], model_scores: List[Dict[str, float]]) -> List[dict]:
    # Build per-model rank maps (missing scores treated as worst).
    rep_smiles = [r["smiles"] for r in reps]
    rank_maps: List[Dict[str, int]] = []
    for scores in model_scores:
        items = [(scores.get(smi, 0.0), smi) for smi in rep_smiles]
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


def _rank_by_avg_score(reps: List[dict], model_scores: List[Dict[str, float]]) -> List[dict]:
    ranked = []
    for rep in reps:
        smi = rep["smiles"]
        scores = [scores.get(smi, 0.0) for scores in model_scores]
        if not scores:
            continue
        avg_score = sum(scores) / len(scores)
        ranked.append({**rep, "avg_score": avg_score})
    ranked.sort(key=lambda r: r["avg_score"], reverse=True)
    return ranked


def _baseline_hit_keys(
    baseline: Path,
    bits_list: List[int],
    topk: int,
    scaffold_mode: str,
) -> List[List[str]]:
    hit_keys: List[List[str]] = []
    max_bits = max(bits_list) if bits_list else 0
    with baseline.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if len(hit_keys) >= topk:
                break
            smi = row.get("smiles")
            if not smi:
                continue
            mol = Chem.MolFromSmiles(smi)
            if mol is None:
                continue
            scaf = hier._scaffold_smiles(mol, mode=scaffold_mode)
            scaf_hash = hier._hash_scaffold(scaf)
            sig = hier._minhash_signature(mol) if max_bits > 0 else 0
            keys = [hier._cluster_key(scaf_hash, sig, bits) for bits in bits_list]
            hit_keys.append(keys)
    return hit_keys


def _select_fraction(ranked: List[dict], fraction: float, parents: set[str] | None = None) -> List[dict]:
    if fraction <= 0 or fraction > 1:
        raise ValueError("fraction must be in (0,1]")
    filtered = ranked if parents is None else [r for r in ranked if r.get("parent_key") in parents]
    if not filtered:
        return []
    k = max(1, int(round(len(filtered) * fraction)))
    return filtered[:k]


def _sigmoid(x: np.ndarray) -> np.ndarray:
    x = np.clip(x, -50, 50)
    return 1.0 / (1.0 + np.exp(-x))


def _train_logreg(
    X: np.ndarray,
    y: np.ndarray,
    epochs: int = 200,
    lr: float = 0.1,
    l2: float = 1e-4,
    batch: int = 4096,
    seed: int = 13,
) -> Tuple[np.ndarray, float, np.ndarray, np.ndarray]:
    n, d = X.shape
    mean = X.mean(axis=0)
    std = X.std(axis=0)
    std[std < 1e-8] = 1.0
    Xs = (X - mean) / std

    w = np.zeros(d, dtype=np.float32)
    b = 0.0

    pos = float(y.sum())
    neg = float(n - pos)
    w_pos = (n / (2.0 * pos)) if pos > 0 else 1.0
    w_neg = (n / (2.0 * neg)) if neg > 0 else 1.0

    rng = np.random.default_rng(seed)
    idx = np.arange(n)

    for _ in range(epochs):
        rng.shuffle(idx)
        for start in range(0, n, batch):
            sl = idx[start : start + batch]
            xb = Xs[sl]
            yb = y[sl]
            logits = xb @ w + b
            preds = _sigmoid(logits)
            weights = np.where(yb > 0, w_pos, w_neg)
            err = (preds - yb) * weights
            grad = (xb.T @ err) / len(yb) + l2 * w
            grad_b = float(err.mean())
            w -= lr * grad
            b -= lr * grad_b
    return w, b, mean, std


def _predict_logreg(X: np.ndarray, w: np.ndarray, b: float, mean: np.ndarray, std: np.ndarray) -> np.ndarray:
    Xs = (X - mean) / std
    return _sigmoid(Xs @ w + b)


def _stratified_subsample(
    X: np.ndarray,
    y: np.ndarray,
    max_n: int | None,
    seed: int = 13,
) -> Tuple[np.ndarray, np.ndarray]:
    if not max_n or len(y) <= max_n:
        return X, y
    rng = np.random.default_rng(seed)
    pos_idx = np.where(y > 0)[0]
    neg_idx = np.where(y <= 0)[0]
    max_pos = min(len(pos_idx), max_n // 2)
    max_neg = max_n - max_pos
    if max_pos == len(pos_idx):
        # keep all positives, sample negatives
        choose_pos = pos_idx
        choose_neg = rng.choice(neg_idx, size=min(max_neg, len(neg_idx)), replace=False)
    else:
        choose_pos = rng.choice(pos_idx, size=max_pos, replace=False)
        choose_neg = rng.choice(neg_idx, size=min(max_neg, len(neg_idx)), replace=False)
    idx = np.concatenate([choose_pos, choose_neg])
    rng.shuffle(idx)
    return X[idx], y[idx]


def _build_features(
    reps: List[dict],
    model_scores: List[Dict[str, float]],
    use_count: bool = True,
) -> np.ndarray:
    n = len(reps)
    m = len(model_scores)
    dim = m + (1 if use_count else 0)
    X = np.zeros((n, dim), dtype=np.float32)
    for i, rep in enumerate(reps):
        smi = rep["smiles"]
        for j, scores in enumerate(model_scores):
            X[i, j] = scores.get(smi, 0.0)
        if use_count:
            try:
                c = float(rep.get("count", 0))
            except Exception:
                c = 0.0
            X[i, m] = math.log1p(max(c, 0.0))
    return X


def _labels_from_hits(reps: List[dict], hit_keys: set[str]) -> np.ndarray:
    y = np.zeros(len(reps), dtype=np.float32)
    for i, rep in enumerate(reps):
        if rep.get("cluster_key") in hit_keys:
            y[i] = 1.0
    return y


def _pct_list(s: str | None) -> List[float]:
    if not s:
        return [1.0]
    return [float(x) / 100.0 for x in s.split(",") if x.strip()]


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
    ap.add_argument("--methods", type=str, default="avg_rank,avg_score,logreg")
    ap.add_argument("--gate", type=str, default="strict", choices=["strict", "relaxed"],
                    help="Selection gate: strict=parent-filtered AND, relaxed=layer-wise OR")
    ap.add_argument("--scaffold-mode", type=str, default="murcko",
                    choices=["murcko", "generic", "none"])
    ap.add_argument("--p1", type=str, default="10,20,30,40,50")
    ap.add_argument("--p2", type=str, default="10,20,30,40,50")
    ap.add_argument("--p3", type=str, default=None)
    ap.add_argument("--epochs", type=int, default=200)
    ap.add_argument("--lr", type=float, default=0.1)
    ap.add_argument("--l2", type=float, default=1e-4)
    ap.add_argument("--batch", type=int, default=4096)
    ap.add_argument("--max-train", type=int, default=200000)
    ap.add_argument("--seed", type=int, default=13)
    ap.add_argument("--no-count", action="store_true", help="Do not include log(count) feature")
    ap.add_argument("--out", type=Path, default=Path("data/results/pruning_optimization_ml.csv"))
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

    # Load reps per layer.
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

    # Baseline hit keys (per hit per layer) and per-layer hit sets for labels.
    hit_keys = _baseline_hit_keys(args.baseline, bits_list, args.baseline_topk, args.scaffold_mode)
    hit_key_sets: List[set[str]] = [set() for _ in bits_list]
    for keys in hit_keys:
        for li, key in enumerate(keys):
            hit_key_sets[li].add(key)

    methods = [m.strip() for m in args.methods.split(",") if m.strip()]
    p_lists = [_pct_list(args.p1), _pct_list(args.p2)]
    if len(bits_list) > 2:
        p_lists.append(_pct_list(args.p3))

    out_rows = []

    for method in methods:
        ranked_layers: List[List[dict]] = []
        if method == "avg_rank":
            ranked_layers = [_rank_by_avg_rank(layer, model_scores) for layer in layers]
        elif method == "avg_score":
            ranked_layers = [_rank_by_avg_score(layer, model_scores) for layer in layers]
        elif method == "logreg":
            use_count = not args.no_count
            for li, layer in enumerate(layers):
                X = _build_features(layer, model_scores, use_count=use_count)
                y = _labels_from_hits(layer, hit_key_sets[li])
                X_train, y_train = _stratified_subsample(X, y, args.max_train, seed=args.seed)
                w, b, mean, std = _train_logreg(
                    X_train, y_train,
                    epochs=args.epochs,
                    lr=args.lr,
                    l2=args.l2,
                    batch=args.batch,
                    seed=args.seed,
                )
                probs = _predict_logreg(X, w, b, mean, std)
                ranked = []
                for rep, prob in zip(layer, probs):
                    ranked.append({**rep, "prob": float(prob)})
                ranked.sort(key=lambda r: r["prob"], reverse=True)
                ranked_layers.append(ranked)
        else:
            raise ValueError(f"Unknown method: {method}")

        combos = list(itertools.product(*p_lists))
        for combo in combos:
            selected_keys: List[set[str]] = []
            total_screened = 0

            for li, ranked in enumerate(ranked_layers):
                frac = combo[li] if li < len(combo) else 1.0
                parents = selected_keys[li - 1] if (li > 0 and args.gate == "strict") else None
                selected = _select_fraction(ranked, frac, parents=parents)
                total_screened += len(selected)
                selected_keys.append({r["cluster_key"] for r in selected})

            kept = 0
            for keys in hit_keys:
                if args.gate == "strict":
                    ok = True
                    for li, key in enumerate(keys):
                        if li >= len(selected_keys):
                            break
                        if key not in selected_keys[li]:
                            ok = False
                            break
                    if ok:
                        kept += 1
                else:
                    # relaxed: any layer key match counts as recovered
                    hit = False
                    for li, key in enumerate(keys):
                        if li >= len(selected_keys):
                            break
                        if key in selected_keys[li]:
                            hit = True
                            break
                    if hit:
                        kept += 1

            recall = kept / max(1, len(hit_keys))
            efficiency = kept / max(1.0, total_screened / 1_000_000)

            out_rows.append({
                "method": method,
                "gate": args.gate,
                "p1": combo[0],
                "p2": combo[1] if len(combo) > 1 else 1.0,
                "p3": combo[2] if len(combo) > 2 else 1.0,
                "total_screened": total_screened,
                "baseline_hits": len(hit_keys),
                "hits_recovered": kept,
                "recall": recall,
                "hits_per_million": efficiency,
            })

    out_rows.sort(key=lambda r: (r["method"], -r["recall"], -r["hits_per_million"]))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "method",
                "gate",
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

    for method in methods:
        best = next((r for r in out_rows if r["method"] == method), None)
        if best:
            print("[best]", best)


if __name__ == "__main__":
    main()
