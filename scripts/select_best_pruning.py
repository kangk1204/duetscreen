#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List


def _read_csv(path: Path, default_method: str | None = None) -> List[Dict[str, float | str]]:
    rows: List[Dict[str, float | str]] = []
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            r: Dict[str, float | str] = {}
            for k, v in row.items():
                if v is None:
                    r[k] = ""
                    continue
                if k in {"method", "gate"}:
                    r[k] = v
                    continue
                try:
                    r[k] = float(v)
                except Exception:
                    r[k] = v
            if "method" not in r or not r["method"]:
                if default_method:
                    r["method"] = default_method
            r["source"] = str(path)
            rows.append(r)
    return rows


def _best_min_screened(rows: List[Dict[str, float | str]], min_recall: float) -> Dict[str, float | str] | None:
    candidates = [r for r in rows if float(r.get("recall", 0.0)) >= min_recall]
    if not candidates:
        return None
    candidates.sort(key=lambda r: (float(r.get("total_screened", 0.0)), -float(r.get("hits_per_million", 0.0))))
    return candidates[0]


def _best_efficiency(rows: List[Dict[str, float | str]]) -> Dict[str, float | str] | None:
    if not rows:
        return None
    rows.sort(key=lambda r: (-float(r.get("hits_per_million", 0.0)), -float(r.get("recall", 0.0))))
    return rows[0]


def _pareto_front(rows: List[Dict[str, float | str]]) -> List[Dict[str, float | str]]:
    # Sort by screened ascending, keep rows with improving recall.
    rows_sorted = sorted(rows, key=lambda r: float(r.get("total_screened", 0.0)))
    best_recall = -1.0
    front = []
    for r in rows_sorted:
        recall = float(r.get("recall", 0.0))
        if recall > best_recall:
            front.append(r)
            best_recall = recall
    return front


def _write_csv(path: Path, rows: List[Dict[str, float | str]]) -> None:
    if not rows:
        return
    keys = [
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
        "source",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: r.get(k, "") for k in keys})


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inputs", action="append", default=None,
                    help="Input CSV (repeat). Defaults to pruning_optimization.csv and pruning_optimization_ml.csv if present.")
    ap.add_argument("--min-recall", type=float, default=0.95)
    ap.add_argument("--out-best", type=Path, default=Path("data/results/pruning_best.csv"))
    ap.add_argument("--out-pareto", type=Path, default=Path("data/results/pruning_pareto.csv"))
    args = ap.parse_args()

    inputs = [Path(p) for p in (args.inputs or [])]
    if not inputs:
        for p in [Path("data/results/pruning_optimization.csv"), Path("data/results/pruning_optimization_ml.csv")]:
            if p.exists():
                inputs.append(p)

    all_rows: List[Dict[str, float | str]] = []
    for p in inputs:
        if not p.exists():
            continue
        default_method = "avg_rank" if p.name == "pruning_optimization.csv" else None
        all_rows.extend(_read_csv(p, default_method=default_method))

    if not all_rows:
        print("No input rows found.")
        return

    best_min = _best_min_screened(all_rows, args.min_recall)
    best_eff = _best_efficiency(all_rows)
    pareto = _pareto_front(all_rows)

    if best_min:
        print("[best_min_screened]", best_min)
    if best_eff:
        print("[best_efficiency]", best_eff)

    # Write outputs
    out_best = []
    if best_min:
        out_best.append(best_min)
    if best_eff:
        out_best.append(best_eff)
    _write_csv(args.out_best, out_best)
    _write_csv(args.out_pareto, pareto)


if __name__ == "__main__":
    main()
