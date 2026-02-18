#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import heapq
from pathlib import Path
from typing import Dict, List, Tuple


def _top_k(path: Path, k: int) -> List[Tuple[float, str]]:
    heap: List[Tuple[float, str]] = []
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            smi = row.get("smiles")
            if not smi:
                continue
            try:
                score = float(row["score"])
            except Exception:
                continue
            if len(heap) < k:
                heapq.heappush(heap, (score, smi))
            elif score > heap[0][0]:
                heapq.heapreplace(heap, (score, smi))
    return sorted(heap, key=lambda x: x[0], reverse=True)


def _ligand_id(smiles: str) -> str:
    return "L" + hashlib.sha1(smiles.encode("utf-8")).hexdigest()[:12]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build union of per-model top-k SMILES.")
    parser.add_argument("--moltrans", type=Path, required=True)
    parser.add_argument("--drugban", type=Path, required=True)
    parser.add_argument("--graphdta", type=Path, required=True)
    parser.add_argument("--topk", type=int, default=10000)
    parser.add_argument("--out-csv", type=Path, required=True)
    parser.add_argument("--out-smi", type=Path, required=True)
    args = parser.parse_args()

    top_m = _top_k(args.moltrans, args.topk)
    top_d = _top_k(args.drugban, args.topk)
    top_g = _top_k(args.graphdta, args.topk)

    union: Dict[str, Dict[str, object]] = {}

    def _add(model: str, top: List[Tuple[float, str]]) -> None:
        for rank, (score, smi) in enumerate(top, start=1):
            row = union.setdefault(smi, {"smiles": smi})
            row[f"{model}_score"] = score
            row[f"{model}_rank"] = rank
            row[f"in_{model}"] = 1

    _add("moltrans", top_m)
    _add("drugban", top_d)
    _add("graphdta", top_g)

    out_rows = []
    for smi, row in union.items():
        row["ligand_id"] = _ligand_id(smi)
        row["source_count"] = int(row.get("in_moltrans", 0)) + int(row.get("in_drugban", 0)) + int(
            row.get("in_graphdta", 0)
        )
        out_rows.append(row)

    out_rows.sort(key=lambda r: (-r["source_count"], r.get("moltrans_rank", 10**9)))

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    args.out_smi.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "ligand_id",
        "smiles",
        "source_count",
        "in_moltrans",
        "in_drugban",
        "in_graphdta",
        "moltrans_score",
        "drugban_score",
        "graphdta_score",
        "moltrans_rank",
        "drugban_rank",
        "graphdta_rank",
    ]

    with args.out_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in out_rows:
            writer.writerow(row)

    with args.out_smi.open("w") as f:
        for row in out_rows:
            f.write(f"{row['smiles']}\t{row['ligand_id']}\n")

    print(f"union_count={len(out_rows)}")
    print(f"wrote_csv={args.out_csv}")
    print(f"wrote_smi={args.out_smi}")


if __name__ == "__main__":
    main()
