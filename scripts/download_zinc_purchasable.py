#!/usr/bin/env python3
"""Batch-download ZINC purchasable druglike ligands and concatenate into one CSV."""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import List, Optional, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import fetch_zinc_ligands


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download purchasable druglike subsets from ZINC.")
    parser.add_argument(
        "--output",
        required=True,
        help="Destination CSV file that aggregates all downloaded ligands.",
    )
    parser.add_argument(
        "--total",
        type=int,
        default=200_000,
        help="Total number of ligands to collect across all batches (default: 200000).",
    )
    parser.add_argument(
        "--per-run",
        type=int,
        default=50_000,
        help="Number of ligands to request per fetch invocation (default: 50000).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=500,
        help="Number of ligands to request per page within each run (default: 500).",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay between page requests passed through to fetch_zinc_ligands (default: 1 second).",
    )
    parser.add_argument(
        "--tmp-dir",
        help="Directory for intermediate chunk CSVs (default: alongside output).",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Retain intermediate chunk CSVs instead of deleting after concatenation.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    output_path = Path(args.output).expanduser().resolve()
    tmp_dir = Path(args.tmp_dir).expanduser().resolve() if args.tmp_dir else output_path.parent
    tmp_dir.mkdir(parents=True, exist_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.per_run <= 0 or args.chunk_size <= 0:
        raise ValueError("per-run and chunk-size must be positive integers.")
    if args.total <= 0:
        raise ValueError("total must be positive.")

    runs = math.ceil(args.total / args.per_run)
    pages_per_run = math.ceil(args.per_run / args.chunk_size)
    collected_files: List[Path] = []

    for run_idx in range(runs):
        run_total = min(args.per_run, args.total - run_idx * args.per_run)
        page_start = 1 + run_idx * pages_per_run
        chunk_path = tmp_dir / f"zinc_pd_chunk_{run_idx+1:03d}.csv"
        collected_files.append(chunk_path)

        fetch_args = [
            "--output",
            str(chunk_path),
            "--count",
            str(run_total),
            "--chunk-size",
            str(args.chunk_size),
            "--delay",
            str(args.delay),
            "--subsets",
            "purchasable",
            "druglike",
            "--pipeline-ready",
            "--page-start",
            str(page_start),
        ]

        print(f"[run {run_idx+1}/{runs}] fetching {run_total} ligands starting at page {page_start}")
        status = fetch_zinc_ligands.main(fetch_args)
        if status != 0:
            print(f"Fetch run {run_idx+1} failed with status {status}", file=sys.stderr)
            return status

    _concatenate_csv(collected_files, output_path)
    print(f"Concatenated {len(collected_files)} chunks into {output_path}")

    if not args.keep_temp:
        for path in collected_files:
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    return 0


def _concatenate_csv(chunks: Sequence[Path], destination: Path) -> None:
    if not chunks:
        raise ValueError("No chunk files provided for concatenation.")

    with destination.open("w", encoding="utf-8") as dest:
        dest.write("id,type,value\n")
        for idx, chunk in enumerate(chunks, start=1):
            with chunk.open("r", encoding="utf-8") as source:
                for line_no, line in enumerate(source, start=1):
                    if idx > 1 and line_no == 1:
                        continue  # skip header
                    dest.write(line)


if __name__ == "__main__":
    raise SystemExit(main())
