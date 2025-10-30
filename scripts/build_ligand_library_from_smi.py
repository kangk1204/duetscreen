#!/usr/bin/env python3
"""Convert downloaded ZINC .smi tranche files into a pipeline-ready ligand CSV."""

from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path
from typing import Iterable, Iterator, List, Optional, Sequence, Tuple


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build ligand CSV from ZINC .smi files.")
    parser.add_argument("--input-dir", required=True, help="Directory containing .smi tranche files.")
    parser.add_argument("--output", required=True, help="Destination CSV path.")
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional limit on number of ligands to export (useful for sampling/testing).",
    )
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Skip rows missing either SMILES or ZINC ID (default: raise error).",
    )
    parser.add_argument(
        "--random-sample",
        action="store_true",
        help="Use reservoir sampling when --limit is set to draw a random subset.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Random seed when --random-sample is enabled (default: system random).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    input_dir = Path(args.input_dir).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    limit = args.limit if args.limit is not None and args.limit > 0 else None

    if args.random_sample and limit is None:
        raise ValueError("--random-sample requires --limit to be set.")

    sampler: Optional[random.Random] = None
    if args.random_sample:
        sampler = random.Random(args.seed)

    entries = iter_tranche_entries(input_dir, skip_missing=args.skip_missing)

    if limit is not None and args.random_sample:
        selected = reservoir_sample(entries, limit, sampler or random.Random())
        count = len(selected)
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "type", "value"])
            for smiles, zinc_id in selected:
                writer.writerow([zinc_id, "ligand", smiles])
    else:
        count = 0
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "type", "value"])
            for smiles, zinc_id in entries:
                writer.writerow([zinc_id, "ligand", smiles])
                count += 1
                if limit is not None and count >= limit:
                    break

    print(f"Wrote {count} ligands to {output_path}")
    return 0


def iter_tranche_entries(directory: Path, *, skip_missing: bool = False) -> Iterator[Tuple[str, str]]:
    for path in sorted(directory.rglob("*.smi")):
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                if line_number == 1 and stripped.lower().startswith("smiles"):
                    continue
                parts = stripped.split()
                if len(parts) < 2:
                    if skip_missing:
                        continue
                    raise ValueError(f"Unexpected format in {path}:{line_number}: {line!r}")
                smiles = parts[0]
                zinc_id = parts[1]
                yield smiles, zinc_id


def reservoir_sample(
    iterable: Iterable[Tuple[str, str]],
    k: int,
    rng: random.Random,
) -> List[Tuple[str, str]]:
    reservoir: List[Tuple[str, str]] = []
    # Classic reservoir sampling: keep the first k entries, then randomly replace existing ones
    # with decreasing probability.  Ensures every ligand has equal probability of inclusion.
    for index, entry in enumerate(iterable):
        if index < k:
            reservoir.append(entry)
            continue
        j = rng.randint(0, index)
        if j < k:
            reservoir[j] = entry
    return reservoir


if __name__ == "__main__":
    raise SystemExit(main())
