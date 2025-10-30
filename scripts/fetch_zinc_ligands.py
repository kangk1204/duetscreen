#!/usr/bin/env python3
"""Download ligand libraries from ZINC and export as CSV."""

from __future__ import annotations

import argparse
import csv
import sys
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import requests

BASE_URL = "https://zinc15.docking.org/substances.txt"


@dataclass
class FetchResult:
    ligands: List[Tuple[str, str]]
    exhausted: bool


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download ligand library entries from ZINC.")
    parser.add_argument("--output", required=True, help="Destination CSV file for ligand library.")
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Total number of ligands to download (default: 100).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=50,
        help="Number of ligands to request per API call (default: 50).",
    )
    parser.add_argument(
        "--where",
        help='Optional ZINC where clause, e.g. \'molecular_weight between (250,350)\'.',
    )
    parser.add_argument(
        "--subsets",
        nargs="*",
        help="Optional ZINC subset filters, e.g. purchasable biogenic druglike.",
    )
    parser.add_argument(
        "--catalogs",
        nargs="*",
        help="Optional catalog filters, values should match ZINC catalog identifiers.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.5,
        help="Delay between API calls to avoid overwhelming the service (seconds).",
    )
    parser.add_argument(
        "--pipeline-ready",
        action="store_true",
        help="Emit columns compatible with HyperVS1000 library ingestion (id,type,value).",
    )
    parser.add_argument(
        "--page-start",
        type=int,
        default=1,
        help="Starting page index (1-based) when chunking massive downloads over multiple runs.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    if args.subsets and "druglike" in args.subsets and "purchasable" in args.subsets:
        print(
            "ZINC Rsync/TAR downloads are required for purchasable+druglike. "
            "Use download_zinc_purchasable.py to fetch via rsync.",
            file=sys.stderr,
        )
        return 2

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    collected: List[Tuple[str, str]] = []
    page = max(1, args.page_start)

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; HyperVS1000/0.1; +https://github.com/keunsoo/04_dtidock)"
    }

    while len(collected) < args.count:
        remaining = args.count - len(collected)
        chunk_size = min(args.chunk_size, remaining)
        result = _fetch_page(
            session=session,
            page=page,
            limit=chunk_size,
            headers=headers,
            where=args.where,
            subsets=args.subsets,
            catalogs=args.catalogs,
        )
        if not result.ligands:
            break
        collected.extend(result.ligands[:remaining])
        page += 1
        if result.exhausted:
            break
        time.sleep(max(0.0, args.delay))

    if not collected:
        print("No ligands retrieved from ZINC. Check query parameters.", file=sys.stderr)
        return 1

    _write_output(output_path, collected, pipeline_ready=args.pipeline_ready)
    print(f"Wrote {len(collected)} ligands to {output_path}")
    return 0


def _fetch_page(
    *,
    session: requests.Session,
    page: int,
    limit: int,
    headers: dict,
    where: Optional[str],
    subsets: Optional[Sequence[str]],
    catalogs: Optional[Sequence[str]],
) -> FetchResult:
    endpoint = BASE_URL
    params: List[Tuple[str, str]] = [
        ("page", str(page)),
        ("count", str(limit)),
        ("output_fields", "zinc_id"),
        ("output_fields", "smiles"),
    ]
    if where:
        params.append(("where", where))
    if subsets:
        subset_path = "+".join(subsets)
        endpoint = f"https://zinc15.docking.org/substances/subsets/{subset_path}.txt"
    if catalogs:
        for catalog in catalogs:
            params.append(("catalogs", catalog))

    response = session.get(endpoint, headers=headers, params=params, timeout=60)
    response.raise_for_status()
    text = response.text.strip()
    if not text:
        return FetchResult(ligands=[], exhausted=True)
    if text.lstrip().startswith("<"):
        raise RuntimeError("Received unexpected HTML response from ZINC. Adjust query parameters.")

    ligands = []
    for line in text.splitlines():
        parts = line.strip().split("\t")
        if len(parts) < 2:
            continue
        ligands.append((parts[0], parts[1]))
    exhausted = len(ligands) < limit
    return FetchResult(ligands=ligands, exhausted=exhausted)


def _write_output(path: Path, ligands: Iterable[Tuple[str, str]], *, pipeline_ready: bool) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        if pipeline_ready:
            writer.writerow(["id", "type", "value"])
            for zinc_id, smiles in ligands:
                writer.writerow([zinc_id, "ligand", smiles])
        else:
            writer.writerow(["id", "smiles"])
            for zinc_id, smiles in ligands:
                writer.writerow([zinc_id, smiles])


if __name__ == "__main__":
    raise SystemExit(main())
