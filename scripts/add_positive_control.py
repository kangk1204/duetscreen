#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
from pathlib import Path


def _ligand_id(smiles: str) -> str:
    return "L" + hashlib.sha1(smiles.encode("utf-8")).hexdigest()[:12]


def main() -> None:
    parser = argparse.ArgumentParser(description="Append or tag a positive control in union CSV/SMI.")
    parser.add_argument("--union-csv", type=Path, required=True)
    parser.add_argument("--union-smi", type=Path, required=True)
    parser.add_argument("--smiles", type=str, required=True)
    parser.add_argument("--name", type=str, default="positive_control")
    args = parser.parse_args()

    smiles = args.smiles.strip()
    ligand_id = _ligand_id(smiles)

    tmp = args.union_csv.with_suffix(".tmp.csv")
    found = False

    with args.union_csv.open("r", newline="") as fin, tmp.open("w", newline="") as fout:
        reader = csv.DictReader(fin)
        fieldnames = list(reader.fieldnames or [])
        if "is_positive_control" not in fieldnames:
            fieldnames.append("is_positive_control")
        if "positive_control_name" not in fieldnames:
            fieldnames.append("positive_control_name")
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        for row in reader:
            if row.get("smiles") == smiles:
                row["is_positive_control"] = "1"
                row["positive_control_name"] = args.name
                if not row.get("ligand_id"):
                    row["ligand_id"] = ligand_id
                found = True
            else:
                row.setdefault("is_positive_control", "0")
                row.setdefault("positive_control_name", "")
            writer.writerow(row)

        if not found:
            writer.writerow(
                {
                    "ligand_id": ligand_id,
                    "smiles": smiles,
                    "source_count": "",
                    "in_moltrans": "",
                    "in_drugban": "",
                    "in_graphdta": "",
                    "moltrans_score": "",
                    "drugban_score": "",
                    "graphdta_score": "",
                    "moltrans_rank": "",
                    "drugban_rank": "",
                    "graphdta_rank": "",
                    "is_positive_control": "1",
                    "positive_control_name": args.name,
                }
            )

    tmp.replace(args.union_csv)

    # Update SMI list for docking.
    args.union_smi.parent.mkdir(parents=True, exist_ok=True)
    smi_present = False
    if args.union_smi.exists():
        with args.union_smi.open("r") as f:
            for line in f:
                if line.strip().startswith(smiles):
                    smi_present = True
                    break
    if not smi_present:
        with args.union_smi.open("a") as f:
            f.write(f"{smiles}\t{ligand_id}\n")

    print(f"ligand_id={ligand_id}")
    print(f"found_existing={found}")


if __name__ == "__main__":
    main()
