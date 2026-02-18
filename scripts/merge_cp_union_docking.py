#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def _prefix_columns(df: pd.DataFrame, prefix: str, keep: set[str]) -> pd.DataFrame:
    rename = {}
    for col in df.columns:
        if col in keep:
            continue
        rename[col] = f"{prefix}{col}"
    return df.rename(columns=rename)


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge union top-k DTI with docking results.")
    parser.add_argument("--union", type=Path, required=True)
    parser.add_argument("--dock-4enz", type=Path, required=True)
    parser.add_argument("--dock-af", type=Path, required=True)
    parser.add_argument("--out-csv", type=Path, required=True)
    parser.add_argument("--out-xlsx", type=Path, required=True)
    args = parser.parse_args()

    union = pd.read_csv(args.union)
    dock_4enz = pd.read_csv(args.dock_4enz)
    dock_af = pd.read_csv(args.dock_af)

    keep = {"ligand_id", "smiles"}
    dock_4enz = _prefix_columns(dock_4enz, "pdb_", keep)
    dock_af = _prefix_columns(dock_af, "af_", keep)

    merged = union.merge(dock_4enz, on=["ligand_id", "smiles"], how="left")
    merged = merged.merge(dock_af, on=["ligand_id", "smiles"], how="left")

    args.out_csv.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(args.out_csv, index=False)
    merged.to_excel(args.out_xlsx, index=False)
    print(f"wrote_csv={args.out_csv}")
    print(f"wrote_xlsx={args.out_xlsx}")


if __name__ == "__main__":
    main()
