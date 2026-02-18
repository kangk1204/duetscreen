#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Set

from pdbfixer import PDBFixer
from openmm.app import PDBFile


def _filter_pdb(
    src: Path,
    dest: Path,
    keep_resnames: Set[str],
    keep_water: bool = False,
    keep_chains: Set[str] | None = None,
) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    in_model = False
    kept = 0
    with src.open("r") as fin, dest.open("w") as fout:
        for line in fin:
            record = line[:6].strip()
            if record == "MODEL":
                if in_model:
                    break
                in_model = True
                fout.write(line)
                continue
            if record == "ENDMDL":
                break
            if record == "ATOM":
                chain_id = line[21].strip()
                if keep_chains and chain_id not in keep_chains:
                    continue
                altloc = line[16]
                if altloc not in (" ", "A"):
                    continue
                fout.write(line)
                kept += 1
                continue
            if record == "HETATM":
                chain_id = line[21].strip()
                if keep_chains and chain_id not in keep_chains:
                    continue
                resname = line[17:20].strip()
                if resname in {"HOH", "WAT", "DOD"} and not keep_water:
                    continue
                if resname not in keep_resnames:
                    continue
                altloc = line[16]
                if altloc not in (" ", "A"):
                    continue
                fout.write(line)
                kept += 1
                continue
            # Skip TER/END records to avoid orphaned chain markers after filtering.
            if record in {"TER", "END"}:
                continue
    if kept == 0:
        raise RuntimeError(f"No atoms retained after filtering {src}")
    # Ensure file is terminated for downstream parsers.
    with dest.open("a") as fout:
        fout.write("END\n")


def _prep_with_pdbfixer(src: Path, dest: Path, ph: float) -> None:
    fixer = PDBFixer(filename=str(src))
    fixer.findMissingResidues()
    fixer.missingResidues = {}
    fixer.findMissingAtoms()
    fixer.addMissingAtoms()
    fixer.addMissingHydrogens(pH=ph)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w") as f:
        PDBFile.writeFile(fixer.topology, fixer.positions, f, keepIds=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Preprocess receptor PDB with filtering + hydrogens.")
    parser.add_argument("--input", type=Path, required=True, help="Input PDB file")
    parser.add_argument("--output", type=Path, required=True, help="Output prepped PDB file")
    parser.add_argument("--ph", type=float, default=7.4)
    parser.add_argument("--keep-resname", action="append", default=["CU"], help="HETATM residue names to keep")
    parser.add_argument("--keep-water", action="store_true", help="Keep crystallographic waters")
    parser.add_argument("--keep-chain", action="append", default=None, help="Chain ID(s) to keep")
    args = parser.parse_args()

    filtered = args.output.with_suffix(".filtered.pdb")
    keep = {r.strip().upper() for r in args.keep_resname if r}
    keep_chains = {c.strip() for c in args.keep_chain} if args.keep_chain else None
    _filter_pdb(args.input, filtered, keep_resnames=keep, keep_water=args.keep_water, keep_chains=keep_chains)
    _prep_with_pdbfixer(filtered, args.output, ph=args.ph)
    print(f"filtered={filtered}")
    print(f"prepped={args.output}")


if __name__ == "__main__":
    main()
