from __future__ import annotations

import csv
from pathlib import Path
from typing import List, Tuple

from duetscreen.docking.utils import run_cmd, which


def read_ligands(
    path: Path,
    smiles_column: str = "smiles",
    id_column: str | None = None,
    limit: int | None = None,
) -> List[Tuple[str, str]]:
    ligands: List[Tuple[str, str]] = []
    suffix = path.suffix.lower()
    if suffix in {".smi", ".smiles"}:
        with path.open("r") as f:
            for line in f:
                if limit and len(ligands) >= limit:
                    break
                parts = line.strip().split()
                if not parts:
                    continue
                smi = parts[0]
                lig_id = parts[1] if len(parts) > 1 else f"L{len(ligands)+1:06d}"
                ligands.append((lig_id, smi))
        return ligands

    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if limit and len(ligands) >= limit:
                break
            smi = row.get(smiles_column)
            if not smi:
                continue
            lig_id = row.get(id_column) if id_column else None
            if not lig_id:
                lig_id = f"L{len(ligands)+1:06d}"
            ligands.append((lig_id, smi))
    return ligands


def write_ligand_table(ligands: List[Tuple[str, str]], out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ligand_id", "smiles"])
        writer.writeheader()
        for lig_id, smi in ligands:
            writer.writerow({"ligand_id": lig_id, "smiles": smi})
    return out_path


def smiles_to_sdf(ligands: List[Tuple[str, str]], out_sdf: Path, embed: bool = True) -> Path:
    out_sdf.parent.mkdir(parents=True, exist_ok=True)
    try:
        from rdkit import Chem
        from rdkit.Chem import AllChem
    except Exception:
        Chem = None

    if Chem is not None:
        writer = Chem.SDWriter(str(out_sdf))
        for lig_id, smi in ligands:
            mol = Chem.MolFromSmiles(smi)
            if mol is None:
                continue
            mol = Chem.AddHs(mol)
            if embed:
                params = AllChem.ETKDGv3()
                params.randomSeed = 0xC0FFEE
                status = AllChem.EmbedMolecule(mol, params)
                if status != 0:
                    # Retry with default params before skipping.
                    status = AllChem.EmbedMolecule(mol)
                if status != 0:
                    # Skip ligands that cannot be embedded.
                    continue
                try:
                    AllChem.UFFOptimizeMolecule(mol, maxIters=200)
                except Exception:
                    pass
            mol.SetProp("_Name", lig_id)
            mol.SetProp("SMILES", smi)
            writer.write(mol)
        writer.close()
        return out_sdf

    if which("obabel"):
        tmp_smi = out_sdf.with_suffix(".smi")
        with tmp_smi.open("w") as f:
            for lig_id, smi in ligands:
                f.write(f"{smi}\t{lig_id}\n")
        run_cmd(["obabel", "-ismi", str(tmp_smi), "-O", str(out_sdf), "--gen3d"])
        return out_sdf

    raise RuntimeError("RDKit or OpenBabel is required to build ligand SDF files.")
