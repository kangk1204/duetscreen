from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Dict, List, Tuple

from duetscreen.docking.utils import run_cmd


def _parse_sdf_scores(sdf_path: Path, score_fields: List[str]) -> Dict[str, float]:
    best: Dict[str, float] = {}
    if not sdf_path.exists():
        return best
    with sdf_path.open("r") as f:
        while True:
            name = f.readline()
            if not name:
                break
            ligand_id = name.strip()
            for _ in range(3):
                if not f.readline():
                    return best
            while True:
                line = f.readline()
                if not line:
                    return best
                if line.startswith("M  END"):
                    break
            props = {}
            while True:
                line = f.readline()
                if not line:
                    return best
                if line.startswith("$$$$"):
                    break
                if line.startswith(">"):
                    key = line.strip().strip("> <")
                    value = f.readline().strip()
                    props[key] = value
            score = None
            for key in score_fields:
                if key in props:
                    try:
                        score = float(props[key])
                    except ValueError:
                        score = None
                    break
            if score is None:
                continue
            if ligand_id not in best or score > best[ligand_id]:
                best[ligand_id] = score
    return best


def _parse_csv_scores(csv_path: Path, score_fields: List[str]) -> Dict[str, float]:
    scores: Dict[str, float] = {}
    if not csv_path.exists():
        return scores
    with csv_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lig_id = row.get("complex_name") or row.get("ligand_id") or row.get("name")
            if not lig_id:
                continue
            score = None
            for key in score_fields:
                if row.get(key):
                    try:
                        score = float(row[key])
                    except ValueError:
                        score = None
                    break
            if score is None:
                continue
            scores[lig_id] = score
    return scores


def dock_diffdock(
    receptor_pdb: Path,
    ligands: List[Tuple[str, str]],
    out_dir: Path,
    diffdock_dir: Path | None = None,
    inference_steps: int = 20,
    samples_per_complex: int = 1,
    python_cmd: str | None = None,
    conda_env: str | None = None,
) -> Path:
    diffdock_dir = diffdock_dir or Path(os.environ.get("DIFFDOCK_DIR", ""))
    if not diffdock_dir or not diffdock_dir.exists():
        raise RuntimeError("DiffDock directory not found. Set DIFFDOCK_DIR or pass diffdock_dir.")

    out_dir.mkdir(parents=True, exist_ok=True)
    input_csv = out_dir / "diffdock_input.csv"
    with input_csv.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["complex_name", "protein_path", "ligand_description"],
        )
        writer.writeheader()
        for lig_id, smi in ligands:
            writer.writerow(
                {
                    "complex_name": lig_id,
                    "protein_path": str(receptor_pdb),
                    "ligand_description": smi,
                }
            )

    if python_cmd is None:
        python_cmd = os.environ.get("DIFFDOCK_PYTHON")
    if conda_env is None:
        conda_env = os.environ.get("DIFFDOCK_CONDA_ENV")

    if python_cmd:
        cmd = [
            python_cmd,
            "-m",
            "inference",
            "--protein_ligand_csv",
            str(input_csv),
            "--out_dir",
            str(out_dir),
            "--inference_steps",
            str(inference_steps),
            "--samples_per_complex",
            str(samples_per_complex),
        ]
    elif conda_env:
        cmd = [
            "conda",
            "run",
            "-n",
            conda_env,
            "python",
            "-m",
            "inference",
            "--protein_ligand_csv",
            str(input_csv),
            "--out_dir",
            str(out_dir),
            "--inference_steps",
            str(inference_steps),
            "--samples_per_complex",
            str(samples_per_complex),
        ]
    else:
        cmd = [
            "python",
            "-m",
            "inference",
            "--protein_ligand_csv",
            str(input_csv),
            "--out_dir",
            str(out_dir),
            "--inference_steps",
            str(inference_steps),
            "--samples_per_complex",
            str(samples_per_complex),
        ]

    run_cmd(cmd, cwd=diffdock_dir, check=True)

    score_fields = ["confidence", "score", "affinity"]
    scores: Dict[str, float] = {}

    ranked_sdf = out_dir / "ranked_poses.sdf"
    if ranked_sdf.exists():
        scores = _parse_sdf_scores(ranked_sdf, score_fields)
    else:
        for sdf in out_dir.glob("*.sdf"):
            scores.update(_parse_sdf_scores(sdf, score_fields))

    if not scores:
        for csv_file in out_dir.glob("*.csv"):
            scores = _parse_csv_scores(csv_file, score_fields)
            if scores:
                break

    out_csv = out_dir / "diffdock_scores.csv"
    with out_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["ligand_id", "score"])
        writer.writeheader()
        for lig_id, score in scores.items():
            writer.writerow({"ligand_id": lig_id, "score": score})
    return out_csv
