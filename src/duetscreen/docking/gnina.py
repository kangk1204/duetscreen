from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Dict, List

from duetscreen.docking.pockets import Pocket
from duetscreen.docking.utils import run_cmd, which


def _parse_sdf_gnina_scores(sdf_path: Path) -> Dict[str, Dict[str, float | None]]:
    best: Dict[str, Dict[str, float | None]] = {}
    best_key: Dict[str, tuple] = {}
    if not sdf_path.exists():
        return best
    with sdf_path.open("r") as f:
        while True:
            name = f.readline()
            if not name:
                break
            ligand_id = name.strip()
            # Skip header (3 lines) and counts line follows.
            for _ in range(3):
                if not f.readline():
                    return best
            # Read until properties.
            while True:
                line = f.readline()
                if not line:
                    return best
                if line.startswith("M  END"):
                    break
            props: Dict[str, str] = {}
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

            def _to_float(val: str | None) -> float | None:
                if val is None or val == "":
                    return None
                try:
                    return float(val)
                except ValueError:
                    return None

            cnn_score = _to_float(props.get("CNNscore"))
            cnn_affinity = _to_float(props.get("CNNaffinity"))
            vina_affinity = _to_float(props.get("minimizedAffinity"))
            if vina_affinity is None:
                vina_affinity = _to_float(props.get("affinity"))
            if vina_affinity is None:
                vina_affinity = _to_float(props.get("score"))

            if cnn_score is not None:
                key = (2, cnn_score, cnn_affinity if cnn_affinity is not None else float("-inf"))
            elif cnn_affinity is not None:
                key = (1, cnn_affinity, -vina_affinity if vina_affinity is not None else float("-inf"))
            elif vina_affinity is not None:
                key = (0, -vina_affinity, 0.0)
            else:
                continue

            current = best_key.get(ligand_id)
            if current is None or key > current:
                best_key[ligand_id] = key
                score = cnn_score if cnn_score is not None else cnn_affinity
                if score is None and vina_affinity is not None:
                    score = -vina_affinity
                best[ligand_id] = {
                    "score": score,
                    "cnn_score": cnn_score,
                    "cnn_affinity": cnn_affinity,
                    "vina_affinity": vina_affinity,
                }
    return best


def dock_gnina(
    receptor_pdb: Path,
    ligands_sdf: Path,
    pockets: List[Pocket],
    out_dir: Path,
    exhaustiveness: int = 8,
    num_modes: int = 3,
    score_fields: List[str] | None = None,
) -> List[Path]:
    if not which("gnina"):
        raise RuntimeError("gnina executable not found in PATH.")

    out_dir.mkdir(parents=True, exist_ok=True)
    score_fields = score_fields or ["CNNscore", "CNNaffinity", "minimizedAffinity", "affinity", "score"]
    score_paths = []

    for pocket in pockets:
        out_sdf = out_dir / f"gnina_{pocket.pocket_id}.sdf"
        log_path = out_dir / f"gnina_{pocket.pocket_id}.log"
        args = [
            "gnina",
            "-r",
            str(receptor_pdb),
            "-l",
            str(ligands_sdf),
            "-o",
            str(out_sdf),
            "--center_x",
            str(pocket.center_x),
            "--center_y",
            str(pocket.center_y),
            "--center_z",
            str(pocket.center_z),
            "--size_x",
            str(pocket.size_x),
            "--size_y",
            str(pocket.size_y),
            "--size_z",
            str(pocket.size_z),
            "--exhaustiveness",
            str(exhaustiveness),
            "--num_modes",
            str(num_modes),
        ]
        device = os.environ.get("GNINA_DEVICE")
        if device:
            args.extend(["--device", device])
        env = {}
        conda_prefix = Path(os.environ.get("CONDA_PREFIX", ""))
        if conda_prefix.exists():
            lib_path = str(conda_prefix / "lib")
            existing = os.environ.get("LD_LIBRARY_PATH", "")
            env["LD_LIBRARY_PATH"] = f"{lib_path}:{existing}" if existing else lib_path
        run_cmd(args, check=True, env=env if env else None)

        scores = _parse_sdf_gnina_scores(out_sdf)
        score_path = out_dir / f"gnina_{pocket.pocket_id}_scores.csv"
        with score_path.open("w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "ligand_id",
                    "score",
                    "cnn_score",
                    "cnn_affinity",
                    "vina_affinity",
                    "pocket_id",
                ],
            )
            writer.writeheader()
            for lig_id, row in scores.items():
                writer.writerow(
                    {
                        "ligand_id": lig_id,
                        "score": row.get("score"),
                        "cnn_score": row.get("cnn_score"),
                        "cnn_affinity": row.get("cnn_affinity"),
                        "vina_affinity": row.get("vina_affinity"),
                        "pocket_id": pocket.pocket_id,
                    }
                )
        score_paths.append(score_path)
        log_path.write_text(f"gnina finished for {pocket.pocket_id}\n")

    return score_paths
