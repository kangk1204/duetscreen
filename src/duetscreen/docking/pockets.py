from __future__ import annotations

import csv
from dataclasses import dataclass
import re
import os
from pathlib import Path
from typing import List

from duetscreen.docking.utils import run_cmd, which


@dataclass
class Pocket:
    pocket_id: str
    center_x: float
    center_y: float
    center_z: float
    size_x: float
    size_y: float
    size_z: float
    score: float


def _parse_pdb_bounds(pdb_path: Path) -> tuple[list[float], list[float]]:
    mins = [float("inf"), float("inf"), float("inf")]
    maxs = [float("-inf"), float("-inf"), float("-inf")]
    with pdb_path.open("r") as f:
        for line in f:
            if not (line.startswith("ATOM") or line.startswith("HETATM")):
                continue
            try:
                x = float(line[30:38])
                y = float(line[38:46])
                z = float(line[46:54])
            except ValueError:
                continue
            mins[0] = min(mins[0], x)
            mins[1] = min(mins[1], y)
            mins[2] = min(mins[2], z)
            maxs[0] = max(maxs[0], x)
            maxs[1] = max(maxs[1], y)
            maxs[2] = max(maxs[2], z)
    if mins[0] == float("inf"):
        raise RuntimeError(f"No coordinates found in {pdb_path}")
    return mins, maxs


def fallback_pocket(pdb_path: Path, padding: float = 6.0) -> List[Pocket]:
    mins, maxs = _parse_pdb_bounds(pdb_path)
    center = [(mi + ma) / 2.0 for mi, ma in zip(mins, maxs)]
    size = [(ma - mi) + padding * 2 for mi, ma in zip(mins, maxs)]
    return [
        Pocket(
            pocket_id="fallback_1",
            center_x=center[0],
            center_y=center[1],
            center_z=center[2],
            size_x=size[0],
            size_y=size[1],
            size_z=size[2],
            score=0.0,
        )
    ]


def predict_pockets_p2rank(
    pdb_path: Path,
    out_dir: Path,
    topk: int = 3,
    config: str = "alphafold",
    padding: float = 4.0,
    min_size: float = 16.0,
    max_size: float = 32.0,
) -> List[Pocket]:
    if os.environ.get("DUETSCREEN_DISABLE_P2RANK", "").lower() in {"1", "true", "yes"}:
        return fallback_pocket(pdb_path)
    if not which("prank"):
        return fallback_pocket(pdb_path)

    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        run_cmd(
            ["prank", "predict", "-f", str(pdb_path), "-o", str(out_dir), "-c", config],
            check=True,
        )
    except Exception:
        return fallback_pocket(pdb_path)

    pred_path = out_dir / f"{pdb_path.stem}_predictions.csv"
    if not pred_path.exists():
        return fallback_pocket(pdb_path)

    pockets = []
    with pred_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    def score_of(row: dict) -> float:
        for key in ("score", "probability", "prob"):
            val = row.get(key)
            if val:
                try:
                    return float(val)
                except ValueError:
                    continue
        return 0.0

    rows.sort(key=score_of, reverse=True)
    for i, row in enumerate(rows[:topk], start=1):
        try:
            cx = float(row["center_x"])
            cy = float(row["center_y"])
            cz = float(row["center_z"])
        except Exception:
            continue
        radius = None
        for key in ("radius", "pocket_radius"):
            if row.get(key):
                try:
                    radius = float(row[key])
                except ValueError:
                    radius = None
                break
        if radius is None:
            size = min_size
        else:
            size = max(min_size, min(max_size, 2.0 * (radius + padding)))
        pockets.append(
            Pocket(
                pocket_id=f"pocket_{i}",
                center_x=cx,
                center_y=cy,
                center_z=cz,
                size_x=size,
                size_y=size,
                size_z=size,
                score=score_of(row),
            )
        )

    return pockets or fallback_pocket(pdb_path)


def _parse_fpocket_info(info_path: Path) -> dict[int, float]:
    scores: dict[int, float] = {}
    if not info_path.exists():
        return scores
    current_id: int | None = None
    with info_path.open("r") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            match = re.match(r"Pocket\s+(\d+)", text)
            if match:
                current_id = int(match.group(1))
                continue
            if current_id is None:
                continue
            lower = text.lower()
            if "druggability score" in lower:
                try:
                    scores[current_id] = float(text.split(":")[-1].strip())
                except ValueError:
                    continue
            elif lower.startswith("score") and current_id not in scores:
                try:
                    scores[current_id] = float(text.split(":")[-1].strip())
                except ValueError:
                    continue
    return scores


def predict_pockets_fpocket(
    pdb_path: Path,
    out_dir: Path,
    topk: int = 3,
    padding: float = 4.0,
    min_size: float = 16.0,
    max_size: float = 32.0,
) -> List[Pocket]:
    if not which("fpocket"):
        return fallback_pocket(pdb_path)

    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        run_cmd(["fpocket", "-f", str(pdb_path)], cwd=out_dir, check=True)
    except Exception:
        return fallback_pocket(pdb_path)

    fpocket_dir = out_dir / f"{pdb_path.stem}_out"
    if not fpocket_dir.exists():
        alt_dir = pdb_path.parent / f"{pdb_path.stem}_out"
        if alt_dir.exists():
            fpocket_dir = alt_dir
    pockets_dir = fpocket_dir / "pockets"
    info_path = fpocket_dir / "info.txt"
    if not info_path.exists():
        alt_info = fpocket_dir / f"{pdb_path.stem}_info.txt"
        if alt_info.exists():
            info_path = alt_info
    if not pockets_dir.exists():
        return fallback_pocket(pdb_path)

    scores = _parse_fpocket_info(info_path)
    pocket_files = sorted(pockets_dir.glob("pocket*_atm.pdb"))
    pockets: list[Pocket] = []
    for pocket_path in pocket_files:
        match = re.search(r"pocket(\d+)_atm\.pdb", pocket_path.name)
        if not match:
            continue
        pocket_idx = int(match.group(1))
        try:
            mins, maxs = _parse_pdb_bounds(pocket_path)
        except Exception:
            continue
        center = [(mi + ma) / 2.0 for mi, ma in zip(mins, maxs)]
        size = [(ma - mi) + padding * 2 for mi, ma in zip(mins, maxs)]
        size = [
            max(min_size, min(max_size, size[0])),
            max(min_size, min(max_size, size[1])),
            max(min_size, min(max_size, size[2])),
        ]
        pockets.append(
            Pocket(
                pocket_id=f"pocket_{pocket_idx}",
                center_x=center[0],
                center_y=center[1],
                center_z=center[2],
                size_x=size[0],
                size_y=size[1],
                size_z=size[2],
                score=float(scores.get(pocket_idx, 0.0)),
            )
        )

    if not pockets:
        return fallback_pocket(pdb_path)

    pockets.sort(key=lambda p: (p.score, p.size_x * p.size_y * p.size_z), reverse=True)
    return pockets[:topk] or fallback_pocket(pdb_path)


def predict_pockets(
    pdb_path: Path,
    out_dir: Path,
    topk: int = 3,
    config: str = "alphafold",
    padding: float = 4.0,
    min_size: float = 16.0,
    max_size: float = 32.0,
) -> List[Pocket]:
    tool = os.environ.get("DUETSCREEN_POCKET_TOOL", "p2rank").strip().lower()
    if tool == "fpocket":
        return predict_pockets_fpocket(
            pdb_path,
            out_dir,
            topk=topk,
            padding=padding,
            min_size=min_size,
            max_size=max_size,
        )
    return predict_pockets_p2rank(
        pdb_path,
        out_dir,
        topk=topk,
        config=config,
        padding=padding,
        min_size=min_size,
        max_size=max_size,
    )


def write_pockets(pockets: List[Pocket], out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "pocket_id",
                "center_x",
                "center_y",
                "center_z",
                "size_x",
                "size_y",
                "size_z",
                "score",
            ],
        )
        writer.writeheader()
        for pocket in pockets:
            writer.writerow(pocket.__dict__)
    return out_path
