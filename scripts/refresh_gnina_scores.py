#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from duetscreen.docking.aggregate import aggregate_rankings
from duetscreen.docking.agents import _merge_score_files
from duetscreen.docking.gnina import _parse_sdf_gnina_scores


def _as_cell(value):
    if value is None:
        return ""
    return value


def _write_scores(out_path: Path, pocket_id: str, scores: dict[str, dict[str, float | None]]):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "ligand_id",
        "score",
        "cnn_score",
        "cnn_affinity",
        "vina_affinity",
        "pocket_id",
    ]
    with out_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for lig_id, row in scores.items():
            writer.writerow(
                {
                    "ligand_id": lig_id,
                    "score": _as_cell(row.get("score")),
                    "cnn_score": _as_cell(row.get("cnn_score")),
                    "cnn_affinity": _as_cell(row.get("cnn_affinity")),
                    "vina_affinity": _as_cell(row.get("vina_affinity")),
                    "pocket_id": pocket_id,
                }
            )


def refresh_dir(dock_dir: Path, update_ranked: bool) -> bool:
    gnina_dir = dock_dir / "gnina"
    if not gnina_dir.exists():
        return False

    score_paths: list[Path] = []
    for sdf in sorted(gnina_dir.glob("gnina_pocket_*.sdf")):
        if sdf.stat().st_size == 0:
            continue
        pocket_id = sdf.stem.replace("gnina_", "")
        scores = _parse_sdf_gnina_scores(sdf)
        if not scores:
            continue
        score_path = gnina_dir / f"gnina_{pocket_id}_scores.csv"
        _write_scores(score_path, pocket_id, scores)
        score_paths.append(score_path)

    if not score_paths:
        return False

    merged = _merge_score_files(score_paths, gnina_dir / "gnina_scores.csv", higher_is_better=True)

    if update_ranked:
        ligand_table = dock_dir / "ligands.csv"
        out_rank = dock_dir / "docking_ranked.csv"
        aggregate_rankings([("gnina", merged, "higher")], ligand_table if ligand_table.exists() else None, out_rank)

    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh GNINA score tables from existing SDF outputs.")
    parser.add_argument("--base", required=True, type=Path, help="Base docking directory")
    parser.add_argument("--update-ranked", action="store_true", help="Regenerate docking_ranked.csv")
    args = parser.parse_args()

    base = args.base
    if not base.exists():
        print(f"Base directory not found: {base}")
        return 1

    refreshed = 0
    for dock_dir in sorted([p for p in base.iterdir() if p.is_dir()]):
        if refresh_dir(dock_dir, update_ranked=args.update_ranked):
            refreshed += 1

    print(f"refreshed {refreshed} docking dirs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
