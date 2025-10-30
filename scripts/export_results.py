#!/usr/bin/env python3
"""Export aggregated DUET-Screen rankings to JSON and Excel."""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from openpyxl import Workbook
except ImportError as exc:  # pragma: no cover - runtime dependency
    raise SystemExit("openpyxl is required. Install with `pip install .[report]` or `pip install openpyxl`.") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export aggregate rankings to JSON/Excel.")
    parser.add_argument("--aggregate", required=True, help="Path to aggregate/final_rankings.json.")
    parser.add_argument("--output-json", required=True, help="Destination JSON path.")
    parser.add_argument("--output-xlsx", required=True, help="Destination Excel path.")
    parser.add_argument(
        "--input-id",
        action="append",
        help="Optional input IDs to export (repeatable). Defaults to all inputs in aggregate.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum partners per input to export (default: 50).",
    )
    parser.add_argument(
        "--ligand-library",
        help="Optional CSV file with columns id/value for ligands (adds SMILES to export).",
    )
    parser.add_argument(
        "--protein-library",
        help="Optional CSV file with columns id/value for proteins (adds sequences to export).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    aggregate_path = Path(args.aggregate).expanduser().resolve()
    if not aggregate_path.exists():
        raise FileNotFoundError(f"Aggregate file not found: {aggregate_path}")

    with aggregate_path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)

    inputs = data.get("inputs", [])
    selected_inputs = set(args.input_id) if args.input_id else None
    export_rows: Dict[str, List[Dict[str, Any]]] = {}

    ligand_values = _load_value_map(args.ligand_library) if args.ligand_library else {}
    protein_values = _load_value_map(args.protein_library) if args.protein_library else {}

    for entry in inputs:
        input_id = entry.get("input_id")
        if not input_id:
            continue
        if selected_inputs and input_id not in selected_inputs:
            continue
        partners = entry.get("partners", [])
        enriched: List[Dict[str, Any]] = []
        for partner in partners[: args.limit]:
            partner_copy = dict(partner)
            partner_type = partner_copy.get("partner_type")
            value = None
            if partner_type == "ligand" and ligand_values:
                value = ligand_values.get(str(partner_copy.get("partner_id")))
            elif partner_type == "protein" and protein_values:
                value = protein_values.get(str(partner_copy.get("partner_id")))
            if value is not None:
                partner_copy["value"] = value
            enriched.append(partner_copy)
        export_rows[input_id] = enriched

    if not export_rows:
        raise ValueError("No inputs matched the export criteria.")

    _write_json(Path(args.output_json), export_rows, data)
    _write_excel(Path(args.output_xlsx), export_rows)

    print(
        f"Exported {sum(len(rows) for rows in export_rows.values())} partner rows "
        f"for {len(export_rows)} inputs."
    )
    return 0


def _write_json(path: Path, export_rows: Dict[str, List[Dict[str, Any]]], original: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": original.get("generated_at"),
        "config_digest": original.get("config_digest"),
        "inputs": [
            {
                "input_id": input_id,
                "partners": [
                    {
                        **partner,
                        "analysis": _score_stats(partner.get("scores") or {}),
                    }
                    for partner in partners
                ],
            }
            for input_id, partners in export_rows.items()
        ],
    }
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _write_excel(path: Path, export_rows: Dict[str, List[Dict[str, Any]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    workbook = Workbook()
    # Remove the default sheet
    default_sheet = workbook.active
    workbook.remove(default_sheet)

    header = [
        "rank",
        "partner_id",
        "partner_type",
        "value",
        "consensus_score",
        "dti_score",
        "docking_score",
        "mmgbsa_score",
        "stages_present",
        "stage_count",
        "stage_mean",
        "stage_min",
        "stage_max",
        "stage_std",
    ]

    for input_id, partners in export_rows.items():
        sheet_name = _sanitize_sheet_name(input_id)
        worksheet = workbook.create_sheet(title=sheet_name)
        worksheet.append(header)
        for partner in partners:
            scores = partner.get("scores") or {}
            stats = _score_stats(scores)
            worksheet.append(
                [
                    partner.get("rank"),
                    partner.get("partner_id"),
                    partner.get("partner_type"),
                    partner.get("value"),
                    partner.get("consensus_score"),
                    scores.get("dti"),
                    scores.get("docking"),
                    scores.get("mmgbsa"),
                    ",".join(stats["stages_present"]),
                    stats["stage_count"],
                    stats["mean"],
                    stats["min"],
                    stats["max"],
                    stats["std"],
                ]
            )

    workbook.save(path)


def _sanitize_sheet_name(name: str) -> str:
    invalid = set('[]:*?/\\')
    cleaned = "".join(ch if ch not in invalid else "_" for ch in name)
    return cleaned[:31] or "Sheet1"


def _load_value_map(path_str: Optional[str]) -> Dict[str, str]:
    if not path_str:
        return {}
    path = Path(path_str).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Library file not found: {path}")
    value_map: Dict[str, str] = {}
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            identifier = row.get("id") or row.get("ID") or row.get("zinc_id")
            value = row.get("value") or row.get("smiles") or row.get("SMILES") or row.get("sequence")
            if not identifier or not value:
                continue
            value_map[str(identifier)] = value
    return value_map


def _score_stats(scores: Dict[str, Optional[float]]) -> Dict[str, Any]:
    """Return basic descriptive statistics for the available stage scores."""
    stages_present = sorted(stage for stage, value in scores.items() if value is not None)
    values = [float(value) for value in scores.values() if value is not None]
    if not values:
        return {
            "stages_present": stages_present,
            "stage_count": 0,
            "mean": None,
            "min": None,
            "max": None,
            "std": None,
        }
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return {
        "stages_present": stages_present,
        "stage_count": len(values),
        "mean": mean,
        "min": min(values),
        "max": max(values),
        "std": math.sqrt(variance),
    }


if __name__ == "__main__":
    raise SystemExit(main())
