"""Reporting stage - assembles human readable summary."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from duet_screen.config import Config
from duet_screen.utils import ensure_directory


def run_report(config: Config) -> Path:
    """Generate report files from aggregated rankings."""

    aggregate_path = Path(config.paths.workdir) / "aggregate" / "final_rankings.json"
    if not aggregate_path.exists():
        raise FileNotFoundError(f"Aggregate results missing: {aggregate_path}")

    data = json.loads(aggregate_path.read_text(encoding="utf-8"))

    ensure_directory(config.paths.reports)
    report_json = Path(config.paths.reports) / "report.json"
    report_json.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")

    report_txt = Path(config.paths.reports) / "report.txt"
    lines: List[str] = []
    lines.append("DUET-Screen Report")
    lines.append("==================")
    lines.append(f"Generated at: {data.get('generated_at', 'unknown')}")
    lines.append("")

    for entry in data.get("inputs", []):
        input_id = entry.get("input_id", "unknown")
        lines.append(f"Input: {input_id}")
        partners = entry.get("partners", [])
        if not partners:
            lines.append("  No partners ranked.")
        else:
            for partner in partners[:5]:
                partner_id = partner.get("partner_id")
                partner_type = partner.get("partner_type")
                score = partner.get("consensus_score")
                lines.append(f"  {partner['rank']}. {partner_id} ({partner_type}) -> {score:.6f}")
        lines.append("")

    report_txt.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return report_txt
