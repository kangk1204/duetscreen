"""MM/GBSA simulated scoring stage."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, List, Optional

from duet_screen.config import Config
from duet_screen.utils import deterministic_score, read_jsonl, write_jsonl


def run_mmgbsa(config: Config, source: Optional[Path] = None) -> Path:
    """Simulate MM/GBSA scoring."""

    source_path = source or (Path(config.paths.workdir) / "docking" / "results.jsonl")
    if not source_path.exists():
        raise FileNotFoundError(f"Docking results missing: {source_path}")

    grouped: DefaultDict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in read_jsonl(source_path):
        grouped[str(row["input_id"])].append(dict(row))

    rows: List[Dict[str, object]] = []
    top_k = max(1, config.pipeline.mmgbsa_top_k)
    for input_id, candidates in grouped.items():
        scored = []
        for item in candidates:
            # Simulated MM/GBSA scoring mirrors the deterministic approach used by the
            # other stages for repeatability.
            score = deterministic_score(str(item["input_id"]), str(item["partner_id"]), "mmgbsa")
            scored.append((item, score))
        scored.sort(key=lambda entry: entry[1], reverse=True)
        for rank, (item, score) in enumerate(scored[:top_k], start=1):
            rows.append(
                {
                    "input_id": item["input_id"],
                    "partner_id": item["partner_id"],
                    "partner_type": item["partner_type"],
                    "stage": "mmgbsa",
                    "score": score,
                    "rank": rank,
                }
            )

    output = Path(config.paths.workdir) / "mmgbsa" / "results.jsonl"
    write_jsonl(output, rows)
    return output
