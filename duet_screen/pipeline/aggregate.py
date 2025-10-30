"""Aggregation stage combining DTI, docking, and MM/GBSA results."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from duet_screen.config import Config
from duet_screen.consensus import weighted_reciprocal_rank_fusion
from duet_screen.utils import now_utc_iso, read_jsonl


def run_aggregate(config: Config) -> Path:
    """Aggregate all stages into a final consensus ranking."""

    dti_path = Path(config.paths.workdir) / "dti" / "results.jsonl"
    docking_path = Path(config.paths.workdir) / "docking" / "results.jsonl"
    mmgbsa_path = Path(config.paths.workdir) / "mmgbsa" / "results.jsonl"

    for path in (dti_path, docking_path, mmgbsa_path):
        if not path.exists():
            raise FileNotFoundError(f"Required stage output missing: {path}")

    stage_data = {
        "dti": _load_stage(dti_path),
        "docking": _load_stage(docking_path),
        "mmgbsa": _load_stage(mmgbsa_path),
    }

    stage_weights = [
        config.pipeline.stage_weights.dti,
        config.pipeline.stage_weights.docking,
        config.pipeline.stage_weights.mmgbsa,
    ]
    stage_names = ["dti", "docking", "mmgbsa"]
    constant = config.pipeline.consensus_constant

    per_input: List[Dict[str, object]] = []
    global_entries: List[Tuple[str, str, float]] = []

    input_ids = sorted(
        set().union(*(data.scores.keys() for data in stage_data.values()))
    )
    for input_id in input_ids:
        rank_lists: List[List[str]] = []
        weights: List[float] = []
        per_stage_scores: Dict[str, Dict[str, float]] = {}
        partner_types: Dict[str, str] = {}

        for name, weight in zip(stage_names, stage_weights):
            stage = stage_data[name]
            scores = stage.scores.get(input_id)
            if not scores:
                continue
            ordered = sorted(scores.items(), key=lambda item: item[1], reverse=True)
            rank_lists.append([item[0] for item in ordered])
            weights.append(weight)
            per_stage_scores[name] = {item[0]: float(item[1]) for item in ordered}
            for partner_id in scores:
                partner_types[partner_id] = stage.partner_types[input_id][partner_id]

        if not rank_lists:
            continue
        # Normalise weights on the fly so partial stage availability (e.g. missing docking output)
        # still produces a properly weighted fusion.
        weight_total = sum(weights)
        normalized_weights = [weight / weight_total for weight in weights] if weight_total else weights
        fused = weighted_reciprocal_rank_fusion(rank_lists, normalized_weights, constant=constant)

        partners: List[Dict[str, object]] = []
        for rank, (partner_id, score) in enumerate(fused.items(), start=1):
            partners.append(
                {
                    "partner_id": partner_id,
                    "partner_type": partner_types.get(partner_id, "unknown"),
                    "scores": {
                        name: per_stage_scores.get(name, {}).get(partner_id)
                        for name in stage_names
                    },
                    "consensus_score": score,
                    "rank": rank,
                }
            )
            global_entries.append((input_id, partner_id, score))

        per_input.append({"input_id": input_id, "partners": partners})

    global_entries.sort(key=lambda item: item[2], reverse=True)
    global_ranking = [
        {
            "input_id": input_id,
            "partner_id": partner_id,
            "consensus_score": score,
            "rank": index,
        }
        for index, (input_id, partner_id, score) in enumerate(global_entries, start=1)
    ]

    snapshot = {
        "generated_at": now_utc_iso(),
        "inputs": per_input,
        "global_ranking": global_ranking,
        "config_digest": _config_digest(config),
    }

    output = Path(config.paths.workdir) / "aggregate" / "final_rankings.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(snapshot, indent=2, sort_keys=True), encoding="utf-8")
    return output


@dataclass
class StageSnapshot:
    scores: Dict[str, Dict[str, float]]
    partner_types: Dict[str, Dict[str, str]]


def _load_stage(path: Path) -> StageSnapshot:
    scores: Dict[str, Dict[str, float]] = {}
    partner_types: Dict[str, Dict[str, str]] = {}
    for row in read_jsonl(path):
        input_id = str(row["input_id"])
        partner_id = str(row["partner_id"])
        scores.setdefault(input_id, {})[partner_id] = float(row["score"])
        partner_types.setdefault(input_id, {})[partner_id] = str(row.get("partner_type", "unknown"))
    return StageSnapshot(scores=scores, partner_types=partner_types)


def _config_digest(config: Config) -> str:
    data = {
        "pipeline": {
            "chunk_size": config.pipeline.chunk_size,
            "num_workers": config.pipeline.num_workers,
            "devices": config.pipeline.devices,
            "dti_top_k": config.pipeline.dti_top_k,
            "docking_top_k": config.pipeline.docking_top_k,
            "mmgbsa_top_k": config.pipeline.mmgbsa_top_k,
            "consensus_constant": config.pipeline.consensus_constant,
            "stage_weights": {
                "dti": config.pipeline.stage_weights.dti,
                "docking": config.pipeline.stage_weights.docking,
                "mmgbsa": config.pipeline.stage_weights.mmgbsa,
            },
        },
        "inputs": {"sequences": str(config.inputs.sequences)},
        "paths": {
            "workdir": str(config.paths.workdir),
            "manifest": str(config.paths.manifest),
            "reports": str(config.paths.reports),
        },
    }
    blob = json.dumps(data, sort_keys=True).encode("utf-8")
    import hashlib

    return hashlib.blake2s(blob, digest_size=12).hexdigest()
