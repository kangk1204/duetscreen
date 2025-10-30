"""Drug-Target Interaction stage."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from hypervs1000.config import Config
from hypervs1000.pipeline.data import load_input_records, opposite_partners
from hypervs1000.pipeline.models import InputRecord, PartnerRecord
from hypervs1000.scheduler import GPUScheduler, Task
from hypervs1000.utils import chunked, deterministic_score, write_jsonl


def run_dti(config: Config, devices: Optional[Sequence[int]] = None) -> Path:
    """Run DTI scoring. Returns path to JSONL results."""

    inputs = load_input_records(config)
    target_devices = devices or config.pipeline.devices
    scheduler = GPUScheduler(target_devices, max_retries=0)
    chunk_size = max(1, config.pipeline.chunk_size)
    tasks = [
        Task(name=f"dti_chunk_{index}", payload=chunk)
        for index, chunk in enumerate(chunked(inputs, chunk_size))
    ]

    def worker(task: Task[List[InputRecord]], device: int) -> List[Dict[str, object]]:
        return _score_chunk(config, task.payload)

    results = scheduler.dispatch(tasks, worker)
    rows: List[Dict[str, object]] = []
    for _, device, payload in results:
        rows.extend(payload)
    output_path = Path(config.paths.workdir) / "dti" / "results.jsonl"
    write_jsonl(output_path, rows)
    return output_path


def _score_chunk(config: Config, chunk: Sequence[InputRecord]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    top_k = max(1, config.pipeline.dti_top_k)
    for record in chunk:
        partners = opposite_partners(config, record.type)
        ranked = _rank_partners(record, partners, top_k)
        rows.extend(ranked)
    return rows


def _rank_partners(record: InputRecord, partners: Sequence[PartnerRecord], top_k: int) -> List[Dict[str, object]]:
    # The simulator uses a deterministic hash-based score so outputs stay reproducible
    # regardless of platform or execution order.
    scored = [
        (
            partner,
            deterministic_score(record.value, partner.value, "dti"),
        )
        for partner in partners
    ]
    scored.sort(key=lambda item: item[1], reverse=True)
    limited = scored[: top_k]
    rows: List[Dict[str, object]] = []
    for rank, (partner, score) in enumerate(limited, start=1):
        rows.append(
            {
                "input_id": record.id,
                "input_type": record.type,
                "partner_id": partner.id,
                "partner_type": partner.type,
                "stage": "dti",
                "score": score,
                "rank": rank,
            }
        )
    return rows
