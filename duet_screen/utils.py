"""Utility helpers for DUET-Screen."""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Sequence, Tuple, TypeVar

T = TypeVar("T")


def chunked(iterable: Iterable[T], size: int) -> Iterator[List[T]]:
    """Yield fixed-size chunks from *iterable*."""

    iterator = iter(iterable)
    while True:
        block = list(itertools.islice(iterator, size))
        if not block:
            break
        yield block


def deterministic_score(*components: str) -> float:
    """Return a deterministic pseudo-random score in [0, 1)."""

    digest = hashlib.blake2b("::".join(components).encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big") / float(2**64)


def now_utc_iso() -> str:
    """UTC timestamp formatted for manifests."""

    return datetime.now(timezone.utc).isoformat()


def ensure_directory(path: Path) -> None:
    """Create directory if missing."""

    path.mkdir(parents=True, exist_ok=True)


def write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    """Write iterable of dict rows to JSON Lines."""

    ensure_directory(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True))
            handle.write("\n")


def read_jsonl(path: Path) -> Iterator[Dict[str, Any]]:
    """Stream rows from a JSON Lines file."""

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                yield json.loads(stripped)


def load_csv(path: Path) -> List[Dict[str, str]]:
    """Load a small CSV file without extra dependencies."""

    import csv

    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def safe_mean(values: Sequence[float]) -> float:
    """Compute mean guarding against empty sequences."""

    if not values:
        return math.nan
    return sum(values) / len(values)


def git_commit_hash() -> str:
    """Return current git commit hash if available, else empty string."""

    head = Path(".git/HEAD")
    if not head.exists():
        return ""
    content = head.read_text(encoding="utf-8").strip()
    if content.startswith("ref:"):
        ref_path = Path(".git") / content.split(" ", 1)[1]
        if ref_path.exists():
            return ref_path.read_text(encoding="utf-8").strip()
        return ""
    return content


def env_or_default(name: str, default: str) -> str:
    """Return environment override if present."""

    return os.environ.get(name, default)
