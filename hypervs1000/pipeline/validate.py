"""Validation command."""

from __future__ import annotations

from pathlib import Path

from hypervs1000.config import Config
from hypervs1000.utils import ensure_directory


def run_validate(config: Config) -> None:
    """Ensure configuration and inputs exist."""

    sequences = config.inputs.sequences
    if not sequences.exists():
        raise FileNotFoundError(f"Input sequence file not found: {sequences}")

    ensure_directory(config.paths.workdir)
    ensure_directory(config.paths.reports)
