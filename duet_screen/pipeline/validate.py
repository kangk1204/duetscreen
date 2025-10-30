"""Validation command."""

from __future__ import annotations

from pathlib import Path

from duet_screen.config import Config
from duet_screen.utils import ensure_directory


def run_validate(config: Config) -> None:
    """Ensure configuration and inputs exist."""

    sequences = config.inputs.sequences
    if not sequences.exists():
        raise FileNotFoundError(f"Input sequence file not found: {sequences}")

    ensure_directory(config.paths.workdir)
    ensure_directory(config.paths.reports)
