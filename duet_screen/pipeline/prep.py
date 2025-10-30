"""Preparation stage for workspace layout."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

from duet_screen.config import Config
from duet_screen.utils import ensure_directory


WORK_SUBDIRS: List[str] = ["dti", "docking", "mmgbsa", "aggregate", "logs"]


def run_prep(config: Config) -> None:
    """Prepare working directories."""

    ensure_directory(config.paths.workdir)
    ensure_directory(config.paths.reports)
    for name in WORK_SUBDIRS:
        ensure_directory(Path(config.paths.workdir) / name)
