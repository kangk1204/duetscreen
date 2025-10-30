"""Data loading utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Sequence

from duet_screen.config import Config, LibraryLigand, LibraryProtein
from duet_screen.pipeline.models import InputRecord, PartnerRecord
from duet_screen.utils import load_csv


def load_input_records(config: Config) -> List[InputRecord]:
    """Load user inputs from CSV."""

    records: List[InputRecord] = []
    for row in load_csv(config.inputs.sequences):
        identifier = row.get("id")
        entry_type = row.get("type")
        value = row.get("value")
        if not identifier or not entry_type or not value:
            raise ValueError("Input row missing required fields (id, type, value).")
        if entry_type not in {"protein", "ligand"}:
            raise ValueError(f"Invalid input type {entry_type}; expected 'protein' or 'ligand'.")
        records.append(InputRecord(id=identifier, type=entry_type, value=value))
    if not records:
        raise ValueError("No input records found.")
    return records


def library_partners(config: Config, target_type: str) -> List[PartnerRecord]:
    """Return library partners of opposite type."""

    if target_type == "protein":
        return [PartnerRecord(id=item.id, type="protein", value=item.sequence) for item in config.library.proteins]
    if target_type == "ligand":
        return [PartnerRecord(id=item.id, type="ligand", value=item.smiles) for item in config.library.ligands]
    raise ValueError(f"Unsupported partner type: {target_type}")


def opposite_partners(config: Config, input_type: str) -> List[PartnerRecord]:
    """Return partners that can bind to *input_type*."""

    if input_type == "protein":
        return library_partners(config, "ligand")
    if input_type == "ligand":
        return library_partners(config, "protein")
    raise ValueError(f"Invalid input type {input_type}.")
