"""Data models for pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Literal, Optional

PartnerType = Literal["protein", "ligand"]


@dataclass(frozen=True)
class InputRecord:
    """User supplied sequence or SMILES entry."""

    id: str
    type: PartnerType
    value: str


@dataclass(frozen=True)
class PartnerRecord:
    """Library partner entry."""

    id: str
    type: PartnerType
    value: str


@dataclass
class InteractionScore:
    """Score for an input-partner pair at a specific stage."""

    input_id: str
    partner_id: str
    partner_type: PartnerType
    stage: str
    score: float
    rank: int
    extra: Optional[Dict[str, float]] = None
