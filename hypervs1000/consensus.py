"""Consensus scoring utilities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, MutableMapping, Sequence


def weighted_average_rank(rank_lists: Sequence[Sequence[str]], weights: Sequence[float]) -> Dict[str, float]:
    """Compute weighted average rank per candidate.

    Parameters
    ----------
    rank_lists:
        Ordered iterables of candidate identifiers where position implies rank (1-indexed).
    weights:
        Non-negative weights for each ranking list. Length must match *rank_lists*.
    """

    if len(rank_lists) != len(weights):
        raise ValueError("rank_lists and weights must be the same length.")
    totals: MutableMapping[str, float] = {}
    weight_sum = sum(weights)
    if weight_sum <= 0:
        raise ValueError("Sum of weights must be positive.")
    for ranking, weight in zip(rank_lists, weights):
        for index, candidate in enumerate(ranking, start=1):
            totals[candidate] = totals.get(candidate, 0.0) + weight * index
    return {candidate: score / weight_sum for candidate, score in totals.items()}


def weighted_reciprocal_rank_fusion(
    rank_lists: Sequence[Sequence[str]],
    weights: Sequence[float],
    *,
    constant: int = 60,
) -> Dict[str, float]:
    """Weighted reciprocal rank fusion.

    Score = sum_i weight_i / (constant + rank_i).
    """

    if len(rank_lists) != len(weights):
        raise ValueError("rank_lists and weights must be the same length.")
    if constant <= 0:
        raise ValueError("constant must be positive.")
    fused: MutableMapping[str, float] = {}
    for ranking, weight in zip(rank_lists, weights):
        if weight <= 0:
            continue
        for index, candidate in enumerate(ranking, start=1):
            fused[candidate] = fused.get(candidate, 0.0) + weight / (constant + index)
    return dict(sorted(fused.items(), key=lambda item: item[1], reverse=True))


@dataclass(frozen=True)
class ConsensusResult:
    """Container for consensus scores mapped to sorted candidate order."""

    scores: Dict[str, float]

    def ranked(self) -> List[str]:
        return sorted(self.scores, key=self.scores.__getitem__, reverse=True)
