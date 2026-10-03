"""Geriye donuk uyumluluk cephesi.

Icerik ``criteria`` ve ``strategies`` paketlerine tasindi; mevcut
``selection_service.processing.Selection`` import'lari ayni sekilde calisir.
"""

from .criteria import ScoringWeights, SearchCriteria, SelectionConfig
from .strategies import (
    BaseSelectionStrategy,
    ConstraintSelectionStrategy,
    EurocodeSelectionStrategy,
    ISelectionStrategy,
    ParetoSelectionStrategy,
    SpectrumMatchStrategy,
    TBDY2018ConstraintStrategy,
    TBDYSelectionStrategy,
)

__all__ = [
    "BaseSelectionStrategy",
    "ConstraintSelectionStrategy",
    "EurocodeSelectionStrategy",
    "ISelectionStrategy",
    "ParetoSelectionStrategy",
    "ScoringWeights",
    "SearchCriteria",
    "SelectionConfig",
    "SpectrumMatchStrategy",
    "TBDY2018ConstraintStrategy",
    "TBDYSelectionStrategy",
]
