"""Eurocode 8 secim stratejisi."""

from typing import Any, Dict
import pandas as pd
from .base import BaseSelectionStrategy


class EurocodeSelectionStrategy(BaseSelectionStrategy):
    """Eurocode 8 seçim stratejisi"""
    
    def _calculate_score(self, record: pd.Series, target_params: Dict[str, Any]) -> float:
        """Eurocode 8'e göre puan hesapla"""
        # Eurocode spesifik implementasyon
        return 0.0  # Implementasyon
