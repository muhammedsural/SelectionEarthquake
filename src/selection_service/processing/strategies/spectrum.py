"""Spektrum eslestirme stratejisi."""

from typing import Any, Dict, List
import pandas as pd
from ..criteria import SearchCriteria
from .tbdy import TBDY2018ConstraintStrategy


class SpectrumMatchStrategy(TBDY2018ConstraintStrategy):
    """Prioritize spectral/intensity proxy metrics when response spectra are absent."""

    spectral_criteria = ("pga", "pgv", "pgd", "arias", "t90")

    def get_name(self) -> str:
        return "Spectrum_Match"

    def _evaluate_record(
        self, record: pd.Series, criteria: SearchCriteria
    ) -> Dict[str, Any]:
        result = super()._evaluate_record(record, criteria)
        spectral_errors = [
            item["normalized_error"]
            for item in result["error_metrics"]
            if item.get("criterion") in self.spectral_criteria
            and item.get("status") in ("active", "missing")
        ]
        if spectral_errors:
            spectrum_error = sum(spectral_errors) / len(spectral_errors)
            result["spectrum_error"] = spectrum_error
            result["error_total"] = spectrum_error
            result["fit_score"] = 100.0 / (1.0 + spectrum_error)
        else:
            result["spectrum_error"] = result["error_total"]
        return result

    def _add_strategy_columns(self, scored_df: pd.DataFrame) -> None:
        scored_df["SPECTRUM_ERROR"] = scored_df["ERROR_METRICS"].apply(
            self._spectrum_error_from_metrics
        )

    def _candidate_order(self, candidate_df: pd.DataFrame) -> pd.DataFrame:
        return candidate_df.sort_values(
            ["SPECTRUM_ERROR", "ERROR_TOTAL", "SCORE"],
            ascending=[True, True, False],
        )

    def _spectrum_error_from_metrics(self, metrics: List[Dict[str, Any]]) -> float:
        errors = [
            item["normalized_error"]
            for item in metrics
            if item.get("criterion") in self.spectral_criteria
            and item.get("status") in ("active", "missing")
        ]
        return sum(errors) / len(errors) if errors else float("inf")
