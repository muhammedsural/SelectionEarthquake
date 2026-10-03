"""TBDY 2018 stratejileri."""

import math
from typing import Any, Dict, List, Tuple
import pandas as pd
from ...core.config import (
    SCORING_MAP,
)
from ..criteria import SearchCriteria
from .base import BaseSelectionStrategy


class TBDYSelectionStrategy(BaseSelectionStrategy):
    """TBDY 2018 seçim stratejisi"""
    def get_name(self) -> str:
        return "TBDY_2018_Gaussian"

class TBDY2018ConstraintStrategy(BaseSelectionStrategy):
    """Constraint-first selection with explicit error metrics and diversity limits."""

    def get_name(self) -> str:
        return "TBDY_2018_Constraint"

    def select_and_score(
        self, df: pd.DataFrame, criteria: SearchCriteria
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        scored_df = df.copy()
        evaluation = scored_df.apply(
            lambda row: self._evaluate_record(row, criteria), axis=1
        )
        scored_df["HARD_FILTERS"] = evaluation.apply(lambda item: item["hard_filters"])
        scored_df["ERROR_METRICS"] = evaluation.apply(lambda item: item["error_metrics"])
        scored_df["ERROR_TOTAL"] = evaluation.apply(lambda item: item["error_total"])
        scored_df["SCORE"] = evaluation.apply(lambda item: item["fit_score"])
        scored_df["SCORE_BREAKDOWN"] = evaluation.apply(lambda item: item["error_metrics"])
        scored_df["SELECTION_STATUS"] = "not_evaluated"
        scored_df["SELECTION_REASON"] = ""
        self._add_strategy_columns(scored_df)

        failed_mask = scored_df["HARD_FILTERS"].apply(
            lambda filters: any(item["status"] == "failed" for item in filters)
        )
        scored_df.loc[failed_mask, "SELECTION_STATUS"] = "rejected"
        scored_df.loc[failed_mask, "SELECTION_REASON"] = scored_df.loc[
            failed_mask, "HARD_FILTERS"
        ].apply(self._format_filter_reasons)

        candidate_df = self._candidate_order(scored_df.loc[~failed_mask])
        selected_indices = self._select_diverse_candidates(candidate_df, scored_df)

        scored_df.loc[selected_indices, "SELECTION_STATUS"] = "selected"
        scored_df.loc[selected_indices, "SELECTION_REASON"] = "selected"

        remaining_mask = scored_df["SELECTION_STATUS"].eq("not_evaluated")
        scored_df.loc[remaining_mask, "SELECTION_STATUS"] = "rejected"
        scored_df.loc[remaining_mask, "SELECTION_REASON"] = (
            f"num_records_limit:{self.config.num_records}"
        )

        selected_df = scored_df.loc[selected_indices].copy()
        return selected_df, scored_df

    def _add_strategy_columns(self, scored_df: pd.DataFrame) -> None:
        """Hook for strategy-specific ranking columns."""

    def _candidate_order(self, candidate_df: pd.DataFrame) -> pd.DataFrame:
        """Return candidates in preferred selection order."""
        return candidate_df.sort_values(["ERROR_TOTAL", "SCORE"], ascending=[True, False])

    def _evaluate_record(
        self, record: pd.Series, criteria: SearchCriteria
    ) -> Dict[str, Any]:
        hard_filters = self._hard_filter_results(record, criteria)
        error_metrics = self._error_metrics(record, criteria)
        active_errors = [
            item["normalized_error"]
            for item in error_metrics
            if item["status"] in ("active", "missing")
        ]
        error_total = (
            sum(active_errors) / len(active_errors)
            if active_errors
            else float("inf")
        )
        fit_score = 0.0 if math.isinf(error_total) else 100.0 / (1.0 + error_total)
        return {
            "hard_filters": hard_filters,
            "error_metrics": error_metrics,
            "error_total": error_total,
            "fit_score": fit_score,
        }

    def _hard_filter_results(
        self, record: pd.Series, criteria: SearchCriteria
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for key, config in SCORING_MAP.items():
            column = config["column"]
            if config["type"] == "numeric":
                min_val = criteria._get_range_value("min", key)
                max_val = criteria._get_range_value("max", key)
                if min_val is None and max_val is None:
                    continue
                value = record.get(column)
                status = "passed"
                reason = ""
                if value is None or pd.isna(value):
                    status = "failed"
                    reason = f"missing:{column}"
                elif min_val is not None and value < min_val:
                    status = "failed"
                    reason = f"{key}_below_min:{min_val}"
                elif max_val is not None and value > max_val:
                    status = "failed"
                    reason = f"{key}_above_max:{max_val}"
                results.append({
                    "criterion": key,
                    "column": column,
                    "status": status,
                    "value": None if value is None or pd.isna(value) else value,
                    "min": min_val,
                    "max": max_val,
                    "reason": reason,
                })
            elif key == "mechanism":
                targets = criteria.get_mechanism_targets()
                if not targets:
                    continue
                value = record.get(column)
                match = self._categorical_score(value, targets)
                status = "passed" if match > 0 else "failed"
                results.append({
                    "criterion": key,
                    "column": column,
                    "status": status,
                    "value": value,
                    "target": targets,
                    "reason": "" if status == "passed" else "mechanism_mismatch",
                })
        return results

    def _error_metrics(
        self, record: pd.Series, criteria: SearchCriteria
    ) -> List[Dict[str, Any]]:
        metrics: List[Dict[str, Any]] = []
        for key, config in SCORING_MAP.items():
            column = config["column"]
            if key == "mechanism":
                targets = criteria.get_mechanism_targets()
                if not targets:
                    continue
                value = record.get(column)
                match = self._categorical_score(value, targets)
                metrics.append({
                    "criterion": key,
                    "column": column,
                    "status": "active",
                    "target": targets,
                    "value": value,
                    "absolute_error": 0.0 if match > 0 else 1.0,
                    "normalized_error": 0.0 if match == 1.0 else 0.3 if match > 0 else 1.0,
                    "match": match,
                })
                continue

            target = criteria.get_effective_target(key)
            if target is None:
                continue
            value = record.get(column)
            if value is None or pd.isna(value):
                metrics.append({
                    "criterion": key,
                    "column": column,
                    "status": "missing",
                    "target": target,
                    "value": None,
                    "absolute_error": None,
                    "normalized_error": 1.0,
                })
                continue

            absolute_error = abs(float(value) - float(target))
            scale = self._error_scale(criteria, key, target)
            metrics.append({
                "criterion": key,
                "column": column,
                "status": "active",
                "target": target,
                "value": value,
                "absolute_error": absolute_error,
                "normalized_error": absolute_error / scale,
                "scale": scale,
            })
        return metrics

    def _error_scale(
        self, criteria: SearchCriteria, key: str, target: float
    ) -> float:
        min_val = criteria._get_range_value("min", key)
        max_val = criteria._get_range_value("max", key)
        if min_val is not None and max_val is not None and max_val > min_val:
            return max_val - min_val
        return max(abs(float(target)) * 0.1, 1.0)

    def _format_filter_reasons(self, filters: List[Dict[str, Any]]) -> str:
        reasons = [
            item["reason"]
            for item in filters
            if item["status"] == "failed" and item.get("reason")
        ]
        return ";".join(reasons) if reasons else "hard_filter_failed"

    def _select_diverse_candidates(
        self, candidate_df: pd.DataFrame, scored_df: pd.DataFrame
    ) -> List[Any]:
        selected_indices: List[Any] = []
        station_counts: Dict[Any, int] = {}
        event_counts: Dict[Any, int] = {}

        for idx, record in candidate_df.iterrows():
            if len(selected_indices) >= self.config.num_records:
                scored_df.at[idx, "SELECTION_STATUS"] = "rejected"
                scored_df.at[idx, "SELECTION_REASON"] = (
                    f"num_records_limit:{self.config.num_records}"
                )
                continue

            station = record.get("STATION", "")
            event = record.get("EVENT", "")
            if station_counts.get(station, 0) >= self.config.max_per_station:
                scored_df.at[idx, "SELECTION_STATUS"] = "rejected"
                scored_df.at[idx, "SELECTION_REASON"] = (
                    f"max_per_station:{self.config.max_per_station}"
                )
                continue
            if event_counts.get(event, 0) >= self.config.max_per_event:
                scored_df.at[idx, "SELECTION_STATUS"] = "rejected"
                scored_df.at[idx, "SELECTION_REASON"] = (
                    f"max_per_event:{self.config.max_per_event}"
                )
                continue

            selected_indices.append(idx)
            station_counts[station] = station_counts.get(station, 0) + 1
            event_counts[event] = event_counts.get(event, 0) + 1

        return selected_indices


class ConstraintSelectionStrategy(TBDY2018ConstraintStrategy):
    """Backward-compatible name for TBDY2018ConstraintStrategy."""
