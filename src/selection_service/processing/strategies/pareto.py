"""Pareto tabanli secim stratejisi."""

from typing import Any, Dict
import pandas as pd
from .tbdy import TBDY2018ConstraintStrategy


class ParetoSelectionStrategy(TBDY2018ConstraintStrategy):
    """Select nondominated records before applying diversity limits."""

    def get_name(self) -> str:
        return "Pareto_Selection"

    def _add_strategy_columns(self, scored_df: pd.DataFrame) -> None:
        ranks = self._pareto_ranks(scored_df)
        scored_df["PARETO_RANK"] = scored_df.index.map(ranks)
        scored_df["PARETO_FRONT"] = scored_df["PARETO_RANK"].eq(0)

    def _candidate_order(self, candidate_df: pd.DataFrame) -> pd.DataFrame:
        return candidate_df.sort_values(
            ["PARETO_RANK", "ERROR_TOTAL", "SCORE"],
            ascending=[True, True, False],
        )

    def _pareto_ranks(self, df: pd.DataFrame) -> Dict[Any, int]:
        remaining = list(df.index)
        ranks: Dict[Any, int] = {}
        rank = 0
        while remaining:
            front = []
            for idx in remaining:
                row = df.loc[idx]
                dominated = any(
                    self._dominates(df.loc[other], row)
                    for other in remaining
                    if other != idx
                )
                if not dominated:
                    front.append(idx)
            for idx in front:
                ranks[idx] = rank
            remaining = [idx for idx in remaining if idx not in front]
            rank += 1
        return ranks

    def _dominates(self, left: pd.Series, right: pd.Series) -> bool:
        left_metrics = self._metric_vector(left)
        right_metrics = self._metric_vector(right)
        keys = set(left_metrics) | set(right_metrics)
        if not keys:
            return False
        left_values = [left_metrics.get(key, 1.0) for key in keys]
        right_values = [right_metrics.get(key, 1.0) for key in keys]
        return all(l <= r for l, r in zip(left_values, right_values)) and any(
            l < r for l, r in zip(left_values, right_values)
        )

    def _metric_vector(self, record: pd.Series) -> Dict[str, float]:
        metrics = record.get("ERROR_METRICS", [])
        return {
            item["criterion"]: float(item.get("normalized_error", 1.0))
            for item in metrics
            if item.get("status") in ("active", "missing")
        }
