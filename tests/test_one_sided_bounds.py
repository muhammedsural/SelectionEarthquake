"""Tek tarafli sinir (yalnizca min veya yalnizca max) hedef degil, filtredir.

2.1.0 oncesi ``get_effective_target`` tek tarafli siniri hedef olarak
donduruyordu. Yalnizca ``min_magnitude=5.0`` ile arama yapildiginda secim M5
civarina yigiliyor, buyuk depremler ``num_records_limit`` ile eleniyordu.

Bu testler:
  - yalnizca min_magnitude -> buyuk depremler sistematik olarak elenmez,
  - acik target_magnitude ve iki tarafli aralik -> eski (hedefe yakinlik) davranis,
  - hedef yoksa varsayilan siralama: buyuk MAGNITUDE once, sonra kucuk mesafe,
  - cesitlilik limitleri (max_per_event / max_per_station) degismez.
"""

import pandas as pd
import pytest

from selection_service.enums.enums import DesignCode
from selection_service.processing.criteria import SearchCriteria, SelectionConfig
from selection_service.processing.strategies import (
    TBDY2018ConstraintStrategy,
    TBDYSelectionStrategy,
)

MAGNITUDES = [4.5, 5.0, 5.0, 5.1, 5.2, 5.5, 6.0, 6.5, 7.0, 7.4, 7.8]


def _record(rsn, mag, rjb=20.0, event=None, station=None, rrup=None):
    return {
        "RSN": rsn,
        "PROVIDER": "PEER",
        "EVENT": event or f"EQ{rsn}",
        "STATION": station or f"ST{rsn}",
        "YEAR": 2000,
        "MAGNITUDE": mag,
        "RJB(km)": rjb,
        "RRUP(km)": rrup if rrup is not None else rjb,
        "VS30(m/s)": 400.0,
        "MECHANISM": "StrikeSlip",
    }


@pytest.fixture
def records():
    return pd.DataFrame(
        [_record(i + 1, mag) for i, mag in enumerate(MAGNITUDES)]
    )


def _criteria(**kwargs):
    return SearchCriteria(start_date="2000-01-01", end_date="2025-01-01", **kwargs)


def _config(num_records=4, **kwargs):
    kwargs.setdefault("min_score", 50.0)
    return SelectionConfig(
        design_code=DesignCode.TBDY_2018, num_records=num_records, **kwargs
    )


STRATEGIES = [TBDYSelectionStrategy, TBDY2018ConstraintStrategy]


@pytest.mark.parametrize("strategy_cls", STRATEGIES)
class TestOnlyMinMagnitude:

    def test_larger_events_are_not_excluded(self, strategy_cls, records):
        """Hata: yalnizca min_magnitude=5.0 -> secim M5.0-5.2'ye yigiliyordu."""
        strategy = strategy_cls(config=_config(num_records=4))
        selected, scored = strategy.select_and_score(
            records, _criteria(min_magnitude=5.0)
        )

        assert selected["MAGNITUDE"].tolist() == [7.8, 7.4, 7.0, 6.5]
        assert selected["MAGNITUDE"].max() == max(MAGNITUDES)

    def test_one_sided_bound_does_not_create_magnitude_target(
        self, strategy_cls, records
    ):
        strategy = strategy_cls(config=_config(num_records=4))
        _, scored = strategy.select_and_score(records, _criteria(min_magnitude=5.0))
        for breakdown in scored["SCORE_BREAKDOWN"]:
            assert all(item["criterion"] != "magnitude" for item in breakdown)

    def test_only_max_magnitude_prefers_largest_allowed(self, strategy_cls, records):
        strategy = strategy_cls(config=_config(num_records=3))
        selected, _ = strategy.select_and_score(records, _criteria(max_magnitude=6.0))
        if strategy_cls is TBDY2018ConstraintStrategy:
            # max sert filtre: 6.0 ustu elenir
            assert selected["MAGNITUDE"].tolist() == [6.0, 5.5, 5.2]
        else:
            # Gaussian strateji sinir filtrelemesini provider'a birakir;
            # hedef olmadigi icin varsayilan siralama uygulanir.
            assert selected["MAGNITUDE"].tolist() == [7.8, 7.4, 7.0]


def test_constraint_only_min_still_hard_filters_below_min(records):
    strategy = TBDY2018ConstraintStrategy(config=_config(num_records=20))
    selected, scored = strategy.select_and_score(records, _criteria(min_magnitude=5.0))

    assert 4.5 not in selected["MAGNITUDE"].tolist()
    reasons = dict(zip(scored["RSN"], scored["SELECTION_REASON"]))
    assert reasons[1] == "magnitude_below_min:5.0"
    assert len(selected) == len(MAGNITUDES) - 1


@pytest.mark.parametrize("strategy_cls", STRATEGIES)
class TestTargetsKeepProximityOrdering:

    def test_explicit_target_prefers_near_target(self, strategy_cls, records):
        strategy = strategy_cls(config=_config(num_records=4, min_score=0.0))
        selected, _ = strategy.select_and_score(
            records, _criteria(min_magnitude=5.0, target_magnitude=5.0)
        )
        assert selected["MAGNITUDE"].tolist() == [5.0, 5.0, 5.1, 5.2]

    def test_two_sided_range_uses_midpoint(self, strategy_cls, records):
        strategy = strategy_cls(config=_config(num_records=4, min_score=0.0))
        selected, _ = strategy.select_and_score(
            records, _criteria(min_magnitude=5.0, max_magnitude=6.0)
        )
        # hedef 5.5; |6.0-5.5| == |5.0-5.5| esitliginde buyuk magnitude once
        assert selected["MAGNITUDE"].tolist() == [5.5, 5.2, 5.1, 6.0]


@pytest.mark.parametrize("strategy_cls", STRATEGIES)
class TestDefaultOrdering:

    def test_magnitude_desc_then_distance_asc(self, strategy_cls):
        rows = pd.DataFrame([
            _record(1, 6.0, rjb=50.0),
            _record(2, 7.0, rjb=80.0),
            _record(3, 6.0, rjb=10.0),
            _record(4, 7.0, rjb=30.0),
            _record(5, 6.5, rjb=5.0),
        ])
        strategy = strategy_cls(config=_config(num_records=5))
        selected, _ = strategy.select_and_score(rows, _criteria(min_magnitude=5.0))
        assert selected["RSN"].tolist() == [4, 2, 5, 3, 1]

    def test_distance_falls_back_to_rrup_when_rjb_missing(self, strategy_cls):
        rows = pd.DataFrame([
            _record(1, 6.0, rjb=40.0),
            _record(2, 6.0, rjb=None, rrup=15.0),
            _record(3, 6.0, rjb=None, rrup=None),
        ])
        strategy = strategy_cls(config=_config(num_records=3))
        selected, _ = strategy.select_and_score(rows, _criteria())
        assert selected["RSN"].tolist() == [2, 1, 3]

    def test_diversity_limits_unchanged(self, strategy_cls):
        rows = pd.DataFrame([
            _record(1, 7.8, event="BIG", station="A"),
            _record(2, 7.8, event="BIG", station="B"),
            _record(3, 7.0, event="E3", station="A"),
            _record(4, 6.0, event="E4", station="C"),
        ])
        strategy = strategy_cls(
            config=_config(num_records=3, max_per_event=1, max_per_station=1)
        )
        selected, scored = strategy.select_and_score(rows, _criteria(min_magnitude=5.0))
        assert selected["RSN"].tolist() == [1, 4]
        reasons = dict(zip(scored["RSN"], scored["SELECTION_REASON"]))
        assert reasons[2] == "max_per_event:1"
        assert reasons[3] == "max_per_station:1"
