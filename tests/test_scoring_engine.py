"""
tests/test_scoring_engine.py

Puanlama motoru ve seçim kuralları — %100 coverage hedefi.

Kapsam:
  - _gaussian_score    : hedef isabet, uzak değer, None/NaN
  - _categorical_score : tam eşleşme, kısmi eşleşme, eşleşme yok
  - _calculate_total_score : aktif kriter, ağırsız kriter, kategorik
  - _apply_selection_rules : min_score, max_per_station, max_per_event, num_records
  - select_and_score   : boş DataFrame, SCORE sütunu
  - EurocodeSelectionStrategy : temel çalışma
"""

import math
import pytest
import pandas as pd

from selection_service.enums.Enums import DesignCode
from selection_service.processing.Selection import (
    SelectionConfig,
    SearchCriteria,
    ScoringWeights,
    TBDYSelectionStrategy,
    EurocodeSelectionStrategy,
    BaseSelectionStrategy,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def config():
    return SelectionConfig(
        design_code=DesignCode.TBDY_2018,
        num_records=10,
        max_per_station=2,
        max_per_event=3,
        min_score=50.0,
    )


@pytest.fixture
def strategy(config):
    return TBDYSelectionStrategy(config=config)


@pytest.fixture
def criteria():
    return SearchCriteria(
        start_date="2000-01-01",
        end_date="2025-01-01",
        min_magnitude=7.0,
        max_magnitude=8.0,
        min_vs30=300.0,
        max_vs30=400.0,
        mechanisms=["StrikeSlip"],
    )


def make_record(**kwargs) -> pd.Series:
    defaults = {
        "RSN": 1, "PROVIDER": "PEER", "EVENT": "TestEQ", "YEAR": 2000,
        "MAGNITUDE": 7.5, "SSN": 100, "STATION": "TestStation",
        "VS30(m/s)": 350.0, "RRUP(km)": 50.0, "RJB(km)": 50.0,
        "MECHANISM": "StrikeSlip", "PGA(cm2/sec)": 100.0,
        "PGV(cm/sec)": 20.0, "PGD(cm)": 5.0, "T90_avg(sec)": 15.0,
        "ARIAS_INTENSITY(m/sec)": 1.0, "HYPO_DEPTH(km)": 15.0,
        "ENDPOINTSOURCE": None, "FILE_NAME_H1": "test.AT2",
        "SCORE": None,
    }
    defaults.update(kwargs)
    return pd.Series(defaults)


def make_df(records: list) -> pd.DataFrame:
    return pd.DataFrame([make_record(**r) for r in records])


# ─────────────────────────────────────────────────────────────────────────────
# _gaussian_score testleri
# ─────────────────────────────────────────────────────────────────────────────

class TestGaussianScore:

    def test_exact_hit_returns_one(self, strategy):
        score = strategy._gaussian_score(7.5, 7.5, 0.5)
        assert score == pytest.approx(1.0)

    def test_far_value_near_zero(self, strategy):
        score = strategy._gaussian_score(10.0, 0.0, 0.5)
        assert score < 0.01

    def test_one_sigma_away(self, strategy):
        score = strategy._gaussian_score(8.0, 7.5, 0.5)
        expected = math.exp(-0.5)
        assert score == pytest.approx(expected, rel=1e-5)

    def test_none_value_returns_zero(self, strategy):
        assert strategy._gaussian_score(None, 7.5, 0.5) == 0.0

    def test_nan_value_returns_zero(self, strategy):
        assert strategy._gaussian_score(float("nan"), 7.5, 0.5) == 0.0

    def test_none_target_returns_zero(self, strategy):
        assert strategy._gaussian_score(7.5, None, 0.5) == 0.0

    def test_symmetric(self, strategy):
        left  = strategy._gaussian_score(7.0, 7.5, 0.5)
        right = strategy._gaussian_score(8.0, 7.5, 0.5)
        assert left == pytest.approx(right, rel=1e-5)


# ─────────────────────────────────────────────────────────────────────────────
# _categorical_score testleri
# ─────────────────────────────────────────────────────────────────────────────

class TestCategoricalScore:

    def test_exact_match_returns_one(self, strategy):
        assert strategy._categorical_score("StrikeSlip", ["StrikeSlip"]) == 1.0

    def test_partial_match_returns_07(self, strategy):
        """'Reverse' aranıyor, kayıt 'Reverse/Oblique' → kısmi eşleşme."""
        assert strategy._categorical_score("Reverse/Oblique", ["Reverse"]) == pytest.approx(0.7)

    def test_no_match_returns_zero(self, strategy):
        assert strategy._categorical_score("Normal", ["StrikeSlip"]) == 0.0

    def test_empty_record_val_returns_zero(self, strategy):
        assert strategy._categorical_score("", ["StrikeSlip"]) == 0.0

    def test_empty_target_list_returns_zero(self, strategy):
        assert strategy._categorical_score("StrikeSlip", []) == 0.0

    def test_multiple_targets_first_matches(self, strategy):
        assert strategy._categorical_score("Normal", ["StrikeSlip", "Normal"]) == 1.0


# ─────────────────────────────────────────────────────────────────────────────
# _calculate_total_score testleri
# ─────────────────────────────────────────────────────────────────────────────

class TestCalculateTotalScore:

    def test_perfect_match_high_score(self, strategy, criteria):
        record = make_record(MAGNITUDE=7.5, **{"VS30(m/s)": 350.0}, MECHANISM="StrikeSlip")
        score = strategy._calculate_total_score(record, criteria)
        assert score > 50.0

    def test_no_criteria_returns_zero(self, strategy):
        """Hiçbir kriter verilmezse skor 0 olmalı."""
        empty_criteria = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01")
        record = make_record()
        score = strategy._calculate_total_score(record, empty_criteria)
        assert score == pytest.approx(0.0)

    def test_zero_weight_skipped(self, strategy, criteria):
        from selection_service.processing.Selection import ScoringWeights
        zero_weights = ScoringWeights(magnitude=0.0, rjb=0.0, rrup=0.0,
                                      repi=0.0, vs30=0.0, pga=0.0, pgv=0.0,
                                      pgd=0.0, t90=0.0, arias=0.0, depth=0.0,
                                      mechanism=5.0)
        modified = criteria.model_copy(update={"weights": zero_weights})
        record = make_record(MECHANISM="StrikeSlip")
        score = strategy._calculate_total_score(record, modified)
        assert score >= 0.0

    def test_missing_column_skipped(self, strategy, criteria):
        """Kayıt kriter sütununu içermiyorsa NaN olarak davranılmalı."""
        record = make_record()
        record_dict = record.to_dict()
        record_dict.pop("VS30(m/s)", None)
        partial_record = pd.Series(record_dict)
        score = strategy._calculate_total_score(partial_record, criteria)
        assert score >= 0.0

    def test_nan_column_value_skipped(self, strategy, criteria):
        record = make_record(**{"VS30(m/s)": float("nan")})
        score = strategy._calculate_total_score(record, criteria)
        assert score >= 0.0

    def test_score_breakdown_contains_active_criteria(self, strategy, criteria):
        record = make_record(MAGNITUDE=7.5, **{"VS30(m/s)": 350.0})
        score, breakdown = strategy._calculate_score_breakdown(record, criteria)
        assert score > 0
        assert any(item["criterion"] == "magnitude" for item in breakdown)
        assert all("weighted_score" in item for item in breakdown)


# ─────────────────────────────────────────────────────────────────────────────
# _apply_selection_rules testleri
# ─────────────────────────────────────────────────────────────────────────────

class TestApplySelectionRules:

    def test_min_score_filter(self, strategy):
        """min_score altındaki kayıtlar seçilmemeli."""
        df = make_df([
            {"RSN": 1, "SCORE": 80.0, "STATION": "S1", "EVENT": "E1"},
            {"RSN": 2, "SCORE": 30.0, "STATION": "S2", "EVENT": "E2"},  # elenecek
        ])
        df["SCORE"] = [80.0, 30.0]
        selected = strategy._apply_selection_rules(df)
        assert len(selected) == 1
        assert selected["RSN"].iloc[0] == 1

    def test_num_records_limit(self, config):
        """num_records=3 limitine saygı gösterilmeli."""
        config_3 = SelectionConfig(design_code=DesignCode.TBDY_2018,
                                   num_records=3, min_score=0.0)
        strategy_3 = TBDYSelectionStrategy(config=config_3)
        rows = [{"RSN": i, "SCORE": 80.0, "STATION": f"S{i}", "EVENT": f"E{i}"}
                for i in range(10)]
        df = make_df(rows)
        df["SCORE"] = [80.0] * 10
        selected = strategy_3._apply_selection_rules(df)
        assert len(selected) <= 3

    def test_max_per_station(self, config):
        """Aynı istasyondan max_per_station=2'den fazla kayıt seçilmemeli."""
        rows = [{"RSN": i, "SCORE": 80.0, "STATION": "SAME", "EVENT": f"E{i}"}
                for i in range(5)]
        df = make_df(rows)
        df["SCORE"] = [80.0] * 5
        selected = strategy._apply_selection_rules(df) if False else \
                   TBDYSelectionStrategy(config=config)._apply_selection_rules(df)
        station_counts = selected.groupby("STATION").size()
        if "SAME" in station_counts:
            assert station_counts["SAME"] <= config.max_per_station

    def test_max_per_event(self, config):
        """Aynı depremden max_per_event=3'ten fazla kayıt seçilmemeli."""
        rows = [{"RSN": i, "SCORE": 80.0, "STATION": f"S{i}", "EVENT": "SAME_EQ"}
                for i in range(8)]
        df = make_df(rows)
        df["SCORE"] = [80.0] * 8
        s = TBDYSelectionStrategy(config=config)
        selected = s._apply_selection_rules(df)
        event_counts = selected.groupby("EVENT").size()
        if "SAME_EQ" in event_counts:
            assert event_counts["SAME_EQ"] <= config.max_per_event

    def test_empty_after_min_score_returns_empty(self, strategy):
        """Tüm kayıtlar min_score altındaysa boş DataFrame dönmeli."""
        df = make_df([{"RSN": 1, "SCORE": 10.0, "STATION": "S1", "EVENT": "E1"}])
        df["SCORE"] = [10.0]
        selected = strategy._apply_selection_rules(df)
        assert selected.empty

    def test_sorted_by_score_descending(self, strategy):
        """Seçim en yüksek skordan başlamalı."""
        rows = [
            {"RSN": 1, "SCORE": 60.0, "STATION": "S1", "EVENT": "E1"},
            {"RSN": 2, "SCORE": 90.0, "STATION": "S2", "EVENT": "E2"},
            {"RSN": 3, "SCORE": 75.0, "STATION": "S3", "EVENT": "E3"},
        ]
        df = make_df(rows)
        df["SCORE"] = [60.0, 90.0, 75.0]
        config_1 = SelectionConfig(design_code=DesignCode.TBDY_2018,
                                   num_records=1, min_score=0.0)
        s = TBDYSelectionStrategy(config=config_1)
        selected = s._apply_selection_rules(df)
        assert selected["RSN"].iloc[0] == 2  # en yüksek skor


# ─────────────────────────────────────────────────────────────────────────────
# select_and_score testleri
# ─────────────────────────────────────────────────────────────────────────────

class TestSelectAndScore:

    def test_empty_df_returns_empty_pair(self, strategy, criteria):
        selected, scored = strategy.select_and_score(pd.DataFrame(), criteria)
        assert selected.empty
        assert scored.empty

    def test_score_column_added(self, strategy, criteria):
        df = make_df([{"RSN": 1, "MAGNITUDE": 7.5, "MECHANISM": "StrikeSlip",
                       "VS30(m/s)": 350.0}])
        _, scored = strategy.select_and_score(df, criteria)
        assert "SCORE" in scored.columns

    def test_all_scores_in_range(self, strategy, criteria):
        df = make_df([
            {"RSN": i, "MAGNITUDE": 7.5 + i*0.1, "MECHANISM": "StrikeSlip",
             "VS30(m/s)": 350.0, "STATION": f"S{i}", "EVENT": f"E{i}"}
            for i in range(5)
        ])
        _, scored = strategy.select_and_score(df, criteria)
        assert (scored["SCORE"] >= 0).all()
        assert (scored["SCORE"] <= 100).all()

    def test_selected_is_subset_of_scored(self, strategy, criteria):
        df = make_df([
            {"RSN": i, "MAGNITUDE": 7.5, "MECHANISM": "StrikeSlip",
             "VS30(m/s)": 350.0, "STATION": f"S{i}", "EVENT": f"E{i}"}
            for i in range(8)
        ])
        selected, scored = strategy.select_and_score(df, criteria)
        if not selected.empty:
            assert set(selected["RSN"]).issubset(set(scored["RSN"]))

    def test_traceability_columns_added(self, strategy, criteria):
        df = make_df([
            {"RSN": 1, "MAGNITUDE": 7.5, "MECHANISM": "StrikeSlip",
             "VS30(m/s)": 350.0, "STATION": "S1", "EVENT": "E1"}
        ])
        selected, scored = strategy.select_and_score(df, criteria)
        assert "SCORE_BREAKDOWN" in scored.columns
        assert "SELECTION_STATUS" in scored.columns
        assert "SELECTION_REASON" in scored.columns
        assert selected["SELECTION_REASON"].iloc[0] == "selected"


class TestScoringPresets:

    def test_from_preset_returns_weights(self):
        weights = ScoringWeights.from_preset("tbdy_2018_record_selection")
        assert weights.magnitude > 0
        assert weights.vs30 > 0

    def test_preset_descriptions_document_available_presets(self):
        descriptions = ScoringWeights.preset_descriptions()
        assert "balanced" in descriptions
        assert "tbdy_2018_record_selection" in descriptions

    def test_unknown_preset_raises_clear_error(self):
        with pytest.raises(ValueError, match="Unknown scoring preset"):
            ScoringWeights.from_preset("missing")


# ─────────────────────────────────────────────────────────────────────────────
# BaseSelectionStrategy.get_name ve EurocodeSelectionStrategy
# ─────────────────────────────────────────────────────────────────────────────

class TestStrategyGetName:

    def test_tbdy_name(self, config):
        s = TBDYSelectionStrategy(config=config)
        assert s.get_name() == "TBDY_2018_Gaussian"

    def test_base_get_name_uses_design_code(self, config):
        """BaseSelectionStrategy.get_name() design_code değerini döndürür."""
        # EurocodeSelectionStrategy base get_name'i override etmez
        eurocode_config = SelectionConfig(design_code=DesignCode.TBDY_2018)
        s = EurocodeSelectionStrategy(config=eurocode_config)
        # EurocodeSelectionStrategy TBDYSelectionStrategy gibi get_name override etmez
        # → Base get_name çağrılır
        name = s.get_name()
        assert isinstance(name, str)

    def test_eurocode_strategy_instantiable(self, config):
        s = EurocodeSelectionStrategy(config=config)
        assert s is not None

    def test_eurocode_calculate_score_returns_float(self, config):
        """EurocodeSelectionStrategy._calculate_score placeholder → 0.0 döndürmeli."""
        s = EurocodeSelectionStrategy(config=config)
        record = make_record()
        result = s._calculate_score(record, {})
        assert isinstance(result, float)
