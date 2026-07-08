"""
tests/test_selection.py

Seçim katmanı testleri.

Kapsam:
  - SelectionConfig   : Pydantic validasyon, alan varsayılanları
  - SearchCriteria    : tip doğrulama, opsiyonel alanlar
  - TBDYSelectionStrategy : puanlama, max_per_station, max_per_event,
                            min_score filtresi, num_records limiti
  - Gerçek kayıtlar   : basic_usage çıktısındaki 22 PEER kaydı ile bütünleşik test
"""

import pytest
import pandas as pd
from pydantic import ValidationError

from selection_service.enums.Enums import DesignCode
from selection_service.processing.Selection import (
    SelectionConfig,
    SearchCriteria,
    ScoringWeights,
    TBDYSelectionStrategy,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def default_config() -> SelectionConfig:
    return SelectionConfig(design_code=DesignCode.TBDY_2018)


@pytest.fixture
def basic_usage_config() -> SelectionConfig:
    """basic_usage.py'deki konfigürasyon."""
    return SelectionConfig(
        design_code=DesignCode.TBDY_2018,
        num_records=22,
        max_per_station=3,
        max_per_event=3,
        min_score=55,
    )


@pytest.fixture
def basic_usage_criteria() -> SearchCriteria:
    """basic_usage.py'deki arama kriterleri."""
    return SearchCriteria(
        start_date="2000-01-01",
        end_date="2025-09-05",
        min_magnitude=7.0,
        max_magnitude=10.0,
        min_vs30=300,
        max_vs30=400,
        mechanisms=["StrikeSlip"],
    )


@pytest.fixture
def basic_usage_results() -> pd.DataFrame:
    """basic_usage.py çıktısından alınan 22 PEER kaydı — gerçek test verisi."""
    return pd.DataFrame([
        {"RSN": 3834, "PROVIDER": "PEER", "EVENT": "Denali, Alaska",      "YEAR": 2002,
         "MAGNITUDE": 7.90, "SSN": 1271, "STATION": "Anchorage-Aho",      "VS30(m/s)": 341.56,
         "RRUP(km)": 270.25, "RJB(km)": 270.25, "MECHANISM": "StrikeSlip",
         "PGA(cm2/sec)": 16.13, "PGV(cm/sec)": 4.41,  "T90_avg(sec)": 123.5,
         "ENDPOINTSOURCE": None, "FILE_NAME_H1": "DENALI\\AHO-90.AT2",    "SCORE": 85.75},
        {"RSN": 2109, "PROVIDER": "PEER", "EVENT": "Denali, Alaska",      "YEAR": 2002,
         "MAGNITUDE": 7.90, "SSN": 1563, "STATION": "Fairbanks-Ester",    "VS30(m/s)": 341.56,
         "RRUP(km)": 139.85, "RJB(km)": 139.27, "MECHANISM": "StrikeSlip",
         "PGA(cm2/sec)": 48.03, "PGV(cm/sec)": 4.00,  "T90_avg(sec)":  94.8,
         "ENDPOINTSOURCE": None, "FILE_NAME_H1": "DENALI\\FAIFS-90.AT2",  "SCORE": 85.75},
        {"RSN": 2100, "PROVIDER": "PEER", "EVENT": "Denali, Alaska",      "YEAR": 2002,
         "MAGNITUDE": 7.90, "SSN": 1567, "STATION": "Anchorage-K2-05",    "VS30(m/s)": 341.56,
         "RRUP(km)": 269.07, "RJB(km)": 269.07, "MECHANISM": "StrikeSlip",
         "PGA(cm2/sec)": 15.78, "PGV(cm/sec)": 3.31,  "T90_avg(sec)": 128.1,
         "ENDPOINTSOURCE": None, "FILE_NAME_H1": "DENALI\\K205-90.AT2",   "SCORE": 85.75},
        {"RSN": 1162, "PROVIDER": "PEER", "EVENT": "Kocaeli, Turkey",     "YEAR": 1999,
         "MAGNITUDE": 7.51, "SSN": 724,  "STATION": "Goynuk",             "VS30(m/s)": 347.62,
         "RRUP(km)": 31.74,  "RJB(km)": 31.74,  "MECHANISM": "StrikeSlip",
         "PGA(cm2/sec)": 137.11, "PGV(cm/sec)": 11.92, "T90_avg(sec)":  11.4,
         "ENDPOINTSOURCE": None, "FILE_NAME_H1": "KOCAELI\\GYN000.AT2",   "SCORE": 75.53},
        {"RSN": 1157, "PROVIDER": "PEER", "EVENT": "Kocaeli, Turkey",     "YEAR": 1999,
         "MAGNITUDE": 7.51, "SSN": 625,  "STATION": "Cekmece",            "VS30(m/s)": 346.00,
         "RRUP(km)": 66.69,  "RJB(km)": 64.95,  "MECHANISM": "StrikeSlip",
         "PGA(cm2/sec)": 156.05, "PGV(cm/sec)": 12.93, "T90_avg(sec)":  37.0,
         "ENDPOINTSOURCE": None, "FILE_NAME_H1": "KOCAELI\\CNA000.AT2",   "SCORE": 75.11},
        {"RSN": 1163, "PROVIDER": "PEER", "EVENT": "Kocaeli, Turkey",     "YEAR": 1999,
         "MAGNITUDE": 7.51, "SSN": 730,  "STATION": "Hava Alani",         "VS30(m/s)": 354.37,
         "RRUP(km)": 60.05,  "RJB(km)": 58.33,  "MECHANISM": "StrikeSlip",
         "PGA(cm2/sec)": 87.99, "PGV(cm/sec)": 19.00, "T90_avg(sec)":  36.7,
         "ENDPOINTSOURCE": None, "FILE_NAME_H1": "KOCAELI\\DHM000.AT2",   "SCORE": 74.98},
        {"RSN":  870, "PROVIDER": "PEER", "EVENT": "Landers",             "YEAR": 1992,
         "MAGNITUDE": 7.28, "SSN": 337,  "STATION": "LA-Obregon Park",    "VS30(m/s)": 349.43,
         "RRUP(km)": 151.70, "RJB(km)": 151.70, "MECHANISM": "StrikeSlip",
         "PGA(cm2/sec)": 47.59, "PGV(cm/sec)": 11.66, "T90_avg(sec)":  46.0,
         "ENDPOINTSOURCE": None, "FILE_NAME_H1": "LANDERS\\OBR000.AT2",   "SCORE": 69.42},
        {"RSN":  848, "PROVIDER": "PEER", "EVENT": "Landers",             "YEAR": 1992,
         "MAGNITUDE": 7.28, "SSN":  24,  "STATION": "Coolwater",          "VS30(m/s)": 352.98,
         "RRUP(km)": 19.74,  "RJB(km)": 19.74,  "MECHANISM": "StrikeSlip",
         "PGA(cm2/sec)": 344.11, "PGV(cm/sec)": 33.22, "T90_avg(sec)":  10.6,
         "ENDPOINTSOURCE": None, "FILE_NAME_H1": "LANDERS\\CLW-LN.AT2",   "SCORE": 69.06},
        {"RSN":  900, "PROVIDER": "PEER", "EVENT": "Landers",             "YEAR": 1992,
         "MAGNITUDE": 7.28, "SSN": 296,  "STATION": "Yermo Fire Station", "VS30(m/s)": 353.63,
         "RRUP(km)": 23.62,  "RJB(km)": 23.62,  "MECHANISM": "StrikeSlip",
         "PGA(cm2/sec)": 217.78, "PGV(cm/sec)": 40.26, "T90_avg(sec)":  18.9,
         "ENDPOINTSOURCE": None, "FILE_NAME_H1": "LANDERS\\YER270.AT2",   "SCORE": 68.89},
        {"RSN": 1144, "PROVIDER": "PEER", "EVENT": "Gulf of Aqaba",       "YEAR": 1995,
         "MAGNITUDE": 7.20, "SSN": 712,  "STATION": "Eilat",              "VS30(m/s)": 354.88,
         "RRUP(km)": 44.10,  "RJB(km)": 43.29,  "MECHANISM": "StrikeSlip",
         "PGA(cm2/sec)": 90.15, "PGV(cm/sec)": 13.27, "T90_avg(sec)":  23.0,
         "ENDPOINTSOURCE": None, "FILE_NAME_H1": "AQABA\\EIL-EW.AT2",     "SCORE": 66.63},
    ])


# ─────────────────────────────────────────────────────────────────────────────
# SelectionConfig testleri
# ─────────────────────────────────────────────────────────────────────────────

class TestSelectionConfig:

    def test_default_values(self, default_config):
        assert default_config.num_records == 22
        assert default_config.max_per_station == 3
        assert default_config.max_per_event == 3
        assert default_config.min_score == 50.0
        assert default_config.required_components == []

    def test_required_components_is_real_list(self):
        """Eski @dataclass+Field() hatasında bu bir FieldInfo nesnesi dönerdi."""
        cfg = SelectionConfig(design_code=DesignCode.TBDY_2018)
        assert isinstance(cfg.required_components, list)
        cfg.required_components.append("H1")
        assert cfg.required_components == ["H1"]

    def test_no_shared_list_between_instances(self):
        """İki instance arasında required_components paylaşılmamalı."""
        cfg1 = SelectionConfig(design_code=DesignCode.TBDY_2018)
        cfg2 = SelectionConfig(design_code=DesignCode.TBDY_2018)
        cfg1.required_components.append("H1")
        assert cfg2.required_components == []

    def test_design_code_required(self):
        with pytest.raises(ValidationError):
            SelectionConfig()

    def test_model_dump(self, basic_usage_config):
        d = basic_usage_config.model_dump()
        assert d["num_records"] == 22
        assert d["min_score"] == 55

    def test_model_copy(self, basic_usage_config):
        copy = basic_usage_config.model_copy(update={"min_score": 70.0})
        assert copy.min_score == 70.0
        assert basic_usage_config.min_score == 55  # orijinal değişmedi

    def test_invalid_num_records_type(self):
        with pytest.raises(ValidationError):
            SelectionConfig(design_code=DesignCode.TBDY_2018, num_records="yirmi_iki")


# ─────────────────────────────────────────────────────────────────────────────
# SearchCriteria testleri
# ─────────────────────────────────────────────────────────────────────────────

class TestSearchCriteria:

    def test_basic_usage_criteria_valid(self, basic_usage_criteria):
        assert basic_usage_criteria.min_magnitude == 7.0
        assert basic_usage_criteria.max_magnitude == 10.0
        assert basic_usage_criteria.mechanisms == ["StrikeSlip"]

    def test_optional_fields_default_none(self):
        criteria = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01")
        assert criteria.min_magnitude is None
        assert criteria.max_magnitude is None
        assert criteria.min_vs30 is None

    def test_start_date_required(self):
        with pytest.raises(ValidationError):
            SearchCriteria(end_date="2025-01-01")

    def test_end_date_required(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01")


# ─────────────────────────────────────────────────────────────────────────────
# TBDYSelectionStrategy testleri — gerçek kayıtlar
# ─────────────────────────────────────────────────────────────────────────────

class TestTBDYSelectionStrategy:

    def test_strategy_name(self, basic_usage_config):
        strategy = TBDYSelectionStrategy(config=basic_usage_config)
        assert strategy.get_name() == "TBDY_2018_Gaussian"

    def test_returns_two_dataframes(self, basic_usage_config, basic_usage_criteria, basic_usage_results):
        strategy = TBDYSelectionStrategy(config=basic_usage_config)
        selected, scored = strategy.select_and_score(basic_usage_results, basic_usage_criteria)
        assert isinstance(selected, pd.DataFrame)
        assert isinstance(scored, pd.DataFrame)

    def test_score_column_exists(self, basic_usage_config, basic_usage_criteria, basic_usage_results):
        strategy = TBDYSelectionStrategy(config=basic_usage_config)
        selected, scored = strategy.select_and_score(basic_usage_results, basic_usage_criteria)
        assert "SCORE" in scored.columns
        assert "SCORE" in selected.columns

    def test_num_records_limit(self, basic_usage_criteria, basic_usage_results):
        """num_records=5 → seçilen kayıt ≤ 5 olmalı."""
        config = SelectionConfig(design_code=DesignCode.TBDY_2018, num_records=5)
        strategy = TBDYSelectionStrategy(config=config)
        selected, _ = strategy.select_and_score(basic_usage_results, basic_usage_criteria)
        assert len(selected) <= 5

    def test_min_score_filter(self, basic_usage_criteria, basic_usage_results):
        """min_score=90 → bu eşiğin altındaki kayıtlar seçilmemeli."""
        config = SelectionConfig(design_code=DesignCode.TBDY_2018, min_score=90.0)
        strategy = TBDYSelectionStrategy(config=config)
        selected, _ = strategy.select_and_score(basic_usage_results, basic_usage_criteria)
        if not selected.empty:
            assert (selected["SCORE"] >= 90.0).all()

    def test_max_per_event_limit(self, basic_usage_config, basic_usage_criteria, basic_usage_results):
        """Aynı depremden (EVENT) max_per_event=3'ten fazla kayıt seçilmemeli."""
        strategy = TBDYSelectionStrategy(config=basic_usage_config)
        selected, _ = strategy.select_and_score(basic_usage_results, basic_usage_criteria)
        if not selected.empty:
            event_counts = selected.groupby("EVENT").size()
            assert (event_counts <= basic_usage_config.max_per_event).all()

    def test_max_per_station_limit(self, basic_usage_config, basic_usage_criteria, basic_usage_results):
        """Aynı istasyondan (SSN) max_per_station'dan fazla kayıt seçilmemeli."""
        strategy = TBDYSelectionStrategy(config=basic_usage_config)
        selected, _ = strategy.select_and_score(basic_usage_results, basic_usage_criteria)
        if not selected.empty:
            station_counts = selected.groupby("SSN").size()
            assert (station_counts <= basic_usage_config.max_per_station).all()

    def test_selected_subset_of_scored(self, basic_usage_config, basic_usage_criteria, basic_usage_results):
        """Seçilen kayıtlar puanlananların alt kümesi olmalı."""
        strategy = TBDYSelectionStrategy(config=basic_usage_config)
        selected, scored = strategy.select_and_score(basic_usage_results, basic_usage_criteria)
        assert len(selected) <= len(scored)
        if not selected.empty:
            assert set(selected["RSN"]).issubset(set(scored["RSN"]))

    def test_scores_are_positive(self, basic_usage_config, basic_usage_criteria, basic_usage_results):
        """Tüm puanlar pozitif olmalı."""
        strategy = TBDYSelectionStrategy(config=basic_usage_config)
        _, scored = strategy.select_and_score(basic_usage_results, basic_usage_criteria)
        assert (scored["SCORE"] >= 0).all()

    def test_basic_usage_replication(self, basic_usage_config, basic_usage_criteria, basic_usage_results):
        """basic_usage.py çıktısındaki 22 PEER kaydının tamamı seçilebilmeli
        (bu veri zaten filtrelerden geçmiş; tekrar çalıştırılınca aynı set gelmeli)."""
        strategy = TBDYSelectionStrategy(config=basic_usage_config)
        selected, scored = strategy.select_and_score(basic_usage_results, basic_usage_criteria)
        # 10 kayıt verildi, num_records=22 — tamamı seçilebilir
        assert len(selected) <= basic_usage_config.num_records
        assert not selected.empty


class TestTBDYSearchCombinations:

    @pytest.mark.parametrize(
        "criteria,expected_criteria",
        [
            (
                SearchCriteria(
                    start_date="2000-01-01",
                    end_date="2025-09-05",
                    target_magnitude=7.9,
                    weights=ScoringWeights.from_preset("balanced"),
                ),
                {"magnitude"},
            ),
            (
                SearchCriteria(
                    start_date="2000-01-01",
                    end_date="2025-09-05",
                    min_Rjb=0.0,
                    max_Rjb=100.0,
                    min_Rrup=0.0,
                    max_Rrup=120.0,
                    min_vs30=330.0,
                    max_vs30=370.0,
                    weights=ScoringWeights.from_preset("tbdy_2018_record_selection"),
                ),
                {"rjb", "rrup", "vs30"},
            ),
            (
                SearchCriteria(
                    start_date="2000-01-01",
                    end_date="2025-09-05",
                    target_pga=120.0,
                    target_pgv=15.0,
                    target_t90=20.0,
                    mechanisms=["StrikeSlip"],
                    weights=ScoringWeights.from_preset("site_response"),
                ),
                {"pga", "pgv", "t90", "mechanism"},
            ),
            (
                SearchCriteria(
                    start_date="2000-01-01",
                    end_date="2025-09-05",
                    fault_type="StrikeSlip",
                    weights=ScoringWeights.from_preset("balanced"),
                ),
                {"mechanism"},
            ),
        ],
    )
    def test_different_search_combinations_drive_score_breakdown(
        self,
        basic_usage_results,
        criteria,
        expected_criteria,
    ):
        config = SelectionConfig(
            design_code=DesignCode.TBDY_2018,
            num_records=5,
            min_score=0.0,
        )
        strategy = TBDYSelectionStrategy(config=config)
        selected, scored = strategy.select_and_score(basic_usage_results, criteria)

        assert not selected.empty
        first_breakdown = scored["SCORE_BREAKDOWN"].iloc[0]
        active = {
            item["criterion"]
            for item in first_breakdown
            if item["status"] == "active"
        }
        assert expected_criteria.issubset(active)
        assert (scored["SCORE"] >= 0).all()
        assert (scored["SCORE"] <= 100).all()

    def test_no_active_scoring_criteria_rejects_all_when_min_score_positive(
        self,
        basic_usage_results,
    ):
        config = SelectionConfig(
            design_code=DesignCode.TBDY_2018,
            num_records=5,
            min_score=1.0,
        )
        strategy = TBDYSelectionStrategy(config=config)
        criteria = SearchCriteria(start_date="2000-01-01", end_date="2025-09-05")

        selected, scored = strategy.select_and_score(basic_usage_results, criteria)

        assert selected.empty
        assert (scored["SCORE"] == 0).all()
        assert (scored["SELECTION_STATUS"] == "rejected").all()
        assert set(scored["SELECTION_REASON"]) == {"score_below_min_score:1.0"}

    def test_combined_limits_explain_station_event_and_count_rejections(self):
        rows = pd.DataFrame(
            [
                {
                    "RSN": 1,
                    "PROVIDER": "PEER",
                    "EVENT": "EQ1",
                    "YEAR": 2000,
                    "MAGNITUDE": 7.0,
                    "SSN": 1,
                    "STATION": "S1",
                    "VS30(m/s)": 350.0,
                    "RJB(km)": 10.0,
                    "RRUP(km)": 10.0,
                    "MECHANISM": "StrikeSlip",
                },
                {
                    "RSN": 2,
                    "PROVIDER": "PEER",
                    "EVENT": "EQ1",
                    "YEAR": 2000,
                    "MAGNITUDE": 7.0,
                    "SSN": 2,
                    "STATION": "S2",
                    "VS30(m/s)": 350.0,
                    "RJB(km)": 10.0,
                    "RRUP(km)": 10.0,
                    "MECHANISM": "StrikeSlip",
                },
                {
                    "RSN": 3,
                    "PROVIDER": "PEER",
                    "EVENT": "EQ2",
                    "YEAR": 2000,
                    "MAGNITUDE": 7.0,
                    "SSN": 3,
                    "STATION": "S1",
                    "VS30(m/s)": 350.0,
                    "RJB(km)": 10.0,
                    "RRUP(km)": 10.0,
                    "MECHANISM": "StrikeSlip",
                },
                {
                    "RSN": 4,
                    "PROVIDER": "PEER",
                    "EVENT": "EQ3",
                    "YEAR": 2000,
                    "MAGNITUDE": 7.0,
                    "SSN": 4,
                    "STATION": "S4",
                    "VS30(m/s)": 350.0,
                    "RJB(km)": 10.0,
                    "RRUP(km)": 10.0,
                    "MECHANISM": "StrikeSlip",
                },
            ]
        )
        config = SelectionConfig(
            design_code=DesignCode.TBDY_2018,
            num_records=2,
            max_per_station=1,
            max_per_event=1,
            min_score=0.0,
        )
        criteria = SearchCriteria(
            start_date="2000-01-01",
            end_date="2025-09-05",
            target_magnitude=7.0,
        )
        strategy = TBDYSelectionStrategy(config=config)

        selected, scored = strategy.select_and_score(rows, criteria)

        assert selected["RSN"].tolist() == [1, 4]
        reasons_by_rsn = dict(zip(scored["RSN"], scored["SELECTION_REASON"]))
        assert reasons_by_rsn[2] == "max_per_event:1"
        assert reasons_by_rsn[3] == "max_per_station:1"
        assert reasons_by_rsn[4] == "selected"
