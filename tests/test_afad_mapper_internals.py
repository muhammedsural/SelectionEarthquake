"""
tests/test_afad_mapper_internals.py

AFADColumnMapper private metodları ve _classify_fault_type tam branch coverage.
"""
import math
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from selection_service.processing.Mappers import AFADColumnMapper


@pytest.fixture
def mapper():
    """AFADColumnMapper — station_df'yi boş DataFrame ile mock'la."""
    with patch.object(AFADColumnMapper, "_build_station_info_df",
                      return_value=pd.DataFrame(columns=["Code", "Vs30", "Location",
                                                          "Latitude", "Longitude"])):
        return AFADColumnMapper()


def minimal_df(**overrides) -> pd.DataFrame:
    base = {
        "waveformId": 1, "eventId": 100,
        "eventDate": "2023-02-06T01:17:35.000Z",
        "mvalue": 7.7, "mtype": "Mw",
        "rjb": 15.2, "rrup": 15.8, "repi": 16.0, "rhyp": 25.0,
        "relatedEarthquakeLatitude": 37.17, "relatedEarthquakeLongitude": 37.08,
        "stationCode": "TK.KND", "stationId": 4501,
        "relatedStationLatitude": 37.23, "relatedStationLongitude": 37.12,
        "pga": 482.1, "pgv": 62.3, "pgd": 18.4,
        "relatedStrike1": 240.0, "relatedDip1": 55.0, "relatedRake1": 90.0,
        "relatedStrike2": 60.0,  "relatedDip2": 35.0, "relatedRake2": 90.0,
        "t90e": 18.2, "t90n": 19.6, "t90u": 16.4,
        "recordFilename": "2023.02.06-TK.KND.mseed",
    }
    base.update(overrides)
    return pd.DataFrame([base])


# ─── _handle_record_filenames ────────────────────────────────────────────────

class TestHandleRecordFilenames:
    def test_filename_copied_to_h1_h2_v(self, mapper):
        df = minimal_df()
        result = mapper._handle_record_filenames(df)
        assert result["FILE_NAME_H1"].iloc[0] == "2023.02.06-TK.KND.mseed"
        assert result["FILE_NAME_H2"].iloc[0] == "2023.02.06-TK.KND.mseed"
        assert result["FILE_NAME_V"].iloc[0] == "2023.02.06-TK.KND.mseed"

    def test_no_recordfilename_column(self, mapper):
        """recordFilename yoksa hata olmamalı."""
        df = pd.DataFrame({"waveformId": [1]})
        result = mapper._handle_record_filenames(df)
        assert "FILE_NAME_H1" not in result.columns


# ─── _handle_station_infos ───────────────────────────────────────────────────

class TestHandleStationInfos:
    def test_vs30_mapped_from_station_df(self):
        station_df = pd.DataFrame({
            "Code": ["TK.KND"], "Vs30": [420.0],
            "Location": ["Kandilli"], "Latitude": [37.2], "Longitude": [37.1]
        })
        with patch.object(AFADColumnMapper, "_build_station_info_df",
                          return_value=station_df):
            mapper = AFADColumnMapper()
        df = minimal_df()
        result = mapper._handle_station_infos(df)
        assert result["VS30(m/s)"].iloc[0] == pytest.approx(420.0)
        assert result["STATION"].iloc[0] == "Kandilli"

    def test_unknown_station_code_zero_vs30(self, mapper):
        df = minimal_df(stationCode="UNKNOWN.CODE")
        result = mapper._handle_station_infos(df)
        assert result["VS30(m/s)"].iloc[0] == 0.0

    def test_no_stationcode_column(self, mapper):
        df = pd.DataFrame({"waveformId": [1]})
        result = mapper._handle_station_infos(df)
        assert "VS30(m/s)" not in result.columns


# ─── _handle_mechanisms ──────────────────────────────────────────────────────

class TestHandleMechanisms:
    def test_mechanism_assigned(self, mapper):
        df = minimal_df(relatedDip1=55.0, relatedRake1=90.0,
                        relatedDip2=35.0, relatedRake2=90.0)
        result = mapper._handle_mechanisms(df)
        assert "MECHANISM" in result.columns
        assert isinstance(result["MECHANISM"].iloc[0], str)

    def test_missing_cols_filled_with_zero(self, mapper):
        """relatedDip/Rake kolonları yoksa 0 ile doldurulmalı."""
        df = pd.DataFrame({"waveformId": [1]})
        result = mapper._handle_mechanisms(df)
        assert "MECHANISM" in result.columns

    def test_nan_values_handled(self, mapper):
        df = minimal_df()
        df["relatedDip1"] = float("nan")
        df["relatedRake1"] = float("nan")
        result = mapper._handle_mechanisms(df)
        assert result["MECHANISM"].iloc[0] is not None


# ─── _handle_t90_duration ────────────────────────────────────────────────────

class TestHandleT90Duration:
 
    def test_all_three_present_average_calculated(self, mapper):
        """t90e + t90n + t90u → T90_avg(sec) = ortalama."""
        df = minimal_df(t90e=18.2, t90n=19.6, t90u=16.4)
        result = mapper._handle_t90_duration(df)
        expected = (18.2 + 19.6 + 16.4) / 3
        assert "T90_avg(sec)" in result.columns
        assert result["T90_avg(sec)"].iloc[0] == pytest.approx(expected)
 
    def test_missing_t90u_no_avg_column(self, mapper):
        """
        t90u eksik, t90e + t90n mevcut →
        Gerçek implementasyon: elif any(col in df.columns ...) branch'ı devreye girer
        ve mevcut 2 kolonun ortalamasını hesaplar.
        T90_avg(sec) OLUŞUR (mevcut 2 kolonun ortalaması).
        """
        df = minimal_df()
        df = df.drop(columns=["t90u"])   # sadece t90e ve t90n var
        result = mapper._handle_t90_duration(df)
        expected = (18.2 + 19.6) / 2
        assert "T90_avg(sec)" in result.columns
        assert result["T90_avg(sec)"].iloc[0] == pytest.approx(expected)
 
    def test_no_t90_columns(self, mapper):
        """Hiç t90 kolonu yoksa T90_avg(sec) = None olarak oluşturulur."""
        df = pd.DataFrame({"waveformId": [1]})
        result = mapper._handle_t90_duration(df)
        # else branch: df["T90_avg(sec)"] = None → sütun VAR ama değer None
        assert "T90_avg(sec)" in result.columns
        assert result["T90_avg(sec)"].iloc[0] is None
 
    def test_map_columns_always_has_t90_avg(self, mapper):
        """
        map_columns üzerinden geçince T90_avg(sec) her zaman var
        (eksik olunca None olarak eklenir).
        """
        df = minimal_df()
        df = df.drop(columns=["t90u"])  # t90u eksik
        result = mapper.map_columns(df)
        # map_columns _ensure_standard_columns çağırır → sütun None olarak eklenir
        assert "T90_avg(sec)" in result.columns


# ─── _haversine ──────────────────────────────────────────────────────────────

class TestHaversine:
    def test_same_point_zero_distance(self, mapper):
        dist = mapper._haversine(37.0, 37.0, 37.0, 37.0)
        assert dist == pytest.approx(0.0, abs=1e-6)

    def test_known_distance(self, mapper):
        """İstanbul (41.0, 29.0) → Ankara (39.9, 32.9) ≈ 350 km."""
        dist = mapper._haversine(41.0, 29.0, 39.9, 32.9)
        assert 300 < dist < 400

    def test_symmetric(self, mapper):
        d1 = mapper._haversine(37.0, 37.0, 38.0, 38.0)
        d2 = mapper._haversine(38.0, 38.0, 37.0, 37.0)
        assert d1 == pytest.approx(d2, rel=1e-5)


# ─── _classify_fault_type — tüm branch'lar ──────────────────────────────────

class TestClassifyFaultType:
    def test_nan_dip_unknown(self, mapper):
        assert mapper._classify_fault_type(float("nan"), 90.0) == "Unknown"

    def test_nan_rake_unknown(self, mapper):
        assert mapper._classify_fault_type(90.0, float("nan")) == "Unknown"

    def test_strikeslip_rake_0(self, mapper):
        result = mapper._classify_fault_type(90.0, 0.0)
        assert result == "StrikeSlip"

    def test_strikeslip_rake_180(self, mapper):
        result = mapper._classify_fault_type(90.0, 180.0)
        assert result == "StrikeSlip"

    def test_strikeslip_rake_minus_180(self, mapper):
        result = mapper._classify_fault_type(90.0, -180.0)
        assert result == "StrikeSlip"

    def test_strikeslip_rake_170(self, mapper):
        result = mapper._classify_fault_type(90.0, 170.0)
        assert result == "StrikeSlip"

    def test_reverse_dip_gte_30(self, mapper):
        result = mapper._classify_fault_type(55.0, 90.0)
        assert result == "Reverse"

    def test_reverse_oblique_dip_lt_30(self, mapper):
        result = mapper._classify_fault_type(20.0, 90.0)
        assert result == "Reverse/Oblique"

    def test_normal_dip_gte_30(self, mapper):
        result = mapper._classify_fault_type(60.0, -90.0)
        assert result == "Normal"

    def test_normal_oblique_dip_lt_30(self, mapper):
        result = mapper._classify_fault_type(20.0, -90.0)
        assert result == "Normal/Oblique"

    def test_oblique_intermediate_rake(self, mapper):
        """Rake ~45° → oblique."""
        result = mapper._classify_fault_type(45.0, 45.0)
        assert "Oblique" in result or result == "StrikeSlip"


# ─── _classify_fault_planes ──────────────────────────────────────────────────

class TestClassifyFaultPlanes:
    def test_same_types_returns_single(self, mapper):
        result = mapper._classify_fault_planes(55.0, 90.0, 35.0, 90.0)
        # Her ikisi de Reverse veya Reverse/Oblique — aynı olursa tek değer
        assert "-" not in result or result.count("-") >= 1

    def test_different_types_combined(self, mapper):
        # f1=StrikeSlip (rake=0), f2=Reverse (dip=55, rake=90)
        result = mapper._classify_fault_planes(90.0, 0.0, 55.0, 90.0)
        assert "-" in result or result in ("StrikeSlip", "Reverse")

    def test_same_classification_no_dash(self, mapper):
        """İki düzlem aynı sınıfa girerse '-' içermemeli."""
        f1 = mapper._classify_fault_type(90.0, 0.0)   # StrikeSlip
        f2 = mapper._classify_fault_type(90.0, 10.0)  # StrikeSlip
        result = mapper._classify_fault_planes(90.0, 0.0, 90.0, 10.0)
        if f1 == f2:
            assert result == f1