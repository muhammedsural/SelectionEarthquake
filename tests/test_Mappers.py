"""
tests/test_mappers.py

Kolon eşleme katmanı testleri.

Kapsam:
  - PEERColumnMapper : CSV → STANDARD_COLUMNS dönüşümü
  - AFADColumnMapper : T90 ortalama, ENDPOINTSOURCE placeholder, mekanizma
  - ColumnMapperFactory : registry ve OCP
  - BaseColumnMapper._ensure_standard_columns : eksik kolon tamamlama
"""

import math
import pytest
import pandas as pd
from unittest.mock import patch

# ── Proje import'ları ─────────────────────────────────────────────────────────
from selection_service.core.Config import STANDARD_COLUMNS
from selection_service.processing.Mappers import (
    AFADColumnMapper,
    PEERColumnMapper,
    ColumnMapperFactory,
    BaseColumnMapper,
)
from selection_service.enums.Enums import ProviderName


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures — tekrar kullanılacak test verileri
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def peer_csv_row() -> pd.DataFrame:
    """NGA-West2_flatfile.csv formatında tek satır (basic_usage çıktısından alındı)."""
    return pd.DataFrame([{
        "RSN": 848,
        "EVENT": "Landers",
        "YEAR": 1992,
        "MAGNITUDE": 7.28,
        "MAGNITUDE_TYPE": "Mw",
        "STATION": "Coolwater",
        "SSN": 24,
        "STATION_ID": "CWC",
        "STATION_LAT": 34.60,
        "STATION_LON": -116.67,
        "VS30(m/s)": 352.98,
        "STRIKE1": 340.0,
        "DIP1": 90.0,
        "RAKE1": 180.0,
        "MECHANISM": "StrikeSlip",
        "EPICENTER_DEPTH(km)": 0.0,
        "HYPOCENTER_DEPTH(km)": 0.0,
        "RJB(km)": 19.74,
        "RRUP(km)": 19.74,
        "HYPO_LAT": 34.20,
        "HYPO_LON": -116.44,
        "HYPO_DEPTH(km)": 0.0,
        "LOWFREQ(Hz)": 0.20,
        "FILE_NAME_H1": "LANDERS\\CLW-LN.AT2",
        "FILE_NAME_H2": "LANDERS\\CLW-TR.AT2",
        "FILE_NAME_V":  "LANDERS\\CLW-UP.AT2",
        "PGA(g)": 0.351,
        "PGV(cm/sec)": 33.224,
        "PGD(cm)": 7.1,
        "5-75%Duration(sec)": 8.5,
        "5-95%Duration(sec)": 10.6,
        "AriasIntensity(m/sec)": 1.8,
    }])


@pytest.fixture
def afad_api_row() -> pd.DataFrame:
    """AFAD TADAS API ham yanıt formatında tek satır."""
    return pd.DataFrame([{
        "waveformId":                  9001,
        "eventId":                     2023001,
        "eventDate":                   "2023-02-06T01:17:35.000Z",
        "mvalue":                      7.7,
        "mtype":                       "Mw",
        "rjb":                         15.2,
        "rrup":                        15.8,
        "repi":                        16.0,
        "rhyp":                        25.0,
        "relatedEarthquakeLatitude":   37.17,
        "relatedEarthquakeLongitude":  37.08,
        "stationCode":                 "TK.KND",
        "stationId":                   4501,
        "relatedStationLatitude":      37.23,
        "relatedStationLongitude":     37.12,
        "pga":                         482.1,
        "pgv":                         62.3,
        "pgd":                         18.4,
        "relatedStrike1":              240.0,
        "relatedDip1":                 55.0,
        "relatedRake1":                90.0,
        "relatedStrike2":              60.0,
        "relatedDip2":                 35.0,
        "relatedRake2":                90.0,
        "t90e":                        18.2,
        "t90n":                        19.6,
        "t90u":                        16.4,
        "recordFilename":              "2023.02.06-01.17.35-TK.KND.mseed",
    }])


# ─────────────────────────────────────────────────────────────────────────────
# PEERColumnMapper testleri
# ─────────────────────────────────────────────────────────────────────────────

class TestPEERColumnMapper:

    def test_output_is_exactly_standard_columns(self, peer_csv_row):
        """Çıktı tam olarak STANDARD_COLUMNS listesini içermeli."""
        mapper = PEERColumnMapper()
        result = mapper.map_columns(peer_csv_row)
        assert list(result.columns) == STANDARD_COLUMNS

    def test_pga_unit_conversion(self, peer_csv_row):
        """PGA: g → cm/s² (× 980.665)."""
        mapper = PEERColumnMapper()
        result = mapper.map_columns(peer_csv_row)
        expected = 0.351 * 980.665
        assert math.isclose(result["PGA(cm2/sec)"].iloc[0], expected, rel_tol=1e-5)

    def test_pga_g_column_removed(self, peer_csv_row):
        """PGA(g) sütunu çıktıda bulunmamalı."""
        mapper = PEERColumnMapper()
        result = mapper.map_columns(peer_csv_row)
        assert "PGA(g)" not in result.columns

    def test_t90_mapped_to_avg(self, peer_csv_row):
        """5-95%Duration(sec) → T90_avg(sec) olarak dönüşmeli."""
        mapper = PEERColumnMapper()
        result = mapper.map_columns(peer_csv_row)
        assert result["T90_avg(sec)"].iloc[0] == pytest.approx(10.6)

    def test_t90_original_column_removed(self, peer_csv_row):
        """5-95%Duration(sec) sütunu çıktıda bulunmamalı."""
        mapper = PEERColumnMapper()
        result = mapper.map_columns(peer_csv_row)
        assert "5-95%Duration(sec)" not in result.columns

    def test_arias_renamed(self, peer_csv_row):
        """AriasIntensity(m/sec) → ARIAS_INTENSITY(m/sec)."""
        mapper = PEERColumnMapper()
        result = mapper.map_columns(peer_csv_row)
        assert result["ARIAS_INTENSITY(m/sec)"].iloc[0] == pytest.approx(1.8)

    def test_endpointsource_is_none(self, peer_csv_row):
        """PEER için ENDPOINTSOURCE None olmalı (download linki yok)."""
        mapper = PEERColumnMapper()
        result = mapper.map_columns(peer_csv_row)
        assert result["ENDPOINTSOURCE"].iloc[0] is None

    def test_critical_columns_preserved(self, peer_csv_row):
        """RSN, MAGNITUDE, FILE_NAME_H1 gibi kritik kolonlar kaybolmamalı."""
        mapper = PEERColumnMapper()
        result = mapper.map_columns(peer_csv_row)
        assert result["RSN"].iloc[0] == 848
        assert result["MAGNITUDE"].iloc[0] == pytest.approx(7.28)
        assert result["FILE_NAME_H1"].iloc[0] == "LANDERS\\CLW-LN.AT2"
        assert result["VS30(m/s)"].iloc[0] == pytest.approx(352.98)
        assert result["MECHANISM"].iloc[0] == "StrikeSlip"

    def test_non_standard_columns_excluded(self, peer_csv_row):
        """STANDARD_COLUMNS dışındaki sütunlar (5-75%Duration vb.) çıktıda olmamalı."""
        mapper = PEERColumnMapper()
        result = mapper.map_columns(peer_csv_row)
        assert "5-75%Duration(sec)" not in result.columns

    def test_multiple_rows(self):
        """Birden fazla satırda dönüşüm tutarlı olmalı (basic_usage çıktısından 3 kayıt)."""
        rows = pd.DataFrame([
            {"RSN": 848,  "EVENT": "Landers",         "YEAR": 1992, "MAGNITUDE": 7.28,
             "SSN": 24,   "STATION": "Coolwater",     "VS30(m/s)": 352.98,
             "RJB(km)": 19.74, "RRUP(km)": 19.74,    "MECHANISM": "StrikeSlip",
             "PGA(g)": 0.351, "PGV(cm/sec)": 33.224, "PGD(cm)": 7.1,
             "5-95%Duration(sec)": 10.6, "AriasIntensity(m/sec)": 1.8,
             "MAGNITUDE_TYPE": "Mw", "STATION_ID": "", "STATION_LAT": 0.0,
             "STATION_LON": 0.0, "STRIKE1": 0.0, "DIP1": 0.0, "RAKE1": 0.0,
             "EPICENTER_DEPTH(km)": 0.0, "HYPOCENTER_DEPTH(km)": 0.0,
             "HYPO_LAT": 0.0, "HYPO_LON": 0.0, "HYPO_DEPTH(km)": 0.0,
             "LOWFREQ(Hz)": 0.2, "FILE_NAME_H1": "A.AT2", "FILE_NAME_H2": "B.AT2",
             "FILE_NAME_V": "C.AT2"},
            {"RSN": 870,  "EVENT": "Landers",         "YEAR": 1992, "MAGNITUDE": 7.28,
             "SSN": 337,  "STATION": "LA-Obregon",    "VS30(m/s)": 349.43,
             "RJB(km)": 151.70, "RRUP(km)": 151.70,  "MECHANISM": "StrikeSlip",
             "PGA(g)": 0.049, "PGV(cm/sec)": 11.66,  "PGD(cm)": 3.2,
             "5-95%Duration(sec)": 46.0, "AriasIntensity(m/sec)": 0.3,
             "MAGNITUDE_TYPE": "Mw", "STATION_ID": "", "STATION_LAT": 0.0,
             "STATION_LON": 0.0, "STRIKE1": 0.0, "DIP1": 0.0, "RAKE1": 0.0,
             "EPICENTER_DEPTH(km)": 0.0, "HYPOCENTER_DEPTH(km)": 0.0,
             "HYPO_LAT": 0.0, "HYPO_LON": 0.0, "HYPO_DEPTH(km)": 0.0,
             "LOWFREQ(Hz)": 0.1, "FILE_NAME_H1": "D.AT2", "FILE_NAME_H2": "E.AT2",
             "FILE_NAME_V": "F.AT2"},
        ])
        mapper = PEERColumnMapper()
        result = mapper.map_columns(rows)
        assert len(result) == 2
        assert list(result.columns) == STANDARD_COLUMNS
        assert result["T90_avg(sec)"].tolist() == [10.6, 46.0]
        assert result["ENDPOINTSOURCE"].isna().all()


# ─────────────────────────────────────────────────────────────────────────────
# AFADColumnMapper testleri
# ─────────────────────────────────────────────────────────────────────────────

class TestAFADColumnMapper:

    def test_t90_average_all_components(self, afad_api_row):
        """Üç t90 bileşeninin ortalaması T90_avg(sec) olarak hesaplanmalı."""
        mapper = AFADColumnMapper()
        result = mapper.map_columns(afad_api_row)
        expected_avg = (18.2 + 19.6 + 16.4) / 3
        assert "T90_avg(sec)" in result.columns
        assert result["T90_avg(sec)"].iloc[0] == pytest.approx(expected_avg, rel=1e-3)

    def test_t90_partial_components(self):
        """Sadece iki t90 bileşeni varsa mevcut olanların ortalaması alınmalı."""
        df = pd.DataFrame([{
            "waveformId": 1, "eventId": 100, "mvalue": 6.0, "mtype": "Mw",
            "stationCode": "TK.ABC", "stationId": 1,
            "rjb": 10.0, "rrup": 11.0, "repi": 12.0, "rhyp": 15.0,
            "pga": 100.0, "pgv": 10.0, "pgd": 2.0,
            "relatedEarthquakeLatitude": 37.0, "relatedEarthquakeLongitude": 37.0,
            "relatedStationLatitude": 37.1, "relatedStationLongitude": 37.1,
            "relatedStrike1": 0.0, "relatedDip1": 90.0, "relatedRake1": 180.0,
            "relatedStrike2": 0.0, "relatedDip2": 90.0, "relatedRake2": 180.0,
            "t90e": 15.0, "t90n": 17.0,  # t90u YOK
            "recordFilename": "test.mseed",
            "eventDate": "2020-01-01T00:00:00.000Z",
        }])
        mapper = AFADColumnMapper()
        result = mapper.map_columns(df)
        assert result["T90_avg(sec)"].iloc[0] == pytest.approx(16.0)

    def test_t90_no_components_gives_none(self):
        """Hiç t90 bileşeni yoksa T90_avg(sec) = None."""
        df = pd.DataFrame([{
            "waveformId": 1, "eventId": 100, "mvalue": 6.0, "mtype": "Mw",
            "stationCode": "TK.ABC", "stationId": 1,
            "rjb": 10.0, "rrup": 11.0, "repi": 12.0, "rhyp": 15.0,
            "pga": 100.0, "pgv": 10.0, "pgd": 2.0,
            "relatedEarthquakeLatitude": 37.0, "relatedEarthquakeLongitude": 37.0,
            "relatedStationLatitude": 37.1, "relatedStationLongitude": 37.1,
            "relatedStrike1": 0.0, "relatedDip1": 90.0, "relatedRake1": 180.0,
            "relatedStrike2": 0.0, "relatedDip2": 90.0, "relatedRake2": 180.0,
            "recordFilename": "test.mseed",
            "eventDate": "2020-01-01T00:00:00.000Z",
        }])
        mapper = AFADColumnMapper()
        result = mapper.map_columns(df)
        assert result["T90_avg(sec)"].iloc[0] is None

    def test_year_extracted_from_eventdate(self, afad_api_row):
        """eventDate'ten YEAR sütunu doğru çıkarılmalı."""
        mapper = AFADColumnMapper()
        result = mapper.map_columns(afad_api_row)
        assert result["YEAR"].iloc[0] == 2023

    def test_reverse_mechanism_classified(self, afad_api_row):
        """Dip=55, Rake=90 → Reverse mekanizma."""
        mapper = AFADColumnMapper()
        result = mapper.map_columns(afad_api_row)
        assert result["MECHANISM"].iloc[0] == "Reverse"

    def test_output_is_exactly_standard_columns(self, afad_api_row):
        """Çıktı tam olarak STANDARD_COLUMNS listesini içermeli."""
        mapper = AFADColumnMapper()
        result = mapper.map_columns(afad_api_row)
        assert list(result.columns) == STANDARD_COLUMNS

    def test_file_name_h1_from_recordfilename(self, afad_api_row):
        """recordFilename → FILE_NAME_H1, H2, V olarak atanmalı."""
        mapper = AFADColumnMapper()
        result = mapper.map_columns(afad_api_row)
        assert result["FILE_NAME_H1"].iloc[0] == "2023.02.06-01.17.35-TK.KND.mseed"


# ─────────────────────────────────────────────────────────────────────────────
# ColumnMapperFactory testleri
# ─────────────────────────────────────────────────────────────────────────────
 
class TestColumnMapperFactory:
 
    def test_create_afad_mapper(self):
        """AFAD provider için AFADColumnMapper döndürmeli."""
        with patch.object(AFADColumnMapper, "_build_station_info_df",
                          return_value=pd.DataFrame(
                              columns=["Code", "Vs30", "Location", "Latitude", "Longitude"]
                          )):
            mapper = ColumnMapperFactory.create_mapper(ProviderName.AFAD)
        assert isinstance(mapper, AFADColumnMapper)
 
    def test_create_peer_mapper(self):
        """PEER provider için PEERColumnMapper döndürmeli."""
        mapper = ColumnMapperFactory.create_mapper(ProviderName.PEER)
        assert isinstance(mapper, PEERColumnMapper)
 
    def test_unknown_provider_returns_base(self):
        """Bilinmeyen provider → BaseColumnMapper dönmeli."""
        class FakeProvider:
            value = "UNKNOWN"
        result = ColumnMapperFactory.create_mapper(FakeProvider())
        assert isinstance(result, BaseColumnMapper)
 
    def test_ocp_register_new_provider(self):
        """OCP: register_mapper ile factory'ye dokunmadan yeni mapper eklenmeli."""
        class CustomMapper(PEERColumnMapper):
            pass
 
        class NewProvider:
            value = "CUSTOM_OCP_TEST"
 
        ColumnMapperFactory.register_mapper(NewProvider(), CustomMapper)
        mapper = ColumnMapperFactory.get_mapper(NewProvider())
        assert isinstance(mapper, CustomMapper)
 
    # Eğer Mappers.py'e create/register alias eklendiyse bu testler de geçmeli:
 
    def test_create_alias_afad(self):
        """create() alias'ı — Mappers.py'e eklenmiş olmalı."""
        if not hasattr(ColumnMapperFactory, "create"):
            pytest.skip("create() alias henüz eklenmedi")
        with patch.object(AFADColumnMapper, "_build_station_info_df",
                          return_value=pd.DataFrame(
                              columns=["Code", "Vs30", "Location", "Latitude", "Longitude"]
                          )):
            mapper = ColumnMapperFactory.create(ProviderName.AFAD)
        assert isinstance(mapper, AFADColumnMapper)
 
    def test_create_alias_peer(self):
        if not hasattr(ColumnMapperFactory, "create"):
            pytest.skip("create() alias henüz eklenmedi")
        mapper = ColumnMapperFactory.create(ProviderName.PEER)
        assert isinstance(mapper, PEERColumnMapper)
 
    def test_register_alias(self):
        if not hasattr(ColumnMapperFactory, "register"):
            pytest.skip("register() alias henüz eklenmedi")
 
        class AnotherMapper(PEERColumnMapper):
            pass
 
        class AnotherProvider:
            value = "ANOTHER_TEST"
 
        ColumnMapperFactory.register(AnotherProvider(), AnotherMapper)
        mapper = ColumnMapperFactory.get_mapper(AnotherProvider())
        assert isinstance(mapper, AnotherMapper)
 
    def test_create_mapper_backward_compat(self):
        """create_mapper() geriye dönük uyumluluk."""
        mapper = ColumnMapperFactory.create_mapper(ProviderName.PEER)
        assert isinstance(mapper, PEERColumnMapper)
 
    def test_get_mapper_backward_compat(self):
        """get_mapper() geriye dönük uyumluluk."""
        with patch.object(AFADColumnMapper, "_build_station_info_df",
                          return_value=pd.DataFrame(
                              columns=["Code", "Vs30", "Location", "Latitude", "Longitude"]
                          )):
            mapper = ColumnMapperFactory.get_mapper(ProviderName.AFAD)
        assert isinstance(mapper, AFADColumnMapper)
 
    def test_register_mapper_backward_compat(self):
        """register_mapper() geriye dönük uyumluluk."""
        class CompatMapper(PEERColumnMapper):
            pass
 
        class CompatProvider:
            value = "COMPAT_TEST"
 
        ColumnMapperFactory.register_mapper(CompatProvider(), CompatMapper)
        mapper = ColumnMapperFactory.get_mapper(CompatProvider())
        assert isinstance(mapper, CompatMapper)

# ─────────────────────────────────────────────────────────────────────────────
# Bütünleşik test: PEER + AFAD aynı STANDARD_COLUMNS çıktısını üretmeli
# ─────────────────────────────────────────────────────────────────────────────

class TestMapperInteroperability:

    def test_peer_afad_outputs_have_same_columns(self, peer_csv_row, afad_api_row):
        """İki provider'ın çıktıları aynı kolon setine sahip olmalı."""
        peer_result = PEERColumnMapper().map_columns(peer_csv_row)
        afad_result = AFADColumnMapper().map_columns(afad_api_row)
        assert list(peer_result.columns) == list(afad_result.columns) == STANDARD_COLUMNS

    def test_concat_no_column_loss(self, peer_csv_row, afad_api_row):
        """İki çıktı birleştirildiğinde ENDPOINTSOURCE dahil hiçbir sütun kaybolmamalı."""
        peer_df = PEERColumnMapper().map_columns(peer_csv_row)
        afad_df = AFADColumnMapper().map_columns(afad_api_row)

        combined = pd.concat([peer_df, afad_df], ignore_index=True)
        assert "ENDPOINTSOURCE" in combined.columns
        assert "T90_avg(sec)" in combined.columns

    def test_endpointsource_peer_none_afad_filled(self, peer_csv_row, afad_api_row):
        """Birleşik DataFrame'de PEER satırı None, AFAD satırı None (URL provider'da eklenir)."""
        peer_df = PEERColumnMapper().map_columns(peer_csv_row)
        peer_df["PROVIDER"] = "PEER"
        afad_df = AFADColumnMapper().map_columns(afad_api_row)
        afad_df["PROVIDER"] = "AFAD"

        combined = pd.concat([peer_df, afad_df], ignore_index=True)
        peer_rows = combined[combined["PROVIDER"] == "PEER"]
        afad_rows = combined[combined["PROVIDER"] == "AFAD"]

        assert peer_rows["ENDPOINTSOURCE"].isna().all()
        # AFAD provider URL'yi _process_response_data'da ekler, mapper None döner
        assert afad_rows["ENDPOINTSOURCE"].isna().all()