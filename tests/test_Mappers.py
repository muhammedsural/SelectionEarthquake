import pytest
import pandas as pd
import numpy as np
from selection_service.core.Config import MECHANISM_MAP
from selection_service.processing.Mappers import (
    BaseColumnMapper,
    AFADColumnMapper,
    PEERColumnMapper,
    ColumnMapperFactory
)
from selection_service.enums.Enums import ProviderName
from unittest.mock import patch

# --- BaseColumnMapper Testi (Abstrakt olduğu için bir dummy sınıf ile test edilir) ---

class DummyMapper(BaseColumnMapper):
    def __init__(self):
        super().__init__({"OLD_COL": "MAGNITUDE"})

def test_base_column_mapper_standardization():
    mapper = DummyMapper()
    df = pd.DataFrame({"OLD_COL": [5.5], "EXTRA": [1]})
    
    result = mapper.map_columns(df)
    
    # Standart kolonun oluştuğunu kontrol et
    assert "MAGNITUDE" in result.columns
    # Standartta olmayan kolonların (EXTRA gibi) temizlendiğini kontrol et
    assert "EXTRA" not in result.columns
    # Eksik olan standart kolonların NaN olarak eklendiğini kontrol et
    assert "STATION" in result.columns 
    assert pd.isna(result.loc[0, "STATION"])

# --- AFADColumnMapper Testleri ---

class TestAFADColumnMapper:
    def test_map_columns_basic(self):
        mapper = AFADColumnMapper()
        # AFAD formatında bir veri
        df = pd.DataFrame({
            "mvalue": [6.0],
            "eventId": [123],
            "rjb": [10.5]
        })
        
        result = mapper.map_columns(df)
        assert result.iloc[0]["MAGNITUDE"] == 6.0
        assert result.iloc[0]["RJB(km)"] == 10.5

    # def test_haversine_distance(self):
    #     mapper = AFADColumnMapper()
    #     # Beyaz Saray (38.898° N, 77.037° E) ile Eyfel Kulesi (48.858° N, 2.294° E) arasındaki jeodezik mesafe mesafe yaklaşık 6177,45 km'dir
    #     distance = mapper._haversine(38.898, 77.037, 48.858, 2.294)
    #     assert distance == 6177
    #     assert np.isclose(distance, 6177, atol=10)

@pytest.fixture
def mapper():
    # Station file path is not needed for fault type classification
    return AFADColumnMapper(station_file_path="data\stations.xlsx")

#-----------------------------------------------------------------------------------------------

@pytest.mark.parametrize("dip, rake, expected", [
    (45, 0, MECHANISM_MAP[0]),           # Strike-slip (rake ~ 0)
    (45, 180, MECHANISM_MAP[0]),         # Strike-slip (rake ~ 180)
    (45, 90, MECHANISM_MAP[2]),          # Reverse (rake ~ +90, dip >= 30)
    (20, 90, MECHANISM_MAP[3]),          # Reverse/Oblique (rake ~ +90, dip < 30)
    (45, -90, MECHANISM_MAP[1]),         # Normal (rake ~ -90, dip >= 30)
    (20, -90, MECHANISM_MAP[4]),         # Normal/Oblique (rake ~ -90, dip < 30)
    (45, 45, MECHANISM_MAP[5]),          # Oblique (other angles)
    (45, -45, MECHANISM_MAP[5]),         # Oblique (other angles)
    (None, 90, "Unknown"),               # dip is None
    (45, None, "Unknown"),               # rake is None
    (float('nan'), 90, "Unknown"),       # dip is NaN
    (45, float('nan'), "Unknown"),       # rake is NaN
])
def test_classify_fault_type(mapper, dip, rake, expected):
    result = mapper._classify_fault_type(dip, rake)
    assert result == expected

#-----------------------------------------------------------------------------------------------
def test_haversine_zero_distance(mapper):
    # Same point, distance should be 0
    lat, lon = 40.0, 29.0
    assert pytest.approx(mapper._haversine(lat, lon, lat, lon), 0.0001) == 0.0

def test_haversine_known_distance(mapper):
    # Istanbul (41.0082, 28.9784) to Ankara (39.9334, 32.8597)
    dist = mapper._haversine(41.0082, 28.9784, 39.9334, 32.8597)
    # Real-world distance is about 350-450 km, allow some tolerance
    assert 349 < dist < 450

def test_haversine_equator_to_pole(mapper):
    # From equator (0,0) to north pole (90,0)
    dist = mapper._haversine(0, 0, 90, 0)
    # Should be about a quarter of Earth's circumference
    earth_radius = 6371.0
    expected = earth_radius * 3.141592653589793 / 2
    assert pytest.approx(dist, 0.1) == expected

def test_haversine_antipodal_points(mapper):
    # Opposite points on globe: (0,0) and (0,180)
    dist = mapper._haversine(0, 0, 0, 180)
    earth_radius = 6371.0
    expected = earth_radius * 3.141592653589793
    assert pytest.approx(dist, 0.1) == expected

#-----------------------------------------------------------------------------------------------

@pytest.fixture
def sample_station_df():
    # 3 stations, 2 with valid Vs30, 1 missing
    data = {
        "Code": ["STA1", "STA2", "STA3"],
        "Vs30": [760, 0, np.nan],
        "Location": ["Loc1", "Loc2", "Loc3"],
        "Latitude": [39.0, 39.1, 39.05],
        "Longitude": [32.0, 32.1, 32.05]
    }
    return pd.DataFrame(data)

def mock_read_excel(file_path):
    # Return the fixture DataFrame regardless of file_path
    return sample_station_df()

# @patch("pandas.read_excel", side_effect=mock_read_excel)
# def test_fill_missing_vs30_within_distance(mock_excel):
#     mappers = AFADColumnMapper()
#     df = mappers._build_station_info_df("dummy_path.xlsx", max_distance_km=20.0)
#     # STA2 and STA3 should get Vs30 from STA1 (closest valid)
#     assert df.loc[1, "Vs30"] == 760
#     assert df.loc[2, "Vs30"] == 760

# @patch("pandas.read_excel", side_effect=mock_read_excel)
# def test_fill_missing_vs30_outside_distance(mock_excel):
#     mapper = AFADColumnMapper()
#     # Set max_distance_km very small so no station is close enough
#     df = mapper._build_station_info_df("dummy_path.xlsx", max_distance_km=0.001)
#     # STA2 and STA3 should get Vs30 = 0.0
#     assert df.loc[1, "Vs30"] == 0.0
#     assert df.loc[2, "Vs30"] == 0.0

@patch("pandas.read_excel", side_effect=Exception("File not found"))
def test_exception_returns_empty_df(mock_excel):
    mapper = AFADColumnMapper()
    df = mapper._build_station_info_df("not_found.xlsx")
    assert isinstance(df, pd.DataFrame)
    assert df.empty

@patch("pandas.read_excel", side_effect=mock_read_excel)
def test_no_missing_vs30(mock_excel):
    # All Vs30 values are valid
    valid_data = {
        "Code": ["STA1", "STA2"],
        "Vs30": [500, 600],
        "Location": ["Loc1", "Loc2"],
        "Latitude": [39.0, 39.1],
        "Longitude": [32.0, 32.1]
    }
    with patch("selection_service.processing.Mappers.pd.read_excel", return_value=pd.DataFrame(valid_data)):
        mapper = AFADColumnMapper()
        df = mapper._build_station_info_df("dummy_path.xlsx")
        assert (df["Vs30"] == pd.Series([500, 600])).all()

# --- PEERColumnMapper Testleri ---

class TestPEERColumnMapper:
    def test_map_columns_peer(self):
        mapper = PEERColumnMapper()
        # PEER formatında bir veri (Flatfile kolonları)
        df = pd.DataFrame({
            "Earthquake Magnitude": [7.2],
            "Station Name": ["PEER_ST"],
            "PGA(g)": [0.5]
        })
        
        result = mapper.map_columns(df)
        
        assert result.loc[0, "MAGNITUDE"] == 7.2
        assert result.loc[0, "STATION"] == "PEER_ST"
        assert result.loc[0, "PGA(cm2/sec)"] == 0.5* 980.665

# --- ColumnMapperFactory Testleri ---

class TestColumnMapperFactory:
    def test_create_mapper_afad(self):
        mapper = ColumnMapperFactory.create_mapper(ProviderName.AFAD)
        assert isinstance(mapper, AFADColumnMapper)

    def test_create_mapper_peer(self):
        mapper = ColumnMapperFactory.create_mapper(ProviderName.PEER)
        assert isinstance(mapper, PEERColumnMapper)

    # def test_get_mapper_fallback(self):
    #     # Kayıtlı olmayan bir provider verilirse BaseColumnMapper dönmeli
    #     mapper = ColumnMapperFactory.get_mapper("UNKNOWN")
    #     # BaseColumnMapper abstract olduğu için instance'ı direkt kontrol etmek yerine tipine bakıyoruz
    #     assert issubclass(type(mapper), BaseColumnMapper)

    def test_register_mapper(self):
        class NewProviderMapper(BaseColumnMapper):
            def __init__(self): super().__init__({})
            
        ColumnMapperFactory.register_mapper("NEW", NewProviderMapper)
        mapper = ColumnMapperFactory.get_mapper("NEW")
        assert isinstance(mapper, NewProviderMapper)