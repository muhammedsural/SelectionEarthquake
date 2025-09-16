import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch
from selection_service.processing.Mappers import AFADColumnMapper
from selection_service.processing.Mappers import AFADColumnMapper
from selection_service.core.Config import MECHANISM_MAP

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