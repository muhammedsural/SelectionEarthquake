import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from selection_service.processing.ResultHandle import Result
from selection_service.providers.PeerProvider import PeerWest2Provider
from selection_service.core.ErrorHandle import ProviderError
from selection_service.processing.Selection import SearchCriteria


@pytest.fixture
def dummy_df():
    return pd.DataFrame({
        "MAGNITUDE": [5.5, 7.2, 6.0],
        "RJB(km)": [10, 30, 50],
        "RRUP(km)": [12, 40, 60],
        "VS30(m/s)": [200, 400, 800],
        "HYPO_DEPTH(km)": [5, 15, 25],
        "PGA(cm2/sec)": [100, 200, 300],
        "PGV(cm/sec)": [10, 20, 30],
        "PGD(cm)": [1, 2, 3],
        "MECHANISM": [1, 2, 3],
    })


@pytest.fixture
def dummy_mapper():
    mapper = MagicMock()
    mapper.map_columns.side_effect = lambda df: df.copy()
    return mapper


@pytest.fixture
def provider(tmp_path, dummy_df, dummy_mapper):
    # file_path = tmp_path / "nga.csv"
    # dummy_df.to_csv(file_path, index=False)
    # return PeerWest2Provider(column_mapper=dummy_mapper, file_path=str(file_path))
     with patch("selection_service.providers.PeerProvider.pd.read_csv", return_value=dummy_df.copy()):
        prov = PeerWest2Provider(column_mapper=dummy_mapper, file_path="fake.csv")
        yield prov


@pytest.fixture
def empty_criteria():
    """Tüm filtreler None olan boş kriter"""
    # Burada tarihleri None bırakmak yerine dummy değer veriyoruz
    return SearchCriteria(
        start_date="2000-01-01",
        end_date="2025-01-01"
    )


@pytest.fixture
def sample_criteria():
    """Örnek dolu kriter"""
    return SearchCriteria(
        start_date="2020-01-01", end_date="2025-01-01",
        min_magnitude=6.0, max_magnitude=8.0,
        min_Rjb=5, max_Rjb=40,
        min_Rrup=10, max_Rrup=50,
        min_vs30=300, max_vs30=700,
        min_depth=5, max_depth=20,
        min_pga=50, max_pga=250,
        min_pgv=5, max_pgv=25,
        min_pgd=1, max_pgd=3,
        mechanisms=[1, 2]
    )


def test_get_name(provider):
    assert provider.get_name() == "PEER"


def test_map_criteria(provider, sample_criteria):
    result = provider.map_criteria(sample_criteria)
    assert isinstance(result, dict)
    assert "min_magnitude" in result
    assert result["min_magnitude"] == 6.0

@pytest.mark.parametrize(
    "criteria_dict, expected_indices",
    [
        # min magnitude >= 6.0
        ({
            "min_magnitude": 6.0, "max_magnitude": None,
            "min_vs30": None, "max_vs30": None,
            "min_depth": None, "max_depth": None,
            "min_pga": None, "max_pga": None,
            "min_pgv": None, "max_pgv": None,
            "min_pgd": None, "max_pgd": None,
            "min_Rjb": None, "max_Rjb": None,
            "min_Rrup": None, "max_Rrup": None,
            "mechanisms": None
        }, [1,2]),

        # max magnitude <= 6.0
        ({
            "min_magnitude": None, "max_magnitude": 6.0,
            "min_vs30": None, "max_vs30": None,
            "min_depth": None, "max_depth": None,
            "min_pga": None, "max_pga": None,
            "min_pgv": None, "max_pgv": None,
            "min_pgd": None, "max_pgd": None,
            "min_Rjb": None, "max_Rjb": None,
            "min_Rrup": None, "max_Rrup": None,
            "mechanisms": None
        }, [0,2]),

        # min VS30 >= 300
        ({
            "min_magnitude": None, "max_magnitude": None,
            "min_vs30": 300, "max_vs30": None,
            "min_depth": None, "max_depth": None,
            "min_pga": None, "max_pga": None,
            "min_pgv": None, "max_pgv": None,
            "min_pgd": None, "max_pgd": None,
            "min_Rjb": None, "max_Rjb": None,
            "min_Rrup": None, "max_Rrup": None,
            "mechanisms": None
        }, [1,2]),

        # max VS30 <= 500
        ({
            "min_magnitude": None, "max_magnitude": None,
            "min_vs30": None, "max_vs30": 500,
            "min_depth": None, "max_depth": None,
            "min_pga": None, "max_pga": None,
            "min_pgv": None, "max_pgv": None,
            "min_pgd": None, "max_pgd": None,
            "min_Rjb": None, "max_Rjb": None,
            "min_Rrup": None, "max_Rrup": None,
            "mechanisms": None
        }, [0,1]),

        # min depth >= 10
        ({
            "min_depth": 10, "max_depth": None,
            "min_magnitude": None, "max_magnitude": None,
            "min_vs30": None, "max_vs30": None,
            "min_pga": None, "max_pga": None,
            "min_pgv": None, "max_pgv": None,
            "min_pgd": None, "max_pgd": None,
            "min_Rjb": None, "max_Rjb": None,
            "min_Rrup": None, "max_Rrup": None,
            "mechanisms": None
        }, [1,2]),

        # min PGA >= 150
        ({
            "min_pga": 150, "max_pga": None,
            "min_magnitude": None, "max_magnitude": None,
            "min_vs30": None, "max_vs30": None,
            "min_depth": None, "max_depth": None,
            "min_pgv": None, "max_pgv": None,
            "min_pgd": None, "max_pgd": None,
            "min_Rjb": None, "max_Rjb": None,
            "min_Rrup": None, "max_Rrup": None,
            "mechanisms": None
        }, [1,2]),

        # min PGV >= 15
        ({
            "min_pgv": 15, "max_pgv": None,
            "min_magnitude": None, "max_magnitude": None,
            "min_vs30": None, "max_vs30": None,
            "min_depth": None, "max_depth": None,
            "min_pga": None, "max_pga": None,
            "min_pgd": None, "max_pgd": None,
            "min_Rjb": None, "max_Rjb": None,
            "min_Rrup": None, "max_Rrup": None,
            "mechanisms": None
        }, [1,2]),

        # min PGD >= 2
        ({
            "min_pgd": 2, "max_pgd": None,
            "min_magnitude": None, "max_magnitude": None,
            "min_vs30": None, "max_vs30": None,
            "min_depth": None, "max_depth": None,
            "min_pga": None, "max_pga": None,
            "min_pgv": None, "max_pgv": None,
            "min_Rjb": None, "max_Rjb": None,
            "min_Rrup": None, "max_Rrup": None,
            "mechanisms": None
        }, [1,2]),

        # min RJB >= 20
        ({
            "min_Rjb": 20, "max_Rjb": None,
            "min_magnitude": None, "max_magnitude": None,
            "min_vs30": None, "max_vs30": None,
            "min_depth": None, "max_depth": None,
            "min_pga": None, "max_pga": None,
            "min_pgv": None, "max_pgv": None,
            "min_pgd": None, "max_pgd": None,
            "min_Rrup": None, "max_Rrup": None,
            "mechanisms": None
        }, [1,2]),

        # Mechanism filter
        ({
            "mechanisms": [1,3], "min_magnitude": None, "max_magnitude": None,
            "min_vs30": None, "max_vs30": None,
            "min_depth": None, "max_depth": None,
            "min_pga": None, "max_pga": None,
            "min_pgv": None, "max_pgv": None,
            "min_pgd": None, "max_pgd": None,
            "min_Rjb": None, "max_Rjb": None,
            "min_Rrup": None, "max_Rrup": None
        }, [0,2])
    ]
)
def test_apply_filters_full(provider, dummy_df, criteria_dict, expected_indices):
    filtered = provider._apply_filters(dummy_df, criteria_dict)
    assert list(filtered.index) == expected_indices

def test_apply_filters_min_mag(provider, dummy_df, empty_criteria):
    crit = empty_criteria.to_peer_params()
    crit["min_magnitude"] = 6.0
    filtered = provider._apply_filters(dummy_df, crit)
    assert (filtered["MAGNITUDE"] >= 6.0).all()


def test_apply_filters_with_mechanisms(provider, dummy_df, empty_criteria):
    crit = empty_criteria.to_peer_params()
    crit["mechanisms"] = [1, 3]
    filtered = provider._apply_filters(dummy_df, crit)
    assert set(filtered["MECHANISM"]) <= {1, 3}


def test_apply_filters_invalid(provider):
    with pytest.raises(ProviderError):
        provider._apply_filters(pd.DataFrame({"wrong": [1, 2]}), {"min_magnitude": 5})


def test_fetch_data_sync(provider, empty_criteria):
    result = provider.fetch_data_sync(empty_criteria.to_peer_params())
    assert isinstance(result, Result)
    assert result.success
    df = result.unwrap()
    assert isinstance(df, pd.DataFrame)
    assert "PROVIDER" in df.columns
    assert df["PROVIDER"].iloc[0] == "PEER"


@pytest.mark.asyncio
async def test_fetch_data_async(provider, empty_criteria):
    result = await provider.fetch_data_async(empty_criteria.to_peer_params())
    assert isinstance(result, Result)
    assert result.success
    df = result.unwrap()
    assert isinstance(df, pd.DataFrame)
    assert "PROVIDER" in df.columns
    assert result.value["PROVIDER"].iloc[0] == "PEER"


def test_fetch_data_sync_raises(provider):
    provider.flatfile_df = None  # bozuyoruz
    result = provider.fetch_data_sync({"min_magnitude": 5})
    assert isinstance(result, Result)
    assert result.success is False
    # error objesini kontrol edebiliriz
    assert isinstance(result.error, Exception)

def test_convert_mechanism_string_type(provider, empty_criteria):
    provider.flatfile_df["MECHANISM"] = ["SS", "NM", "TF"]
    result: Result = provider.fetch_data_sync(empty_criteria.to_peer_params())
    assert result.success
    df = result.unwrap()
    assert set(df["MECHANISM"]) == {"SS", "NM", "TF"}

def test_convert_mechanism_numeric(provider, empty_criteria):
    provider.flatfile_df["MECHANISM"] = [1, 2, 3]
    result: Result = provider.fetch_data_sync(empty_criteria.to_peer_params())
    assert result.success
    df = result.unwrap()
    assert all(isinstance(m, str) for m in df["MECHANISM"])

