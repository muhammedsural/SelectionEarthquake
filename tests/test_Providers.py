from pathlib import Path
import pytest
import pandas as pd
import os
import zipfile
from unittest.mock import MagicMock, AsyncMock
from selection_service.processing.ResultHandle import Result
from selection_service.providers import Providers as providers
from selection_service.providers.Providers import PeerWest2Provider
from selection_service.processing.Mappers import IColumnMapper
from selection_service.processing.Selection import SearchCriteria
from selection_service.enums.Enums import ProviderName

# ---- Ortak yardımcı mock class ----


class DummyMapper(IColumnMapper):
    def map_columns(self, df):
        df = df.copy()
        if "MAGNITUDE" not in df.columns:
            df["MAGNITUDE"] = [6.0]
        if "MECHANISM" not in df.columns:
            df["MECHANISM"] = [1]
        if "RJB(km)" not in df.columns:
            df["RJB(km)"] = [10]
        return df

# ----------------------------------------------------------
# PeerWest2Provider Testleri
# ----------------------------------------------------------

# ---------------------------------------------------------
# Fixture: küçük bir geçici CSV dosyası oluştur
# ---------------------------------------------------------


@pytest.fixture
def peer_csv(tmp_path: Path):
    df = pd.DataFrame({
        "MAGNITUDE": [5.0, 6.5, 7.2],
        "MECHANISM": [1, 2, 1],
        "RJB(km)": [5, 15, 30],
        "RRUP(km)": [10, 20, 40],
        "VS30(m/s)": [300, 760, 1000],
        "HYPO_DEPTH(km)": [5, 10, 15],
        "PGA(cm2/sec)": [50, 100, 200],
        "PGV(cm/sec)": [5, 20, 40],
        "PGD(cm)": [1, 3, 5]
    })
    file_path = tmp_path / "peer.csv"
    df.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def sample_df(tmp_path):
    """Geçici CSV dosyası ile sahte NGA-West2 dataset"""
    df = pd.DataFrame({
        "MAGNITUDE": [5.5, 7.0],
        "RJB(km)": [10, 30],
        "RRUP(km)": [12, 28],
        "VS30(m/s)": [500, 800],
        "HYPO_DEPTH(km)": [5, 15],
        "PGA(cm2/sec)": [100, 300],
        "PGV(cm/sec)": [20, 50],
        "PGD(cm)": [5, 15],
        "MECHANISM": [1, 2]
    })
    file_path = tmp_path / "peer.csv"
    df.to_csv(file_path, index=False)
    return df, str(file_path)

# ---------------------------------------------------------
# Testler
# ---------------------------------------------------------


def test_get_name(peer_csv):
    provider = PeerWest2Provider(DummyMapper(), file_path=str(peer_csv))
    assert provider.get_name() == ProviderName.PEER.value


def test_map_criteria(peer_csv):
    provider = PeerWest2Provider(DummyMapper(), file_path=str(peer_csv))
    criteria = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                              min_magnitude=5.0, max_magnitude=7.0)
    params = provider.map_criteria(criteria)
    assert "min_magnitude" in params
    assert "max_magnitude" in params


def test_fetch_data_sync_success(peer_csv):
    provider = PeerWest2Provider(DummyMapper(), file_path=str(peer_csv))
    criteria = {
        "min_magnitude": 5.5, "max_magnitude": 7.5,
        "min_Rjb": None, "max_Rjb": None,
        "min_Rrup": None, "max_Rrup": None,
        "min_vs30": None, "max_vs30": None,
        "min_depth": None, "max_depth": None,
        "min_pga": None, "max_pga": None,
        "min_pgv": None, "max_pgv": None,
        "min_pgd": None, "max_pgd": None,
        "mechanisms": []
    }
    result = provider.fetch_data_sync(criteria)
    assert result.success
    assert isinstance(result.value, pd.DataFrame)
    assert "PROVIDER" in result.value.columns
    # sadece 6.5 ve 7.2 büyüklükler kalmalı
    assert all(result.value["MAGNITUDE"] >= 5.5)


@pytest.mark.asyncio
async def test_fetch_data_async_success(peer_csv):
    provider = PeerWest2Provider(DummyMapper(), file_path=str(peer_csv))
    criteria = {
        "min_magnitude": 5.0, "max_magnitude": 7.0,
        "min_Rjb": None, "max_Rjb": None,
        "min_Rrup": None, "max_Rrup": None,
        "min_vs30": None, "max_vs30": None,
        "min_depth": None, "max_depth": None,
        "min_pga": None, "max_pga": None,
        "min_pgv": None, "max_pgv": None,
        "min_pgd": None, "max_pgd": None,
        "mechanisms": []
    }
    result = await provider.fetch_data_async(criteria)
    assert result.success
    assert isinstance(result.value, pd.DataFrame)
    assert "PROVIDER" in result.value.columns
    assert all(result.value["MAGNITUDE"] <= 7.0)


def test_apply_filters_with_mechanism(peer_csv):
    provider = PeerWest2Provider(DummyMapper(), file_path=str(peer_csv))
    df = provider.flatfile_df
    criteria = {
        "min_magnitude": None, "max_magnitude": None,
        "min_Rjb": None, "max_Rjb": None,
        "min_Rrup": None, "max_Rrup": None,
        "min_vs30": None, "max_vs30": None,
        "min_depth": None, "max_depth": None,
        "min_pga": None, "max_pga": None,
        "min_pgv": None, "max_pgv": None,
        "min_pgd": None, "max_pgd": None,
        "mechanisms": [2]
    }
    filtered = provider._apply_filters(df, criteria)
    assert not filtered.empty
    assert all(filtered["MECHANISM"] == 2)


def test_fetch_data_sync_failure(monkeypatch, sample_df):
    _, file_path = sample_df
    provider = PeerWest2Provider(DummyMapper(), file_path=file_path)

    # map_columns hata fırlatsın
    def broken_map_columns(df):
        raise ValueError("Mapping failed")

    monkeypatch.setattr(provider.column_mapper, "map_columns",
                        broken_map_columns)

    criteria = SearchCriteria(start_date="2000-01-01",
                              end_date="2025-01-01").to_peer_params()
    result = provider.fetch_data_sync(criteria)

    assert not result.success
    assert isinstance(result.error, Exception)
    assert "Mapping failed" in str(result.error)


@pytest.mark.asyncio
async def test_fetch_data_async_failure(monkeypatch, sample_df):
    _, file_path = sample_df
    provider = PeerWest2Provider(DummyMapper(), file_path=file_path)

    # pd.read_csv hata fırlatsın
    monkeypatch.setattr("pandas.read_csv",
                        lambda *a,
                        **k: (_ for _ in ()).throw(IOError("CSV read error")))

    criteria = SearchCriteria(start_date="2000-01-01",
                              end_date="2025-01-01").to_peer_params()
    result = await provider.fetch_data_async(criteria)

    assert not result.success
    assert isinstance(result.error, Exception)
    assert "CSV read error" in str(result.error)

# ----------------------------------------------------------
# AFADDataProvider Testleri
# ----------------------------------------------------------


def test_afad_sync_fetch(monkeypatch: pytest.MonkeyPatch):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"id": 1, "MAGNITUDE": 6.0}]
    monkeypatch.setattr("requests.post", lambda *a, **k: mock_response)

    provider = providers.AFADDataProvider(DummyMapper())
    result = provider.fetch_data_sync({"dummy": "criteria"})
    assert result.success
    assert isinstance(result.value, pd.DataFrame)


@pytest.mark.asyncio
async def test_afad_async_fetch(monkeypatch: pytest.MonkeyPatch):
    async def mock_post(*a, **k):
        class DummyResp:
            status = 200
            async def json(self): return [{"id": 1, "MAGNITUDE": 6.5}]
            async def text(self): return "error"
            async def __aenter__(self): return self
            async def __aexit__(self, *exc): pass
        return DummyResp()

    mock_session = AsyncMock()
    mock_session.post = mock_post
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: mock_session)

    provider = providers.AFADDataProvider(DummyMapper())
    result = await provider.fetch_data_async({"dummy": "criteria"})

    assert isinstance(result, Result)
    assert result.success, f"{result.error}"
    assert isinstance(result.value, pd.DataFrame)
    assert not result.value.empty


def test_afad_get_event_details(monkeypatch: pytest.MonkeyPatch):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": 1}
    monkeypatch.setattr("requests.get", lambda *a, **k: mock_response)

    provider = providers.AFADDataProvider(DummyMapper())
    result = provider.get_event_details([1, 2])

    assert result.success, f"Beklenmedik hata: {result.error}"
    assert isinstance(result.value, pd.DataFrame)
    assert not result.value.empty


def test_afad_extract_and_organize_zip_batch(tmp_path: Path):
    zip_path = tmp_path / "test.zip"
    event_dir = tmp_path / "event"
    os.makedirs(event_dir, exist_ok=True)

    # Sahte zip oluştur
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("station1_file1.mseed", "dummydata")

    provider = providers.AFADDataProvider(DummyMapper())
    files = provider.extract_and_organize_zip_batch(str(event_dir),
                                                    str(zip_path),
                                                    ["station1_file1.mseed"],
                                                    "mseed")
    assert files

# ----------------------------------------------------------
# FDSNProvider Testleri
# ----------------------------------------------------------


def test_fdsn_build_params():
    provider = providers.FDSNProvider(DummyMapper())
    params = provider._build_params({"min_magnitude": 5, "max_magnitude": 7})
    assert "minmag" in params and "maxmag" in params


def test_provider_factory_create():
    afad = providers.ProviderFactory.create_provider(
        providers.ProviderName.AFAD)
    peer = providers.ProviderFactory.create_provider(
        providers.ProviderName.PEER)
    assert afad.get_name() == "AFAD"
    assert peer.get_name() == "PEER"
