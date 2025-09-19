import pytest
import pandas as pd
import numpy as np
import asyncio
import os
import zipfile
from unittest.mock import patch, MagicMock, AsyncMock
from selection_service.providers import Providers as providers

# ---- Ortak yardımcı mock class ----
class DummyMapper:
    def map_columns(self, df):
        df = df.copy()
        if "MAGNITUDE" not in df.columns:
            df["MAGNITUDE"] = [5.5]
        if "MECHANISM" not in df.columns:
            df["MECHANISM"] = [1]
        return df

# ----------------------------------------------------------
# PeerWest2Provider Testleri
# ----------------------------------------------------------
def test_peer_sync_fetch(tmp_path):
    # Sahte CSV dosyası hazırla
    file_path = tmp_path / "peer.csv"
    df = pd.DataFrame({"MAGNITUDE":[6.0], "MECHANISM":[1], "RJB(km)":[10], "RRUP(km)":[15],
                       "VS30(m/s)":[760], "HYPO_DEPTH(km)":[5], "PGA(cm2/sec)":[100],
                       "PGV(cm/sec)":[20], "PGD(cm)":[3]})
    df.to_csv(file_path, index=False)

    provider = providers.PeerWest2Provider(column_mapper=DummyMapper(), file_path=str(file_path))
    result = provider.fetch_data_sync({
        "min_magnitude":5.0, "max_magnitude":7.0,
        "min_Rjb":None, "max_Rjb":None,
        "min_Rrup":None, "max_Rrup":None,
        "min_vs30":None, "max_vs30":None,
        "min_depth":None, "max_depth":None,
        "min_pga":None, "max_pga":None,
        "min_pgv":None, "max_pgv":None,
        "min_pgd":None, "max_pgd":None,
        "mechanisms":[1]
    })
    assert result.success
    assert "PROVIDER" in result.value.columns

@pytest.mark.asyncio
async def test_peer_async_fetch(monkeypatch, tmp_path):
    file_path = tmp_path / "peer.csv"
    pd.DataFrame({"MAGNITUDE":[6], "MECHANISM":[1]}).to_csv(file_path, index=False)

    provider = providers.PeerWest2Provider(column_mapper=DummyMapper(), file_path=str(file_path))

    result = await provider.fetch_data_async({"min_magnitude":5, "max_magnitude":7,
                                              "min_Rjb":None,"max_Rjb":None,
                                              "min_Rrup":None,"max_Rrup":None,
                                              "min_vs30":None,"max_vs30":None,
                                              "min_depth":None,"max_depth":None,
                                              "min_pga":None,"max_pga":None,
                                              "min_pgv":None,"max_pgv":None,
                                              "min_pgd":None,"max_pgd":None,
                                              "mechanisms":[1]})
    assert isinstance(result, pd.DataFrame)
    assert "PROVIDER" in result.columns

# ----------------------------------------------------------
# AFADDataProvider Testleri
# ----------------------------------------------------------
def test_afad_sync_fetch(monkeypatch):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [{"id": 1, "MAGNITUDE": 6.0}]
    monkeypatch.setattr("requests.post", lambda *a, **k: mock_response)

    provider = providers.AFADDataProvider(DummyMapper())
    result = provider.fetch_data_sync({"dummy": "criteria"})
    assert result.success
    assert isinstance(result.value, pd.DataFrame)

@pytest.mark.asyncio
async def test_afad_async_fetch(monkeypatch):
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
    assert isinstance(result, pd.DataFrame)

def test_afad_get_event_details(monkeypatch):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": 1}
    monkeypatch.setattr("requests.get", lambda *a, **k: mock_response)

    provider = providers.AFADDataProvider(DummyMapper())
    df = provider.get_event_details([1,2])
    assert not df.empty

def test_afad_extract_and_organize_zip_batch(tmp_path):
    zip_path = tmp_path / "test.zip"
    event_dir = tmp_path / "event"
    os.makedirs(event_dir, exist_ok=True)

    # Sahte zip oluştur
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("station1_file1.mseed", "dummydata")

    provider = providers.AFADDataProvider(DummyMapper())
    files = provider.extract_and_organize_zip_batch(str(event_dir), str(zip_path), ["station1_file1.mseed"], "mseed")
    assert files

# ----------------------------------------------------------
# FDSNProvider Testleri
# ----------------------------------------------------------
def test_fdsn_build_params():
    provider = providers.FDSNProvider(DummyMapper())
    params = provider._build_params({"min_magnitude": 5, "max_magnitude": 7})
    assert "minmag" in params and "maxmag" in params

def test_provider_factory_create():
    afad = providers.ProviderFactory.create_provider(providers.ProviderName.AFAD)
    peer = providers.ProviderFactory.create_provider(providers.ProviderName.PEER, file_path="dummy.csv")
    assert afad.get_name() == "AFAD"
    assert peer.get_name() == "PEER"
