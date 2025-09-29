import os
import io
import pytest
import zipfile
import pandas as pd
import asyncio
from unittest.mock import patch, MagicMock, AsyncMock
from selection_service.processing.ResultHandle import Result
from src.selection_service.providers.AfadProvider import AFADDataProvider
from src.selection_service.processing.Selection import SearchCriteria
from src.selection_service.core.ErrorHandle import NetworkError, ProviderError


class DummyMapper:
    @staticmethod
    def map_columns(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["mapped"] = True
        return df


@pytest.fixture
def provider(tmp_path):
    p = AFADDataProvider(column_mapper=DummyMapper)
    p.base_download_dir = str(tmp_path)  # test klasörüne indir
    return p


def test_map_criteria(provider):
    crit = SearchCriteria(start_date="2020-01-01", end_date="2020-01-02")
    mapped = provider.map_criteria(crit)
    assert isinstance(mapped, dict)


@patch("src.selection_service.providers.AfadProvider.requests.post")
def test_fetch_data_sync_success(mock_post, provider):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = [{"a": 1}, {"a": 2}]
    mock_post.return_value = mock_resp

    result = provider.fetch_data_sync({"dummy": "x"})
    assert result.success
    df = result.unwrap()
    assert isinstance(df, pd.DataFrame)
    assert "PROVIDER" in df.columns


@patch("src.selection_service.providers.AfadProvider.requests.post")
def test_fetch_data_sync_failure(mock_post, provider):
    mock_resp = MagicMock()
    mock_resp.status_code = 500
    mock_resp.text = "Server error"
    mock_post.return_value = mock_resp

    result = provider.fetch_data_sync({"dummy": "x"})
    assert not result.success
    assert isinstance(result.error, Exception)


# @pytest.mark.asyncio
# async def test_fetch_data_async_success(provider):
#     fake_json = [{"x": 10}, {"x": 20}]

#     mock_response = AsyncMock()
#     mock_response.status = 200
#     mock_response.json = AsyncMock(return_value=fake_json)

#     mock_session = AsyncMock()
#     mock_session.post.return_value.__aenter__.return_value = mock_response

#     class DummySession:
#         async def __aenter__(self):
#             return mock_session
#         async def __aexit__(self, exc_type, exc, tb):
#             return False

#     with patch("src.selection_service.providers.AfadProvider.aiohttp.ClientSession", return_value=DummySession()):
#         result = await provider.fetch_data_async({"y": 1})
#         assert isinstance(result, Result)
#         assert result.success
#         df = result.unwrap()
#         assert "PROVIDER" in df.columns


@pytest.mark.asyncio
async def test_fetch_data_async_failure(provider):
    mock_response = AsyncMock()
    mock_response.status = 500
    mock_response.text = AsyncMock(return_value="fail")

    mock_session = AsyncMock()
    mock_session.post.return_value.__aenter__.return_value = mock_response

    with patch("src.selection_service.providers.AfadProvider.aiohttp.ClientSession", return_value=mock_session):
        result = await provider.fetch_data_async({"y": 1})
        assert result.success is False
        assert isinstance(result.error, Exception)


@patch("src.selection_service.providers.AfadProvider.requests.get")
def test_get_event_details_success(mock_get, provider):
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"id": 1, "name": "test"}
    mock_get.return_value = mock_resp

    result = provider.get_event_details([123])
    assert result.success
    df = result.unwrap()
    assert not df.empty
    assert "id" in df.columns


def test_extract_and_organize_zip_batch(provider, tmp_path):
    # fake zip dosyası oluştur
    zip_path = tmp_path / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("file_station1.txt", "dummy content")

    extracted = provider.extract_and_organize_zip_batch(
        event_path=str(tmp_path),
        zip_path=str(zip_path),
        expected_filenames=["file_station1.txt"],
        export_type="mseed",
    )
    assert len(extracted) == 1
    assert os.path.exists(extracted[0])
