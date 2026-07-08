import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from selection_service.enums.Enums import ProviderName
from selection_service.processing.ResultHandle import Result
from selection_service.providers.CacheManager import CacheManager
from selection_service.providers.ProvidersFactory import CachedProviderProxy, ProviderFactory
from selection_service.providers.AfadProvider import AFADDataProvider
from selection_service.providers.PeerProvider import PeerWest2Provider


def test_cache_manager_roundtrip(tmp_path):
    cache = CacheManager(cache_dir=str(tmp_path), expiry_hours=1)
    criteria = {"b": 2, "a": 1}
    df = pd.DataFrame({"RSN": [1], "MAGNITUDE": [7.0]})

    cache.set("PEER", criteria, df)
    cached = cache.get("PEER", criteria)

    pd.testing.assert_frame_equal(cached, df)


def test_cache_manager_returns_none_for_missing_and_empty(tmp_path):
    cache = CacheManager(cache_dir=str(tmp_path), expiry_hours=1)
    assert cache.get("PEER", {"x": 1}) is None

    with patch.object(pd.DataFrame, "to_parquet") as to_parquet:
        cache.set("PEER", {"x": 1}, pd.DataFrame())
    to_parquet.assert_not_called()


def test_cache_manager_expired_file_is_removed(tmp_path, monkeypatch):
    cache = CacheManager(cache_dir=str(tmp_path), expiry_hours=0)
    df = pd.DataFrame({"RSN": [1]})
    criteria = {"x": 1}
    cache.set("PEER", criteria, df)

    monkeypatch.setattr("selection_service.providers.CacheManager.time.time", lambda: 9999999999)

    assert cache.get("PEER", criteria) is None


def test_cached_provider_proxy_sync_cache_hit():
    cached_df = pd.DataFrame({"RSN": [1]})
    provider = MagicMock()
    provider.get_name.return_value = "PEER"
    provider.fetch_data_sync.return_value = Result.ok(pd.DataFrame({"RSN": [2]}))
    cache = MagicMock()
    cache.get.return_value = cached_df

    proxy = CachedProviderProxy(provider, cache)
    result = proxy.fetch_data_sync({"criteria": 1})

    assert result.success is True
    pd.testing.assert_frame_equal(result.value, cached_df)
    provider.fetch_data_sync.assert_not_called()


def test_cached_provider_proxy_sync_cache_miss_writes_result():
    df = pd.DataFrame({"RSN": [2]})
    provider = MagicMock()
    provider.get_name.return_value = "PEER"
    provider.fetch_data_sync.return_value = Result.ok(df)
    cache = MagicMock()
    cache.get.return_value = None

    proxy = CachedProviderProxy(provider, cache)
    result = proxy.fetch_data_sync({"criteria": 1})

    assert result.value.equals(df)
    cache.set.assert_called_once_with("PEER", {"criteria": 1}, df)


def test_cached_provider_proxy_async_cache_miss_writes_result():
    df = pd.DataFrame({"RSN": [3]})
    provider = MagicMock()
    provider.get_name.return_value = "AFAD"
    provider.fetch_data_async = AsyncMock(return_value=Result.ok(df))
    cache = MagicMock()
    cache.get.return_value = None

    proxy = CachedProviderProxy(provider, cache)
    result = asyncio.run(proxy.fetch_data_async({"criteria": 1}))

    assert result.value.equals(df)
    cache.set.assert_called_once_with("AFAD", {"criteria": 1}, df)


def test_cached_provider_proxy_delegates_unknown_attributes():
    provider = MagicMock()
    provider.get_name.return_value = "AFAD"
    provider.download_single_waveforms.return_value = True
    proxy = CachedProviderProxy(provider, MagicMock())

    assert proxy.download_single_waveforms("x") is True


def test_provider_factory_creates_uncached_providers():
    afad = ProviderFactory.create_provider(ProviderName.AFAD, use_cache=False)
    peer = ProviderFactory.create_provider(ProviderName.PEER, use_cache=False)

    assert isinstance(afad, AFADDataProvider)
    assert isinstance(peer, PeerWest2Provider)


def test_provider_factory_wraps_cache_when_enabled():
    provider = ProviderFactory.create_provider(ProviderName.PEER, use_cache=True)

    assert isinstance(provider, CachedProviderProxy)


def test_provider_factory_unknown_provider_raises():
    with pytest.raises(ValueError, match="Unknown provider"):
        ProviderFactory.create_provider("UNKNOWN", use_cache=False)
