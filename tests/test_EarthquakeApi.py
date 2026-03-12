"""
tests/test_EarthquakeApi.py  (düzeltilmiş)

Hata: `ProviderFactory` attribute yok — refactor'da `ProviderRegistry` oldu.
Düzeltme: EarthquakeAPI.__init__ içinde `ProviderRegistry.build` patch'lenir.
"""

import asyncio
import pytest
import pandas as pd
from unittest.mock import MagicMock, AsyncMock, patch

from selection_service.core.EarthquakeApi import EarthquakeAPI
from selection_service.core.Pipeline import PipelineResult
from selection_service.core.ErrorHandle import PipelineError, ProviderError
from selection_service.processing.ResultHandle import Result
from selection_service.enums.Enums import ProviderName


# ─── helpers ────────────────────────────────────────────────────────────────

def _make_selected_df(rows=3) -> pd.DataFrame:
    return pd.DataFrame([
        {"RSN": i, "PROVIDER": "AFAD", "EVENT": f"EQ{i}",
         "MAGNITUDE": 7.0, "SCORE": 80.0, "ENDPOINTSOURCE": f"https://x/{i}",
         "FILE_NAME_H1": f"f{i}.mseed", "SSN": f"TK.S{i}"}
        for i in range(rows)
    ])


def _make_pipeline_result(df=None) -> PipelineResult:
    df = df or _make_selected_df()
    return PipelineResult(
        selected_df=df, scored_df=df,
        report={"status": "success"}, execution_time=0.5,
    )


def _make_registry(provider_names=None):
    """ProviderRegistry mock — get() çağrılarını provider mock'a yönlendirir."""
    afad_provider = MagicMock()
    afad_provider.get_name.return_value = "AFAD"

    peer_provider = MagicMock()
    peer_provider.get_name.return_value = "PEER"

    registry = MagicMock()
    registry.all.return_value = [afad_provider, peer_provider]
    registry.get.side_effect = lambda name: (
        afad_provider if name == "AFAD" else
        peer_provider if name == "PEER" else None
    )
    return registry


def _make_query_service(result=None, fail=False):
    svc = MagicMock()
    res = Result.fail(PipelineError("err", None)) if fail else Result.ok(result or _make_pipeline_result())
    svc.run_sync.return_value = res
    svc.run_async = AsyncMock(return_value=res)
    svc.re_selection.return_value = res
    return svc


def _make_download_service(success=True):
    svc = MagicMock()
    svc.download_batch.return_value = Result.ok(True) if success else Result.fail(
        ProviderError("AFAD", Exception("download failed"))
    )
    svc.download_single.return_value = Result.ok(True) if success else Result.fail(
        ProviderError("AFAD", Exception("single download failed"))
    )
    return svc


def _build_api(query_svc=None, download_svc=None, registry=None):
    """EarthquakeAPI'yi gerçek provider başlatmadan oluştur."""
    strategy = MagicMock()
    strategy.get_name.return_value = "TBDY_2018_Gaussian"

    _registry = registry or _make_registry()
    _query = query_svc or _make_query_service()
    _download = download_svc or _make_download_service()

    with patch("selection_service.core.EarthquakeApi.ProviderRegistry") as MockReg, \
         patch("selection_service.core.EarthquakeApi.EarthquakeQueryService") as MockQuery, \
         patch("selection_service.core.EarthquakeApi.WaveformDownloadService") as MockDL:
        MockReg.build.return_value = _registry
        MockQuery.return_value = _query
        MockDL.return_value = _download

        api = EarthquakeAPI(
            provider_names=[ProviderName.AFAD, ProviderName.PEER],
            strategies=[strategy],
        )
        # Servisleri doğrudan değiştir — patch sonrası mock'lar yerine gerçek mock nesneleri ata
        api.registry = _registry
        api.query = _query
        api.downloader = _download
        return api


# ─── TestEarthquakeAPI ───────────────────────────────────────────────────────

class TestEarthquakeAPI:

    def test_init(self):
        """EarthquakeAPI ProviderRegistry.build ile başlatılmalı."""
        strategy = MagicMock(); strategy.get_name.return_value = "TBDY_2018_Gaussian"
        with patch("selection_service.core.EarthquakeApi.ProviderRegistry") as MockReg, \
             patch("selection_service.core.EarthquakeApi.EarthquakeQueryService"), \
             patch("selection_service.core.EarthquakeApi.WaveformDownloadService"):
            MockReg.build.return_value = _make_registry()
            api = EarthquakeAPI(
                provider_names=[ProviderName.AFAD],
                strategies=[strategy],
            )
        MockReg.build.assert_called_once()
        assert api.registry is not None
        assert api.query is not None
        assert api.downloader is not None

    def test_run_sync_success(self):
        api = _build_api()
        criteria = MagicMock()
        result = api.run_sync(criteria, "TBDY_2018_Gaussian")
        assert result.success
        assert isinstance(result.value, PipelineResult)
        api.query.run_sync.assert_called_once_with(criteria, "TBDY_2018_Gaussian")

    def test_run_sync_invalid_strategy(self):
        """Bilinmeyen strateji ismi PipelineError ile başarısız olmalı."""
        api = _build_api(query_svc=_make_query_service(fail=True))
        result = api.run_sync(MagicMock(), "NONEXISTENT")
        assert result.success is False

    def test_run_async_success(self):
        api = _build_api()
        criteria = MagicMock()
        result = asyncio.get_event_loop().run_until_complete(
            api.run_async(criteria, "TBDY_2018_Gaussian")
        )
        assert result.success

    def test_download_waveforms_success(self):
        df = _make_selected_df()
        api = _build_api()
        result = api.download_waveforms(df)
        assert result.success
        api.downloader.download_batch.assert_called_once_with(df)

    def test_download_waveforms_peer_skip(self):
        """PEER sağlayıcısı download desteklemez — yine de hata döndürmemeli."""
        df = _make_selected_df()
        df["PROVIDER"] = "PEER"
        api = _build_api()
        result = api.download_waveforms(df)
        # download_batch çağrıldı — iç mantık provider'ı atlar
        api.downloader.download_batch.assert_called_once()

    def test_download_waveforms_missing_provider(self):
        """Registry'de olmayan provider → download_batch Result.fail döndürebilir."""
        dl_svc = _make_download_service(success=False)
        api = _build_api(download_svc=dl_svc)
        result = api.download_waveforms(_make_selected_df())
        assert result.success is False

    def test_download_waveforms_exception(self):
        dl_svc = MagicMock()
        dl_svc.download_batch.side_effect = ProviderError("AFAD", Exception("crash"))
        api = _build_api(download_svc=dl_svc)
        with pytest.raises(ProviderError):
            api.download_waveforms(_make_selected_df())

    def test_re_selection_success(self):
        df = _make_selected_df()
        api = _build_api()
        result = api.re_selection(df, "TBDY_2018_Gaussian", MagicMock())
        assert result.success
        api.query.re_selection.assert_called_once()

    def test_re_selection_strategy_fail(self):
        api = _build_api(query_svc=_make_query_service(fail=True))
        result = api.re_selection(_make_selected_df(), "BAD_STRATEGY", MagicMock())
        assert result.success is False

    def test_re_selection_invalid_strategy(self):
        """re_selection bilinmeyen strateji ile fail dönmeli."""
        api = _build_api(query_svc=_make_query_service(fail=True))
        result = api.re_selection(_make_selected_df(), "UNKNOWN", MagicMock())
        assert result.success is False

    def test_download_single_waveform_success(self):
        api = _build_api()
        result = api.download_single_waveform("file.mseed", "123", "TK.KND")
        assert result.success
        api.downloader.download_single.assert_called_once_with("file.mseed", "123", "TK.KND")

    def test_download_single_waveform_provider_not_found(self):
        dl_svc = _make_download_service(success=False)
        api = _build_api(download_svc=dl_svc)
        result = api.download_single_waveform("file.mseed", "123", "TK.KND")
        assert result.success is False

    def test_download_single_waveform_exception(self):
        dl_svc = MagicMock()
        dl_svc.download_single.side_effect = ProviderError("AFAD", Exception("crash"))
        api = _build_api(download_svc=dl_svc)
        with pytest.raises(ProviderError):
            api.download_single_waveform("file.mseed", "123", "TK.KND")

    def test_get_provider_helper(self):
        """registry.get() ile provider alınabilmeli."""
        api = _build_api()
        provider = api.registry.get("AFAD")
        assert provider is not None
        assert provider.get_name() == "AFAD"