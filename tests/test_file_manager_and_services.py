import asyncio
import io
import zipfile
from unittest.mock import MagicMock, patch

import pandas as pd

from selection_service.core.ErrorHandle import ProviderError
from selection_service.core.Pipeline import PipelineResult
from selection_service.processing.ResultHandle import Result
from selection_service.providers.afad.AfadFileManager import AfadFileManager
from selection_service.services.EarthquakeQueryService import EarthquakeQueryService
from selection_service.services.ProviderRegistry import ProviderRegistry
from selection_service.services.WaveformDownloadService import WaveformDownloadService


def _zip_bytes(files: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return buffer.getvalue()


def test_afad_file_manager_save_and_extract_zip(tmp_path):
    manager = AfadFileManager(base_dir=str(tmp_path))
    content = _zip_bytes({"wave.asc": b"1234567890" * 20})

    zip_path = manager.save_zip(content, event_id=42, filename="waveforms.zip")
    extracted = manager.extract_zip(zip_path, export_type="asc2")

    assert len(extracted) == 1
    assert extracted[0].endswith("wave.asc")
    assert not zip_path.exists() if hasattr(zip_path, "exists") else True


def test_afad_file_manager_extracts_nested_zip(tmp_path):
    nested = _zip_bytes({"inner.asc": b"1234567890" * 20})
    outer = _zip_bytes({"nested.zip": nested})
    manager = AfadFileManager(base_dir=str(tmp_path))

    zip_path = manager.save_zip(outer, event_id=7, filename="outer.zip")
    extracted = manager.extract_zip(zip_path, export_type="asc2")

    assert any(path.endswith("inner.asc") for path in extracted)


def test_afad_file_manager_rejects_too_small_zip(tmp_path):
    manager = AfadFileManager(base_dir=str(tmp_path))
    zip_path = manager.save_zip(b"bad", event_id=1, filename="bad.zip")

    try:
        manager.extract_zip(zip_path)
    except ProviderError as exc:
        assert "Extraction failed" in str(exc)
    else:
        raise AssertionError("ProviderError expected")


def test_afad_file_manager_rejects_non_zip_error_page(tmp_path):
    manager = AfadFileManager(base_dir=str(tmp_path))
    zip_path = manager.save_zip(b"<html>not a zip</html>", event_id=1, filename="bad.zip")

    try:
        manager.extract_zip(zip_path)
    except ProviderError as exc:
        assert "Extraction failed" in str(exc)
    else:
        raise AssertionError("ProviderError expected")


def test_afad_file_manager_rejects_unsafe_zip_member(tmp_path):
    manager = AfadFileManager(base_dir=str(tmp_path))
    content = _zip_bytes({"../escape.asc": b"1234567890" * 20})
    zip_path = manager.save_zip(content, event_id=1, filename="unsafe.zip")

    try:
        manager.extract_zip(zip_path)
    except ProviderError as exc:
        assert "Extraction failed" in str(exc)
    else:
        raise AssertionError("ProviderError expected")


def test_provider_registry_get_all_and_list_names():
    peer = MagicMock()
    peer.get_name.return_value = "PEER"
    afad = MagicMock()
    afad.get_name.return_value = "AFAD"
    registry = ProviderRegistry([peer, afad])

    assert registry.get("PEER") is peer
    assert registry.get("missing") is None
    assert registry.all() == [peer, afad]
    assert registry.list_names() == ["PEER", "AFAD"]


def test_provider_registry_build_uses_factory():
    provider = MagicMock()
    provider.get_name.return_value = "PEER"

    with patch(
        "selection_service.services.ProviderRegistry.ProviderFactory.create_provider",
        return_value=provider,
    ) as create_provider:
        registry = ProviderRegistry.build(["PEER"], use_cache=False, data_dir="x")

    assert registry.get("PEER") is provider
    create_provider.assert_called_once_with("PEER", use_cache=False, data_dir="x")


def test_waveform_download_batch_calls_supported_provider():
    provider = MagicMock()
    provider.download_waveforms_batch.return_value = {"downloaded": 2}
    registry = MagicMock()
    registry.get.return_value = provider
    df = pd.DataFrame({
        "PROVIDER": ["AFAD", "AFAD"],
        "FILE_NAME_H1": ["a.mseed", "b.mseed"],
        "EVENT": [1, 2],
    })

    with patch("selection_service.services.WaveformDownloadService.supports_download", return_value=True):
        result = WaveformDownloadService(registry).download_batch(df, export_type="mseed")

    assert result.success is True
    provider.download_waveforms_batch.assert_called_once()


def test_waveform_download_batch_fails_when_supported_provider_fails():
    provider = MagicMock()
    provider.download_waveforms_batch.return_value = Result.fail(
        ProviderError("AFAD", Exception("broken zip"), "download failed")
    )
    registry = MagicMock()
    registry.get.return_value = provider
    df = pd.DataFrame({
        "PROVIDER": ["AFAD"],
        "FILE_NAME_H1": ["a.mseed"],
        "EVENT": [1],
    })

    with patch("selection_service.services.WaveformDownloadService.supports_download", return_value=True):
        result = WaveformDownloadService(registry).download_batch(df, export_type="mseed")

    assert result.success is False
    assert isinstance(result.error, ProviderError)


def test_waveform_download_batch_fails_when_no_files_downloaded():
    provider = MagicMock()
    provider.download_waveforms_batch.return_value = {
        "total": 1,
        "downloaded": 0,
        "batches": [{"batch": 1, "success": False, "error": "corrupt"}],
    }
    registry = MagicMock()
    registry.get.return_value = provider
    df = pd.DataFrame({
        "PROVIDER": ["AFAD"],
        "FILE_NAME_H1": ["a.mseed"],
        "EVENT": [1],
    })

    with patch("selection_service.services.WaveformDownloadService.supports_download", return_value=True):
        result = WaveformDownloadService(registry).download_batch(df, export_type="mseed")

    assert result.success is False


def test_waveform_download_batch_requires_provider_column():
    result = WaveformDownloadService(MagicMock()).download_batch(pd.DataFrame({"x": [1]}))

    assert result.success is False
    assert isinstance(result.error, ProviderError)


def test_waveform_download_single_success_and_not_supported():
    provider = MagicMock()
    registry = MagicMock()
    registry.get.return_value = provider
    service = WaveformDownloadService(registry)

    with patch("selection_service.services.WaveformDownloadService.supports_download", return_value=True):
        ok = service.download_single("a.mseed", "1", "AFAD.TK.KND")
    assert ok.success is True
    provider.download_single_waveforms.assert_called_once()

    with patch("selection_service.services.WaveformDownloadService.supports_download", return_value=False):
        failed = service.download_single("a.mseed", "1", "AFAD.TK.KND")
    assert failed.success is False


def test_waveform_download_single_fails_when_provider_returns_fail():
    provider = MagicMock()
    provider.download_single_waveforms.return_value = Result.fail(
        ProviderError("AFAD", Exception("bad zip"), "download failed")
    )
    registry = MagicMock()
    registry.get.return_value = provider
    service = WaveformDownloadService(registry)

    with patch("selection_service.services.WaveformDownloadService.supports_download", return_value=True):
        result = service.download_single("a.mseed", "1", "AFAD.TK.KND")

    assert result.success is False


def test_earthquake_query_service_run_sync_and_reselection():
    provider = MagicMock()
    registry = MagicMock()
    registry.all.return_value = [provider]
    registry.list_names.return_value = ["PEER"]
    strategy = MagicMock()
    strategy.get_name.return_value = "strategy"
    pipeline = MagicMock()
    pipeline.execute_sync.return_value = Result.ok(
        PipelineResult(
            selected_df=pd.DataFrame({"RSN": [1]}),
            scored_df=pd.DataFrame({"RSN": [1]}),
            report={"status": "success"},
            execution_time=0.1,
        )
    )
    pipeline.reporter.generate_report.return_value = {"status": "success"}

    service = EarthquakeQueryService(registry, [strategy], pipeline=pipeline)
    criteria = MagicMock()

    result = service.run_sync(criteria, "strategy")

    assert result.success is True
    assert service.list_strategies() == ["strategy"]
    assert service.list_providers() == ["PEER"]


def test_earthquake_query_service_missing_strategy_and_async():
    registry = MagicMock()
    registry.all.return_value = []
    pipeline = MagicMock()
    service = EarthquakeQueryService(registry, [], pipeline=pipeline)

    failed = service.run_sync(MagicMock(), "missing")
    assert failed.success is False

    async_failed = asyncio.run(service.run_async(MagicMock(), "missing"))
    assert async_failed.success is False
