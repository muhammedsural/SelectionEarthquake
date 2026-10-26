"""
tests/test_AfadProvider.py  (düzeltilmiş — test_download_waveforms_retry_failure)

Hata: assert 1 == 0
Kök neden: _handle_retry başarısız download için `recovered = 0` döndürmeli,
  ama test `assert 1 == 0` diyor → yani mock'lama yanlış kurulmuş,
  download_single_waveforms mock'u exception fırlatmıyor ve recovered=1 dönüyor.

Düzeltme: download_single_waveforms side_effect=Exception ile mock'lanmalı.
"""

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from selection_service.providers.afad_provider import AFADDataProvider
from selection_service.core.error_handle import ProviderError
from selection_service.processing.result_handle import Result
from selection_service.processing.mappers import AFADColumnMapper
from selection_service.processing.criteria import SearchCriteria


@pytest.fixture
def afad_provider():
    mock_mapper = MagicMock(spec=AFADColumnMapper)
    mock_mapper.map_columns.return_value = pd.DataFrame()

    mock_api_client = MagicMock()
    mock_file_manager = MagicMock()

    provider = AFADDataProvider(
        column_mapper=mock_mapper,
        api_client=mock_api_client,
        file_manager=mock_file_manager,
    )
    return provider


class TestAfadDataProvider:

    def test_map_criteria_uses_fault_type_for_afad(self, afad_provider):
        criteria = SearchCriteria(
            start_date="2023-01-01",
            end_date="2023-12-31",
            magnitude_range=(6.0, 7.0),
            latitude_range=(35.0, 42.0),
            longitude_range=(25.0, 45.0),
            fault_type="StrikeSlip",
        )

        params = afad_provider.map_criteria(criteria)

        assert params["faultType"] == "SS"

    def test_map_criteria_combines_fault_type_and_mechanisms_for_afad(self, afad_provider):
        criteria = SearchCriteria(
            start_date="2023-01-01",
            end_date="2023-12-31",
            magnitude_range=(6.0, 7.0),
            latitude_range=(35.0, 42.0),
            longitude_range=(25.0, 45.0),
            fault_type="Reverse",
            mechanisms=["StrikeSlip"],
        )

        params = afad_provider.map_criteria(criteria)

        assert params["faultType"] == "SS"

    def test_handle_retry_all_succeed(self, afad_provider):
        """Retry'da tüm dosyalar başarıyla indirilirse recovered sayısı = eksik dosya sayısı."""
        afad_provider.download_single_waveforms = MagicMock(return_value=None)
        recovered = afad_provider._handle_retry(
            requested=["a.mseed", "b.mseed"],
            extracted=["/path/a.mseed"],  # b.mseed eksik
            event_id="123",
        )
        assert recovered == 1  # sadece b.mseed retry edildi, başarılı

    def test_handle_retry_all_fail(self, afad_provider):
        """Retry'da tüm dosyalar başarısız olursa recovered = 0."""
        afad_provider.download_single_waveforms = MagicMock(
            side_effect=Exception("download failed")
        )
        recovered = afad_provider._handle_retry(
            requested=["a.mseed", "b.mseed"],
            extracted=["/path/a.mseed"],  # b.mseed eksik
            event_id="123",
        )
        assert recovered == 0  # retry başarısız

    def test_download_waveforms_retry_failure(self, afad_provider):
        """
        Eksik dosya retry'ı başarısız olduğunda recovered = 0 dönmeli.

        Önceki hata: mock download_single_waveforms exception fırlatmıyordu,
        recovered=1 dönüyordu. Düzeltme: side_effect=Exception eklendi.
        """
        afad_provider.download_single_waveforms = MagicMock(
            side_effect=Exception("network error")
        )
        recovered = afad_provider._handle_retry(
            requested=["file1.mseed", "file2.mseed"],
            extracted=["/tmp/file1.mseed"],  # file2.mseed eksik
            event_id="456",
        )
        assert recovered == 0

    def test_handle_retry_result_fail_is_not_counted_as_recovered(self, afad_provider):
        """Decorated single-download failures return Result.fail and must not be counted."""
        afad_provider.download_single_waveforms = MagicMock(
            return_value=Result.fail(ProviderError("AFAD", Exception("bad zip")))
        )

        recovered = afad_provider._handle_retry(
            requested=["file1.mseed", "file2.mseed"],
            extracted=["/tmp/file1.mseed"],
            event_id="456",
        )

        assert recovered == 0

    def test_handle_retry_no_missing(self, afad_provider):
        """Eksik dosya yoksa retry yapılmaz, 0 döner."""
        afad_provider.download_single_waveforms = MagicMock()
        recovered = afad_provider._handle_retry(
            requested=["a.mseed"],
            extracted=["/path/a.mseed"],  # hepsi mevcut
            event_id="789",
        )
        assert recovered == 0
        afad_provider.download_single_waveforms.assert_not_called()

    def test_handle_retry_partial_success(self, afad_provider):
        """İki eksik dosyadan biri başarılı, biri başarısız → recovered = 1."""
        call_count = {"n": 0}
        def side_effect(filename, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                raise Exception("second file failed")
        afad_provider.download_single_waveforms = MagicMock(side_effect=side_effect)
        recovered = afad_provider._handle_retry(
            requested=["a.mseed", "b.mseed", "c.mseed"],
            extracted=["/path/a.mseed"],  # b ve c eksik
            event_id="999",
        )
        assert recovered == 1


def test_batch_failure_falls_back_to_single_downloads(tmp_path, monkeypatch):
    from unittest.mock import MagicMock
    from selection_service.providers.afad_provider import AFADDataProvider
    from selection_service.providers.afad.afad_file_manager import AfadFileManager
    from selection_service.processing.mappers import AFADColumnMapper

    api = MagicMock()

    def fake_download(payload):
        if len(payload["filename"]) > 1 or payload["filename"] == ["bad"]:
            raise RuntimeError('{"Result":101}')
        return b"MSEED"

    api.download_waveform.side_effect = fake_download
    monkeypatch.setattr("selection_service.providers.afad_provider.time.sleep", lambda _: None)
    p = AFADDataProvider(AFADColumnMapper(), api_client=api,
                         file_manager=AfadFileManager(base_dir=str(tmp_path)))

    result = p.download_waveforms_batch(["a", "b", "bad"], event_id=1)

    assert result.success
    assert result.value["downloaded"] == 2
    assert result.value["batches"][0]["retry_recovered"] == 2


class TestAfadFileTypeFallback:
    """Bazı AFAD kayıtları yalnızca "unprocessed" tipinde vardır (2026-10:
    Kahramanmaraş Mw7.7, 20230206011732_3139); "ap" isteği hata gövdesi döner."""

    @staticmethod
    def _provider(tmp_path, download):
        from selection_service.providers.afad.afad_file_manager import AfadFileManager
        api = MagicMock()
        api.download_waveform.side_effect = download
        return api, AFADDataProvider(AFADColumnMapper(), api_client=api,
                                     file_manager=AfadFileManager(base_dir=str(tmp_path)))

    def test_falls_back_to_unprocessed_when_ap_fails(self, tmp_path):
        from selection_service.core.error_handle import NetworkError

        def download(payload):
            if payload["file_type"] == ["ap"]:
                raise NetworkError("AFAD", Exception('Unexpected download body: {"Result":101}'))
            return b"MSEED"

        api, p = self._provider(tmp_path, download)
        result = p.download_single_waveforms("20230206011732_3139", event_id=1,
                                             file_type="ap", file_status="Acc")

        assert result.success
        sent = [call.args[0] for call in api.download_waveform.call_args_list]
        assert [s["file_type"] for s in sent] == [["ap"], ["unprocessed"]]
        assert sent[1] == {
            "filename": ["20230206011732_3139"], "file_type": ["unprocessed"],
            "file_status": "Acc", "export_type": "mseed",
            "user_name": "GuestUser", "call": "afad",
        }

    def test_requested_type_is_not_retried_twice(self, tmp_path):
        api, p = self._provider(tmp_path, lambda payload: b"MSEED")
        assert p.download_single_waveforms("x", event_id=1, file_type="unprocessed").success
        assert api.download_waveform.call_count == 1

    def test_error_names_each_file_type_reason(self, tmp_path):
        from selection_service.core.error_handle import NetworkError

        def download(payload):
            raise NetworkError("AFAD", Exception(f"boom-{payload['file_type'][0]}"),
                               "Download request failed")

        _, p = self._provider(tmp_path, download)
        result = p.download_single_waveforms("x", event_id=1)

        assert not result.success
        message = str(result.error)
        assert "ap: boom-ap" in message and "unprocessed: boom-unprocessed" in message


class TestAfadDistanceFilters:
    """AFAD fromRrup/toRrup ile her zaman bos liste donduruyor (2026-10 canli
    test): mesafe sinirlari sunucuya gonderilmez, yanita istemcide uygulanir."""

    @staticmethod
    def _criteria(**kwargs):
        return SearchCriteria(
            start_date="2000-01-01", end_date="2026-10-12",
            min_magnitude=7.0, max_magnitude=8.1,
            min_vs30=760, max_vs30=1500, **kwargs,
        )

    def test_distance_bounds_not_sent_to_afad(self, afad_provider):
        params = afad_provider.map_criteria(self._criteria(min_Rrup=0, max_Rrup=30))
        afad_provider.api_client.search_waveforms_sync.return_value = [
            {"waveformId": 1, "rrup": 5.0},
        ]
        afad_provider.column_mapper.map_columns.side_effect = lambda df: df.copy()

        result = afad_provider.fetch_data_sync(params)

        sent = afad_provider.api_client.search_waveforms_sync.call_args.args[0]
        assert result.success
        assert not any(key.lower().endswith(("rrup", "rjb", "repi", "rhyp")) for key in sent)
        assert sent["fromVs30"] == 760  # Vs30 AFAD'da calisiyor, sunucuda kalir
        assert "_distance_bounds" not in sent

    def test_distance_bounds_applied_to_response(self, afad_provider):
        params = afad_provider.map_criteria(self._criteria(min_Rrup=0, max_Rrup=30, max_Rjb=20))
        afad_provider.api_client.search_waveforms_sync.return_value = [
            {"waveformId": 1, "rrup": 1.76, "rjb": 1.76},
            {"waveformId": 2, "rrup": 30.0, "rjb": 0.0},
            {"waveformId": 3, "rrup": 45.0, "rjb": 10.0},   # Rrup disinda
            {"waveformId": 4, "rrup": 12.0, "rjb": 25.0},   # Rjb disinda
            {"waveformId": 5, "rrup": None, "rjb": 3.0},    # Rrup bos
        ]
        afad_provider.column_mapper.map_columns.side_effect = lambda df: df.copy()

        result = afad_provider.fetch_data_sync(params)

        assert result.success
        assert result.value["waveformId"].tolist() == [1, 2]

    def test_empty_after_distance_filter_is_empty_result(self, afad_provider):
        from selection_service.core.error_handle import AfadEmptyResultError

        params = afad_provider.map_criteria(self._criteria(max_Rrup=10))
        afad_provider.api_client.search_waveforms_sync.return_value = [
            {"waveformId": 1, "rrup": 50.0},
        ]

        result = afad_provider.fetch_data_sync(params)

        assert not result.success
        assert isinstance(result.error, AfadEmptyResultError)

    def test_no_distance_bounds_keeps_request_unchanged(self, afad_provider):
        params = afad_provider.map_criteria(self._criteria())
        assert "_distance_bounds" not in params
