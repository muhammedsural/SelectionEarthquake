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

from selection_service.providers.AfadProvider import AFADDataProvider
from selection_service.core.ErrorHandle import ProviderError
from selection_service.processing.Mappers import AFADColumnMapper
from selection_service.processing.Selection import SearchCriteria


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
