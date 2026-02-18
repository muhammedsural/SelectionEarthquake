import pytest
import pandas as pd
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from selection_service.core.EarthquakeApi import EarthquakeAPI
from selection_service.enums.Enums import ProviderName
from selection_service.processing.Selection import SearchCriteria, ISelectionStrategy
from selection_service.providers.IProvider import IDataProvider
from selection_service.processing.ResultHandle import Result
from selection_service.core.ErrorHandle import ProviderError, StrategyError, PipelineError
from selection_service.core.Pipeline import PipelineResult

# --- Fixtures ---

@pytest.fixture
def mock_strategy():
    strategy = MagicMock(spec=ISelectionStrategy)
    strategy.get_name.return_value = "TestStrategy"
    # select_and_score dönüş değerleri (selected_df, scored_df)
    strategy.select_and_score.return_value = (pd.DataFrame({'A': [1]}), pd.DataFrame({'A': [1, 2]}))
    return strategy

@pytest.fixture
def mock_provider():
    provider = MagicMock(spec=IDataProvider)
    provider.get_name.return_value = "AFAD"
    return provider

@pytest.fixture
def mock_pipeline_result():
    return PipelineResult(
        selected_df=pd.DataFrame(),
        scored_df=pd.DataFrame(),
        report={},
        execution_time=1.0
    )

@pytest.fixture
def earthquake_api(mock_strategy, mock_provider):
    # ProviderFactory ve Pipeline'ı mocklayalım
    with patch("selection_service.core.EarthquakeApi.ProviderFactory") as MockFactory, \
         patch("selection_service.core.EarthquakeApi.EarthquakePipeline") as MockPipeline:
        
        MockFactory.return_value.create_provider.return_value = mock_provider
        
        api = EarthquakeAPI(
            provider_names=[ProviderName.AFAD],
            strategies=[mock_strategy],
            use_cache=False
        )
        return api

# --- Tests ---

class TestEarthquakeAPI:

    def test_init(self, mock_strategy):
        """Doğru başlatıldığını kontrol et"""
        with patch("selection_service.core.EarthquakeApi.ProviderFactory") as MockFactory:
            api = EarthquakeAPI(
                provider_names=[ProviderName.AFAD],
                strategies=[mock_strategy],
                use_cache=True,
                extra_param="value"
            )
            
            # Factory doğru parametrelerle çağrıldı mı?
            MockFactory.return_value.create_provider.assert_called_with(
                ProviderName.AFAD, 
                use_cache=True, 
                extra_param="value"
            )
            assert "TestStrategy" in api.strategies
            assert len(api.providers) == 1

    def test_run_sync_success(self, earthquake_api, mock_pipeline_result):
        """Senkron çalıştırma başarılı"""
        earthquake_api.pipeline.execute_sync.return_value = Result.ok(mock_pipeline_result)
        criteria = MagicMock(spec=SearchCriteria)
        
        result = earthquake_api.run_sync(criteria, "TestStrategy")
        
        assert result.success is True
        assert result.value == mock_pipeline_result
        earthquake_api.pipeline.execute_sync.assert_called_once()

    def test_run_sync_invalid_strategy(self, earthquake_api):
        """Geçersiz strateji ismi hatası"""
        criteria = MagicMock(spec=SearchCriteria)
        result = earthquake_api.run_sync(criteria, "InvalidStrategy")
        
        assert result.success is False
        assert isinstance(result.error, ValueError)

    @pytest.mark.asyncio
    async def test_run_async_success(self, earthquake_api, mock_pipeline_result):
        """Asenkron çalıştırma başarılı"""
        # AsyncMock ile execute_async'i yapılandır
        earthquake_api.pipeline.execute_async = AsyncMock(return_value=Result.ok(mock_pipeline_result))
        criteria = MagicMock(spec=SearchCriteria)
        
        result = await earthquake_api.run_async(criteria, "TestStrategy")
        
        assert result.success is True
        assert result.value == mock_pipeline_result
        earthquake_api.pipeline.execute_async.assert_called_once()

    def test_download_waveforms_success(self, earthquake_api):
        """Batch download başarılı senaryo"""
        # Test verisi hazırla
        df = pd.DataFrame({
            'PROVIDER': ['AFAD', 'AFAD'],
            'FILE_NAME_H1': ['f1', 'f2'],
            'EVENT': [101, 102]
        })
        
        # Provider mock davranışını ayarla
        afad_provider = earthquake_api.providers[0]
        
        result = earthquake_api.download_waveforms(df)
        
        assert result.success is True
        # Provider'ın batch download metodu çağrıldı mı?
        afad_provider.download_waveforms_batch.assert_called_once()
        
        # Argümanları kontrol et
        call_args = afad_provider.download_waveforms_batch.call_args[1]
        assert call_args['filenames'] == ['f1', 'f2']
        assert call_args['event_ids'] == [101, 102]

    def test_download_waveforms_peer_skip(self, earthquake_api):
        """PEER provider'ın batch indirmeyi atlaması gerektiğini doğrula"""
        # PEER provider ekle
        peer_provider = MagicMock()
        peer_provider.get_name.return_value = ProviderName.PEER.value
        earthquake_api.providers.append(peer_provider)
        
        df = pd.DataFrame({
            'PROVIDER': [ProviderName.PEER.value],
            'FILE_NAME_H1': ['peer_file']
        })
        
        result = earthquake_api.download_waveforms(df)
        
        assert result.success is True
        # PEER için batch çağrılmamalı
        peer_provider.download_waveforms_batch.assert_not_called()

    def test_download_waveforms_missing_provider(self, earthquake_api):
        """Listede olmayan bir provider ismi gelirse uyarı verip devam etmeli"""
        df = pd.DataFrame({
            'PROVIDER': ['UNKNOWN_PROVIDER', ProviderName.AFAD.value],
            'FILE_NAME_H1': ['f1','f2'],
        })
        
        # print uyarısı verir ama işlem başarılı döner (kod mantığına göre)
        result = earthquake_api.download_waveforms(df)
        assert result.success is True

    def test_download_waveforms_exception(self, earthquake_api):
        """İndirme sırasında hata oluşursa Result.fail dönmeli"""
        df = pd.DataFrame({
            'PROVIDER': ['AFAD'],
            'FILE_NAME_H1': ['f1']
        })
        
        # Hata fırlat
        earthquake_api.providers[0].download_waveforms_batch.side_effect = Exception("Download Error")
        
        result = earthquake_api.download_waveforms(df)
        
        assert result.success is False
        assert isinstance(result.error, ProviderError)
        assert "Bulk download failed" in str(result.error)

    def test_re_selection_success(self, earthquake_api):
        """Re-selection mantığı başarılı"""
        df = pd.DataFrame({'A': [1]})
        new_criteria = MagicMock(spec=SearchCriteria)
        
        # Pipeline reporter mockla
        earthquake_api.pipeline.reporter.generate_report.return_value = {"status": "ok"}
        
        result = earthquake_api.re_selection(df, "TestStrategy", new_criteria)
        
        assert result.success is True
        assert isinstance(result.value, PipelineResult)
        assert result.value.report == {"status": "ok"}
        
        # Strateji tekrar çalıştırıldı mı?
        earthquake_api.strategies["TestStrategy"].select_and_score.assert_called_with(df, new_criteria)

    def test_re_selection_strategy_fail(self, earthquake_api):
        """Re-selection sırasında strateji hata verirse"""
        df = pd.DataFrame()
        new_criteria = MagicMock()
        
        # Strateji hata fırlatsın
        earthquake_api.strategies["TestStrategy"].select_and_score.side_effect = Exception("Calc Error")
        
        result = earthquake_api.re_selection(df, "TestStrategy", new_criteria)
        
        assert result.success is False
        assert isinstance(result.error, StrategyError)

    def test_re_selection_invalid_strategy(self, earthquake_api):
        """Geçersiz strateji ismi ile re-selection"""
        result = earthquake_api.re_selection(pd.DataFrame(), "Invalid", MagicMock())
        assert result.success is False
        assert isinstance(result.error, ValueError)

    def test_download_single_waveform_success(self, earthquake_api):
        """Tekli indirme başarılı"""
        # AFAD provider'ı bulmak için station_code 'AFAD' ile başlasın (kod mantığına göre split('.')[0])
        # Ancak kodda station_code.split('.')[0] kullanılıyor. Provider ismiyle eşleşmeli.
        # AFAD provider ismini "AFAD" olarak mockladık.
        
        result = earthquake_api.download_single_waveform(
            filename="file.ms", 
            event_id="123", 
            station_code="AFAD.Station1"
        )
        
        assert result.success is True
        earthquake_api.providers[0].download_single_waveforms.assert_called_once()
        
        # Parametre kontrolü
        kwargs = earthquake_api.providers[0].download_single_waveforms.call_args[1]
        assert kwargs['filename'] == "file.ms"
        assert kwargs['event_id'] == "123"

    def test_download_single_waveform_provider_not_found(self, earthquake_api):
        """Provider bulunamazsa hata dönmeli"""
        result = earthquake_api.download_single_waveform(
            filename="file.ms", 
            event_id="123", 
            station_code="UNKNOWN.Station1"
        )
        
        assert result.success is False
        assert isinstance(result.error, ProviderError)
        assert "Download failed" in str(result.error)

    def test_download_single_waveform_exception(self, earthquake_api):
        """Tekli indirmede exception"""
        earthquake_api.providers[0].download_single_waveforms.side_effect = Exception("Single Fail")
        
        result = earthquake_api.download_single_waveform(
            filename="file.ms", 
            event_id="123", 
            station_code="AFAD.Station1"
        )
        
        assert result.success is False
        assert "Single waveform download failed" in str(result.error)

    def test_get_provider_helper(self, earthquake_api):
        """_get_provider metodunun doğru çalışması"""
        # AFAD var
        p = earthquake_api._get_provider("AFAD")
        assert p is not None
        assert p.get_name() == "AFAD"
        
        # XYZ yok
        p2 = earthquake_api._get_provider("XYZ")
        assert p2 is None