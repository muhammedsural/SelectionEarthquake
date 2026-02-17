import pytest
import pandas as pd
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock
from selection_service.processing.ResultHandle import Result
from selection_service.providers.AfadProvider import AFADDataProvider
from selection_service.processing.Selection import SearchCriteria
from selection_service.core.ErrorHandle import ProviderError

# --- Fixtures (Test Ortamı Hazırlığı) ---

@pytest.fixture
def mock_mapper():
    """Column Mapper Mock'u"""
    mapper = MagicMock()
    # map_columns çağrıldığında kendisine gelen dataframe'i olduğu gibi geri döndürsün
    mapper.map_columns.side_effect = lambda df: df
    return mapper

@pytest.fixture
def mock_api_client():
    """AfadApiClient Mock'u"""
    # AsyncMock, await edilebilir metotlar için gereklidir
    with patch("selection_service.providers.AfadProvider.AfadApiClient") as MockClient:
        client_instance = MockClient.return_value
        # search_waveforms_async metodunu açıkça AsyncMock yapıyoruz
        client_instance.search_waveforms_async = AsyncMock()
        yield client_instance

@pytest.fixture
def mock_file_manager():
    """AfadFileManager Mock'u"""
    with patch("selection_service.providers.AfadProvider.AfadFileManager") as MockManager:
        manager_instance = MockManager.return_value
        yield manager_instance

@pytest.fixture
def afad_provider(mock_mapper, mock_api_client, mock_file_manager):
    """
    Test edilecek ana Provider nesnesi.
    Dependencies otomatik olarak mocklanmış şekilde gelir.
    """
    provider = AFADDataProvider(column_mapper=mock_mapper)
    # Constructor içinde oluşan nesneleri fixture'dan gelenlerle değiştiriyoruz
    provider.api_client = mock_api_client
    provider.file_manager = mock_file_manager
    return provider

# --- Test Senaryoları ---

class TestAfadDataProvider:

    # 1. Initialization Testi
    def test_init(self, mock_mapper):
        """Provider'ın doğru başlatıldığını kontrol et"""
        with patch("selection_service.providers.AfadProvider.AfadApiClient") as MockClient, \
             patch("selection_service.providers.AfadProvider.AfadFileManager") as MockManager:
            
            provider = AFADDataProvider(column_mapper=mock_mapper, timeout=45)
            
            assert provider.name == "AFAD"
            # ApiClient'ın timeout ile çağrıldığını kontrol et
            MockClient.assert_called_with(timeout=45)
            MockManager.assert_called_with(base_dir="Afad_events")

    # 2. Map Criteria Testi
    def test_map_criteria(self, afad_provider):
        """Arama kriterlerinin doğru map edildiğini kontrol et"""
        mock_criteria = MagicMock(spec=SearchCriteria)
        mock_criteria.to_afad_params.return_value = {"param": "value"}
        
        result = afad_provider.map_criteria(mock_criteria)
        
        assert result == {"param": "value"}
        mock_criteria.to_afad_params.assert_called_once()

    # 3. Fetch Data Async (Başarılı) - DÜZELTİLEN TEST
    @pytest.mark.asyncio
    async def test_fetch_data_async_success(self, afad_provider):
        """Asenkron veri çekme - Başarılı Senaryo"""
        # Mock Setup: API Client ham veri (List[Dict]) döner
        raw_data = [{"col1": 1}, {"col1": 2}]
        
        # AsyncMock kullanımı: return_value await edildiğinde bu değeri döner
        afad_provider.api_client.search_waveforms_async.return_value = raw_data
        
        # Action
        criteria = {"min_magnitude": 5.0}
        result = await afad_provider.fetch_data_async(criteria)
        
        # Assertion
        assert result.success is True
        assert isinstance(result.value, pd.DataFrame)
        assert len(result.value) == 2
        # Dataframe içeriğini kontrol et
        assert result.value.iloc[0]["col1"] == 1
        
        # Doğru metodun çağrıldığını kontrol et
        afad_provider.api_client.search_waveforms_async.assert_called_once_with(criteria)
        afad_provider.column_mapper.map_columns.assert_called_once()

    # 4. Fetch Data Async (Başarısız - API Hatası) - DÜZELTİLEN TEST
    @pytest.mark.asyncio
    async def test_fetch_data_async_failure(self, afad_provider):
        """Asenkron veri çekme - API Hatası Senaryosu"""
        # Mock Setup: Hata fırlatması için side_effect
        afad_provider.api_client.search_waveforms_async.side_effect = Exception("Connection Timeout")
        
        # Action
        result = await afad_provider.fetch_data_async({})
        
        # Assertion
        # Result decorator hatayı yakalayıp Result.fail dönmeli
        assert result.success is False
        assert "Connection Timeout" in str(result.error)

    # 5. Fetch Data Sync (Başarılı)
    def test_fetch_data_sync_success(self, afad_provider):
        """Senkron veri çekme - Başarılı Senaryo"""
        # Sync metod için standart MagicMock yeterlidir
        raw_data = [{"col1": 10}]
        afad_provider.api_client.search_waveforms_sync.return_value = raw_data
        
        result = afad_provider.fetch_data_sync({})
        
        assert result.success is True
        assert result.value.iloc[0]["col1"] == 10

    # 6. Download Waveforms Batch - Happy Path
    def test_download_waveforms_batch_success(self, afad_provider):
        """Tüm dosyaların sorunsuz indiği senaryo"""
        filenames = ["f1.mseed", "f2.mseed"]
        event_ids = [101, 101]
        
        # Mocklar
        afad_provider.api_client.download_waveform.return_value = b"zip_content"
        afad_provider.file_manager.save_zip.return_value = "path/to/zip"
        afad_provider.file_manager.extract_zip.return_value = filenames 

        results_wrapper = afad_provider.download_waveforms_batch(filenames, event_ids=event_ids)
        results = results_wrapper.unwrap() # Veya results_wrapper.value
        
        assert results['total'] == 2
        assert results['downloaded'] == 2
        assert len(results['batches']) == 1
        assert results['batches'][0]['success'] is True
        
        # Payload hazırlayıcı çağrılmış mı?
        # Not: _prepare_download_payload private metod ama dolaylı yoldan api_client çağrısını kontrol edebiliriz
        afad_provider.api_client.download_waveform.assert_called()

    # 7. Download Waveforms Batch - Multiple Events
    
    # 8. Download Waveforms Batch - Retry Success
    def test_download_waveforms_retry_success(self, afad_provider):
        """Eksik dosya durumunda retry başarısı"""
        filenames = ["f1", "f2"]
        event_ids = 101
        
        # İlk çağrıda f2 eksik, ikinci çağrıda (retry) f2 geliyor
        afad_provider.file_manager.extract_zip.side_effect = [
            ["f1"], # İlk deneme
            ["f2"]  # Retry denemesi
        ]
        
        afad_provider.api_client.download_waveform.return_value = b"content"

        results_wrapper = afad_provider.download_waveforms_batch(filenames, event_ids=event_ids)
        results = results_wrapper.unwrap()

        assert results['downloaded'] == 2
        # extract_zip toplam 2 kere çağrılmalı
        assert afad_provider.file_manager.extract_zip.call_count == 2

    # 9. Download Waveforms Batch - Retry Failure
    def test_download_waveforms_retry_failure(self, afad_provider):
        """Retry mekanizmasının başarısız olması"""
        filenames = ["f1"]
        event_ids = [101]
        
        # Dosya hiç inmiyor
        afad_provider.file_manager.extract_zip.return_value = []
        
        # Sleep'i hızlandır
        with patch("time.sleep"):
            results_wrapper = afad_provider.download_waveforms_batch(filenames, event_ids=event_ids)

        results = results_wrapper.unwrap()
        assert results['downloaded'] == 0
        assert results['batches'][0]['success'] is True # Batch işlemi teknik olarak bitti ama count 0
        # Veya koduna göre success False dönüyorsa:
        # assert results['batches'][0]['success'] is False

    # 10. API Exception in Batch
    def test_download_waveforms_batch_api_exception(self, afad_provider):
        """Batch sırasında API hatası"""
        afad_provider.api_client.download_waveform.side_effect = Exception("Network Error")
        
        results_wrapper = afad_provider.download_waveforms_batch(["f1"], event_ids=[101])
        results = results_wrapper.unwrap()
        
        assert results['batches'][0]['success'] is False
        assert "Network Error" in str(results['batches'][0]['error'])

    # 11. Download Single Waveform
    def test_download_single_waveform(self, afad_provider):
        """Tekli indirme testi - Yeni Mimariye Uygun"""
        
        filename = "test.mseed"
        event_id = 999
        
        # Metodun içindeki bağımlılıkları mocklayalım
        with patch.object(afad_provider.api_client, 'download_waveform') as mock_download, \
            patch.object(afad_provider.file_manager, 'save_zip') as mock_save, \
            patch.object(afad_provider.file_manager, 'extract_zip') as mock_extract:
            
            # Mock dönüş değerlerini ayarla
            mock_download.return_value = b"fake_zip_content"
            mock_save.return_value = "path/to/fake.zip"
            mock_extract.return_value = [filename]
            
            # Çalıştır
            result = afad_provider.download_single_waveforms(filename, event_id=event_id, export_type="mseed")
            
            # Sonucu kontrol et (result_decorator olduğu için .success kontrolü yapılır)
            assert result.success is True
            
            # API Client doğru çağrıldı mı?
            mock_download.assert_called_once()
            payload = mock_download.call_args[0][0]
            assert filename in payload['filename']
            
            # File Manager doğru kaydedildi mi?
            # save_zip(content, event_id, zip_name) sırasıyla
            mock_save.assert_called_once_with(b"fake_zip_content", event_id, f"waveforms_{event_id}_unknown.zip")
            
            # File Manager doğru ayıkladı mı?
            mock_extract.assert_called_once_with("path/to/fake.zip", "mseed") # Varsayılan mseed ise

    # 12. Helper: Prepare Payload
    def test_prepare_download_payload(self, afad_provider):
        """Payload hazırlama testi"""
        filenames = ["f1", "f2"]
        kwargs = {"user_name": "TestUser", "export_type": "mseed"}
        
        payload = afad_provider._prepare_download_payload(filenames, **kwargs)
        
        assert payload["filename"] == filenames
        assert payload["user_name"] == "TestUser"
        assert payload["export_type"] == "mseed"
        assert payload["call"] == "afad"