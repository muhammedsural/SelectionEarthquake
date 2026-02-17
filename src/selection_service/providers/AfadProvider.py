import io
import os
import time
from typing import Any, Dict, List, Type
import zipfile
import aiohttp
import pandas as pd
import requests
from ..providers.IProvider import IDataProvider
from ..enums.Enums import ProviderName
from ..processing.Mappers import IColumnMapper
from ..processing.Selection import SearchCriteria
from ..core.ErrorHandle import ProviderError
from ..processing.ResultHandle import async_result_decorator, result_decorator

from .afad.AfadApiClient import AfadApiClient
from .afad.AfadFileManager import AfadFileManager

class AFADDataProvider(IDataProvider):
    """AFAD veri sağlayıcı (Refactored)"""

    def __init__(self, column_mapper: Type[IColumnMapper], timeout: int = 30):
        self.name = ProviderName.AFAD.value
        self.column_mapper = column_mapper
        self.timeout = timeout
        
        # Dependency Injection (Composition)
        self.api_client = AfadApiClient(timeout=self.timeout)
        self.file_manager = AfadFileManager(base_dir="Afad_events")
        
        self.mapped_df = None
        self.response_df = None

    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        return criteria.to_afad_params()

    @async_result_decorator
    async def fetch_data_async(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """Asenkron veri çekme"""
        data = await self.api_client.search_waveforms_async(criteria)
        return self._process_response_data(data)

    @result_decorator
    def fetch_data_sync(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """Senkron veri çekme"""
        data = self.api_client.search_waveforms_sync(criteria)
        return self._process_response_data(data)

    def _process_response_data(self, data: List[Dict]) -> pd.DataFrame:
        """API yanıtını DataFrame'e çevirir ve map eder (DRY Prensibi)"""
        self.response_df = pd.DataFrame(data)
        if self.response_df.empty:
            return pd.DataFrame()
            
        self.mapped_df = self.column_mapper.map_columns(df=self.response_df)
        self.mapped_df['PROVIDER'] = str(self.name)
        
        # Endpoint linki oluşturma
        if 'RSN' in self.mapped_df.columns:
            self.mapped_df['ENDPOINTSOURCE'] = (
                "https://tadas.afad.gov.tr/waveform-detail/" + 
                self.mapped_df['RSN'].astype(str)
            )
            
        print(f"AFAD'dan {len(self.mapped_df)} kayıt alındı.")
        return self.mapped_df

    def get_name(self) -> str:
        return str(self.name)

    @result_decorator
    def get_event_details(self, event_ids: List[int]) -> pd.DataFrame:
        """Event detaylarını getirir"""
        all_details = []
        for event_id in event_ids:
            detail = self.api_client.get_event_details(event_id)
            if detail:
                if isinstance(detail, list) and detail:
                    all_details.append(detail[0])
                else:
                    all_details.append(detail)
            time.sleep(0.1) # Rate limit koruması
        return pd.DataFrame(all_details)

    @result_decorator
    def download_single_waveforms(self, filename: str, **kwargs) -> bool:
        """Tekil indirme"""
        # Parametre hazırlığı
        payload = self._prepare_download_payload([filename], **kwargs)
        event_id = kwargs.get('event_id', int(time.time()))
        station_code = kwargs.get('station_code', 'unknown')
        
        # 1. İndir (Network)
        content = self.api_client.download_waveform(payload)
        
        # 2. Kaydet (File I/O)
        zip_name = f"waveforms_{event_id}_{station_code}.zip"
        zip_path = self.file_manager.save_zip(content, event_id, zip_name)
        
        # 3. Çıkar (Zip Ops)
        self.file_manager.extract_zip(zip_path, kwargs.get('export_type', 'asc2'))
        
        return True
    
    @result_decorator
    def download_waveforms_batch(self, filenames: List[str], **kwargs) -> Dict:
        """Toplu indirme ve batch yönetimi

        Args:
            filenames (List[str]): İndirilecek dosya isimleri
            **kwargs: Ek parametreler (event_id, station_code, file_type, file_status, export_type, user_name, batch_size)
                        batch_size (int, optional): Batch başına dosya sayısı. Defaults to 10, max 10.
                        event_id (int, optional): İlgili deprem olayının ID'leri (klasör yapısı için). Defaults to current timestamp.
                        station_code (str, optional): İstasyon kodu (dosya adlandırması için). Defaults to 'unknown'.
                        file_type (str, optional): Type of file to download. Defaults to 'ap'.
                        file_status (str, optional): Status of the file. Defaults to 'Acc'. Options --> "RawAcc", "Acc", "Vel", "Disp", "ResSpecAcc", "ResSpecVel", "ResSpecDisp", "FFT", "Husid"
                        export_type (str, optional): Export format for the files. Defaults to 'mseed'. Options --> asc2, mseed, asd
                        user_name (str, optional): Name of the user requesting the download. Defaults to 'GuestUser'.
        Returns:
            Dict: Batch indirme sonuçları (toplam, indirilen, batch detayları)
        """
        batch_size = min(kwargs.get('batch_size', 10), 10)
        event_id = kwargs.get('event_id', [int(time.time())])
        export_type = kwargs.get('export_type', 'mseed')
        
        results = {
            'total': len(filenames),
            'downloaded': 0,
            'batches': []
        }

        # Chunking (Batch'lere ayırma)
        batches = [filenames[i:i + batch_size] for i in range(0, len(filenames), batch_size)]
        
        print(f"[INFO] {len(filenames)} dosya, {len(batches)} parti halinde indirilecek.")

        for idx, batch_files in enumerate(batches, 1):
            try:
                # 1. Payload Hazırla
                payload = self._prepare_download_payload(batch_files, **kwargs)
                print(f"[DEBUG] Batch {idx} payload hazırlandı: {payload}")
                
                # 2. İndir
                content = self.api_client.download_waveform(payload)
                print(f"[DEBUG] Batch {idx} indirildi, boyut: {len(content)} bytes")
                
                # 3. Kaydet
                zip_name = f"part_{idx}.zip"
                zip_path = self.file_manager.save_zip(content, event_id, zip_name)
                print(f"[DEBUG] Batch {idx} zip olarak kaydedildi: {zip_path}")
                
                # 4. Çıkar
                extracted = self.file_manager.extract_zip(zip_path, export_type)
                print(f"[DEBUG] Batch {idx} çıkarıldı, {len(extracted)} dosya bulundu.")
                
                # 5. Sonuç Kaydı
                count = len(extracted)
                results['downloaded'] += count
                results['batches'].append({'batch': idx, 'count': count, 'success': True})
                print(f"[OK] Batch {idx} tamamlandı: {count} dosya")

                # Başarısız dosya kontrolü ve Retry mekanizması
                if count < len(batch_files):
                    self._handle_retry(batch_files, extracted, event_id, **kwargs)
                    results['downloaded']  += count # Retry sonrası güncelleme

                time.sleep(2) # Sunucuya nefes aldır

            except Exception as e:
                print(f"[ERROR] Batch {idx} failed: {e}")
                results['batches'].append({'batch': idx, 'success': False, 'error': str(e)})

        return results

    def _prepare_download_payload(self, filenames: List[str], **kwargs) -> Dict:
        """Download payload'unu hazırlar (Helper)
            - AFAD API'si için gerekli parametreleri düzenler
            - Batch ve tekil indirme için ortak bir yapı sağlar
            Args:
#             file_type (str, optional): Type of file to download. Defaults to 'ap'.
#             file_status (str, optional): Status of the file. Defaults to 'Acc'. Options --> "RawAcc", "Acc", "Vel", "Disp", "ResSpecAcc", "ResSpecVel", "ResSpecDisp", "FFT", "Husid"
#             export_type (str, optional): Export format for the files. Defaults to 'mseed'. Options --> asc2, mseed, asd
#             user_name (str, optional): Name of the user requesting the download. Defaults to 'GuestUser'.
            Returns:
                dict: Prepared payload for AFAD API download request.
                                
        """
        payload = {
            "filename": filenames,
            "file_type": [kwargs.get('file_type', 'ap')] * len(filenames),
            "file_status": kwargs.get('file_status', 'Acc'),
            "export_type": kwargs.get('export_type', 'mseed'),
            "user_name": kwargs.get('user_name', 'GuestUser'),
            "call": "afad"
        }
        return payload 

    def _handle_retry(self, requested: List[str], extracted: List[str], event_id: int, **kwargs):
        """Basit retry mekanizması"""
        # Dosya isimlerinden karşılaştırma mantığı (basitleştirilmiş)
        # Gerçek senaryoda path parsing gerekebilir.
        missing_files = set(requested) - set(os.path.basename(f) for f in extracted)
        if missing_files:
            print(f"[WARNING] Eksik dosyalar tespit edildi, retry başlatılıyor: {missing_files}")
            for filename in missing_files:
                try:
                    self.download_single_waveforms(filename, event_id=event_id, **kwargs)
                    print(f"[RETRY OK] {filename} indirildi.")
                except Exception as e:
                    print(f"[RETRY ERROR] {filename} indirilemedi: {e}")
            
