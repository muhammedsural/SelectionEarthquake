import os
import time
from typing import Any, Dict, List, Type
import numpy as np
import pandas as pd
import requests

from selection_service.core.Config import convert_mechanism_to_text
from selection_service.enums.Enums import ProviderName
from selection_service.processing.Selection import SearchCriteria
from .BaseProviders import ISyncDataProvider
from ..processing.Mappers import IColumnMapper
from ..core.ErrorHandle import DataProcessingError, NetworkError, ProviderError
from ..processing.ResultHandle import result_decorator

class SyncAFADDataProvider(ISyncDataProvider):
    """Sync AFAD data provider"""
    
    def __init__(self, column_mapper: IColumnMapper, timeout: int = 30):
        self.column_mapper = column_mapper
        self.timeout = timeout
        self.name = ProviderName.AFAD.value
        self.base_url = "https://ivmeservis.afad.gov.tr/Waveforms/GetWaveforms"
        self.base_download_dir = "Afad_events"
    
    @result_decorator
    def fetch_data(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """Fetch data synchronously"""
        """AFAD verilerini getir (senkron)"""
        try:
            response = self._search_afad(criteria=criteria)
            if response.status_code == 200:
                data = response.json()
                self.response_df = pd.DataFrame(data)
                self.mapped_df = self.column_mapper.map_columns(df=self.response_df)
                self.mapped_df['PROVIDER'] = str(self.name)
                print(f"AFAD'dan {len(self.mapped_df)} kayıt alındı.")
                return self.mapped_df
            else:
                raise NetworkError(
                    self.name,
                    Exception(f"HTTP {response.status_code}: {response.text}"),
                    "AFAD API request failed"
                )
        except requests.RequestException as e:
            raise NetworkError(self.name, e, "AFAD network error")
        except Exception as e:
            raise ProviderError(self.name, e, "AFAD data processing failed")
    
    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        return criteria.to_afad_params()
    
    def get_name(self) -> str:
        return self.name

    def _search_afad(self, criteria: Dict[str, Any]) -> requests.Response:
        """AFAD API'sini kullanarak arama yap"""
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Origin': 'https://tadas.afad.gov.tr',
            'Referer': 'https://tadas.afad.gov.tr/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Username': 'GuestUser',
            'IsGuest': 'true'
        }
        
        payload = criteria
        print(f"AFAD arama kriterleri: {payload}")
                
        response = requests.post(
            self.base_url, 
            json=payload,
            headers=headers,
            timeout=self.timeout
        )
        
        return response

    @result_decorator
    def get_event_details(self, event_ids: List[int]) -> pd.DataFrame:
        """Birden fazla event için detaylı bilgileri alır"""
        all_details = []
        
        for event_id in event_ids:
            detail_url = f"https://ivmeservis.afad.gov.tr/Event/GetEventById/{event_id}"
            
            headers = {
                'Accept': 'application/json, text/plain, */*',
                'Origin': 'https://tadas.afad.gov.tr',
                'Referer': f'https://tadas.afad.gov.tr/event-detail/{event_id}',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Username': 'GuestUser',
                'IsGuest': 'true'
            }
            
            try:
                response = requests.get(url=detail_url, headers=headers, timeout=30)
                if response.status_code == 200:
                    detail_data = response.json()
                    if isinstance(detail_data, dict):
                        all_details.append(detail_data)
                    elif isinstance(detail_data, list) and len(detail_data) > 0:
                        all_details.append(detail_data[0])
                
                time.sleep(0.1)
                
            except Exception as e:
                raise ProviderError(self.name, e, f"Event {event_id} details failed")
        
        return pd.DataFrame(all_details) if all_details else pd.DataFrame()
    
    @result_decorator
    def download_afad_waveforms_batch(self, filenames: List[str], **kwargs) -> Dict:
        """Waveform download with Result pattern"""
        file_type   = kwargs.get('file_type')
        file_status = kwargs.get('file_status')
        export_type = kwargs.get('export_type')
        user_name   = kwargs.get('user_name')
        event_id    = kwargs.get('event_id')
        batch_size  = kwargs.get('batch_size')
        # Batch size'ı maximum 10 ile sınırla
        batch_size = min(batch_size, 10)
        
        url = "https://ivmeprocessguest.afad.gov.tr/ExportData"
        
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Origin': 'https://tadas.afad.gov.tr',
            'Referer': 'https://tadas.afad.gov.tr/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Username': 'GuestUser',
            'IsGuest': 'true'
        }
        
        all_results = {
            'total_files': len(filenames),
            'batches': [],
            'successful_batches': 0,
            'failed_batches': 0,
            'downloaded_files': 0
        }
        
        # Dosyaları batch'lere ayır
        batches = [filenames[i:i + batch_size] for i in range(0, len(filenames), batch_size)]
        
        print(f"   📦 {len(filenames)} dosya, {len(batches)} parti halinde indirilecek (max {batch_size}/parti)")
        for batch_index, batch_filenames in enumerate(batches, 1):
            print(f"   🔽 PARTİ {batch_index}/{len(batches)} - {len(batch_filenames)} dosya")
            
            # Request payload
            payload = {
                "filename": batch_filenames,
                "file_type": [file_type] * len(batch_filenames),
                "file_status": file_status,
                "export_type": export_type,
                "user_name": user_name,
                "call": "afad"
            }
            try:
                # POST isteği gönder
                    response = requests.post(url, headers=headers, json=payload, timeout=50)
                    response.raise_for_status()
                    
                    # Event ID'yi kullanarak klasör yapısı oluştur
                    if event_id:
                        event_dir = os.path.join(self.base_download_dir, str(event_id))
                    else:
                        # Event ID yoksa timestamp kullan
                        event_dir = os.path.join(self.base_download_dir, f"event_{int(time.time())}")
                    
                    # # Batch klasörü oluştur
                    # batch_dir = os.path.join(event_dir, f"batch_{batch_index}")
                    os.makedirs(event_dir, exist_ok=True)
                    
                    # Zip dosyasını kaydet
                    zip_filename = f"part_{batch_index}.zip"
                    zip_path = os.path.join(event_dir, zip_filename)
                    
                    with open(zip_path, 'wb') as f:
                        f.write(response.content) # Zip dosyasını kaydet
                    
                    # Zip dosyasını aç ve organize et
                    extracted_files = self.extract_and_organize_zip_batch(event_path=event_dir, zip_path=zip_path, expected_filenames=batch_filenames,export_type=export_type)
                    
                    batch_result = {
                        'batch_number': batch_index,
                        'filenames': batch_filenames,
                        'batch_size': len(batch_filenames),
                        'zip_file': zip_path,
                        'extracted_files': extracted_files,
                        'extracted_count': len(extracted_files),
                        'success': True,
                        'error': None
                    }

                    # Başarısız dosyaları kontrol et ve yeniden dene
                    if len(extracted_files) < len(batch_filenames):
                        failed_files = [f for f in batch_filenames if f not in [os.path.basename(x) for x in extracted_files]]
                        if failed_files:
                            print(f"⚠️  {len(failed_files)} dosya çıkarılamadı, yeniden deneniyor...")
                            successful_retries = self.retry_failed_downloads(
                                event_id=event_id,
                                failed_filenames=failed_files,
                                export_type='mseed',
                                file_status=file_status
                            )
                            extracted_files.extend(successful_retries)
                    
                    all_results['batches'].append(batch_result)
                    all_results['successful_batches'] += 1
                    all_results['downloaded_files'] += len(extracted_files)
                    
                    print(f"   ✅ Parti {batch_index} başarılı: {len(extracted_files)} dosya")
                    
                    # Partiler arasında bekle (sunucu yükünü azaltmak için)
                    if batch_index < len(batches):
                        wait_time = 10
                        print(f"   ⏳ {wait_time} saniye bekleniyor...")
                        time.sleep(wait_time)
                    
            except Exception as e:
                raise ProviderError(self.name, e, "Waveform download failed")

            return all_results


class SyncPeerDataProvider(ISyncDataProvider):
    def __init__(self, file_path: str, column_mapper: Type[IColumnMapper]):
        self.file_path = file_path
        self.column_mapper = column_mapper
        self.name = ProviderName.PEER.value
        self.flatfile_df = pd.read_csv(self.file_path)

    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        """Genel arama kriterlerini provider'a özel formata dönüştür"""
        return criteria.to_peer_params()

    def get_name(self) -> str:
        return str(self.name)

    def _apply_filters(self, df: pd.DataFrame, criteria: Dict[str, Any]) -> pd.DataFrame:
        """Filtreleme uygula"""
        try:
            if df.empty:
                return df
            filtered_df = df.copy()
            
            # Büyüklük filtreleme
            if criteria['min_magnitude'] is not None:
                filtered_df = filtered_df[filtered_df['MAGNITUDE'] >= criteria['min_magnitude']]
            if criteria['max_magnitude'] is not None:
                filtered_df = filtered_df[filtered_df['MAGNITUDE'] <= criteria['max_magnitude']]
            
            # Mesafe filtreleme (RJB)
            if criteria['min_Rjb'] is not None:
                filtered_df = filtered_df[filtered_df['RJB(km)'] >= criteria['min_Rjb']]
            if criteria['max_Rjb'] is not None:
                filtered_df = filtered_df[filtered_df['RJB(km)'] <= criteria['max_Rjb']]
            
            # Mesafe filtreleme (RRUP)
            if criteria['min_Rrup'] is not None:
                filtered_df = filtered_df[filtered_df['RRUP(km)'] >= criteria['min_Rrup']]
            if criteria['max_Rrup'] is not None:
                filtered_df = filtered_df[filtered_df['RRUP(km)'] <= criteria['max_Rrup']]
            
            # VS30 filtreleme
            if criteria['min_vs30'] is not None:
                filtered_df = filtered_df[filtered_df['VS30(m/s)'] >= criteria['min_vs30']]
            if criteria['max_vs30'] is not None:
                filtered_df = filtered_df[filtered_df['VS30(m/s)'] <= criteria['max_vs30']]
            
            # Derinlik filtreleme
            if criteria['min_depth'] is not None:
                filtered_df = filtered_df[filtered_df['HYPO_DEPTH(km)'] >= criteria['min_depth']]
            if criteria['max_depth'] is not None:
                filtered_df = filtered_df[filtered_df['HYPO_DEPTH(km)'] <= criteria['max_depth']]
            
            # PGA filtreleme
            if criteria['min_pga'] is not None:
                filtered_df = filtered_df[filtered_df['PGA(cm2/sec)'] >= criteria['min_pga']]
            if criteria['max_pga'] is not None:
                filtered_df = filtered_df[filtered_df['PGA(cm2/sec)'] <= criteria['max_pga']]
            
            # PGV filtreleme
            if criteria['min_pgv'] is not None:
                filtered_df = filtered_df[filtered_df['PGV(cm/sec)'] >= criteria['min_pgv']]
            if criteria['max_pgv'] is not None:
                filtered_df = filtered_df[filtered_df['PGV(cm/sec)'] <= criteria['max_pgv']]
            
            # PGD filtreleme
            if criteria['min_pgd'] is not None:
                filtered_df = filtered_df[filtered_df['PGD(cm)'] >= criteria['min_pgd']]
            if criteria['max_pgd'] is not None:
                filtered_df = filtered_df[filtered_df['PGD(cm)'] <= criteria['max_pgd']]
            
            # Mekanizma filtreleme
            if criteria['mechanisms']:
                filtered_df = filtered_df[filtered_df['MECHANISM'].isin(criteria['mechanisms'])]
                
            return filtered_df
        except Exception as e:
            raise DataProcessingError(self.name, e, "Filter application failed")


    @result_decorator
    def fetch_data_sync(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """NGA-West2 verilerini getir (senkron)"""
        try:
            df = self.flatfile_df.copy()
            self.mapped_df = self.column_mapper.map_columns(df=df)
            self.mapped_df = self._apply_filters(self.mapped_df, criteria)
            self.mapped_df['PROVIDER'] = str(self.name)
            
            # Mekanizma dönüşümü
            if self.mapped_df['MECHANISM'].dtype in [np.int64, np.float64, int, float]:
                self.mapped_df = convert_mechanism_to_text(self.mapped_df)
            return self.mapped_df
        except Exception as e:
            raise ProviderError(self.name, e, "PEER sync data fetch failed")
    