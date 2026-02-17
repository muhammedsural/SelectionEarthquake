from typing import Any, Dict, List, Protocol
import pandas as pd
from selection_service.core.ErrorHandle import ProviderError
from selection_service.processing.ResultHandle import Result
from selection_service.processing.Selection import SearchCriteria


class IDataProvider(Protocol):
    """Veri sağlayıcı interface'i"""

    def map_criteria(self, criteria: Any) -> Dict[str, Any]:
        """Genel arama kriterlerini provider'a özel formata dönüştür"""
        ...

    async def fetch_data_async(self, criteria: Dict[str, Any]) -> Result[pd.DataFrame, ProviderError]:
        """Kriterlere göre veri getir"""
        ...

    def fetch_data_sync(self, criteria: Dict[str, Any]) -> Result[pd.DataFrame, ProviderError]:
        """Kriterlere göre veri getir (senkron)"""
        ...

    def get_name(self) -> str:
        """Sağlayıcı adı"""

    def download_waveforms_batch(self, filenames: List[str], **kwargs) -> Dict:
        """Toplu dalga formu indirme
            Args:
                filenames (List[str]): İndirilecek dosya adları listesi
                **kwargs: 
                        * Afad Tadas Ek parametreler
                            - batch_size (int, optional): Batch başına dosya sayısı. Defaults to 10, max 10.
                            - event_id (int, optional): İlgili deprem olayının ID'si (klasör yapısı için). Defaults to current timestamp.
                            - station_code (str, optional): İstasyon kodu (dosya adlandırması için). Defaults to 'unknown'.
                            - file_type (str, optional): Type of file to download. Defaults to 'ap'.
                            - file_status (str, optional): Status of the file. Defaults to 'Acc'. Options --> "RawAcc", "Acc", "Vel", "Disp", "ResSpecAcc", "ResSpecVel", "ResSpecDisp", "FFT", "Husid"
                            - export_type (str, optional): Export format for the files. Defaults to 'mseed'. Options --> asc2, mseed, asd
                            - user_name (str, optional): Name of the user requesting the download. Defaults to 'GuestUser'.
        """
        ...
        
        ...
    def download_single_waveforms(self, filename: str, **kwargs) -> Result[bool, ProviderError]:
        """Tek bir dalga formu dosyasını indir.

        Args:
            filename (str): İndirilecek dosya adı
            **kwargs: Ek parametreler (event_id, station_code, file_type, file_status, export_type, user_name)  
                event_id (int, optional): İlgili deprem olayının ID'si (klasör yapısı için). Defaults to current timestamp.
                station_code (str, optional): İstasyon kodu (dosya adlandırması için). Defaults to 'unknown'.
                file_type (str, optional): Type of file to download. Defaults to 'ap'.
                file_status (str, optional): Status of the file. Defaults to 'Acc'. Options --> "RawAcc", "Acc", "Vel", "Disp", "ResSpecAcc", "ResSpecVel", "ResSpecDisp", "FFT", "Husid"
                export_type (str, optional): Export format for the files. Defaults to 'mseed'. Options --> asc2, mseed, asd
                user_name (str, optional): Name of the user requesting the download. Defaults to 'GuestUser'.
                
        """
        ...
