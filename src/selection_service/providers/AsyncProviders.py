# providers/AsyncProviders.py
import asyncio
from functools import partial
import aiohttp
import numpy as np
import pandas as pd

from selection_service.core.Config import convert_mechanism_to_text
from selection_service.enums.Enums import ProviderName
from ..processing.Selection import SearchCriteria
from .BaseProviders import IAsyncDataProvider
from ..processing.Mappers import IColumnMapper
from ..core.ErrorHandle import DataProcessingError, NetworkError, ProviderError
from ..processing.ResultHandle import async_result_decorator
from typing import Any, Dict, Type

class AsyncAFADDataProvider(IAsyncDataProvider):
    """Async AFAD data provider"""
    
    def __init__(self, column_mapper: IColumnMapper, timeout: int = 30):
        self.column_mapper = column_mapper
        self.timeout = timeout
        self.name = ProviderName.AFAD.value
        self.base_url = "https://ivmeservis.afad.gov.tr/Waveforms/GetWaveforms"
    
    @async_result_decorator
    async def fetch_data(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """Fetch data asynchronously"""
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Origin': 'https://tadas.afad.gov.tr',
            'Referer': 'https://tadas.afad.gov.tr/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Username': 'GuestUser',
            'IsGuest': 'true'
        }
            
        try:
            payload = criteria 
            print(f"AFAD arama kriterleri: {payload}")
            
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.post(
                    self.base_url, 
                    json=payload,
                    timeout=self.timeout
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.response_df = pd.DataFrame(data)
                        self.mapped_df = self.column_mapper.map_columns(df=self.response_df)
                        self.mapped_df['PROVIDER'] = str(self.name)
                        print(f"AFAD'dan {len(self.mapped_df)} kayıt alındı.")
                        return self.mapped_df
                    else:
                        error_text = await response.text()
                        raise NetworkError(
                            self.name, 
                            Exception(f"HTTP {response.status}: {error_text}"),
                            "AFAD API request failed"
                        )
        except aiohttp.ClientError as e:
            raise NetworkError(self.name, e, "AFAD network error")
        except Exception as e:
            raise ProviderError(self.name, e, "AFAD data processing failed")
        
    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        return criteria.to_afad_params()
    
    def get_name(self) -> str:
        return self.name

class AsyncPeerDataProvider(IAsyncDataProvider):
    def __init__(self, file_path: str, column_mapper: Type[IColumnMapper]):
        self.file_path = file_path
        self.column_mapper = column_mapper
        self.name = ProviderName.PEER.value
        self.flatfile_df = pd.read_csv(self.file_path)

    def get_name(self) -> str:
        return str(self.name)
    
    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        """Genel arama kriterlerini provider'a özel formata dönüştür"""
        return criteria.to_peer_params()

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
    
    @async_result_decorator
    async def fetch_data_async(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """NGA-West2 verilerini getir"""
        try:
            loop = asyncio.get_event_loop()
            self.response_df = await loop.run_in_executor(None, pd.read_csv, self.file_path)
            self.mapped_df = await loop.run_in_executor(None, partial(self.column_mapper.map_columns, self.response_df))
            filtered_df = await loop.run_in_executor(None, partial(self._apply_filters, self.mapped_df, criteria))
            filtered_df['PROVIDER'] = str(self.name)
            
            # Mekanizma dönüşümü
            if filtered_df['MECHANISM'].dtype in [np.int64, np.float64, int, float]:
                filtered_df = convert_mechanism_to_text(filtered_df)
            return filtered_df
        except Exception as e:
            raise ProviderError(self.name, e, "PEER async data fetch failed")
    