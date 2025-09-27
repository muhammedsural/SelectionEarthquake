import asyncio
from functools import partial
from typing import Any, Dict, Type
import numpy as np
import pandas as pd
from ..enums.Enums import ProviderName
from ..processing.Mappers import IColumnMapper
from ..core.Config import convert_mechanism_to_text
from ..processing.Selection import SearchCriteria
from ..core.ErrorHandle import DataProcessingError, ProviderError
from ..processing.ResultHandle import async_result_decorator, result_decorator
from ..providers.IProvider import IDataProvider


class PeerWest2Provider(IDataProvider):
    """PEER NGA-West2 veri sağlayıcı"""

    def __init__(self, column_mapper: Type[IColumnMapper], **kwargs):
        self.file_path = kwargs.get("file_path", "..\\data\\NGA-West2_flatfile.csv")
        self.column_mapper = column_mapper
        self.name = ProviderName.PEER.value
        self.flatfile_df = pd.read_csv(self.file_path)
        self.mapped_df = None
        self.response_df = None

    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        """Genel arama kriterlerini provider'a özel formata dönüştür"""
        return criteria.to_peer_params()

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
            raise ProviderError(self.name,
                                e,
                                f"PEER async data fetch failed: {e}")

    @result_decorator
    def fetch_data_sync(self, criteria: Dict[str, Any]) -> pd.DataFrame:
        """NGA-West2 verilerini getir (senkron)"""
        try:
            df = self.flatfile_df.copy()
            self.mapped_df = self.column_mapper.map_columns(df=df)
            self.mapped_df = self._apply_filters(self.mapped_df, criteria)
            print(f"PEER'dan {len(self.mapped_df)} kayıt alındı.")
            self.mapped_df['PROVIDER'] = str(self.name)

            # Mekanizma dönüşümü
            if self.mapped_df['MECHANISM'].dtype in [np.int64, np.float64, int, float]:
                self.mapped_df = convert_mechanism_to_text(self.mapped_df)
            return self.mapped_df
        except Exception as e:
            raise ProviderError(self.name, 
                                e,
                                f"PEER sync data fetch failed: {e}")

    def _apply_filters(self,
                       df: pd.DataFrame,
                       criteria: Dict[str, Any]) -> pd.DataFrame:
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

    def get_name(self) -> str:
        return str(self.name)
