import asyncio
from typing import Any, Dict, Type
import pandas as pd
from selection_service.processing.ResultHandle import Result
from ..processing.Mappers import IColumnMapper
from ..core.ErrorHandle import ProviderError
from ..providers.IProvider import IDataProvider
from obspy.clients.fdsn import Client


class FDSNProvider(IDataProvider):
    def __init__(self,
                 column_mapper: Type[IColumnMapper],
                 name="IRIS",
                 base_url=None,
                 timeout=15):
        self.column_mapper = column_mapper
        self._name = name
        self._client = Client(base_url or name, timeout=timeout)

    def get_name(self):
        return self._name

    def map_criteria(self, criteria: Dict[str, Any]) -> Dict[str, Any]:
        return criteria.to_fdsn_params()

    async def fetch_data_async(self, criteria):
        try:
            mapped = self.map_criteria(criteria)
            loop = asyncio.get_running_loop()
            # ObsPy bloklama yaptığı için thread pool'a atıyoruz
            catalog = await loop.run_in_executor(None, lambda: self._client.get_events(**mapped))
            
            # Catalog → DataFrame dönüştür
            records = []
            for event in catalog:
                origin = event.origins[0]
                mag = event.magnitudes[0]
                records.append({
                    "EQID": event.resource_id.id,
                    "TIME": origin.time.datetime,
                    "LAT": origin.latitude,
                    "LON": origin.longitude,
                    "DEPTH(km)": origin.depth / 1000.0 if origin.depth else None,
                    "MAGNITUDE": mag.mag,
                    "RJB(km)": None,   # burada station data gerekirse eklenebilir
                    "SCORE": None      # strateji hesaplayacak
                })

            df = pd.DataFrame(records)
            
            return Result.ok(df)
        except Exception as e:
            return Result.fail(e)

    def fetch_data_sync(self, criteria: Dict[str, Any]) -> Result[pd.DataFrame, ProviderError]:

        try:
            mapped = self.map_criteria(criteria)
            catalog = self._client.get_events(**mapped)
            
            # Catalog → DataFrame dönüştür
            # TODO Burayı IColumnMapper ile yap
            records = []
            for event in catalog:
                origin = event.origins[0]
                mag = event.magnitudes[0]
                records.append({
                    "EQID": event.resource_id.id,
                    "TIME": origin.time.datetime,
                    "LAT": origin.latitude,
                    "LON": origin.longitude,
                    "DEPTH(km)": origin.depth / 1000.0 if origin.depth else None,
                    "MAGNITUDE": mag.mag,
                    "RJB(km)": None,
                    "SCORE": None
                })

            df = pd.DataFrame(records)
            standartized_df = self.column_mapper.map_columns(df=df)
            standartized_df['PROVIDER'] = str(f"FDSN_{self._name}")
            return Result.ok(standartized_df)
        except Exception as e:
            return Result.fail(e)
