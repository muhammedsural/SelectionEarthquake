from typing import Any, Dict, Protocol
import pandas as pd
from selection_service.core.ErrorHandle import ProviderError
from selection_service.processing.ResultHandle import Result


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
        ...
