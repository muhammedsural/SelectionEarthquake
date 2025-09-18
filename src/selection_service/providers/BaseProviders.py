# providers/BaseProviders.py
from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd
from ..processing.Selection import SearchCriteria
from ..processing.ResultHandle import Result


class IAsyncDataProvider(ABC):
    """Async data provider interface"""
    
    @abstractmethod
    async def fetch_data(self, criteria: Dict[str, Any]) -> Result[pd.DataFrame, Any]:
        pass
    
    @abstractmethod
    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        """Genel arama kriterlerini provider'a özel formata dönüştür"""
        return criteria.to_afad_params()
    
    @abstractmethod
    def get_name(self) -> str:
        return str(self.name)

class ISyncDataProvider(ABC):
    """Sync data provider interface"""
    
    @abstractmethod
    def fetch_data(self, criteria: Dict[str, Any]) -> Result[pd.DataFrame, Any]:
        pass
    
    @abstractmethod
    def map_criteria(self, criteria: SearchCriteria) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        pass

class BaseDataProvider(IAsyncDataProvider,ISyncDataProvider,ABC):
    """Base data provider abstract class"""
    
    @abstractmethod
    def map_criteria(self, criteria: SearchCriteria) -> dict:
        pass
    
    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def fetch_data(self, criteria: Dict[str, Any]) -> Result[pd.DataFrame, Any]:
        pass
