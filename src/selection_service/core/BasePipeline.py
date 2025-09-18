# core/BasePipeline.py
from abc import ABC, abstractmethod
from typing import List, Generic, TypeVar
import pandas as pd
from ..processing.Selection import ISelectionStrategy, SearchCriteria, TargetParameters
from ..providers.Providers import IDataProvider
from .ErrorHandle import PipelineError
from ..processing.ResultHandle import Result

T = TypeVar('T')

class BasePipeline(ABC, Generic[T]):
    """Base pipeline abstract class"""
    
    @abstractmethod
    def execute(self, providers: List[IDataProvider], strategy: ISelectionStrategy,
               search_criteria: SearchCriteria, target_params: TargetParameters) -> Result[T, PipelineError]:
        pass

