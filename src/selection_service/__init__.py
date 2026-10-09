"""
selection_service
=================

A Python library for earthquake ground motion selection and processing.
"""

__version__ = "2.1.0"

# --- Core API ---
from .core.logging_config import setup_logging

# --- Enums ---
from .enums.enums import ProviderName, DesignCode

from .core.pipeline import EarthquakePipeline
from .core.earthquake_api import EarthquakeAPI

# --- Providers ---
from .providers.interfaces import IDataFetcher, IWaveformDownloader
from .providers.providers_factory import ProviderFactory

# --- Processing ---
from .processing.criteria import SelectionConfig, SearchCriteria, DedupConfig
from .processing.strategies import BaseSelectionStrategy, TBDYSelectionStrategy, TBDY2018ConstraintStrategy, ConstraintSelectionStrategy, EurocodeSelectionStrategy
from .processing.mappers import ColumnMapperFactory

__all__ = [
    "__version__",
    "EarthquakePipeline", "EarthquakeAPI",
    "setup_logging",
    "ProviderName", "DesignCode",
    "ProviderFactory", "IDataFetcher", "IWaveformDownloader",
    "SelectionConfig", "SearchCriteria", "DedupConfig", "BaseSelectionStrategy",
    "TBDYSelectionStrategy", "TBDY2018ConstraintStrategy", "ConstraintSelectionStrategy",
    "EurocodeSelectionStrategy",
    "ColumnMapperFactory"
]

