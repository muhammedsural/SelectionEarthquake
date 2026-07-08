"""
selection_service
=================

A Python library for earthquake ground motion selection and processing.
"""

__version__ = "1.2.0"

# --- Core API ---
from .core.LoggingConfig import setup_logging

# --- Enums ---
from .enums.Enums import ProviderName, DesignCode

from .core.Pipeline import EarthquakePipeline
from .core.EarthquakeApi import EarthquakeAPI

# --- Providers ---
from .providers.IProvider import IDataProvider
from .providers.ProvidersFactory import ProviderFactory

# --- Processing ---
from .processing.Selection import (
    SelectionConfig,
    SearchCriteria,
    BaseSelectionStrategy,
    TBDYSelectionStrategy,
    EurocodeSelectionStrategy
)
from .processing.Mappers import ColumnMapperFactory

__all__ = [
    "__version__",
    "EarthquakePipeline", "EarthquakeAPI",
    "setup_logging",
    "ProviderName", "DesignCode",
    "ProviderFactory", "IDataProvider"
    "SelectionConfig", "SearchCriteria", "BaseSelectionStrategy",
    "TBDYSelectionStrategy", "EurocodeSelectionStrategy",
    "ColumnMapperFactory"
]
