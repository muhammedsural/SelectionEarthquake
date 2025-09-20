"""
selection_service
=================

A Python library for earthquake ground motion selection and processing.
"""

__version__ = "0.1.0"

# --- Core API ---
from .core.LoggingConfig import setup_logging

# --- Enums ---
from .enums.Enums import ProviderName, DesignCode

# --- Config ---
from .core.Config import SCORE_RANGES_AND_WEIGHTS

from .core.Pipeline import EarthquakePipeline, EarthquakeAPI

# --- Providers ---
from .providers.Providers import ProviderFactory, IDataProvider

# --- Processing ---
from .processing.Selection import (
    SelectionConfig,
    SearchCriteria,
    TargetParameters,
    BaseSelectionStrategy,
    TBDYSelectionStrategy,
    EurocodeSelectionStrategy
)
from .processing.Mappers import ColumnMapperFactory

__all__ = [
    "__version__",
    "EarthquakePipeline", "EarthquakeAPI",
    "setup_logging",
    "SCORE_RANGES_AND_WEIGHTS",
    "ProviderName", "DesignCode",
    "ProviderFactory", "IDataProvider"
    "SelectionConfig", "SearchCriteria",
    "TargetParameters", "BaseSelectionStrategy",
    "TBDYSelectionStrategy", "EurocodeSelectionStrategy",
    "ColumnMapperFactory"
]
