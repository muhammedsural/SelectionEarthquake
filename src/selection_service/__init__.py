"""
selection_service
=================

A Python library for earthquake ground motion selection and processing.
"""

__version__ = "1.2.1"

# --- Core API ---
from .core.LoggingConfig import setup_logging

# --- Enums ---
from .enums.Enums import ProviderName, DesignCode

from .core.Pipeline import EarthquakePipeline
from .core.EarthquakeApi import EarthquakeAPI

# --- Providers ---
from .providers.interfaces import IDataFetcher, IWaveformDownloader
from .providers.ProvidersFactory import ProviderFactory

# --- Processing ---
from .processing.Selection import (
    SelectionConfig,
    SearchCriteria,
    BaseSelectionStrategy,
    TBDYSelectionStrategy,
    TBDY2018ConstraintStrategy,
    ConstraintSelectionStrategy,
    ParetoSelectionStrategy,
    SpectrumMatchStrategy,
    EurocodeSelectionStrategy
)
from .processing.Mappers import ColumnMapperFactory

__all__ = [
    "__version__",
    "EarthquakePipeline", "EarthquakeAPI",
    "setup_logging",
    "ProviderName", "DesignCode",
    "ProviderFactory", "IDataFetcher", "IWaveformDownloader",
    "SelectionConfig", "SearchCriteria", "BaseSelectionStrategy",
    "TBDYSelectionStrategy", "TBDY2018ConstraintStrategy", "ConstraintSelectionStrategy",
    "ParetoSelectionStrategy", "SpectrumMatchStrategy", "EurocodeSelectionStrategy",
    "ColumnMapperFactory"
]


def __getattr__(name):
    # Geriye donuk uyumluluk: selection_service.IDataProvider hala calisir.
    if name == "IDataProvider":
        import warnings

        warnings.warn(
            "selection_service.IDataProvider kullanimdan kalkti; "
            "IDataFetcher / IWaveformDownloader kullanin. v2.0'da kaldirilacak.",
            DeprecationWarning,
            stacklevel=2,
        )
        from .providers.IProvider import IDataProvider

        return IDataProvider
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
