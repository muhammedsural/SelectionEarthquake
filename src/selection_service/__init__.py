"""
selection_service
=================

A Python library for earthquake ground motion selection and processing.
"""

__version__ = "0.1.0"

# --- Core API ---
from .core.Pipeline import EarthquakePipeline, EarthquakeAPI,PipelineResult, PipelineContext
from .core.Config import (
    SCORE_RANGES_AND_WEIGHTS,
    STANDARD_COLUMNS,
    MECHANISM_MAP,
    REVERSE_MECHANISM_MAP,
    convert_mechanism_to_text,
    convert_mechanism_to_numeric,
    get_mechanism_text,
    get_mechanism_numeric
)
from .core.ErrorHandle import (
    PipelineError,
    ValidationError,
    NoDataError,
    StrategyError,
    ProviderError,
    NetworkError,
    DataProcessingError
)
from .core.LoggingConfig import setup_logging

# --- Enums ---
from .enums.Enums import ProviderName, DesignCode

# --- Providers ---
from .providers.Providers import (
    IDataProvider,
    PeerWest2Provider,
    AFADDataProvider,
    FDSNProvider,
    ProviderFactory,
)

# --- Processing ---
from .processing.Selection import (
    ISelectionStrategy,
    SelectionConfig,
    SearchCriteria,
    TargetParameters,
    ValidationError as SelectionValidationError,
    BaseSelectionStrategy,
    TBDYSelectionStrategy,
    EurocodeSelectionStrategy
)
from .processing.Mappers import ColumnMapperFactory, IColumnMapper, BaseColumnMapper, AFADColumnMapper, PEERColumnMapper
from .processing.ResultHandle import Result, async_result_decorator, result_decorator

__all__ = [
    "__version__",
    # core
    "EarthquakePipeline", "EarthquakeAPI",
    "setup_logging",
    # config
    "SCORE_RANGES_AND_WEIGHTS", "STANDARD_COLUMNS", 
    "MECHANISM_MAP", "REVERSE_MECHANISM_MAP",
    "convert_mechanism_to_text", "convert_mechanism_to_numeric",
    "get_mechanism_text", "get_mechanism_numeric",
    # error handling
    "PipelineError", "ValidationError", "NoDataError", 
    "StrategyError", "ProviderError", "NetworkError", "DataProcessingError",
    # enums
    "ProviderName", "DesignCode",
    # providers
    "IDataProvider", "PeerWest2Provider", "AFADDataProvider",
    "FDSNProvider", "ProviderFactory",
    # processing - selection
    "ISelectionStrategy", "SelectionConfig", "SearchCriteria", 
    "TargetParameters", "BaseSelectionStrategy", 
    "TBDYSelectionStrategy", "EurocodeSelectionStrategy",
    # processing - mappers
    "ColumnMapperFactory", "IColumnMapper", "BaseColumnMapper", 
    "AFADColumnMapper", "PEERColumnMapper",
    # processing - result handling
    "Result", "async_result_decorator", "result_decorator"
]