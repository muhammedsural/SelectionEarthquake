"""
earthquake_selection
====================

A Python library for earthquake ground motion selection and processing.
"""

# Paket sürümü
__version__ = "0.1.0"

# Config & Constants
from .core import Pipeline,Config,ErrorHandle
from .enums.Enums import ProviderName,DesignCode

# Core modules
from .core.Pipeline import EarthquakePipeline, EarthquakeAPI
from .processing.Selection import (
    ISelectionStrategy,
    SearchCriteria,
    TargetParameters,
    ValidationError,
)
from .providers.Providers import (
    IDataProvider,
    PeerWest2Provider,
    AFADDataProvider,
    FDSNProvider,
    ProviderFactory,
)

# Helpers & utilities
from .processing.Mappers import ColumnMapperFactory, IColumnMapper
from .core.ErrorHandle import *
from .processing.ResultHandle import *
