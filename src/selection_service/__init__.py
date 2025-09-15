"""
earthquake_selection
====================

A Python library for earthquake ground motion selection and processing.
"""

# Paket sürümü
__version__ = "0.1.0"

# Config & Constants
from .Config import *  # noqa
from .Enums import ProviderName,DesignCode

# Core modules
from .Pipeline import EarthquakePipeline, EarthquakeAPI
from .Selection import (
    ISelectionStrategy,
    SearchCriteria,
    TargetParameters,
    ValidationError,
)
from .Providers import (
    IDataProvider,
    PeerWest2Provider,
    AFADDataProvider,
    FDSNProvider,
    ProviderFactory,
)

# Helpers & utilities
from .Mappers import ColumnMapperFactory, IColumnMapper
from .ErrorHandle import *
from .ResultHandle import *
