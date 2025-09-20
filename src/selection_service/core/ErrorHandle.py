from typing import TypeVar

# Type variables for generic programming
T = TypeVar('T')
E = TypeVar('E', bound=Exception)

# Domain-specific errors


class PipelineError(Exception):
    """Base pipeline error"""
    pass


class ValidationError(PipelineError):
    """Validation failed"""
    pass


class NoDataError(PipelineError):
    """No data found error"""
    pass


class StrategyError(PipelineError):
    """Strategy application error"""
    pass


class ProviderError(PipelineError):
    """Data provider error"""
    def __init__(self, provider_name: str, original_error: Exception,
                 message: str = None):
        self.provider_name = provider_name
        self.original_error = original_error
        self.message = message or f"{provider_name} error: {original_error}"
        super().__init__(self.message)


class NetworkError(ProviderError):
    """Network related errors"""
    pass


class DataProcessingError(ProviderError):
    """Data processing errors"""
    pass