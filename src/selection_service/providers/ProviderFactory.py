from selection_service.providers.BaseProviders import IAsyncDataProvider, ISyncDataProvider
from .AsyncProviders import AsyncAFADDataProvider, AsyncPeerWest2Provider
from .SyncProviders import SyncAFADDataProvider, SyncPeerWest2Provider
from ..enums.Enums import ProviderName
from ..processing.Mappers import ColumnMapperFactory

class AsyncProviderFactory:
    """Async provider factory"""
    
    @staticmethod
    def create_provider(provider_type: ProviderName, **kwargs) -> IAsyncDataProvider:
        mapper = ColumnMapperFactory.get_mapper(provider_type)
        
        if provider_type == ProviderName.AFAD:
            return AsyncAFADDataProvider(column_mapper=mapper, **kwargs)
        elif provider_type == ProviderName.PEER:
            return AsyncPeerWest2Provider(column_mapper=mapper, **kwargs)
        # ...

class SyncProviderFactory:
    """Sync provider factory"""
    
    @staticmethod
    def create_provider(provider_type: ProviderName, **kwargs) -> ISyncDataProvider:
        mapper = ColumnMapperFactory.get_mapper(provider_type)
        
        if provider_type == ProviderName.AFAD:
            return SyncAFADDataProvider(column_mapper=mapper, **kwargs)
        elif provider_type == ProviderName.PEER:
            return SyncPeerWest2Provider(column_mapper=mapper, **kwargs)