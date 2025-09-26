from ..providers.AfadProvider import AFADDataProvider
from ..providers.IProvider import IDataProvider
from ..providers.PeerProvider import PeerWest2Provider
from ..enums.Enums import ProviderName
from ..processing.Mappers import ColumnMapperFactory


class ProviderFactory:
    """Provider factory sınıfı"""

    @staticmethod
    def create_provider(provider_type: ProviderName,
                        **kwargs) -> IDataProvider:
        # mapper = ColumnMapperFactory.get_mapper(provider_type)
        mapper = ColumnMapperFactory.create_mapper(provider_type, **kwargs)
        
        if provider_type == ProviderName.AFAD:
            return AFADDataProvider(column_mapper=mapper)
        elif provider_type == ProviderName.PEER:
            # file_path = data\NGA-West2_flatfile.csv
            return PeerWest2Provider(column_mapper=mapper, **kwargs)
        # elif provider_type == ProviderName.FDSN:
        #     return FDSNProvider(column_mapper=mapper,**kwargs)
        else:
            raise ValueError(f"Unknown provider: {provider_type}")
