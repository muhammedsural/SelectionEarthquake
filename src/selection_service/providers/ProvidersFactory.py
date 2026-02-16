from typing import Any
from .CacheManager import CacheManager
from ..processing.Selection import SearchCriteria
from ..providers.AfadProvider import AFADDataProvider
from ..providers.IProvider import IDataProvider
from ..providers.PeerProvider import PeerWest2Provider
from ..enums.Enums import ProviderName
from ..processing.Mappers import ColumnMapperFactory

class CachedProviderProxy:
    def __init__(self, provider: IDataProvider, cache_manager: CacheManager):
        self._provider = provider
        self._cache = cache_manager

    # criteria tipi SearchCriteria yerine Any yapıldı, çünkü buraya Dict geliyor
    async def fetch_data_async(self, criteria: Any):
        # 1. Cache'den oku
        try:
            cached_df = self._cache.get(self._provider.get_name(), criteria)
            
            if cached_df is not None and not cached_df.empty:
                # ResultHandle.py içindeki Result sınıfına sarmalayarak döndür
                from ..processing.ResultHandle import Result
                print(f"[CACHE HIT] {self._provider.get_name()} verisi diskten alındı.")
                return Result.ok(cached_df)
        except Exception as e:
            print(f"[CACHE WARNING] Cache okuma hatası, API'ye gidiliyor: {e}")

        # 2. Cache'de yoksa veya eskimişse asıl provider'a git
        result = await self._provider.fetch_data_async(criteria)
        
        # 3. Başarılı sonucu cache'e kaydet
        if result.success and result.value is not None and not result.value.empty:
            try:
                self._cache.set(self._provider.get_name(), criteria, result.value)
            except Exception as e:
                print(f"[CACHE WARNING] Cache yazma hatası: {e}")
            
        return result

    def __getattr__(self, name):
        return getattr(self._provider, name)
    
class ProviderFactory:
    """Provider factory sınıfı"""
    _cache_manager = CacheManager() # Singleton benzeri tek bir cache yönetimi

    @staticmethod
    def create_provider(provider_type: ProviderName, use_cache: bool = True, **kwargs) -> IDataProvider:
        mapper = ColumnMapperFactory.create_mapper(provider_type, **kwargs)
        
        # Temel Provider oluşturma
        if provider_type == ProviderName.AFAD:
            provider = AFADDataProvider(column_mapper=mapper)
        elif provider_type == ProviderName.PEER:
            provider = PeerWest2Provider(column_mapper=mapper, **kwargs)
        else:
            raise ValueError(f"Unknown provider: {provider_type}")

        # Eğer cache isteniyorsa Proxy ile sarmala
        if use_cache:
            return CachedProviderProxy(provider, ProviderFactory._cache_manager)
            
        return provider
