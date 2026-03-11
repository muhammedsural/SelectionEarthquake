"""
providers/ProvidersFactory.py  (refactored)

Değişiklikler (Adım 1 — ISP + LSP):
  - IDataProvider  → IDataFetcher  (dönüş tipi güncellendi)
  - CachedProviderProxy:
      * fetch_data_sync için cache desteği eklendi (Adım 4 ön uygulama).
      * Tip anotasyonu IDataFetcher olarak güncellendi.
  - ProviderFactory.create_provider dönüş tipi IDataFetcher olarak güncellendi.
"""

import logging
from typing import Any

from .CacheManager import CacheManager
from ..processing.ResultHandle import Result
from ..providers.AfadProvider import AFADDataProvider
from ..providers.PeerProvider import PeerWest2Provider
from ..providers.interfaces import IDataFetcher          # ← yeni
from ..enums.Enums import ProviderName
from ..processing.Mappers import ColumnMapperFactory

logger = logging.getLogger(__name__)


class CachedProviderProxy(IDataFetcher):
    """Şeffaf cache katmanı — hem async hem sync çağrıları önbellekler.

    Decorator / Proxy örüntüsü:
      - fetch_data_async / fetch_data_sync için önce cache'e bakılır.
      - Cache miss ise asıl provider çağrılır ve sonuç cache'e yazılır.
      - Diğer tüm metot çağrıları __getattr__ aracılığıyla
        asıl provider'a iletilir (download metodları dahil).
    """

    def __init__(self, provider: IDataFetcher, cache_manager: CacheManager) -> None:
        self._provider = provider
        self._cache = cache_manager

    # ──────────────────────────────────────────────────────────────
    # IDataFetcher sözleşmesi — zorunlu delegasyonlar
    # ──────────────────────────────────────────────────────────────

    def get_name(self) -> str:
        return self._provider.get_name()

    def map_criteria(self, criteria: Any) -> dict:
        return self._provider.map_criteria(criteria)

    # ──────────────────────────────────────────────────────────────
    # Async fetch — cache destekli
    # ──────────────────────────────────────────────────────────────

    async def fetch_data_async(self, criteria: Any) -> Result:
        """Cache'den oku; bulamazsan provider'ı çağır ve sonucu yaz."""
        cached = self._try_read_cache(criteria)
        if cached is not None:
            return Result.ok(cached)

        result = await self._provider.fetch_data_async(criteria)
        self._try_write_cache(criteria, result)
        return result

    # ──────────────────────────────────────────────────────────────
    # Sync fetch — cache destekli (Adım 4 düzeltmesi)
    # ──────────────────────────────────────────────────────────────

    def fetch_data_sync(self, criteria: Any) -> Result:
        """Senkron cache desteği — önceki sürümde eksikti.

        Mevcut (önceki) davranış: her sync çağrı cache'i bypass ediyordu.
        Düzeltilmiş davranış: async ile aynı cache mantığı uygulanır.
        """
        cached = self._try_read_cache(criteria)
        if cached is not None:
            return Result.ok(cached)

        result = self._provider.fetch_data_sync(criteria)
        self._try_write_cache(criteria, result)
        return result

    # ──────────────────────────────────────────────────────────────
    # Proxy — diğer metotları (download vb.) asıl provider'a ilet
    # ──────────────────────────────────────────────────────────────

    def __getattr__(self, name: str) -> Any:
        """fetch_ ve get_name dışındaki her çağrıyı asıl provider'a aktar.

        Bu sayede CachedProviderProxy'nin IWaveformDownloader metodlarını
        (download_single_waveforms, download_waveforms_batch) da şeffaf biçimde
        iletmesi sağlanır — proxy'yi her yeni metot için güncellemek gerekmez.
        """
        return getattr(self._provider, name)

    # ──────────────────────────────────────────────────────────────
    # Private yardımcılar
    # ──────────────────────────────────────────────────────────────

    def _try_read_cache(self, criteria: Any):
        """Cache'den DataFrame oku; hata veya miss durumunda None döndür."""
        try:
            cached_df = self._cache.get(self._provider.get_name(), criteria)
            if cached_df is not None and not cached_df.empty:
                logger.info(
                    "[CACHE HIT] %s verisi diskten alındı.", self._provider.get_name()
                )
                return cached_df
        except Exception as e:
            logger.warning(
                "[CACHE READ WARNING] %s — API'ye gidiliyor: %s",
                self._provider.get_name(), e,
            )
        return None

    def _try_write_cache(self, criteria: Any, result: Result) -> None:
        """Başarılı sonucu cache'e yaz; hata durumunda sessizce geç."""
        if result.success and result.value is not None and not result.value.empty:
            try:
                self._cache.set(self._provider.get_name(), criteria, result.value)
            except Exception as e:
                logger.warning(
                    "[CACHE WRITE WARNING] %s: %s", self._provider.get_name(), e
                )


class ProviderFactory:
    """Provider factory — somut provider örnekleri oluşturur.

    Dönüş tipi IDataFetcher'dır; download desteği olan provider'ları
    ayırt etmek için supports_download(provider) kullanın.
    """

    # Singleton benzeri paylaşımlı cache — tüm provider'lar aynı cache'i kullanır.
    _cache_manager: CacheManager = CacheManager()

    @staticmethod
    def create_provider(
        provider_type: ProviderName,
        use_cache: bool = True,
        **kwargs: Any,
    ) -> IDataFetcher:
        """Provider tipine göre uygun sağlayıcı örneği oluştur.

        Args:
            provider_type: ProviderName enum değeri (AFAD, PEER, …).
            use_cache:     True ise sonuçlar CachedProviderProxy ile önbelleklenir.
            **kwargs:      Provider'a özgü ek parametreler.

        Returns:
            IDataFetcher örneği (gerekirse IWaveformDownloader da karşılar).

        Raises:
            ValueError: Bilinmeyen provider_type.
        """
        mapper = ColumnMapperFactory.create_mapper(provider_type, **kwargs)

        if provider_type == ProviderName.AFAD:
            provider: IDataFetcher = AFADDataProvider(column_mapper=mapper)
        elif provider_type == ProviderName.PEER:
            provider = PeerWest2Provider(column_mapper=mapper, **kwargs)
        else:
            raise ValueError(f"Unknown provider: {provider_type}")

        if use_cache:
            return CachedProviderProxy(provider, ProviderFactory._cache_manager)

        return provider