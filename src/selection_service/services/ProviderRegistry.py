"""
services/ProviderRegistry.py

EarthquakeAPI'den çıkarılan provider yönetimi sorumluluğu.

Sorumluluk:
  - Aktif provider listesini tutar.
  - İsme göre provider arama (get) sağlar.
  - Tüm provider isimlerini listeler (list_names).

Neden ayrı bir sınıf?
  Hem EarthquakeQueryService hem de WaveformDownloadService provider'lara
  ihtiyaç duyar. Registry bu erişimi tek noktada toplar; provider listesi
  değiştiğinde yalnızca buraya dokunulur.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional

from ..enums.Enums import ProviderName
from ..providers.interfaces import IDataFetcher
from ..providers.ProvidersFactory import ProviderFactory

logger = logging.getLogger(__name__)


class ProviderRegistry:
    """Aktif provider örneklerini tutan merkezi kayıt defteri.

    Kullanım:
        registry = ProviderRegistry.build(
            provider_names=[ProviderName.AFAD, ProviderName.PEER],
            use_cache=True,
        )
        afad = registry.get("AFAD")
        all_providers = registry.all()
    """

    def __init__(self, providers: List[IDataFetcher]) -> None:
        # İsme göre O(1) arama için dict'e çevir
        self._providers: dict[str, IDataFetcher] = {
            p.get_name(): p for p in providers
        }

    # ── Factory ───────────────────────────────────────────────────

    @classmethod
    def build(
        cls,
        provider_names: List[ProviderName],
        use_cache: bool = True,
        **kwargs: Any,
    ) -> "ProviderRegistry":
        """ProviderName listesinden registry oluştur.

        Args:
            provider_names: Aktif edilecek provider'lar.
            use_cache:      True ise her provider CachedProviderProxy ile sarmalanır.
            **kwargs:       Provider'lara iletilen ek parametreler.
        """
        factory = ProviderFactory()
        providers = [
            factory.create_provider(name, use_cache=use_cache, **kwargs)
            for name in provider_names
        ]
        return cls(providers)

    # ── Erişim ────────────────────────────────────────────────────

    def get(self, name: str) -> Optional[IDataFetcher]:
        """İsme göre provider döndür; bulunamazsa None."""
        provider = self._providers.get(name)
        if provider is None:
            logger.warning("Provider '%s' registry'de bulunamadı.", name)
        return provider

    def all(self) -> List[IDataFetcher]:
        """Tüm kayıtlı provider örneklerini döndür."""
        return list(self._providers.values())

    def list_names(self) -> List[str]:
        """Kayıtlı provider isimlerini döndür."""
        return list(self._providers.keys())
