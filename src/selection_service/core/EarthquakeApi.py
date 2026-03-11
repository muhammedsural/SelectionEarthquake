"""
core/EarthquakeApi.py  (Adım 2 — SRP refactor)

EarthquakeAPI artık bir Facade'dır.
Tüm iş mantığı üç ayrı servise taşındı:

  ProviderRegistry         → provider yönetimi
  EarthquakeQueryService   → sorgulama + strateji
  WaveformDownloadService  → dalga formu indirme

Bu sınıf yalnızca bu üç servisi bir araya getirir ve
kullanıcıya tek bir giriş noktası sunar.
Yeni iş kuralı eklenecekse ilgili servis dosyasına gidilir,
bu dosyaya dokunulmaz.

Geriye dönük uyumluluk:
  Mevcut kullanım kalıbı (EarthquakeAPI(...).run_sync / run_async /
  download_waveforms / re_selection) korunmuştur.
"""

from __future__ import annotations

import logging
from typing import Any, List

import pandas as pd

from ..core.ErrorHandle import PipelineError, ProviderError
from ..core.Pipeline import PipelineResult
from ..enums.Enums import ProviderName
from ..processing.ResultHandle import Result
from ..processing.Selection import ISelectionStrategy, SearchCriteria
from ..services.EarthquakeQueryService import EarthquakeQueryService
from ..services.ProviderRegistry import ProviderRegistry
from ..services.WaveformDownloadService import WaveformDownloadService

logger = logging.getLogger(__name__)


class EarthquakeAPI:
    """Kullanıcıya yönelik üst düzey Facade.

    Doğrudan kullanım:
        api = EarthquakeAPI(
            provider_names=[ProviderName.AFAD, ProviderName.PEER],
            strategies=[TBDYSelectionStrategy(config)],
        )
        result = api.run_sync(criteria, "TBDY_2018_Gaussian")
        api.download_waveforms(result.value.selected_df)

    Servislerle doğrudan çalışmak isteyenler için:
        api.query      → EarthquakeQueryService
        api.downloader → WaveformDownloadService
        api.registry   → ProviderRegistry
    """

    def __init__(
        self,
        provider_names: List[ProviderName],
        strategies: List[ISelectionStrategy],
        use_cache: bool = True,
        **kwargs: Any,
    ) -> None:
        # 1. Provider'ları oluştur ve kaydet
        self.registry = ProviderRegistry.build(
            provider_names, use_cache=use_cache, **kwargs
        )

        # 2. Sorgulama servisi
        self.query = EarthquakeQueryService(
            registry=self.registry,
            strategies=strategies,
        )

        # 3. Download servisi
        self.downloader = WaveformDownloadService(registry=self.registry)

    # ── Sorgulama (EarthquakeQueryService'e delege) ────────────────

    async def run_async(
        self,
        criteria: SearchCriteria,
        strategy_name: str,
    ) -> Result[PipelineResult, PipelineError]:
        """Pipeline'ı asenkron çalıştır."""
        return await self.query.run_async(criteria, strategy_name)

    def run_sync(
        self,
        criteria: SearchCriteria,
        strategy_name: str,
    ) -> Result[PipelineResult, PipelineError]:
        """Pipeline'ı senkron çalıştır."""
        return self.query.run_sync(criteria, strategy_name)

    def re_selection(
        self,
        df: pd.DataFrame,
        strategy_name: str,
        new_criteria: SearchCriteria,
    ) -> Result[PipelineResult, PipelineError]:
        """Var olan veri üzerinde yeniden strateji uygula."""
        return self.query.re_selection(df, strategy_name, new_criteria)

    # ── Download (WaveformDownloadService'e delege) ────────────────

    def download_waveforms(
        self,
        result_df: pd.DataFrame,
        **kwargs: Any,
    ) -> Result[bool, ProviderError]:
        """Toplu dalga formu indirme."""
        return self.downloader.download_batch(result_df, **kwargs)

    def download_single_waveform(
        self,
        filename: str,
        event_id: str,
        station_code: str,
        **kwargs: Any,
    ) -> Result[bool, ProviderError]:
        """Tekil dalga formu indirme."""
        return self.downloader.download_single(filename, event_id, station_code, **kwargs)