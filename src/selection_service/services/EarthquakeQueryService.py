"""
services/EarthquakeQueryService.py

EarthquakeAPI'den çıkarılan sorgulama + strateji uygulama sorumluluğu.

Sorumluluk:
  - Provider'lardan veri çek (async veya sync).
  - Seçim stratejisini uygula.
  - PipelineResult üret.
  - Mevcut veri üzerinde yeniden strateji uygula (re_selection).

Bu sınıfın hiçbir download bilgisi yoktur.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

import pandas as pd

from ..core.ErrorHandle import PipelineError, StrategyError
from ..core.Pipeline import EarthquakePipeline, PipelineContext, PipelineResult
from ..processing.ResultHandle import Result
from ..processing.Selection import ISelectionStrategy, SearchCriteria
from ..services.ProviderRegistry import ProviderRegistry

logger = logging.getLogger(__name__)


class EarthquakeQueryService:
    """Deprem kaydı sorgulama ve seçim servisi.

    Bağımlılıklar dışarıdan enjekte edilir (DIP); bu sayede
    test ortamında sahte (fake) registry ve pipeline kullanılabilir.

    Kullanım:
        registry = ProviderRegistry.build([ProviderName.AFAD, ProviderName.PEER])
        strategies = [TBDYSelectionStrategy(config)]
        service = EarthquakeQueryService(registry, strategies)

        result = service.run_sync(criteria, "TBDY_2018_Gaussian")
        # veya
        result = await service.run_async(criteria, "TBDY_2018_Gaussian")
    """

    def __init__(
        self,
        registry: ProviderRegistry,
        strategies: List[ISelectionStrategy],
        pipeline: EarthquakePipeline | None = None,
    ) -> None:
        self._registry = registry
        self._strategies: Dict[str, ISelectionStrategy] = {
            s.get_name(): s for s in strategies
        }
        # Pipeline dışarıdan verilebilir; verilmezse varsayılan oluşturulur.
        self._pipeline = pipeline or EarthquakePipeline()

    # ── Sorgulama ─────────────────────────────────────────────────

    async def run_async(
        self,
        criteria: SearchCriteria,
        strategy_name: str,
    ) -> Result[PipelineResult, PipelineError]:
        """Pipeline'ı asenkron çalıştır.

        Args:
            criteria:      Arama kriterleri (Pydantic ile doğrulanmış).
            strategy_name: Kullanılacak stratejinin adı (ör. 'TBDY_2018_Gaussian').

        Returns:
            Result.ok(PipelineResult) — başarı.
            Result.fail(PipelineError) — strateji bulunamadı veya pipeline hatası.
        """
        context = self._build_context(criteria, strategy_name)
        if isinstance(context, Result):
            return context
        return await self._pipeline.execute_async(context)

    def run_sync(
        self,
        criteria: SearchCriteria,
        strategy_name: str,
    ) -> Result[PipelineResult, PipelineError]:
        """Pipeline'ı senkron çalıştır.

        Args:
            criteria:      Arama kriterleri (Pydantic ile doğrulanmış).
            strategy_name: Kullanılacak stratejinin adı.

        Returns:
            Result.ok(PipelineResult) — başarı.
            Result.fail(PipelineError) — strateji bulunamadı veya pipeline hatası.
        """
        context = self._build_context(criteria, strategy_name)
        if isinstance(context, Result):
            return context
        return self._pipeline.execute_sync(context)

    # ── Re-selection ──────────────────────────────────────────────

    def re_selection(
        self,
        df: pd.DataFrame,
        strategy_name: str,
        new_criteria: SearchCriteria,
    ) -> Result[PipelineResult, PipelineError]:
        """Var olan veri üzerinde yeniden strateji uygula.

        Provider'lara tekrar sorgu atmadan, elinizdeki DataFrame üzerinde
        farklı bir strateji veya yeni kriterler denemenizi sağlar.

        Args:
            df:            Daha önce çekilmiş ve birleştirilmiş kayıt DataFrame'i.
            strategy_name: Uygulanacak stratejinin adı.
            new_criteria:  Yeni puanlama kriterleri ve ağırlıkları.

        Returns:
            Result.ok(PipelineResult) — başarı; execution_time=0.0 (fetch yok).
            Result.fail(StrategyError) — strateji bulunamadı veya uygulama hatası.
        """
        strategy = self._get_strategy(strategy_name)
        if strategy is None:
            return Result.fail(
                StrategyError(
                    f"Strategy '{strategy_name}' not found. "
                    f"Available: {list(self._strategies.keys())}"
                )
            )

        try:
            selected, scored = strategy.select_and_score(df, new_criteria)

            # Rapor üretmek için minimal bir context oluştur.
            # Provider listesi boş; sadece strateji + veri kullanılır.
            dummy_ctx = PipelineContext(
                providers=[],
                strategy=strategy,
                search_criteria=new_criteria,
            )
            dummy_ctx.selected_df = selected
            dummy_ctx.scored_df = scored

            report = self._pipeline.reporter.generate_report(dummy_ctx)

            return Result.ok(
                PipelineResult(
                    selected_df=selected,
                    scored_df=scored,
                    report=report,
                    execution_time=0.0,
                )
            )
        except Exception as e:
            return Result.fail(StrategyError(f"Re-selection failed: {e}"))

    # ── Yardımcı erişimciler ──────────────────────────────────────

    def list_strategies(self) -> List[str]:
        """Kayıtlı strateji isimlerini döndür."""
        return list(self._strategies.keys())

    def list_providers(self) -> List[str]:
        """Aktif provider isimlerini döndür."""
        return self._registry.list_names()

    # ── Private ───────────────────────────────────────────────────

    def _build_context(
        self,
        criteria: SearchCriteria,
        strategy_name: str,
    ) -> PipelineContext | Result:
        """PipelineContext oluştur; strateji yoksa Result.fail döndür."""
        strategy = self._get_strategy(strategy_name)
        if strategy is None:
            return Result.fail(
                PipelineError(
                    f"Strategy '{strategy_name}' not found. "
                    f"Available: {list(self._strategies.keys())}"
                )
            )
        return PipelineContext(
            providers=self._registry.all(),
            strategy=strategy,
            search_criteria=criteria,
        )

    def _get_strategy(self, name: str) -> ISelectionStrategy | None:
        strategy = self._strategies.get(name)
        if strategy is None:
            logger.warning(
                "Strategy '%s' bulunamadı. Mevcut: %s",
                name,
                list(self._strategies.keys()),
            )
        return strategy
