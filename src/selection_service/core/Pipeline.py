"""
core/Pipeline.py  (refactored)

Değişiklikler (Adım 1 — ISP + LSP):
  - IDataProvider  → IDataFetcher
  - Kullanılmayan ProviderFactory import'u kaldırıldı.
  - Kullanılmayan ProviderName import'u kaldırıldı.
"""

import asyncio
import inspect
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional
import logging
import pandas as pd

from ..core.ErrorHandle import NoDataError, PipelineError, ProviderError, StrategyError
from ..processing.ResultHandle import Result, async_result_decorator, result_decorator
from ..processing.Selection import ISelectionStrategy, SearchCriteria
from ..providers.interfaces import IDataFetcher              # ← yeni; ProviderFactory import kaldırıldı

logger = logging.getLogger(__name__)
# ──────────────────────────────────────────────────────────────────────────────
# Veri yapıları
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class PipelineResult:
    selected_df    : pd.DataFrame
    scored_df      : pd.DataFrame
    report         : Dict[str, Any]
    execution_time : float
    failed_providers: List[str] = field(default_factory=list)
    logs           : List[str]  = field(default_factory=list)


@dataclass
class PipelineContext:
    providers       : List[IDataFetcher]    # ← IDataProvider → IDataFetcher
    strategy        : ISelectionStrategy
    search_criteria : SearchCriteria
    data            : Optional[List[pd.DataFrame]] = None
    combined_df     : Optional[pd.DataFrame]       = None
    selected_df     : Optional[pd.DataFrame]       = None
    scored_df       : Optional[pd.DataFrame]       = None
    failed_providers: List[str]                    = field(default_factory=list)
    logs            : List[str]                    = field(default_factory=list)
    start_time      : float                        = field(default_factory=time.time)


# ──────────────────────────────────────────────────────────────────────────────
# Rapor üretici
# ──────────────────────────────────────────────────────────────────────────────

class PipelineReporter:
    """Rapor oluşturma işlemlerinden sorumlu sınıf."""

    def generate_report(self, context: PipelineContext) -> Dict[str, Any]:
        if context.selected_df is None or context.selected_df.empty:
            return {"status": "warning", "message": "No records selected"}

        return {
            "status": "success",
            "search_criteria": (
                context.search_criteria.model_dump()
                if hasattr(context.search_criteria, "model_dump")
                else context.search_criteria
            ),
            "selected_count": len(context.selected_df),
            "total_considered": (
                len(context.scored_df) if context.scored_df is not None else 0
            ),
            "strategy": context.strategy.get_name(),
            "providers": [p.get_name() for p in context.providers],
            "records": context.selected_df.to_dict("records"),
            "statistics": self._calculate_statistics(context.selected_df),
        }

    def _calculate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "magnitude_range": (df["MAGNITUDE"].min(), df["MAGNITUDE"].max()),
            "score_range": (df["SCORE"].min(), df["SCORE"].max()),
        }
        if "RJB(km)" in df.columns:
            stats["distance_range"] = (df["RJB(km)"].min(), df["RJB(km)"].max())
        return stats


# ──────────────────────────────────────────────────────────────────────────────
# Pipeline motoru
# ──────────────────────────────────────────────────────────────────────────────

class EarthquakePipeline:
    """Railway Oriented Pipeline Engine.

    Her adım Result[T, E] döndürür; bir adım başarısız olursa
    sonraki adımlar çalıştırılmaz (short-circuit).
    """

    def __init__(self) -> None:
        self.reporter = PipelineReporter()

    # ── Async execution ────────────────────────────────────────────

    async def execute_async(
        self, context: PipelineContext
    ) -> Result[PipelineResult, PipelineError]:
        context.start_time = time.time()
        pipeline_flow = self._compose_async(
            self._validate_inputs,
            self._fetch_data_async,
            self._combine_data,
            self._apply_strategy,
            self._finalize_result,
        )
        return await pipeline_flow(context)

    # ── Sync execution ─────────────────────────────────────────────

    def execute_sync(
        self, context: PipelineContext
    ) -> Result[PipelineResult, PipelineError]:
        context.start_time = time.time()
        pipeline_flow = self._compose_sync(
            self._validate_inputs,
            self._fetch_data_sync,
            self._combine_data,
            self._apply_strategy,
            self._finalize_result,
        )
        return pipeline_flow(context)

    # ── Pipeline adımları ──────────────────────────────────────────

    @result_decorator
    def _validate_inputs(self, context: PipelineContext) -> PipelineContext:
        """Adım 1: Girdi kontrolü."""
        if not context.providers:
            raise PipelineError("Validation", None, "No providers specified")
        return context

    @async_result_decorator
    async def _fetch_data_async(self, context: PipelineContext) -> PipelineContext:
        """Adım 2 (Async): Paralel veri çekme."""

        async def _fetch_safe(
            provider: IDataFetcher,
        ) -> Result[pd.DataFrame, ProviderError]:
            try:
                crit = provider.map_criteria(context.search_criteria)
                return await provider.fetch_data_async(crit)
            except Exception as e:
                return Result.fail(ProviderError(provider.get_name(), e))

        tasks = [_fetch_safe(p) for p in context.providers]
        results = await asyncio.gather(*tasks)

        valid_data = []
        for i, res in enumerate(results):
            p_name = context.providers[i].get_name()
            if res.success and res.value is not None and not res.value.empty:
                valid_data.append(res.value)
                context.logs.append(f"[OK] {p_name} fetched {len(res.value)} records")
            else:
                context.failed_providers.append(p_name)
                reason = str(res.error) if not res.success else "Boş veri döndü"
                context.logs.append(f"[FAIL] {p_name}: {reason}")
                logger.warning("[%s] Veri alınamadı: %s", p_name, reason)

        if not valid_data:
            failed = ", ".join(context.failed_providers) or "bilinmiyor"
            raise NoDataError(
                f"Hiçbir sağlayıcıdan veri alınamadı. "
                f"Başarısız: [{failed}]. "
                "Detaylar için yukarıdaki uyarı mesajlarını inceleyin."
            )

        context.data = valid_data
        return context

    @result_decorator
    def _fetch_data_sync(self, context: PipelineContext) -> PipelineContext:
        """Adım 2 (Sync): Sıralı veri çekme."""
        valid_data = []
        for provider in context.providers:
            p_name = provider.get_name()
            try:
                crit = provider.map_criteria(context.search_criteria)
                res = provider.fetch_data_sync(crit)

                if res.success and res.value is not None and not res.value.empty:
                    valid_data.append(res.value)
                    context.logs.append(f"[OK] {p_name} fetched {len(res.value)} records")
                else:
                    context.failed_providers.append(p_name)
                    reason = str(res.error) if res.error else "Boş veri döndü"
                    context.logs.append(f"[FAIL] {p_name}: {reason}")
                    logger.warning("[%s] Veri alınamadı: %s", p_name, reason)

            except Exception as e:
                context.failed_providers.append(p_name)
                context.logs.append(f"[ERROR] {p_name}: {e}")
                logger.error("[%s] Beklenmedik hata: %s", p_name, e, exc_info=True)

        if not valid_data:
            failed = ", ".join(context.failed_providers) or "bilinmiyor"
            raise NoDataError(
                f"Hiçbir sağlayıcıdan veri alınamadı. "
                f"Başarısız: [{failed}]. "
                "Detaylar için yukarıdaki uyarı mesajlarını inceleyin."
            )

        context.data = valid_data
        return context

    @result_decorator
    def _combine_data(self, context: PipelineContext) -> PipelineContext:
        """Adım 3: Veri birleştirme ve temizleme.

        Önemli: dropna(axis=1, how="all") uygulanmaz.
        Bir provider'ın tüm satırları için None olan sütunlar (ör. PEER'dan
        gelen ENDPOINTSOURCE) diğer provider'ın verilerinde dolu olabilir.
        Sütun silme işlemi bu değerleri de yok edeceğinden uygulanmıyor.
        Bunun yerine eksik string kolonlar "", sayısal kolonlar 0 ile doldurulur;
        ENDPOINTSOURCE gibi URL/None kolonları None olarak bırakılır.
        """
        valid_dfs = [df for df in context.data if not df.empty]

        if not valid_dfs:
            raise NoDataError("No valid columns in retrieved data")

        combined = pd.concat(valid_dfs, ignore_index=True)

        # Sayısal kolonları 0 ile doldur
        num_cols = combined.select_dtypes(include=["number"]).columns
        combined[num_cols] = combined[num_cols].fillna(0)

        # String kolonları "" ile doldur — URL/link kolonları (ENDPOINTSOURCE) hariç
        str_cols = combined.select_dtypes(include=["object"]).columns
        url_cols = {"ENDPOINTSOURCE"}  # None kalması gereken kolonlar
        fill_str_cols = [c for c in str_cols if c not in url_cols]
        combined[fill_str_cols] = combined[fill_str_cols].fillna("")

        context.combined_df = combined
        context.logs.append(f"Combined total: {len(combined)} records")
        return context

    @result_decorator
    def _apply_strategy(self, context: PipelineContext) -> PipelineContext:
        """Adım 4: Seçim stratejisi uygula."""
        if context.combined_df.empty:
            raise NoDataError("Combined data is empty")

        selected, scored = context.strategy.select_and_score(
            context.combined_df, context.search_criteria
        )

        context.selected_df = selected
        context.scored_df = scored
        context.logs.append(
            f"Strategy '{context.strategy.get_name()}' applied. "
            f"Selected: {len(selected)}"
        )
        return context

    @result_decorator
    def _finalize_result(self, context: PipelineContext) -> PipelineResult:
        """Adım 5: Sonuç paketleme."""
        execution_time = time.time() - context.start_time
        report = self.reporter.generate_report(context)

        return PipelineResult(
            selected_df=context.selected_df,
            scored_df=context.scored_df,
            report=report,
            execution_time=execution_time,
            failed_providers=context.failed_providers,
            logs=context.logs,
        )

    # ── Railway composers ──────────────────────────────────────────

    def _compose_async(self, *funcs: Callable) -> Callable:
        async def composed(input_ctx: PipelineContext) -> Result:
            current = Result.ok(input_ctx)
            for func in funcs:
                if not current.success:
                    break
                if inspect.iscoroutinefunction(func):
                    current = await func(current.value)
                else:
                    current = func(current.value)
            return current
        return composed

    def _compose_sync(self, *funcs: Callable) -> Callable:
        def composed(input_ctx: PipelineContext) -> Result:
            current = Result.ok(input_ctx)
            for func in funcs:
                if not current.success:
                    break
                current = func(current.value)
            return current
        return composed