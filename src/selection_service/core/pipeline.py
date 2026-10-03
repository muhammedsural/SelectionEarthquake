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

from ..core.error_handle import NoDataError, PipelineError, ProviderError, StrategyError
from ..processing.result_handle import Result, async_result_decorator, result_decorator
from ..processing.criteria import SearchCriteria
from ..processing.strategies import ISelectionStrategy
from ..providers.interfaces import IDataFetcher              # ← yeni; ProviderFactory import kaldırıldı

logger = logging.getLogger(__name__)

# Bu kolonlarda 0 geçerli bir ölçüm değil, "veri yok" anlamına gelir
# (ör. AFAD mapper'ı bilinmeyen istasyon Vs30'unu 0.0 yazar).
ZERO_MEANS_MISSING = ("VS30(m/s)", "MAGNITUDE")
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
    strategy_input_df: Optional[pd.DataFrame]      = None
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
            return {
                "status": "warning",
                "message": "No records selected",
                "compliance": self._compliance(context),
            }

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
            "selection_summary": self._selection_summary(context.scored_df),
            "score_breakdown": self._selected_score_breakdown(context.selected_df),
            "error_metrics": self._selected_error_metrics(context.selected_df),
            "compliance": self._compliance(context),
        }

    def _compliance(self, context: PipelineContext) -> Dict[str, Any]:
        """Seçim sayısı ve olay başına kayıt sınırı kontrolü.

        TBDY 2018: her yön için 11 kayıt (11*2 = 22) ve aynı depremden en
        fazla 3 kayıt/kayıt takımı. Sınırlar strateji yapılandırmasından
        (``num_records``, ``max_per_event``) okunur.
        """
        config = getattr(context.strategy, "config", None)
        selected = context.selected_df
        count = 0 if selected is None else len(selected)
        result: Dict[str, Any] = {"selected_count": count, "warnings": []}
        required = getattr(config, "num_records", None)
        limit = getattr(config, "max_per_event", None)
        if not isinstance(required, int) or not isinstance(limit, int):
            return result

        per_event: Dict[str, int] = {}
        if selected is not None and not selected.empty and "EVENT" in selected.columns:
            per_event = selected["EVENT"].value_counts().to_dict()
        max_per_event = max(per_event.values(), default=0)

        result.update(
            required_count=required,
            shortfall=max(required - count, 0),
            max_per_event_limit=limit,
            max_selected_per_event=max_per_event,
            max_per_event_ok=max_per_event <= limit,
        )
        if count < required:
            result["warnings"].append(
                f"Yetersiz kayıt: {count}/{required} seçilebildi. Arama "
                "kriterlerini (büyüklük, uzaklık, Vs30, mekanizma) genişletin."
            )
        if max_per_event > limit:
            result["warnings"].append(
                f"Aynı depremden {max_per_event} kayıt seçildi (sınır {limit})."
            )
        result["compliant"] = not result["warnings"]
        return result

    def _calculate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "magnitude_range": (df["MAGNITUDE"].min(), df["MAGNITUDE"].max()),
            "score_range": (df["SCORE"].min(), df["SCORE"].max()),
        }
        if "RJB(km)" in df.columns:
            stats["distance_range"] = (df["RJB(km)"].min(), df["RJB(km)"].max())
        return stats

    def _selection_summary(self, df: pd.DataFrame | None) -> Dict[str, Any]:
        """Summarize selected/rejected records and rejection reasons."""
        if df is None or df.empty or "SELECTION_STATUS" not in df.columns:
            return {"status_counts": {}, "rejection_reasons": {}}

        status_counts = df["SELECTION_STATUS"].value_counts().to_dict()
        if "SELECTION_REASON" not in df.columns:
            return {"status_counts": status_counts, "rejection_reasons": {}}

        rejected = df[df["SELECTION_STATUS"] == "rejected"]
        return {
            "status_counts": status_counts,
            "rejection_reasons": rejected["SELECTION_REASON"].value_counts().to_dict(),
        }

    def _selected_score_breakdown(self, df: pd.DataFrame | None) -> List[Dict[str, Any]]:
        """Expose compact criterion-level score details for selected records."""
        if df is None or df.empty or "SCORE_BREAKDOWN" not in df.columns:
            return []

        id_columns = [c for c in ("PROVIDER", "RSN", "EVENT", "STATION", "SCORE") if c in df.columns]
        rows: List[Dict[str, Any]] = []
        for _, record in df.iterrows():
            item = {col: record.get(col) for col in id_columns}
            item["criteria"] = record.get("SCORE_BREAKDOWN", [])
            item["selection_reason"] = record.get("SELECTION_REASON", "")
            rows.append(item)
        return rows

    def _selected_error_metrics(self, df: pd.DataFrame | None) -> List[Dict[str, Any]]:
        """Expose constraint strategy error metrics for selected records."""
        if df is None or df.empty or "ERROR_METRICS" not in df.columns:
            return []

        id_columns = [
            c for c in ("PROVIDER", "RSN", "EVENT", "STATION", "ERROR_TOTAL", "SCORE")
            if c in df.columns
        ]
        rows: List[Dict[str, Any]] = []
        for _, record in df.iterrows():
            item = {col: record.get(col) for col in id_columns}
            item["metrics"] = record.get("ERROR_METRICS", [])
            item["hard_filters"] = record.get("HARD_FILTERS", [])
            item["selection_reason"] = record.get("SELECTION_REASON", "")
            rows.append(item)
        return rows


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

        # Seçim algoritması eksik veriyi gerçek değer gibi puanlamasın diye
        # 0 doldurmadan önceki hali ayrıca saklanır; çıktılar yine 0 ile
        # doldurulur (aşağı akış hesaplamaları NaN ile hata veriyor).
        context.strategy_input_df = self._mark_missing(combined)

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

    @staticmethod
    def _mark_missing(df: pd.DataFrame) -> pd.DataFrame:
        """0 değeri "bilinmiyor" anlamına gelen kolonları NaN'a çevir."""
        marked = df.copy()
        for col in ZERO_MEANS_MISSING:
            if col in marked.columns:
                values = pd.to_numeric(marked[col], errors="coerce")
                marked[col] = values.mask(values <= 0)
        return marked

    @staticmethod
    def _fill_numeric_nulls(df: pd.DataFrame) -> pd.DataFrame:
        """Seçimden sonra sayısal boşlukları 0 ile doldur (çıktı sözleşmesi)."""
        if df is None or df.empty:
            return df
        filled = df.copy()
        num_cols = filled.select_dtypes(include=["number"]).columns
        filled[num_cols] = filled[num_cols].fillna(0)
        return filled

    @result_decorator
    def _apply_strategy(self, context: PipelineContext) -> PipelineContext:
        """Adım 4: Seçim stratejisi uygula."""
        if context.combined_df.empty:
            raise NoDataError("Combined data is empty")

        strategy_input = (
            context.strategy_input_df
            if context.strategy_input_df is not None
            else context.combined_df
        )
        selected, scored = context.strategy.select_and_score(
            strategy_input, context.search_criteria
        )

        context.selected_df = self._fill_numeric_nulls(selected)
        context.scored_df = self._fill_numeric_nulls(scored)
        context.logs.append(
            f"Strategy '{context.strategy.get_name()}' applied. "
            f"Selected: {len(selected)}"
        )
        for warning in PipelineReporter()._compliance(context)["warnings"]:
            context.logs.append(f"[WARN] {warning}")
            logger.warning(warning)
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
