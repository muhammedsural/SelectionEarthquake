import asyncio
from dataclasses import dataclass, field
import time
from typing import Any, Callable, Dict, List, Optional
import pandas as pd

from ..providers.ProvidersFactory import ProviderFactory

from ..enums.Enums import ProviderName
from ..providers.IProvider import IDataProvider
from ..processing.Selection import (ISelectionStrategy,
                                    SearchCriteria,
                                    )
from ..core.ErrorHandle import (NoDataError,
                                PipelineError,
                                ProviderError,
                                StrategyError)
from ..processing.ResultHandle import (Result,
                                       async_result_decorator,
                                       result_decorator)
import logging

# logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    selected_df: pd.DataFrame
    scored_df: pd.DataFrame
    report: Dict[str, Any]
    execution_time: float
    failed_providers: List[str] = field(default_factory=list)
    logs: List[str] = field(default_factory=list)

@dataclass
class PipelineContext:
    providers       : List[IDataProvider]
    strategy        : ISelectionStrategy
    search_criteria : SearchCriteria
    data            : Optional[List[pd.DataFrame]] = None
    combined_df     : Optional[pd.DataFrame] = None
    selected_df     : Optional[pd.DataFrame] = None
    scored_df       : Optional[pd.DataFrame] = None
    failed_providers: List[str]              = field(default_factory=list)
    logs            : List[str]              = field(default_factory=list)
    start_time      : float                  = field(default_factory=time.time)

class PipelineReporter:
    """Rapor oluşturma işlemlerinden sorumlu sınıf"""
    
    def generate_report(self, context: PipelineContext) -> Dict[str, Any]:
        if context.selected_df is None or context.selected_df.empty:
            return {"status": "warning", "message": "No records selected"}

        return {
            "status": "success",
            "search_criteria": context.search_criteria.model_dump() if hasattr(context.search_criteria, 'model_dump') else context.search_criteria,
            "selected_count": len(context.selected_df),
            "total_considered": len(context.scored_df) if context.scored_df is not None else 0,
            "strategy": context.strategy.get_name(),
            "providers": [p.get_name() for p in context.providers],
            "records": context.selected_df.to_dict("records"),
            "statistics": self._calculate_statistics(context.selected_df)
        }

    def _calculate_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        stats = {
            "magnitude_range": (df["MAGNITUDE"].min(), df["MAGNITUDE"].max()),
            "score_range": (df["SCORE"].min(), df["SCORE"].max())
        }
        if "RJB(km)" in df.columns:
            stats["distance_range"] = (df["RJB(km)"].min(), df["RJB(km)"].max())
        return stats

class EarthquakePipeline:
    """Railway Oriented Pipeline Engine"""

    def __init__(self):
        self.reporter = PipelineReporter()

    # --- ASYNC EXECUTION ---
    async def execute_async(self, context: PipelineContext) -> Result[PipelineResult, PipelineError]:
        context.start_time = time.time()
        
        # Fonksiyonel kompozisyon (Railway Pattern)
        pipeline_flow = self._compose_async(
            self._validate_inputs,      # 1. Validasyon
            self._fetch_data_async,     # 2. Veri Çekme (Async)
            self._combine_data,         # 3. Birleştirme
            self._apply_strategy,       # 4. Strateji Uygulama
            self._finalize_result       # 5. Sonuç Üretme
        )
        
        return await pipeline_flow(context)

    # --- SYNC EXECUTION ---
    def execute_sync(self, context: PipelineContext) -> Result[PipelineResult, PipelineError]:
        context.start_time = time.time()

        pipeline_flow = self._compose_sync(
            self._validate_inputs,      # 1. Validasyon
            self._fetch_data_sync,      # 2. Veri Çekme (Sync)
            self._combine_data,         # 3. Birleştirme
            self._apply_strategy,       # 4. Strateji Uygulama
            self._finalize_result       # 5. Sonuç Üretme
        )
        
        return pipeline_flow(context)

    # --- PIPELINE STEPS ---

    @result_decorator
    def _validate_inputs(self, context: PipelineContext) -> PipelineContext:
        """Adım 1: Girdi Kontrolü"""
        # SearchCriteria Pydantic modeli olduğu için min/max kontrolleri zaten yapıldı.
        # Ekstra iş kuralı varsa buraya eklenir.
        if not context.providers:
            raise PipelineError("Validation", None, "No providers specified")
        return context

    @async_result_decorator
    async def _fetch_data_async(self, context: PipelineContext) -> PipelineContext:
        """Adım 2 (Async): Paralel Veri Çekme"""
        
        async def _fetch_safe(provider):
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
                err_msg = str(res.error) if not res.success else "Empty data"
                context.logs.append(f"[FAIL] {p_name}: {err_msg}")

        if not valid_data:
            raise NoDataError("All providers failed to return data")
            
        context.data = valid_data
        return context

    @result_decorator
    def _fetch_data_sync(self, context: PipelineContext) -> PipelineContext:
        """Adım 2 (Sync): Sıralı Veri Çekme"""
        valid_data = []
        for provider in context.providers:
            try:
                crit = provider.map_criteria(context.search_criteria)
                res = provider.fetch_data_sync(crit)
                
                if res.success and res.value is not None and not res.value.empty:
                    valid_data.append(res.value)
                    context.logs.append(f"[OK] {provider.get_name()} fetched {len(res.value)} records")
                else:
                    context.failed_providers.append(provider.get_name())
            except Exception as e:
                context.failed_providers.append(provider.get_name())
                context.logs.append(f"[ERROR] {provider.get_name()}: {e}")

        if not valid_data:
            raise NoDataError("All providers failed to return data")
        
        context.data = valid_data
        return context

    @result_decorator
    def _combine_data(self, context: PipelineContext) -> PipelineContext:
        """Adım 3: Veri Birleştirme ve Temizleme"""
        # Sütunları tamamen boş olanları temizle
        valid_dfs = [
            df.dropna(axis=1, how='all') 
            for df in context.data 
            if df.dropna(axis=1, how='all').shape[1] > 0
        ]
        
        if not valid_dfs:
            raise NoDataError("No valid columns in retrieved data")

        combined = pd.concat(valid_dfs, ignore_index=True)
        
        # Temizlik
        combined = combined.dropna(axis=1, how='all')
        combined = combined.fillna(0) # Numerik
        
        # Object sütunları string yap
        obj_cols = combined.select_dtypes(include=['object']).columns
        combined[obj_cols] = combined[obj_cols].fillna("")
        
        context.combined_df = combined
        context.logs.append(f"Combined total: {len(combined)} records")
        return context

    @result_decorator
    def _apply_strategy(self, context: PipelineContext) -> PipelineContext:
        """Adım 4: Seçim Stratejisi"""
        if context.combined_df.empty:
            raise NoDataError("Combined data is empty")
            
        selected, scored = context.strategy.select_and_score(
            context.combined_df, 
            context.search_criteria
        )
        
        context.selected_df = selected
        context.scored_df = scored
        context.logs.append(f"Strategy '{context.strategy.get_name()}' applied. Selected: {len(selected)}")
        return context

    @result_decorator
    def _finalize_result(self, context: PipelineContext) -> PipelineResult:
        """Adım 5: Sonuç Paketleme"""
        execution_time = time.time() - context.start_time
        report = self.reporter.generate_report(context)
        
        return PipelineResult(
            selected_df=context.selected_df,
            scored_df=context.scored_df,
            report=report,
            execution_time=execution_time,
            failed_providers=context.failed_providers,
            logs=context.logs
        )

    # --- RAILWAY COMPOSERS ---
    def _compose_async(self, *funcs: Callable) -> Callable:
        import inspect
        async def composed(input_ctx: PipelineContext) -> Result:
            current = Result.ok(input_ctx)
            for func in funcs:
                if not current.success: break
                
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
                if not current.success: break
                current = func(current.value)
            return current
        return composed

