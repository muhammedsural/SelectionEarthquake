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

# class EarthquakePipeline:
#     """Main pipeline engine (stateless) with Result Pattern"""

#     # ASENKRON methods
#     async def execute_async(self,
#                             providers: List[IDataProvider],
#                             strategy: ISelectionStrategy,
#                             search_criteria: SearchCriteria) -> Result[PipelineResult, PipelineError]:
#         """Asenkron pipeline çalıştır"""
       
#         # logger.info(f"Pipeline (async) running: strategy={strategy.get_name()}, providers={len(providers)}")
#         context = PipelineContext(
#             providers=providers,
#             strategy=strategy,
#             search_criteria=search_criteria
#         )
#         return await self._execute_pipeline_async(context)

#     async def _execute_pipeline_async(self, context: PipelineContext) -> Result[PipelineResult, PipelineError]:
#         """Railway oriented pipeline execution"""

#         # Define the processing pipeline
#         result = await self._compose_async(
#             self._validate_inputs_async,
#             self._fetch_data_async,
#             self._combine_data,
#             self._apply_strategy,
#             self._generate_final_result
#         )(context)

#         return result

#     def _compose_async(self, *funcs: Callable) -> Callable:
#         """Compose async and sync functions in railway pattern"""
#         import inspect
#         async def composed(input: PipelineContext) -> Result[PipelineResult, PipelineError]:
#             current_result = Result.ok(input)
            
#             for func in funcs:
#                 if current_result.success:
#                     if inspect.iscoroutinefunction(func):
#                         current_result = await func(current_result.value)
#                     else:
#                         current_result = func(current_result.value)
#                 else:
#                     break
            
#             return current_result
#         return composed

#     @async_result_decorator
#     async def _validate_inputs_async(self, context: PipelineContext) -> PipelineContext:
#         # Pydantic zaten initialization sırasında validate ediyor ama ekstra logic varsa buraya
#         if context.search_criteria.min_magnitude is None and context.search_criteria.target_magnitude is None:
#              context.logs.append("Warning: Neither min_magnitude nor target_magnitude specified.")
#         return context

#     @async_result_decorator
#     async def _fetch_data_async(self,
#                                 context: PipelineContext) -> PipelineContext:
#         """Fetch data from all providers asynchronously"""

#         async def fetch_single_provider(provider: IDataProvider) -> Result[pd.DataFrame, ProviderError]:
#             try:
#                 crit = provider.map_criteria(context.search_criteria)
#                 data = await provider.fetch_data_async(criteria=crit)
#                 return data
#             except Exception as e:
#                 return Result.fail(ProviderError(provider.get_name(), e))

#         # Fetch data from all providers concurrently
#         tasks = [fetch_single_provider(provider) for provider in context.providers]
#         results = await asyncio.gather(*tasks)

#         # Process results
#         successful_data = []        
#         for result in results:
#             if result.success:
#                 successful_data.append(result.value)
#                 context.logs.append(f"[OK] {context.providers[len(successful_data)-1].get_name()} success")
#             else:
#                 context.failed_providers.append(result.error.provider_name)
#                 context.logs.append(f"[ERROR] {result.error}")

#         if not successful_data:
#             raise NoDataError("No data received from any provider")

#         context.data = successful_data
#         return context

#     # SENKRON metodlar
#     def execute_sync(self, providers: List[IDataProvider],
#                      strategy: ISelectionStrategy,
#                      search_criteria: SearchCriteria) -> Result[PipelineResult, PipelineError]:
#         """Senkron pipeline çalıştır"""
    
#         # logger.info(f"Pipeline (sync) running: strategy={strategy.get_name()}, providers={len(providers)}")
    
#         context = PipelineContext(
#             providers=providers,
#             strategy=strategy,
#             search_criteria=search_criteria
#         )
#         return self._execute_pipeline_sync(context=context)

#     def _execute_pipeline_sync(self, context: PipelineContext) -> Result[PipelineResult, PipelineError]:
#         """Senkron pipeline execution"""
#         result = self._compose_sync(
#             self._validate_inputs_sync,
#             self._fetch_data_sync,
#             self._combine_data,
#             self._apply_strategy,
#             self._generate_final_result
#         )(context)

#         return result

#     def _compose_sync(self, *funcs: Callable) -> Callable:
#         """Compose sync functions in railway pattern"""
#         def composed(input: PipelineContext) -> Result[PipelineResult, PipelineError]:
#             current_result = Result.ok(input)

#             for func in funcs:
#                 if current_result.success:
#                     current_result = func(current_result.value)
#                 else:
#                     break

#             return current_result
#         return composed

#     @result_decorator
#     def _validate_inputs_sync(self, context: PipelineContext) -> PipelineContext:
#         """Validate inputs (sync)"""
#         # Pydantic zaten initialization sırasında validate ediyor ama ekstra logic varsa buraya
#         if context.search_criteria.min_magnitude is None and context.search_criteria.target_magnitude is None:
#              context.logs.append("Warning: Neither min_magnitude nor target_magnitude specified.")
#         return context

#     @result_decorator
#     def _fetch_data_sync(self, context: PipelineContext) -> PipelineContext:
#         """Fetch data from all providers synchronously"""
#         results = []

#         for provider in context.providers:
#             try:
#                 crit = provider.map_criteria(context.search_criteria)
#                 result = provider.fetch_data_sync(criteria=crit)

#                 if result.success:
#                     results.append(result.value)
#                     context.logs.append(f"[OK] {provider.get_name()} success")
#                 else:
#                     context.failed_providers.append(provider.get_name())
#                     context.logs.append(f"[ERROR] {provider.get_name()}: {result.error}")

#             except Exception as e:
#                 context.failed_providers.append(provider.get_name())
#                 context.logs.append(f"[ERROR] {provider.get_name()}: {e}")

#         if not results:
#             raise NoDataError("No data received from any provider")

#         context.data = results
#         return context

#     # ORTAK metodlar (hem sync hem async için)

#     @result_decorator
#     def _combine_data(self, context: PipelineContext) -> PipelineContext:
#         """Combine data from multiple providers"""
#         if not context.data:
#             raise NoDataError("No data to combine")

#         #context type -->selection_service.ResultHandle.Result olduğu için value değerleri providerdan gelen dataframelerdir çünkü Result nesnesine çevrilip döndürülüyor.
#         # valid_dfs = [df for df in context.data if isinstance(df, pd.DataFrame) and not df.empty]
#         valid_dfs = [ df.dropna(axis=1, how='all')  
#                      for df in context.data 
#                      if isinstance(df, pd.DataFrame) and not df.empty and df.dropna(axis=1, how='all').shape[1] > 0]
        
#         if not valid_dfs:
#             raise NoDataError("No valid dataframes to combine")
        
#         context.combined_df = pd.concat(valid_dfs, ignore_index=True)
        
#         # Tümü NaN olan sütunları temizle
#         context.combined_df = context.combined_df.dropna(axis=1, how='all')

#         # Kalan NaN değerleri doldur
#         context.combined_df = context.combined_df.fillna(0)  # Sayısal sütunlar için 0
#         # Object tipindeki sütunlarda hala NaN varsa boş string ile doldur
#         object_cols = context.combined_df.select_dtypes(include=['object']).columns
#         context.combined_df[object_cols] = context.combined_df[object_cols].fillna("")
        
#         context.logs.append(f"Combined {len(valid_dfs)} datasets, total {len(context.combined_df)} records")
#         return context

#     @result_decorator
#     def _apply_strategy(self, context: PipelineContext) -> PipelineContext:
#         """Apply selection strategy"""
#         if context.combined_df is None or context.combined_df.empty:
#             raise NoDataError("No data to apply strategy on")
        
#         try:
#             selected_df, scored_df = context.strategy.select_and_score(df= context.combined_df, criteria= context.search_criteria)
#             context.selected_df = selected_df
#             context.scored_df = scored_df
#             context.logs.append(f"Strategy applied: {context.strategy.get_name()}")
#         except Exception as e:
#             raise StrategyError(f"Strategy application failed: {e}")
        
#         return context

#     @result_decorator
#     def _generate_final_result(self, context: PipelineContext) -> PipelineResult:
#         """Generate final pipeline result"""
#         if context.selected_df is None or context.scored_df is None:
#             raise ValueError("No data available for result generation")
        
#         exec_time = time.time() - context.start_time
#         context.logs.append(f"Execution time: {exec_time:.2f} sec")
        
#         report = self._generate_report(
#             context.selected_df, context.scored_df,
#             context.search_criteria,
#             context.strategy, context.providers
#         )
        
#         return PipelineResult(
#             selected_df=context.selected_df,
#             scored_df=context.scored_df,
#             report=report,
#             execution_time=exec_time,
#             failed_providers=context.failed_providers,
#             logs=context.logs
#         )

#     def _generate_report(self, selected_df: pd.DataFrame, scored_df: pd.DataFrame,
#                          search_criteria: SearchCriteria,
#                          strategy: ISelectionStrategy, providers: List[IDataProvider]) -> Dict[str, Any]:
#         """Generate report dictionary"""
#         if selected_df.empty:
#             return {"status": "warning", "message": "No records selected"}

#         return {
#             "status": "success",
#             "search_criteria": search_criteria,
#             "selected_count": len(selected_df),
#             "total_considered": len(scored_df),
#             "strategy": strategy.get_name(),
#             "providers": [p.get_name() for p in providers],
#             "records": selected_df.to_dict("records"),
#             "statistics": {
#                 "magnitude_range": (selected_df["MAGNITUDE"].min(), selected_df["MAGNITUDE"].max()),
#                 "distance_range": (selected_df["RJB(km)"].min(), selected_df["RJB(km)"].max()) if "RJB(km)" in selected_df else None,
#                 "score_range": (selected_df["SCORE"].min(), selected_df["SCORE"].max())
#             }
#         }

# class EarthquakeAPI:
#     """Earthquake API with Result Pattern"""

#     def __init__(self, 
#                  providerNames: List[ProviderName],
#                  strategies: List[ISelectionStrategy],
#                  use_cache: bool = True,**kwargs: Any):
        
#         self.providerFactory = ProviderFactory()
#         self.providers = [self.providerFactory.create_provider(provider_type=name, use_cache=use_cache, **kwargs) for name in providerNames]
#         self.strategies = {s.get_name(): s for s in strategies}
#         self.pipeline = EarthquakePipeline()

#     def run_sync(self,
#                  criteria: SearchCriteria,
#                  strategy_name: str) -> Result[PipelineResult, PipelineError]:
#         """Senkron çalıştırma with Result pattern"""
#         strategy_result = self._get_strategy(strategy_name)
#         if not strategy_result.success:
#             return strategy_result
        
#         return self.pipeline.execute_sync(self.providers, strategy_result.value, criteria)

#     async def run_async(self,
#                         criteria: SearchCriteria,
#                         strategy_name: str) -> Result[PipelineResult, PipelineError]:
#         """
#         Asynchronously executes the pipeline using the specified strategy, search criteria, and target parameters.
#         Args:
#             criteria (SearchCriteria): The search criteria to be used in the pipeline.
#             target (TargetParameters): The target parameters for the pipeline execution.
#             strategy_name (str): The name of the strategy to be used.
#         Returns:
#             Result[PipelineResult, PipelineError]: A Result object containing either the pipeline result on success,
#             or a pipeline error on failure.
#         Raises:
#             None
#         Note:
#             This method uses the Result pattern for error handling and is intended to be run asynchronously.
#         """
#         strategy_result = self._get_strategy(strategy_name)
#         if not strategy_result.success:
#             return strategy_result

#         return await self.pipeline.execute_async(self.providers,
#                                                  strategy_result.value,
#                                                  criteria)

#     def _get_strategy(self,
#                       name: str) -> Result[ISelectionStrategy, ValueError]:
#         """Get strategy with Result pattern"""
#         if name not in self.strategies:
#             return Result.fail(ValueError(f"Strategy {name} not found"))
#         return Result.ok(self.strategies[name])

#     def _get_provider_by_name(self, name: str) -> Optional[IDataProvider]:
#         """Get provider by name"""
#         for provider in self.providers:
#             if provider.get_name() == name:
#                 return provider
#         return None

#     def download_single_waveforms(self, provider_name: str, filename: str, **kwargs) -> Result[bool, ProviderError]:
#         """Download single waveforms from a specific provider
#         Args:
#             provider_name (str): The name of the provider to download from.
#             filename (str): The name of the file to be downloaded.
#             **kwargs: Additional keyword arguments for the download function.
#                         event_id (int): İlgili deprem olayının ID'si (klasör yapısı için)
#                         station_id (str): İstasyon kodu (dosya adlandırması için)
#         Returns:
#             Result[bool, ProviderError]: A Result object containing either True on successful download,
#         """
#         provider = self._get_provider_by_name(provider_name)
#         if not provider:
#             return Result.fail(ProviderError(provider_name, "Provider not found"))
#         try:
#             return provider.download_single_waveforms(filename=filename, **kwargs)
#         except Exception as e:
#             return Result.fail(ProviderError(provider_name, e))
        
#     def download_waveforms(self, result_df : pd.DataFrame) -> Result[bool, ProviderError]:
#         """Download waveforms for a specific event and station"""
#         for provider in self.providers:
#             try:
#                 # if provider.get_name() == ProviderName.PEER.value:
#                 #     print(f"PEER veri setinde dalga formu dosyaları mevcut değil, bu fonksiyon sadece şablon amaçlı bırakılmıştır.")
#                 #     continue

#                 for _, row in result_df.iterrows():
#                     source = row['PROVIDER']
#                     if source != provider.get_name():
#                         continue
#                     filename = row['FILE_NAME_H1']
#                     event_id = row['EVENT']
#                     station_code = row['SSN']
#                     res = provider.download_single_waveforms(filename=filename, event_id=event_id, station_code=station_code)
#                     if not res.success:
#                         return res
#                 else:
#                     Result.fail(ProviderError(provider.get_name()))
#             except Exception as e:
#                 Result.fail(ProviderError(provider.get_name(), e, "Failed to download waveforms"))
        
#         return Result.ok(True)

#     def get_selected_data(self, result: PipelineResult) -> pd.DataFrame:
#         """Get selected data from pipeline result"""
#         if result.selected_df is not None:
#             return result.selected_df
#         else:
#             raise ValueError("No selected data available in the result")

#     def get_scored_data(self, result: PipelineResult) -> pd.DataFrame:
#         """Get scored data from pipeline result"""
#         if result.scored_df is not None:
#             return result.scored_df
#         else:
#             raise ValueError("No scored data available in the result")

#     def get_report(self, result: PipelineResult) -> Dict[str, Any]:
#         """Get report from pipeline result"""
#         if result.report is not None:
#             return result.report
#         else:
#             raise ValueError("No report available in the result")

#     def get_execution_time(self, result: PipelineResult) -> float:
#         """Get execution time from pipeline result"""
#         if result.execution_time is not None:
#             return result.execution_time
#         else:
#             raise ValueError("No execution time available in the result")

#     def get_logs(self, result: PipelineResult) -> List[str]:
#         """Get logs from pipeline result"""
#         if result.logs is not None:
#             return result.logs
#         else:
#             raise ValueError("No logs available in the result")

#     def get_failed_providers(self, result: PipelineResult) -> List[str]:
#         """Get failed providers from pipeline result"""
#         if result.failed_providers is not None:
#             return result.failed_providers
#         else:
#             raise ValueError("No failed providers available in the result")

#     def get_statistics(self, result: PipelineResult) -> Dict[str, Any]:
#         """Get statistics from pipeline result report"""
#         report = self.get_report(result)
#         if 'statistics' in report:
#             return report['statistics']
#         else:
#             raise ValueError("No statistics available in the report")

#     def get_strategy_info(self, result: PipelineResult) -> str:
#         """Get strategy info from pipeline result report"""
#         report = self.get_report(result)
#         if 'strategy' in report:
#             return report['strategy']
#         else:
#             raise ValueError("No strategy info available in the report")

#     def get_provider_info(self, result: PipelineResult) -> List[str]:
#         """Get provider info from pipeline result report"""
#         report = self.get_report(result)
#         if 'providers' in report:
#             return report['providers']
#         else:
#             raise ValueError("No provider info available in the report")

#     def get_search_criteria_info(self, result: PipelineResult) -> Dict[str, Any]:
#         """Get search criteria info from pipeline result report"""
#         report = self.get_report(result)
#         if 'search_criteria' in report:
#             return report['search_criteria']
#         else:
#             raise ValueError("No search criteria info available in the report")

#     def get_selected_records(self, result: PipelineResult) -> List[Dict[str, Any]]:
#         """Get selected records from pipeline result report"""
#         report = self.get_report(result)
#         if 'records' in report:
#             return report['records']
#         else:
#             raise ValueError("No selected records available in the report")

#     def get_total_considered_records(self, result: PipelineResult) -> int:
#         """Get total considered records from pipeline result report"""
#         report = self.get_report(result)
#         if 'total_considered' in report:
#             return report['total_considered']
#         else:
#             raise ValueError("No total considered records info available in the report")

#     def get_selected_count(self, result: PipelineResult) -> int:
#         """Get selected count from pipeline result report"""
#         report = self.get_report(result)
#         if 'selected_count' in report:
#             return report['selected_count']
#         else:
#             raise ValueError("No selected count info available in the report")
    
#     def reSelection(self, df : pd.DataFrame, strategy_name: str, new_criteria: SearchCriteria) -> Result[PipelineResult, PipelineError]:
#         """Re-selection with new criteria and strategy"""
#         strategy_result = self._get_strategy(strategy_name)
#         if not strategy_result.success:
#             return strategy_result
        
#         try:
#             selected_df, scored_df = strategy_result.value.select_and_score(df=df, criteria=new_criteria)
#             report = self.pipeline._generate_report(
#                 selected_df, scored_df,
#                 search_criteria=new_criteria,
#                 strategy=strategy_result.value, providers=self.providers
#             )
#             return Result.ok(PipelineResult(
#                 selected_df=selected_df,
#                 scored_df=scored_df,
#                 report=report,
#                 execution_time=0.0  # Re-selection için zaman ölçümü yapılmaz
#             ))
#         except Exception as e:
#             return Result.fail(StrategyError(f"Re-selection failed: {e}"))

#     def __repr__(self):        return f"EarthquakeAPI(providers={[p.get_name() for p in self.providers]}, strategies={list(self.strategies.keys())})"

#     def __str__(self):        return self.__repr__()

#     def __getattr__(self, name):
#         """Delegate unknown attributes to providers if they exist"""
#         for provider in self.providers:
#             if hasattr(provider, name):
#                 return getattr(provider, name)
#         raise AttributeError(f"'EarthquakeAPI' object has no attribute '{name}'")

    
