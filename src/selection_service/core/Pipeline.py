import asyncio
from dataclasses import dataclass, field
import time
from typing import Any, Callable, Dict, List, Optional
import pandas as pd

from ..enums.Enums import ProviderName
from ..providers.IProvider import IDataProvider
from ..processing.Selection import (ISelectionStrategy,
                                    SearchCriteria,
                                    TargetParameters)
from ..core.ErrorHandle import (NoDataError,
                                PipelineError,
                                ProviderError,
                                StrategyError)
from ..processing.ResultHandle import (Result,
                                       async_result_decorator,
                                       result_decorator)
import logging

logger = logging.getLogger(__name__)


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
    providers       : List[ProviderName]
    strategy        : ISelectionStrategy
    search_criteria : SearchCriteria
    target_params   : TargetParameters
    data            : Optional[List[pd.DataFrame]] = None
    combined_df     : Optional[pd.DataFrame] = None
    selected_df     : Optional[pd.DataFrame] = None
    scored_df       : Optional[pd.DataFrame] = None
    failed_providers: List[str]              = field(default_factory=list)
    logs            : List[str]              = field(default_factory=list)
    start_time      : float                  = field(default_factory=time.time)


class EarthquakePipeline:
    """Ana pipeline engine (stateless) with Result Pattern"""

    # ASENKRON metodlar
    async def execute_async(self,
                            providerNames: List[ProviderName],
                            strategy: ISelectionStrategy,
                            search_criteria: SearchCriteria,
                            target_params: TargetParameters) -> Result[PipelineResult, PipelineError]:
        """Asenkron pipeline çalıştır"""
        # TODO IDataProvider listesi yerine ProviderName listesi alınıp providerfactory ile oluşturulmalı.
        
        # logger.info(f"Pipeline (async) running: strategy={strategy.get_name()}, providers={len(providers)}")
        providers = []
        context = PipelineContext(
            providers=providers,
            strategy=strategy,
            search_criteria=search_criteria,
            target_params=target_params
        )
        return await self._execute_pipeline_async(context)

    async def _execute_pipeline_async(self, context: PipelineContext) -> Result[PipelineResult, PipelineError]:
        """Railway oriented pipeline execution"""

        # Define the processing pipeline
        result = await self._compose_async(
            self._validate_inputs_async,
            self._fetch_data_async,
            self._combine_data,
            self._apply_strategy,
            self._generate_final_result
        )(context)

        return result

    def _compose_async(self, *funcs: Callable) -> Callable:
        """Compose async and sync functions in railway pattern"""
        import inspect
        async def composed(input: PipelineContext) -> Result[PipelineResult, PipelineError]:
            current_result = Result.ok(input)
            
            for func in funcs:
                if current_result.success:
                    if inspect.iscoroutinefunction(func):
                        current_result = await func(current_result.value)
                    else:
                        current_result = func(current_result.value)
                else:
                    break
            
            return current_result
        return composed

    @async_result_decorator
    async def _validate_inputs_async(self,
                                     context: PipelineContext) -> PipelineContext:
        """Validate inputs"""
        context.search_criteria.validate()
        context.target_params.validate()
        return context

    @async_result_decorator
    async def _fetch_data_async(self,
                                context: PipelineContext) -> PipelineContext:
        """Fetch data from all providers asynchronously"""

        async def fetch_single_provider(provider: IDataProvider) -> Result[pd.DataFrame, ProviderError]:
            try:
                crit = provider.map_criteria(context.search_criteria)
                data = await provider.fetch_data_async(criteria=crit)
                return data
            except Exception as e:
                return Result.fail(ProviderError(provider.get_name(), e))

        # Fetch data from all providers concurrently
        tasks = [fetch_single_provider(provider) for provider in context.providers]
        results = await asyncio.gather(*tasks)

        # Process results
        successful_data = []        
        for result in results:
            if result.success:
                successful_data.append(result.value)
                context.logs.append(f"[OK] {context.providers[len(successful_data)-1].get_name()} success")
            else:
                context.failed_providers.append(result.error.provider_name)
                context.logs.append(f"[ERROR] {result.error}")

        if not successful_data:
            raise NoDataError("No data received from any provider")

        context.data = successful_data
        return context

    # SENKRON metodlar
    def execute_sync(self, providers: List[IDataProvider],
                     strategy: ISelectionStrategy,
                     search_criteria: SearchCriteria,
                     target_params: TargetParameters) -> Result[PipelineResult, PipelineError]:
        """Senkron pipeline çalıştır"""
    
        logger.info(f"Pipeline (sync) running: strategy={strategy.get_name()}, providers={len(providers)}")
    
        context = PipelineContext(
            providers=providers,
            strategy=strategy,
            search_criteria=search_criteria,
            target_params=target_params
        )
        return self._execute_pipeline_sync(context=context)

    def _execute_pipeline_sync(self, context: PipelineContext) -> Result[PipelineResult, PipelineError]:
        """Senkron pipeline execution"""
        result = self._compose_sync(
            self._validate_inputs_sync,
            self._fetch_data_sync,
            self._combine_data,
            self._apply_strategy,
            self._generate_final_result
        )(context)

        return result

    def _compose_sync(self, *funcs: Callable) -> Callable:
        """Compose sync functions in railway pattern"""
        def composed(input: PipelineContext) -> Result[PipelineResult, PipelineError]:
            current_result = Result.ok(input)

            for func in funcs:
                if current_result.success:
                    current_result = func(current_result.value)
                else:
                    break

            return current_result
        return composed

    @result_decorator
    def _validate_inputs_sync(self, context: PipelineContext) -> PipelineContext:
        """Validate inputs (sync)"""
        context.search_criteria.validate()
        context.target_params.validate()
        return context

    @result_decorator
    def _fetch_data_sync(self, context: PipelineContext) -> PipelineContext:
        """Fetch data from all providers synchronously"""
        results = []

        for provider in context.providers:
            try:
                crit = provider.map_criteria(context.search_criteria)
                result = provider.fetch_data_sync(criteria=crit)

                if result.success:
                    results.append(result.value)
                    context.logs.append(f"[OK] {provider.get_name()} success")
                else:
                    context.failed_providers.append(provider.get_name())
                    context.logs.append(f"[ERROR] {provider.get_name()}: {result.error}")

            except Exception as e:
                context.failed_providers.append(provider.get_name())
                context.logs.append(f"[ERROR] {provider.get_name()}: {e}")

        if not results:
            raise NoDataError("No data received from any provider")

        context.data = results
        return context

    # ORTAK metodlar (hem sync hem async için)

    @result_decorator
    def _combine_data(self, context: PipelineContext) -> PipelineContext:
        """Combine data from multiple providers"""
        if not context.data:
            raise NoDataError("No data to combine")

        #context type -->selection_service.ResultHandle.Result olduğu için value değerleri providerdan gelen dataframelerdir çünkü Result nesnesine çevrilip döndürülüyor.
        # valid_dfs = [df for df in context.data if isinstance(df, pd.DataFrame) and not df.empty]
        valid_dfs = [ df.dropna(axis=1, how='all')  
                     for df in context.data 
                     if isinstance(df, pd.DataFrame) and not df.empty and df.dropna(axis=1, how='all').shape[1] > 0]
        
        if not valid_dfs:
            raise NoDataError("No valid dataframes to combine")
        
        context.combined_df = pd.concat(valid_dfs, ignore_index=True)
        
        # Tümü NaN olan sütunları temizle
        context.combined_df = context.combined_df.dropna(axis=1, how='all')

        # Kalan NaN değerleri doldur
        context.combined_df = context.combined_df.fillna(0)  # Sayısal sütunlar için 0
        # Object tipindeki sütunlarda hala NaN varsa boş string ile doldur
        object_cols = context.combined_df.select_dtypes(include=['object']).columns
        context.combined_df[object_cols] = context.combined_df[object_cols].fillna("")
        
        context.logs.append(f"Combined {len(valid_dfs)} datasets, total {len(context.combined_df)} records")
        return context

    @result_decorator
    def _apply_strategy(self, context: PipelineContext) -> PipelineContext:
        """Apply selection strategy"""
        if context.combined_df is None or context.combined_df.empty:
            raise NoDataError("No data to apply strategy on")
        
        try:
            selected_df, scored_df = context.strategy.select_and_score(
                context.combined_df, context.target_params.__dict__
            )
            context.selected_df = selected_df
            context.scored_df = scored_df
            context.logs.append(f"Strategy applied: {context.strategy.get_name()}")
        except Exception as e:
            raise StrategyError(f"Strategy application failed: {e}")
        
        return context

    @result_decorator
    def _generate_final_result(self, context: PipelineContext) -> PipelineResult:
        """Generate final pipeline result"""
        if context.selected_df is None or context.scored_df is None:
            raise ValueError("No data available for result generation")
        
        exec_time = time.time() - context.start_time
        context.logs.append(f"Execution time: {exec_time:.2f} sec")
        
        report = self._generate_report(
            context.selected_df, context.scored_df,
            context.search_criteria, context.target_params,
            context.strategy, context.providers
        )
        
        return PipelineResult(
            selected_df=context.selected_df,
            scored_df=context.scored_df,
            report=report,
            execution_time=exec_time,
            failed_providers=context.failed_providers,
            logs=context.logs
        )

    def _generate_report(self, selected_df: pd.DataFrame, scored_df: pd.DataFrame,
                         search_criteria: SearchCriteria, target_params: TargetParameters,
                         strategy: ISelectionStrategy, providers: List[IDataProvider]) -> Dict[str, Any]:
        """Generate report dictionary"""
        if selected_df.empty:
            return {"status": "warning", "message": "No records selected"}

        return {
            "status": "success",
            "target_params": target_params,
            "search_criteria": search_criteria,
            "selected_count": len(selected_df),
            "total_considered": len(scored_df),
            "strategy": strategy.get_name(),
            "providers": [p.get_name() for p in providers],
            "records": selected_df.to_dict("records"),
            "statistics": {
                "magnitude_range": (selected_df["MAGNITUDE"].min(), selected_df["MAGNITUDE"].max()),
                "distance_range": (selected_df["RJB(km)"].min(), selected_df["RJB(km)"].max()) if "RJB(km)" in selected_df else None,
                "score_range": (selected_df["SCORE"].min(), selected_df["SCORE"].max())
            }
        }


class EarthquakeAPI:
    """Dışa açılan facade with Result Pattern"""

    def __init__(self, providers: List[IDataProvider],
                 strategies: List[ISelectionStrategy],
                 search_criteria: SearchCriteria,
                 target_params: TargetParameters):
        self.providers = providers
        self.strategies = {s.get_name(): s for s in strategies}
        self.pipeline = EarthquakePipeline()
        self.search_criteria = search_criteria
        self.target_params = target_params

    def run_sync(self,
                 criteria: SearchCriteria,
                 target: TargetParameters,
                 strategy_name: str) -> Result[PipelineResult, PipelineError]:
        """Senkron çalıştırma with Result pattern"""
        strategy_result = self._get_strategy(strategy_name)
        if not strategy_result.success:
            return strategy_result
        
        return self.pipeline.execute_sync(self.providers, strategy_result.value, criteria, target)

    async def run_async(self,
                        criteria: SearchCriteria,
                        target: TargetParameters,
                        strategy_name: str) -> Result[PipelineResult, PipelineError]:
        """
        Asynchronously executes the pipeline using the specified strategy, search criteria, and target parameters.
        Args:
            criteria (SearchCriteria): The search criteria to be used in the pipeline.
            target (TargetParameters): The target parameters for the pipeline execution.
            strategy_name (str): The name of the strategy to be used.
        Returns:
            Result[PipelineResult, PipelineError]: A Result object containing either the pipeline result on success,
            or a pipeline error on failure.
        Raises:
            None
        Note:
            This method uses the Result pattern for error handling and is intended to be run asynchronously.
        """
        strategy_result = self._get_strategy(strategy_name)
        if not strategy_result.success:
            return strategy_result

        return await self.pipeline.execute_async(self.providers,
                                                 strategy_result.value,
                                                 criteria,
                                                 target)

    def _get_strategy(self,
                      name: str) -> Result[ISelectionStrategy, ValueError]:
        """Get strategy with Result pattern"""
        if name not in self.strategies:
            return Result.fail(ValueError(f"Strategy {name} not found"))
        return Result.ok(self.strategies[name])


async def get_selected_earthquake(criteria: SearchCriteria,
                                  target: TargetParameters,
                                  providers: List[IDataProvider],
                                  strategies: List[ISelectionStrategy],
                                  async_mode: bool = False) -> pd.DataFrame:
    api = EarthquakeAPI(providers=providers, strategies=strategies, search_criteria=criteria, target_params=target)
    strategy_name = strategies[0].get_name() if strategies else ""
    if async_mode:
        result = await api.run_async(criteria, target, strategy_name)
    else:
        result = api.run_sync(criteria, target, strategy_name)
    if result.success:
        return result.value.selected_df
    else:
        raise result.error