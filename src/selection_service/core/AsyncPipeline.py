# core/AsyncPipeline.py
import asyncio
import logging
import time
from typing import Any, Dict, List
import pandas as pd
from .BasePipeline import BasePipeline
from ..processing.Selection import ISelectionStrategy, SearchCriteria, TargetParameters
from ..providers.Providers import IAsyncDataProvider
from .ErrorHandle import PipelineError, NoDataError, StrategyError
from ..processing.ResultHandle import Result, async_result_decorator, result_decorator
from selection_service.core.Pipeline import PipelineContext, PipelineResult

logger = logging.getLogger(__name__)   # burası önemli

class AsyncPipeline(BasePipeline[PipelineResult]):
    """Async pipeline implementation"""
    
    @async_result_decorator
    async def execute(self, providers: List[IAsyncDataProvider], strategy: ISelectionStrategy,
                    search_criteria: SearchCriteria, target_params: TargetParameters) -> PipelineResult:
        
        logger.info(f"Pipeline (async) running: strategy={strategy.get_name()}, providers={len(providers)}")
        
        context = PipelineContext(
            providers=providers,
            strategy=strategy,
            search_criteria=search_criteria,
            target_params=target_params
        )
        
        return await self._execute_pipeline(context)
    
    async def _execute_pipeline(self, context: PipelineContext) -> PipelineResult:
        """Async pipeline execution"""
        result = await self._compose_async(
            self._validate_inputs,
            self._fetch_data,
            self._combine_data,
            self._apply_strategy,
            self._generate_final_result
        )(context)
        
        return result
    
    def _compose_async(self, *funcs: callable) -> callable:
        """Compose async functions in railway pattern"""
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
    async def _validate_inputs(self, context: PipelineContext) -> PipelineContext:
        """Validate inputs"""
        context.search_criteria.validate()
        context.target_params.validate()
        return context
    
    @async_result_decorator
    async def _fetch_data(self, context: PipelineContext) -> PipelineContext:
        """Fetch data from all providers asynchronously"""
        async def fetch_single_provider(provider: IAsyncDataProvider) -> Result[pd.DataFrame, PipelineError]:
            try:
                crit = provider.map_criteria(context.search_criteria)
                data = await provider.fetch_data(criteria=crit)
                return Result.ok(data)
            except Exception as e:
                return Result.fail(PipelineError(f"Provider {provider.get_name()} error: {e}"))
        
        tasks = [fetch_single_provider(provider) for provider in context.providers]
        results = await asyncio.gather(*tasks)
        
        successful_data = []        
        for result in results:
            if result.success:
                successful_data.append(result.value)
                context.logs.append(f"[OK] {context.providers[len(successful_data)-1].get_name()} success")
            else:
                context.failed_providers.append(result.error)
                context.logs.append(f"[ERROR] {result.error}")
                
        if not successful_data:
            raise NoDataError("No data received from any provider")
        
        context.data = successful_data
        return context
    
    # ... diğer ortak metodlar (_combine_data, _apply_strategy, _generate_final_result)
    @result_decorator
    def _combine_data(self, context: PipelineContext) -> PipelineContext:
        """Combine data from multiple providers"""
        if not context.data:
            raise NoDataError("No data to combine")

        #context type -->selection_service.ResultHandle.Result olduğu için value değerleri providerdan gelen dataframelerdir çünkü Result nesnesine çevrilip döndürülüyor.
        valid_dfs = [df for df in context.data if isinstance(df, pd.DataFrame) and not df.empty]
        # valid_dfs = [ df.dropna(axis=1, how='all')  for df in context.data if isinstance(df, pd.DataFrame) and not df.empty and df.dropna(axis=1, how='all').shape[1] > 0]
        
        if not valid_dfs:
            raise NoDataError("No valid dataframes to combine")
        
        context.combined_df = pd.concat(valid_dfs, ignore_index=True)
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
            context.logs.append(f"🏆 Strategy applied: {context.strategy.get_name()}")
        except Exception as e:
            raise StrategyError(f"Strategy application failed: {e}")
        
        return context

    @result_decorator
    def _generate_final_result(self, context: PipelineContext) -> PipelineResult:
        """Generate final pipeline result"""
        if context.selected_df is None or context.scored_df is None:
            raise ValueError("No data available for result generation")
        
        exec_time = time.time() - context.start_time
        context.logs.append(f"⏱ Execution time: {exec_time:.2f} sec")
        
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
                         strategy: ISelectionStrategy, providers: List[IAsyncDataProvider]) -> Dict[str, Any]:
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
