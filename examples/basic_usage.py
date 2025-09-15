import asyncio
import pandas as pd
from selection_service.Enums import DesignCode, ProviderName
from selection_service.Pipeline import EarthquakeAPI, EarthquakePipeline, PipelineContext
from selection_service.Providers import IDataProvider, ProviderFactory
from selection_service.Selection import SearchCriteria, SelectionConfig, TBDYSelectionStrategy, TargetParameters, ValidationError
from selection_service.ErrorHandle import NoDataError, ProviderError
from selection_service.ResultHandle import Result

async def example_usage():
    #ProviderFactory ile provider oluşturma
    prvFactory = ProviderFactory()
    afadProvider = prvFactory.create_provider(provider_type=ProviderName.AFAD)
    peerProvider = prvFactory.create_provider(provider_type=ProviderName.PEER)
    con = SelectionConfig(design_code=DesignCode.TBDY_2018,
                          num_records=22,
                          max_per_station=3,
                          max_per_event=3,
                          min_score=55)
    strategy = TBDYSelectionStrategy(config=con)
    
    # Initialize API
    api = EarthquakeAPI(providers=[afadProvider,peerProvider], strategies=[strategy])
    
    search_criteria = SearchCriteria(
        start_date="2000-01-01",
        end_date="2025-09-05",
        min_magnitude=7.0,
        max_magnitude=10.0,
        min_vs30=300,
        max_vs30=400
        # mechanisms=["StrikeSlip"]
        )
    target_params = TargetParameters(
        magnitude=7.0,
        distance=30.0,
        vs30=400.0,
        pga=200,
        mechanism=["StrikeSlip"]
    )
    
    # Run pipeline with proper error handling
    # result = api.run_sync(
    #     criteria=search_criteria,
    #     target=target_params,
    #     strategy_name=strategy.get_name()
    # )
    result = await api.run_async(criteria=search_criteria, target=target_params, strategy_name=strategy.get_name()    )
    
    if result.success:
        print(f"✅ Success: Target Parameters = {result.value.report['target_params']}")
        print(f"✅ Success: Search Criteria = {result.value.report['search_criteria']}")
        print(f"✅ Success: {result.value.report['strategy']} ")
        print(f"✅ Success: Total find event = {result.value.report['total_considered']} ")
        print(f"✅ Success: {result.value.report['selected_count']} records selected")
        print(f"✅ Success: {result.value.report['statistics']} ")
        print(result.value.selected_df)
        return result.value
    else:
        print(f"❌ Error: {result.error}")
        # Handle specific error types
        if isinstance(result.error, NoDataError):
            print("No data available")
        elif isinstance(result.error, ValidationError):
            print("Validation failed")
        return None
    
if __name__ == "__main__":
    test = asyncio.run(example_usage())