import asyncio
import logging
import pandas as pd
from selection_service.enums.Enums import DesignCode, ProviderName
from selection_service.core.Pipeline import EarthquakeAPI
from selection_service.providers.ProvidersFactory import ProviderFactory
from selection_service.processing.Selection import SelectionConfig,SearchCriteria,TBDYSelectionStrategy,TargetParameters
# from selection_service.core.LoggingConfig import setup_logging

# setup_logging(log_level=logging.DEBUG)

async def example_usage():
    #ProviderFactory ile provider oluşturma
    con = SelectionConfig(design_code=DesignCode.TBDY_2018,
                          num_records=22,
                          max_per_station=3,
                          max_per_event=3,
                          min_score=55)
    strategy = TBDYSelectionStrategy(config=con)

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
    
    # Initialize API
    api = EarthquakeAPI(providerNames= [ProviderName.AFAD, 
                                   ProviderName.PEER],
                        strategies= [strategy])

    result = await api.run_async(criteria=search_criteria,
                                 target=target_params,
                                 strategy_name=strategy.get_name())
    # result = api.run_sync(criteria=search_criteria,
    # target=target_params,
    # strategy_name=strategy.get_name())
    
    
    if result.success:
        print(f"Target Parameters = {result.value.report['target_params'].__repr__()}")
        print(f"Search Criteria = {result.value.report['search_criteria'].__repr__()}")
        print(f"Strategy = {result.value.report['strategy']} ")
        print(f"Total find event = {result.value.report['total_considered']} ")
        print(f"{result.value.report['selected_count']} records selected")
        print(f"Statistic = {result.value.report['statistics']} ")
        print(result.value.selected_df[['PROVIDER','RSN','EVENT','YEAR','MAGNITUDE','STATION','VS30(m/s)','RRUP(km)','MECHANISM','PGA(cm2/sec)','PGV(cm/sec)','SCORE']])
        return result.value
    else:
        print(f"[ERROR]: {result.error}")
        return None
    
if __name__ == "__main__":
    test = asyncio.run(example_usage())