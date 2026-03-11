import asyncio
from selection_service.enums.Enums import DesignCode, ProviderName
from selection_service.core.EarthquakeApi import EarthquakeAPI
from selection_service.processing.Selection import SelectionConfig,SearchCriteria,TBDYSelectionStrategy
from selection_service.core.LoggingConfig import setup_logging

setup_logging()

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
        max_vs30=400,
        mechanisms=["StrikeSlip"]
        # min_pga=0.6,
        # max_pga=1.5,
        # min_pgv=10,
        # max_pgv=100,
        # min_pgD=1,
        # max_pgD=50,
        # min_t90=0.5,
        # max_t90=10,
        # min_arias=0.1,
        # max_arias=10,
        # min_rjb=0,
        # max_rjb=200,
        # min_rrup=0,
        # max_rrup=200,
        # min_repi=0,
        # max_repi=200,
        # min_depth=0,
        # max_depth=100
        )
    
    # Initialize API
    api = EarthquakeAPI(provider_names= [ProviderName.AFAD,ProviderName.PEER],
                        strategies= [strategy], 
                        use_cache=True)

    result = await api.run_async(criteria=search_criteria,
                                 strategy_name=strategy.get_name())
    # result = api.run_sync(criteria=search_criteria,
    # strategy_name=strategy.get_name())
    
    
    if result.success:
        # Tüm kayıtlar için dalga formu indirme
        # api.download_waveforms(result.value.selected_df)
        # Tekil bir dalga formu dosyasını indirme örneği
        
        # if not result.value.selected_df.empty:
        #     download_result = api.download_waveforms(result_df= result.value.selected_df, 
        #                                              batch_size=3, file_type = "ap", file_status = "RawAcc", export_type = "asc2", user_name = "GuestUser")
        
        # print(f"Target Parameters = {result.value.report['target_params'].__repr__()}")
        # print(f"Search Criteria = {result.value.report['search_criteria'].__repr__()}")
        # print(f"Strategy = {result.value.report['strategy']} ")
        # print(f"Total find event = {result.value.report['total_considered']} ")
        # print(f"{result.value.report['selected_count']} records selected")
        print(f"Statistic = {result.value.report['statistics']} ")
        # print(f"Columns: {list(result.value.selected_df.columns)}")
        print(result.value.selected_df[['PROVIDER','RSN','EVENT','YEAR','MAGNITUDE','SSN','STATION','VS30(m/s)','RRUP(km)',"RJB(km)",'MECHANISM','PGA(cm2/sec)','PGV(cm/sec)','T90_avg(sec)','SCORE','ENDPOINTSOURCE','FILE_NAME_H1']])
        # result.value.scored_df.to_excel("events.xlsx")
        return result.value
    else:
        print(f"[ERROR]: {result.error}")
        return None
    
if __name__ == "__main__":
    test = asyncio.run(example_usage())