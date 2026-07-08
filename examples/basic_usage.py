from selection_service.core.EarthquakeApi import EarthquakeAPI
from selection_service.core.LoggingConfig import setup_logging
from selection_service.enums.Enums import DesignCode, ProviderName
from selection_service.processing.Selection import (
    ScoringWeights,
    SearchCriteria,
    SelectionConfig,
    TBDYSelectionStrategy,
)


def example_usage():
    setup_logging()

    config = SelectionConfig(
        design_code=DesignCode.TBDY_2018,
        num_records=11,
        max_per_station=3,
        max_per_event=3,
        min_score=55,
    )
    strategy = TBDYSelectionStrategy(config=config)

    criteria = SearchCriteria(
        start_date="2000-01-01",
        end_date="2025-09-05",
        min_magnitude=7.0,
        max_magnitude=8.0,
        min_vs30=300,
        max_vs30=400,
        min_Rjb=0,
        max_Rjb=100,
        mechanisms=["StrikeSlip"],
        weights=ScoringWeights.from_preset("tbdy_2018_record_selection"),
    )

    api = EarthquakeAPI(
        provider_names=[ProviderName.PEER],
        strategies=[strategy],
        use_cache=True,
    )
    result = api.run_sync(criteria=criteria, strategy_name=strategy.get_name())

    if not result.success:
        print(f"[ERROR]: {result.error}")
        return None

    selected_columns = [
        "PROVIDER",
        "RSN",
        "EVENT",
        "YEAR",
        "MAGNITUDE",
        "STATION",
        "VS30(m/s)",
        "RJB(km)",
        "MECHANISM",
        "SCORE",
        "SELECTION_REASON",
    ]
    print(result.value.selected_df[selected_columns].head())
    print(result.value.report["selection_summary"])
    return result.value


if __name__ == "__main__":
    example_usage()
