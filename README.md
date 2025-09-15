# SelectionEarthquake
This module get the necessary earthquake information from earthquake databases like Peer-West2 and AFAD , assign scores based on your specific characteristics, and filter based on your selection criteria. Also downloads earthquake data from the AFAD database to your workspace if you want.

## Features

- Multiple data source support (PEER, AFAD, etc.)
- Configurable data processing pipelines
- Advanced selection algorithms
- Comprehensive error handling
- Extensible provider system

## Installation

```bash
pip install earthquake-selection

👤 User
   │
   ▼
📦 EarthquakeAPI (run_sync / run_async)
   │
   ▼
🚀 EarthquakePipeline (execute_sync / execute_async)
   │
   ├── 🔍 _validate_inputs
   │        ├─ SearchCriteria.validate()
   │        └─ TargetParameters.validate()
   │
   ├── 📥 _fetch_data
   │        ├─ provider.map_criteria(criteria)
   │        └─ provider.fetch_data_sync/async()
   │              ├─ AFADDataProvider
   │              ├─ PeerWest2Provider
   │              └─ FDSNProvider
   │
   ├── 🔗 _combine_data
   │        └─ pd.concat(valid_dfs) + temizlik
   │
   ├── 🎯 _apply_strategy
   │        └─ strategy.select_and_score()
   │              ├─ TBDYSelectionStrategy
   │              └─ EurocodeSelectionStrategy
   │
   └── 📊 _generate_final_result
            └─ PipelineResult {
                 selected_df,
                 scored_df,
                 report {
                   selected_count,
                   total_considered,
                   statistics,
                   providers,
                   strategy
                 }
               }

   ▼
✅ Output: PipelineResult
