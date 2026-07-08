# Raporlama

`EarthquakeAPI.run_sync()` ve `run_async()` basarili oldugunda
`PipelineResult` dondurur.

```python
result = api.run_sync(criteria, strategy.get_name())
pipeline_result = result.value
```

## PipelineResult alanlari

| Alan | Tip | Aciklama |
| --- | --- | --- |
| `selected_df` | `pd.DataFrame` | Secilen kayitlar |
| `scored_df` | `pd.DataFrame` | Tum puanlanan kayitlar |
| `report` | `dict` | JSON'a yazilabilir rapor |
| `execution_time` | `float` | Calisma suresi |
| `failed_providers` | `list[str]` | Veri cekemeyen provider'lar |
| `logs` | `list[str]` | Pipeline olaylari |

## DataFrame izlenebilirlik kolonlari

`scored_df` tum kayitlar icin su kolonlari tasir:

- `SCORE`
- `SCORE_BREAKDOWN`
- `SELECTION_STATUS`
- `SELECTION_REASON`

`SELECTION_STATUS`:

- `selected`
- `rejected`

`SELECTION_REASON` ornekleri:

- `selected`
- `score_below_min_score:55.0`
- `max_per_station:3`
- `max_per_event:3`
- `num_records_limit:11`

## report alanlari

```python
report = pipeline_result.report
```

Temel alanlar:

- `status`
- `search_criteria`
- `selected_count`
- `total_considered`
- `strategy`
- `providers`
- `records`
- `statistics`

Izlenebilirlik alanlari:

- `selection_summary`
- `score_breakdown`

## Selection summary

```python
print(report["selection_summary"])
```

Ornek:

```python
{
    "status_counts": {"selected": 11, "rejected": 42},
    "rejection_reasons": {
        "score_below_min_score:55.0": 24,
        "max_per_event:3": 10,
        "num_records_limit:11": 8,
    },
}
```

## JSON rapor yazma

```python
import json

with open("selection_report.json", "w", encoding="utf-8") as fp:
    json.dump(pipeline_result.report, fp, indent=2, ensure_ascii=False, default=str)
```
