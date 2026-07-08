# Hizli Baslangic

Bu sayfa PEER lokal flatfile ile ag baglantisi gerektirmeyen bir secim akisini
gosterir.

## 1. Strateji olustur

```python
from selection_service.enums.Enums import DesignCode
from selection_service.processing.Selection import SelectionConfig, TBDYSelectionStrategy

config = SelectionConfig(
    design_code=DesignCode.TBDY_2018,
    num_records=11,
    max_per_station=3,
    max_per_event=3,
    min_score=55,
)
strategy = TBDYSelectionStrategy(config=config)
```

## 2. Arama kriterlerini tanimla

```python
from selection_service.processing.Selection import ScoringWeights, SearchCriteria

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
```

## 3. API'yi calistir

```python
from selection_service.core.EarthquakeApi import EarthquakeAPI
from selection_service.enums.Enums import ProviderName

api = EarthquakeAPI(
    provider_names=[ProviderName.PEER],
    strategies=[strategy],
    use_cache=True,
)

result = api.run_sync(criteria=criteria, strategy_name=strategy.get_name())
```

## 4. Sonucu oku

```python
if not result.success:
    raise RuntimeError(result.error)

selected = result.value.selected_df
scored = result.value.scored_df
report = result.value.report

print(selected[["PROVIDER", "RSN", "EVENT", "MAGNITUDE", "SCORE", "SELECTION_REASON"]])
print(report["selection_summary"])
```

## Async kullanim

```python
import asyncio

async def main():
    result = await api.run_async(criteria=criteria, strategy_name=strategy.get_name())
    return result

result = asyncio.run(main())
```

## Dosyaya cikti alma

```python
selected.to_csv("selected_records.csv", index=False)
scored.to_csv("scored_records.csv", index=False)
```

JSON rapor:

```python
import json

with open("selection_report.json", "w", encoding="utf-8") as fp:
    json.dump(report, fp, indent=2, ensure_ascii=False, default=str)
```
