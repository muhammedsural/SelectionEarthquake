# SelectionEarthquake

Deprem kayitlarini AFAD ve PEER gibi veri saglayicilardan cekip ortak kolona
normalize eden, arama kriterlerine gore puanlayan ve TBDY 2018 odakli kayit
secimi yapan Python kutuphanesi.

## Paket ve Import Adi

Paket kurulum adi:

```bash
pip install earthquake-selection
```

Python import paketi:

```python
import selection_service
```

Yerel gelistirme kurulumu:

```bash
git clone https://github.com/muhammedsural/SelectionEarthquake.git
cd SelectionEarthquake
pip install -e ".[dev]"
```

## Ozellikler

- Coklu veri saglayici destegi: AFAD, PEER
- Ortak `SearchCriteria` modeli ve provider bazli kriter donusumu
- Pipeline tabanli sorgu, birlestirme, puanlama ve secim akisi
- TBDY 2018 icin Gaussian tabanli secim stratejisi
- Kriter bazli skor kirilimi: `SCORE_BREAKDOWN`
- Her kayit icin secim/eleme aciklamasi: `SELECTION_STATUS`, `SELECTION_REASON`
- CSV, JSON rapor ve Pandas DataFrame ciktilari
- AFAD icin waveform indirme destegi
- Pytest tabanli test altyapisi

## Hizli Baslangic

```python
from selection_service.core.EarthquakeApi import EarthquakeAPI
from selection_service.enums.Enums import DesignCode, ProviderName
from selection_service.processing.Selection import (
    ScoringWeights,
    SearchCriteria,
    SelectionConfig,
    TBDYSelectionStrategy,
)

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
    mechanisms=["StrikeSlip"],
    weights=ScoringWeights.from_preset("tbdy_2018_record_selection"),
)

api = EarthquakeAPI(
    provider_names=[ProviderName.PEER],
    strategies=[strategy],
    use_cache=True,
)

result = api.run_sync(criteria=criteria, strategy_name=strategy.get_name())

if result.success:
    selected = result.value.selected_df
    report = result.value.report
    print(selected[["PROVIDER", "RSN", "EVENT", "MAGNITUDE", "SCORE", "SELECTION_REASON"]].head())
    print(report["selection_summary"])
else:
    print(result.error)
```

## Komut Satiri Ornegi

Kurulumdan sonra uc uca arama, secim, CSV ve JSON rapor uretimi:

```bash
earthquake-selection-example --providers peer --num-records 11 --report-path selection_report.json --selected-csv selected_records.csv
```

AFAD ile waveform indirme akisini da baslatmak icin:

```bash
earthquake-selection-example --providers afad --download-waveforms --export-type mseed
```

AFAD API ag baglantisi gerektirir. PEER veri seti lokal flatfile ile calisir ve
waveform indirme desteklemez; download adiminda desteklenmeyen provider atlanir.

## Scoring Presetleri

Hazir agirlik setleri `ScoringWeights.from_preset(...)` ile secilir:

- `balanced`: Varsayilan dengeli agirliklar.
- `tbdy_2018_record_selection`: Magnitude, mesafe, Vs30 ve mekanizmayi one cikarir.
- `site_response`: Vs30, sure ve siddet olcutlerine daha fazla agirlik verir.

Preset aciklamalari:

```python
from selection_service.processing.Selection import ScoringWeights

print(ScoringWeights.preset_descriptions())
```

## Rapor Izlenebilirligi

`PipelineResult.report` asagidaki ek alanlari icerir:

- `selection_summary`: Secilen/reddedilen kayit sayilari ve eleme gerekceleri.
- `score_breakdown`: Secilen kayitlar icin kriter bazli hedef, deger, agirlik ve katki.

`scored_df` tum kayitlar icin su kolonlari tasir:

- `SCORE_BREAKDOWN`
- `SELECTION_STATUS`
- `SELECTION_REASON`

## Mimari

```text
selection_service/
  core/        Pipeline, API facade, config, error types
  services/    Query, provider registry, waveform download orchestration
  providers/   AFAD, PEER, cache, provider interfaces
  processing/  SearchCriteria, selection strategy, mappers, Result
  utility/     Data file loading helpers
  data/        Packaged CSV/XLSX data
tests/         pytest suite
examples/      Usage examples
```

## Test

```bash
pytest
```

Pytest ayarlari tek kaynak olarak `pyproject.toml` icindedir.

## Yol Haritasi

- FDSN provider entegrasyonunu tamamla veya v2 kapsaminda ayri tut.
- Eurocode stratejisini gercek kurallarla doldur veya deneysel olarak etiketle.
- Rapor ciktilarini HTML/PDF formatina genislet.

## Lisans

MIT License
