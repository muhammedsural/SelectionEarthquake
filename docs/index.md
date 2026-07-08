# SelectionEarthquake

SelectionEarthquake; deprem kayitlarini farkli veri kaynaklarindan cekmek,
ortak bir semaya normalize etmek, kullanici kriterlerine gore puanlamak ve
TBDY 2018 odakli kayit secimi yapmak icin gelistirilmis bir Python
kutuphanesidir.

## Ne saglar?

- AFAD ve PEER verilerini tek tip DataFrame formatina donusturur.
- Ortak `SearchCriteria` modeliyle provider farklarini gizler.
- Gaussian tabanli skor uretir ve kriter bazli skor kirilimi verir.
- TBDY 2018 secim sinirlarini uygular:
  - minimum skor
  - toplam kayit sayisi
  - istasyon basina maksimum kayit
  - deprem olayi basina maksimum kayit
- Her kayit icin secildi/reddedildi durumunu ve gerekcesini raporlar.
- AFAD kayitlari icin waveform indirme akisini destekler.
- CLI ile uc uca arama, secim, CSV ve JSON rapor uretimi sunar.

## Paket adi ve import adi

Kurulum paketi:

```bash
pip install earthquake-selection
```

Python import paketi:

```python
import selection_service
```

Bu ayrim normaldir: PyPI dagitim adi `earthquake-selection`, kod icindeki
namespace `selection_service` olarak kalir.

## En kisa ornek

```python
from selection_service.core.EarthquakeApi import EarthquakeAPI
from selection_service.enums.Enums import DesignCode, ProviderName
from selection_service.processing.Selection import (
    ScoringWeights,
    SearchCriteria,
    SelectionConfig,
    TBDYSelectionStrategy,
)

strategy = TBDYSelectionStrategy(
    SelectionConfig(design_code=DesignCode.TBDY_2018, num_records=11, min_score=55)
)
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

api = EarthquakeAPI([ProviderName.PEER], [strategy])
result = api.run_sync(criteria, strategy.get_name())

if result.success:
    print(result.value.selected_df[["RSN", "EVENT", "SCORE", "SELECTION_REASON"]])
else:
    print(result.error)
```

## Dokuman haritasi

- [Kurulum](installation.md): paket, gelistirme ve dokuman kurulumu.
- [Hizli Baslangic](quickstart.md): Python API ile uc uca akisi calistirma.
- [CLI](cli.md): komut satirindan secim, rapor ve indirme.
- [Scoring ve Presetler](scoring.md): agirliklar, skor kirilimi ve hazir presetler.
- [Raporlama](reporting.md): `selected_df`, `scored_df` ve `report` alanlari.
- [Provider Gelistirme](provider-development.md): yeni veri kaynagi ekleme adimlari.
- [API Referansi](api/core.md): mkdocstrings ile uretilen teknik referans.
