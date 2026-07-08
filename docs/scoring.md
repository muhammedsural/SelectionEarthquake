# Scoring ve Presetler

Puanlama motoru kullanicinin aktif ettigi kriterlere gore calisir. Bir kriter
icin hedef yoksa o kriter toplam skora katilmaz.

## Gaussian skor

Sayisal kriterler Gaussian fonksiyonuyla puanlanir:

```text
score = exp(-((value - target)^2) / (2 * sigma^2))
```

Kriterin nihai katkisi:

```text
weighted_score = score * weight
```

Toplam skor:

```text
SCORE = sum(weighted_score) / sum(active_weights) * 100
```

## Kategorik skor

Mekanizma gibi kategorik kriterler:

- Tam eslesme: `1.0`
- Kismi eslesme: `0.7`
- Eslesme yok: `0.0`

## Hazir presetler

```python
from selection_service.processing.Selection import ScoringWeights

weights = ScoringWeights.from_preset("tbdy_2018_record_selection")
```

Mevcut presetler:

| Preset | Amac |
| --- | --- |
| `balanced` | Varsayilan dengeli agirliklar |
| `tbdy_2018_record_selection` | Magnitude, mesafe, Vs30 ve mekanizmayi one cikarir |
| `site_response` | Vs30, sure ve siddet olcutlerine daha fazla agirlik verir |

Preset aciklamalari:

```python
ScoringWeights.preset_descriptions()
```

## Ozel agirliklar

```python
from selection_service.processing.Selection import ScoringWeights

weights = ScoringWeights(
    magnitude=6.0,
    rjb=5.0,
    rrup=5.0,
    vs30=5.0,
    mechanism=4.0,
)
```

Agirligi `0.0` olan kriter skorlamaya katilmaz.

## Skor kirilimi

`scored_df` icinde her satir icin `SCORE_BREAKDOWN` kolonu olusur.

Ornek eleman:

```python
[
    {
        "criterion": "magnitude",
        "column": "MAGNITUDE",
        "status": "active",
        "target": 7.5,
        "value": 7.28,
        "weight": 6.0,
        "sigma": 0.25,
        "raw_score": 0.68,
        "weighted_score": 4.08,
    }
]
```

`status` degerleri:

- `active`: Kriter skora katildi.
- `missing`: Kolon veya deger eksik oldugu icin katilmadi.
- `inactive_weight`: Agirlik `0.0` oldugu icin katilmadi.
