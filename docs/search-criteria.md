# Arama Kriterleri

`SearchCriteria`, tum provider'lar icin ortak arama ve puanlama girdisidir.

## Zorunlu alanlar

```python
SearchCriteria(
    start_date="2000-01-01",
    end_date="2025-09-05",
)
```

Tarihler ISO formatinda verilmelidir. Baslangic tarihi bitis tarihinden sonra
olamaz.

## Sik kullanilan filtreler

```python
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
)
```

## Mesafe alanlari

- `min_Repi`, `max_Repi`
- `min_Rhyp`, `max_Rhyp`
- `min_Rjb`, `max_Rjb`
- `min_Rrup`, `max_Rrup`

Alan adlarinda mevcut API ile uyum icin `Rjb`, `Rrup`, `Repi`, `Rhyp` yazimi
korunur.

## Konum aramasi

Kutu aramasi:

```python
criteria = SearchCriteria(
    start_date="2023-01-01",
    end_date="2023-12-31",
    bbox=(35.0, 42.0, 25.0, 45.0),
)
```

Dairesel arama:

```python
criteria = SearchCriteria(
    start_date="2023-01-01",
    end_date="2023-12-31",
    circleLatitude=37.0,
    circleLongitude=37.0,
    circleRadius=100,
)
```

`circleLatitude`, `circleLongitude` ve `circleRadius` birlikte verilmelidir.

## Target alanlari

Skorlamada hedef degerler su sirayla belirlenir:

1. `target_*` alanlari.
2. `min_*` ve `max_*` ortalamasi.
3. Sadece `min_*` veya sadece `max_*` varsa o deger.
4. Hicbiri yoksa kriter skorlamaya katilmaz.

Ornek:

```python
criteria = SearchCriteria(
    start_date="2000-01-01",
    end_date="2025-09-05",
    min_magnitude=7.0,
    max_magnitude=8.0,
    target_magnitude=7.4,
)
```

Bu durumda magnitude hedefi `7.4` olur.
