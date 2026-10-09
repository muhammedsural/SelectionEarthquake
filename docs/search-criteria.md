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

`bbox` verilirse ayri `min_latitude`, `max_latitude`, `min_longitude` ve
`max_longitude` alanlarinin onune gecer. `bbox` sirasi
`(min_lat, max_lat, min_lon, max_lon)` seklindedir.

### PEER ile tarih ve konum

PEER flatfile'i yalnizca `YEAR` bilgisi tutar; bu nedenle `start_date` ve
`end_date` PEER'de yil hassasiyetinde uygulanir (ornegin `start_date="2000-01-01"`
ile 2000 oncesi kayit gelmez). Kutu ve daire aramalari hiposantr konumuna
(`HYPO_LAT`, `HYPO_LON`) gore uygulanir ve sonuclari daraltir.

## Target alanlari

Skorlamada hedef degerler su sirayla belirlenir:

1. `target_*` alanlari.
2. Hem `min_*` hem `max_*` verildiyse ortalamasi.
3. Aksi halde (hicbiri yok **veya** yalnizca `min_*` / yalnizca `max_*` var)
   kriter skorlamaya katilmaz.

!!! note "Tek tarafli sinir = filtre (2.1.0)"
    Yalnizca `min_magnitude=5.0` vermek "M >= 5.0" filtresidir, M5'e yakinlik
    tercihi degildir. 2.0.x'te tek tarafli sinir hedef sayiliyordu ve secim
    sinir degerine yigiliyordu; buyuk depremler elenebiliyordu.

    Belirli bir degere yakin kayit istiyorsaniz `target_magnitude=...` veya
    iki tarafli aralik (`min_magnitude` + `max_magnitude`) verin.

Hicbir kriter icin hedef (ve mekanizma) yoksa stratejiler varsayilan
siralamayi kullanir:

1. Buyuk `MAGNITUDE` once.
2. Esitlikte kucuk mesafe: `RJB(km)`, bos ise `RRUP(km)`, bos ise `REPI(km)`.
3. Eksik buyukluk/mesafe en sona; tam esitlikte orijinal sira korunur.

Bu durumda `TBDYSelectionStrategy` `min_score` esigini uygulamaz (tum skorlar
0'dir). `max_per_station`, `max_per_event` ve `num_records` limitleri her
durumda aynen uygulanir.

```python
# M >= 5.0, en buyuk depremler once
SearchCriteria(start_date="2000-01-01", end_date="2025-09-05", min_magnitude=5.0)

# M >= 5.0, M6.5'e yakin olanlar once
SearchCriteria(start_date="2000-01-01", end_date="2025-09-05",
               min_magnitude=5.0, target_magnitude=6.5)
```

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
