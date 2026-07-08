# Sorun Giderme

## `ModuleNotFoundError: selection_service`

Yerel gelistirmede editable kurulum yap:

```bash
pip install -e ".[dev]"
```

Alternatif olarak komutu repo kokunden calistir.

## Paket adi ile import adi farkli

Dogru kurulum:

```bash
pip install earthquake-selection
```

Dogru import:

```python
import selection_service
```

## AFAD sonuc donmuyor

Olasiliklar:

- Tarih araligi cok dar.
- Magnitude alt siniri cok yuksek.
- Konum filtresi fazla kisitlayici.
- AFAD/TADAS servisi gecici olarak erisilemiyor.

Kriterleri genislet:

```python
criteria = criteria.model_copy(update={"min_magnitude": 5.0})
```

## PEER download calismiyor

PEER provider download desteklemez. Download sadece destekleyen provider'larda
calisir. Mevcut durumda AFAD desteklenir.

## `Strategy '...' not found`

`EarthquakeAPI` olustururken stratejiyi listeye eklediginden emin ol:

```python
strategy = TBDYSelectionStrategy(config)
api = EarthquakeAPI([ProviderName.PEER], [strategy])
result = api.run_sync(criteria, strategy.get_name())
```

## Hic kayit secilmedi

Kontrol et:

- `min_score` cok yuksek olabilir.
- `num_records` sifir veya cok dusuk olabilir.
- `max_per_station` veya `max_per_event` secimi daraltiyor olabilir.
- Aktif scoring kriterleri kayitlarda eksik olabilir.

Detay icin:

```python
result.value.scored_df[["SCORE", "SELECTION_STATUS", "SELECTION_REASON"]]
result.value.report["selection_summary"]
```

## Git dubious ownership

Windows ortaminda repo sahipligi farkli kullanicida gorunebilir. Sadece bu repo
icin okuma/komut calistirma yaparken:

```bash
git -c safe.directory=D:/github/SelectionEarthquake status
```
