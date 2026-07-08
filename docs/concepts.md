# Temel Kavramlar

## Provider

Provider, deprem kaydini belirli bir kaynaktan getiren bilesendir.

Mevcut provider'lar:

- `ProviderName.PEER`: Paket icindeki NGA-West2 flatfile verisini kullanir.
- `ProviderName.AFAD`: AFAD/TADAS API uzerinden veri ceker ve waveform indirir.

Provider'lar ortak `IDataFetcher` sozlesmesini uygular:

- `get_name()`
- `map_criteria(criteria)`
- `fetch_data_sync(criteria)`
- `fetch_data_async(criteria)`

Download destekleyen provider'lar ek olarak `IWaveformDownloader` uygular.

## Mapper

Her provider kendi ham kolonlarini `STANDARD_COLUMNS` semasina donusturur.
Bu sayede AFAD ve PEER verileri ayni pipeline icinde birlestirilebilir.

## SearchCriteria

Kullanici tarafindan verilen ortak arama modelidir. Provider'lara ozel
parametre formatlarina `to_afad_params()`, `to_peer_params()` ve
`to_fdsn_params()` metotlariyla donusur.

## Strategy

Selection strategy, bir DataFrame'i puanlayip secilecek kayitlari belirler.
Ana strateji `TBDYSelectionStrategy` sinifidir.

## Pipeline

Pipeline sirasi:

1. Girdi kontrolu.
2. Provider'lardan veri cekme.
3. Verileri birlestirme.
4. Stratejiyi uygulama.
5. `PipelineResult` ve rapor uretme.

## Result modeli

Metotlar hata firlatmak yerine cogu yerde `Result.ok(value)` veya
`Result.fail(error)` dondurur.

```python
result = api.run_sync(criteria, strategy.get_name())

if result.success:
    data = result.value
else:
    print(result.error)
```
