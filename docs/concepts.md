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

Ek stratejiler:

- `TBDY2018ConstraintStrategy`: Sert kriter, hata metrikleri ve cesitlilik
  limitleriyle izlenebilir secim yapar.
- `ConstraintSelectionStrategy`: Eski importlari bozmamak icin
  `TBDY2018ConstraintStrategy` alias'i olarak kalir.
- `ParetoSelectionStrategy`: Cok kriterli hata metriklerinde nondominated
  kayitlari one alir.
- `SpectrumMatchStrategy`: `PGA`, `PGV`, `PGD`, `Arias` ve `T90` hedeflerine
  oncelik verir.

Bu stratejiler `ERROR_METRICS`, `ERROR_TOTAL`, `HARD_FILTERS` ve
`SELECTION_REASON` kolonlarini uretir. PEER ve AFAD mapper ciktilari ortak
`STANDARD_COLUMNS` semasina geldigi icin iki provider icin ayni kolonlari
uretmek mumkundur.

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
