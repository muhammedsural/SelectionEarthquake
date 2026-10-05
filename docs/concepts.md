# Temel Kavramlar

## Provider

Provider, deprem kaydini belirli bir kaynaktan getiren bilesendir.

Mevcut provider'lar:

- `ProviderName.PEER`: Paket icindeki NGA-West2 flatfile verisini kullanir.
- `ProviderName.AFAD`: AFAD/TADAS API uzerinden veri ceker ve waveform indirir.
- `ProviderName.FDSN`: ObsPy uyumlu FDSN servislerini sorgular. Varsayilan olarak
  event katalogu icin HTTPS `USGS`, istasyon ve waveform icin HTTPS `IRIS`
  kullanilir. `service`/`base_url` event servisini, `data_service`/
  `data_base_url` ise istasyon ve waveform servisini degistirir.

FDSN etkin bir `EarthquakeAPI` uzerinden istasyon ve waveform servisleri de
kullanilabilir:

```python
stations = api.search_fdsn_stations(
    network="TU", station="*", channel="HN?",
    starttime="2023-02-06", endtime="2023-02-07",
)

stream = api.search_fdsn_waveforms(
    network="TU", station="ANK", location="*", channel="HN?",
    starttime="2023-02-06T01:16:00Z",
    endtime="2023-02-06T01:17:00Z",
)
```

Istasyon sonucu kanal basina bir DataFrame satiri, waveform sonucu ise ObsPy
`Stream` nesnesidir. Ayni metotlar `_async` son ekiyle asenkron kullanilabilir.

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

Bu stratejiler `ERROR_METRICS`, `ERROR_TOTAL`, `HARD_FILTERS` ve
`SELECTION_REASON` kolonlarini uretir. PEER ve AFAD mapper ciktilari ortak
`STANDARD_COLUMNS` semasina geldigi icin iki provider icin ayni kolonlari
uretmek mumkundur.

## Eksik veri

Cikti DataFrame'lerinde sayisal bosluklar `0` ile doludur. Secim algoritmasi ise
bosluklari gorur: `VS30(m/s)` ve `MAGNITUDE` icin `0` "bilinmiyor" sayilir. Vs30
araligi verildiyse Vs30'u bilinmeyen kayit `missing:VS30(m/s)` nedeniyle elenir.

## Tekrar tespiti ve olay limiti

Birden fazla provider kullanildiginda ayni deprem farkli adlarla gelebilir.
Pipeline bunlari `EVENT_GROUP` altinda birlestirir (ayni yil, buyukluk farki
`<= 0.5`, episantr mesafesi `<= 50 km`; provider ici artci soklar birlestirilmez)
ve ayni istasyonun (`<= 1 km`) ikinci kopyasini aday havuzundan cikarir.
"Ayni depremden en fazla `max_per_event` kayit" siniri `EVENT_GROUP`'a gore
uygulanir. Esikler ve tercih edilen provider `SelectionConfig.dedup` ile ayarlanir:

```python
from selection_service.enums.enums import DesignCode, ProviderName
from selection_service.processing.criteria import DedupConfig, SelectionConfig

config = SelectionConfig(
    design_code=DesignCode.TBDY_2018,
    dedup=DedupConfig(
        prefer_provider=ProviderName.AFAD,  # tekrar kayitlarda AFAD korunur
        max_event_distance_km=50,
        max_mag_diff=0.5,
        max_station_distance_km=1.0,
        # enabled=False -> olay birlestirme ve tekrar elemesi kapali
    ),
)
```

Ayrintilar icin [2.0 Gecis Rehberi](migration-v2.md).

## TBDY uygunluk raporu

`report["compliance"]`, secilen ve istenen kayit sayisini (`num_records`; TBDY icin
her yon 11 kayit, 11 x 2 = 22), eksik kayit sayisini, ayni depremden secilen en
fazla kayit sayisini ve `warnings` listesini icerir. Kayit takimi (H1 + H2) olarak
sayiyorsaniz `num_records=11` verin.

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
