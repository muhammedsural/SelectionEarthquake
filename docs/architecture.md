# Mimari

Proje katmanlari sorumluluklara ayrilmistir.

```text
selection_service/
  core/
    EarthquakeApi.py      Public facade
    Pipeline.py           Pipeline engine and report generation
    Config.py             Standard columns, scoring map, presets
    ErrorHandle.py        Domain error hierarchy
  services/
    ProviderRegistry.py   Active provider registry
    EarthquakeQueryService.py
    WaveformDownloadService.py
  providers/
    AfadProvider.py
    PeerProvider.py
    ProvidersFactory.py
    interfaces.py
    afad/
  processing/
    Selection.py          SearchCriteria, scoring, selection strategies
    Mappers.py            Provider column mappers
    ResultHandle.py       Result type and decorators
  utility/
    path_utils.py
  data/
    NGA-West2_flatfile.csv
    stations.xlsx
```

## Facade

`EarthquakeAPI` kullanicinin ana giris noktasidir.

```python
api = EarthquakeAPI([ProviderName.PEER], [strategy])
result = api.run_sync(criteria, strategy.get_name())
```

Facade su servisleri bir araya getirir:

- `ProviderRegistry`
- `EarthquakeQueryService`
- `WaveformDownloadService`

## Query service

`EarthquakeQueryService` provider listesini ve stratejileri kullanarak
pipeline context olusturur.

Ana sorumluluklar:

- strateji adini cozmek
- sync/async pipeline calistirmak
- mevcut DataFrame uzerinde `re_selection` yapmak

## Pipeline

`EarthquakePipeline` adimlari:

1. `_validate_inputs`
2. `_fetch_data_sync` veya `_fetch_data_async`
3. `_combine_data`
4. `_apply_strategy`
5. `_finalize_result`

Her adim `Result` modeliyle basarili/basarisiz sonucu tasir.

## Provider ve mapper ayrimi

Provider veri kaynagini bilir; mapper kolon standardizasyonunu bilir.

Ornek:

- `PeerWest2Provider`: flatfile okur ve filtre uygular.
- `PEERColumnMapper`: PEER kolonlarini `STANDARD_COLUMNS` ile hizalar.
- `AFADDataProvider`: AFAD API'den veri ceker.
- `AFADColumnMapper`: AFAD API yanitini standart kolona cevirir.

## Download yetenegi

`IDataFetcher` sadece veri cekme sozlesmesidir. Download destekleyen provider
ek olarak `IWaveformDownloader` uygular. `supports_download(provider)` bu ayrimi
runtime'da kontrol eder.

Bu sayede PEER gibi download desteklemeyen provider'lar download metodu
uygulamak zorunda kalmaz.
