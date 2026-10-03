# 2.0.0 Gecis Rehberi

2.0.0 kirici degisiklikler icerir. Ust duzey import'lar
(`from selection_service import EarthquakeAPI, SearchCriteria, ...`) aynen calisir;
modul yollari degisti.

## Modul adlari snake_case oldu

| Eski | Yeni |
| --- | --- |
| `core.Config` | `core.config` |
| `core.EarthquakeApi` | `core.earthquake_api` |
| `core.ErrorHandle` | `core.error_handle` |
| `core.LoggingConfig` | `core.logging_config` |
| `core.Pipeline` | `core.pipeline` |
| `enums.Enums` | `enums.enums` |
| `processing.Mappers` | `processing.mappers` |
| `processing.ResultHandle` | `processing.result_handle` |
| `providers.AfadProvider` | `providers.afad_provider` |
| `providers.PeerProvider` | `providers.peer_provider` |
| `providers.FdsnProvider` | `providers.fdsn_provider` |
| `providers.CacheManager` | `providers.cache_manager` |
| `providers.ProvidersFactory` | `providers.providers_factory` |
| `providers.afad.AfadApiClient` | `providers.afad.afad_api_client` |
| `providers.afad.AfadFileManager` | `providers.afad.afad_file_manager` |
| `services.EarthquakeQueryService` | `services.earthquake_query_service` |
| `services.ProviderRegistry` | `services.provider_registry` |
| `services.WaveformDownloadService` | `services.waveform_download_service` |
| `utility.convertPeerFlatfile` | `utility.convert_peer_flatfile` |

## `processing.Selection` bolundu

- `SearchCriteria`, `ScoringWeights`, `SelectionConfig` → `selection_service.processing.criteria`
- Stratejiler → `selection_service.processing.strategies`

## Kaldirilanlar

- `IDataProvider` ve `providers.IProvider`: `IDataFetcher` ve `IWaveformDownloader`
  (`selection_service.providers.interfaces`) kullanin.

## Bagimliliklar

- `obspy` artik opsiyonel: FDSN icin `pip install "earthquake-selection[fdsn]"`.
- Kullanilmayan `pyyaml`, `tqdm`, `python-dateutil`, `setuptools` bagimliliklari kaldirildi.

## Davranis duzeltmeleri

- PEER: `start_date`/`end_date` yil filtresi, `bbox`, `min/max_latitude/longitude`
  ve daire aramasi artik sonuclari daraltiyor (hiposantr konumuna gore).
- AFAD: `HYPO_DEPTH(km)` dolduruluyor (yanittaki derinlik alani, yoksa
  `sqrt(rhyp² - repi²)`).
