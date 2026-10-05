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

- `ParetoSelectionStrategy` (`Pareto_Selection`), `PARETO_RANK` / `PARETO_FRONT` kolonlari ve
  CLI'daki `--strategy pareto` secenegi.
- `SpectrumMatchStrategy` (`Spectrum_Match`), `SPECTRUM_ERROR` kolonu ve CLI'daki
  `--strategy spectrum` secenegi: spektral eslestirme kutuphanenin kapsami disinda
  (kayit ararken sadece bolge ozelliklerine bakilir; spektral eslestirme
  signalanalyzer tarafinda yapilir).

## Bagimliliklar

- `obspy` artik opsiyonel: FDSN icin `pip install "earthquake-selection[fdsn]"`.
- Kullanilmayan `pyyaml`, `tqdm`, `python-dateutil`, `setuptools` bagimliliklari kaldirildi.

## Davranis duzeltmeleri

- PEER: `start_date`/`end_date` yil filtresi, `bbox`, `min/max_latitude/longitude`
  ve daire aramasi artik sonuclari daraltiyor (hiposantr konumuna gore).
- AFAD: `HYPO_DEPTH(km)` dolduruluyor (yanittaki derinlik alani, yoksa
  `sqrt(rhyp² - repi²)`).

## Eksik veri ve TBDY uygunluk raporu

- Cikti DataFrame'lerinde (`selected_df`, `scored_df`, `combined_df`) sayisal bosluklar
  eskisi gibi `0` ile doludur.
- Secim algoritmasi ise bosluklari gormeye devam eder: strateji girdisinde
  bosluklar NaN kalir ve `VS30(m/s)` ile `MAGNITUDE` icin `0` "bilinmiyor" sayilir.
  Boylece Vs30'u bilinmeyen AFAD istasyonu "Vs30 = 0 m/s" gibi puanlanmaz;
  Vs30 araligi verildiyse `missing:VS30(m/s)` nedeniyle elenir.
- Rapora `compliance` bolumu eklendi: secilen/istenen kayit sayisi (`num_records`,
  TBDY icin her yon 11 kayit => 22), eksik sayisi (`shortfall`), ayni depremden en fazla
  secilen kayit sayisi ve `max_per_event` siniri, `warnings` ve `compliant`.
  Ayni uyarilar `PipelineResult.logs` icinde `[WARN]` olarak da yer alir.

## Provider'lar arasi tekrar tespiti

AFAD ve PEER ayni depremi farkli adlarla dondurebilir. Birden fazla provider
kullanildiginda pipeline artik sezgisel bir eslestirme yapar
(`selection_service.processing.dedup`):

- **Olay grubu:** farkli provider'lardaki iki olay, ayni `YEAR`, buyukluk farki
  `<= 0.5` ve episantr mesafesi `<= 50 km` ise ve birbirinin en yakin adayiysa ayni
  depremdir. Provider ici olaylar (artci soklar) birlestirilmez. Sonuc
  `EVENT_GROUP` kolonuna yazilir (kolon her calistirmada bulunur; tek provider'da
  `<PROVIDER>|<EVENT>` degerini alir) ve "ayni depremden en fazla 3 kayit" siniri
  `EVENT` yerine bu kolona gore uygulanir.
- **Tekrar kayit:** ayni olay grubunda, farkli provider'larda ve istasyon konumu
  `<= 1 km` olan kayitlardan ilk provider'inki (provider listesindeki siraya gore)
  korunur, digeri aday havuzundan cikarilir.
- Esikler ve davranis `SelectionConfig.dedup` (`DedupConfig`) ile ayarlanir:
  `enabled`, `max_event_distance_km`, `max_mag_diff`, `max_station_distance_km` ve
  `prefer_provider` (ornegin `ProviderName.AFAD`; verilmezse provider listesinde
  ilk siradaki korunur).
- Gecersiz konumlar (`NaN`, `-999`, `(0, 0)`) eslestirmeye katilmaz.
- Sonuclar `report["deduplication"]` ve `logs` icinde raporlanir.
- Eslestirme sezgiseldir (ortak olay kimligi yoktur); yil siniri yil sonu/basi
  olaylarinda eslesmeyi kacirabilir. Hatali birlestirmenin bedeli yalnizca olay
  limitinin biraz daha siki uygulanmasidir.
