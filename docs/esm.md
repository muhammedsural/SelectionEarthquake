# ESM Saglayicisi

`ProviderName.ESM` (2.2.0), ORFEUS ve INGV'nin islettigi Engineering
Strong-Motion veritabanini ([esm-db.eu](https://esm-db.eu)) baglar. ESM;
Avrupa, Turkiye ve Orta Dogu'daki kuvvetli yer hareketi kayitlarini elle
islenmis (MP) halde, yer hareketi parametreleri ve sonlu fay mesafeleriyle
birlikte sunar.

Iki herkese acik servis kullanilir; anahtar veya kayit gerekmez:

| Servis | Kullanim |
| --- | --- |
| `esmws/flatfile/1/query` | Arama: kayit basina bir satir (deprem, istasyon, mesafe, PGA/PGV/PGD, T90, Arias) |
| `esmws/eventdata/1/query` | Indirme: bir kaydin uc bileseni (ascii, mseed, sac) |

Kisitli veya ambargolu kayitlar ESM'in imzali token ile POST istegini ister;
bu kayitlar desteklenmez ve aramada gorunmez.

## Ornek

```python
from selection_service.core.earthquake_api import EarthquakeAPI
from selection_service.enums.enums import DesignCode, ProviderName
from selection_service.processing.criteria import SearchCriteria, SelectionConfig
from selection_service.processing.strategies import TBDYSelectionStrategy

strategy = TBDYSelectionStrategy(SelectionConfig(design_code=DesignCode.TBDY_2018))
api = EarthquakeAPI([ProviderName.ESM, ProviderName.AFAD], [strategy])

criteria = SearchCriteria(
    start_date="2000-01-01",
    end_date="2024-12-31",
    min_magnitude=6.0,
    max_Rjb=30,
    min_vs30=360,
    max_vs30=760,
)
result = api.run_sync(criteria, strategy.get_name())
esm = result.value.selected_df.query("PROVIDER == 'ESM'")
```

Birden fazla provider secildiginde ayni deprem ve istasyon cifti
[tekrar eleme](concepts.md#tekrar-tespiti-ve-olay-limiti) ile tek kayda indirilir.

## Filtreler

| Kriter | Nerede uygulanir |
| --- | --- |
| `start_date`, `end_date` | ESM sunucusu |
| `min/max_magnitude` | Sunucu (herhangi bir buyukluk turu) ve istemci (secilen `MAGNITUDE`) |
| `min/max_depth` | Sunucu ve istemci |
| `bbox` veya `min/max_latitude/longitude` | Sunucu (deprem konumu) |
| `min/max_vs30` | Sunucu ve istemci |
| `min/max_Rjb` | Sunucu ve istemci |
| `network`, `station_code` | Sunucu (tam eslesme) |
| `mechanisms` (StrikeSlip, Normal, Reverse) | Sunucu (`style-of-faulting`) ve istemci |
| Diger mekanizmalar | Istemci |
| `min/max_Rrup`, `min/max_Repi`, `min/max_Rhyp` | Istemci |
| `min/max_pga`, `pgv`, `pgd` | Istemci |
| `circleLatitude/Longitude/Radius` | Istemci (deprem konumu) |

## Kolonlar

| Standart kolon | ESM alani |
| --- | --- |
| `EVENT` | `esm_event_id` (indirmede kullanilir) |
| `RSN` | `EVENT` ile `FILE_NAME_H1` birlesimi (dikey cizgiyle) |
| `SSN`, `STATION_ID` | `AG.ISTASYON` |
| `FILE_NAME_H1` | `AG.ISTASYON.KONUM.KANAL` (indirme anahtari) |
| `MAGNITUDE`, `MAGNITUDE_TYPE` | Mw, yoksa EMEC Mw, Ms, Ml, Mb |
| `MECHANISM` | `fm_type_code`: NF Normal, TF Reverse, SS StrikeSlip, O Oblique |
| `RJB(km)`, `RRUP(km)` | `jb_dist`, `rup_dist`; bos ise `epi_dist`, `hyp_dist` |
| `VS30(m/s)` | `preferred_vs30_m_s` |
| `PGA(cm2/sec)`, `PGV(cm/sec)`, `PGD(cm)` | RotD50; yoksa iki yatay bilesenin geometrik ortalamasi |
| `T90_avg(sec)` | Iki yatay bilesenin T90 ortalamasi |
| `ARIAS_INTENSITY(m/sec)` | `rotd50_ai` (RotD50 Arias), cm/s -> m/s |
| `LOWFREQ(Hz)` | Yatay bilesenlerin yuksek geciren kesme frekansinin en buyugu |

## Indirme

Bkz. [Waveform Indirme](waveform-download.md#esm-download-parametreleri).
