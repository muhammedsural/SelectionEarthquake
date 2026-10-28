# CESMD Saglayicisi

`ProviderName.CESMD` (2.4.0), USGS ve CGS'nin islettigi Center for Engineering
Strong Motion Data'yi ([strongmotioncenter.org](https://strongmotioncenter.org))
baglar. CESMD ABD basta olmak uzere dunyanin bircok agindan (Turkiye, Yunanistan,
Japonya ...) kuvvetli yer hareketi kayitlarini ve kayit basina PGA/PGV/PGD
degerlerini sunar.

| Servis | Kullanim |
| --- | --- |
| `wserv/records/query?rettype=metadata&groupby=event` | Arama: deprem, istasyon ve kayit metaverisi (JSON) |
| `wserv/records/query?rettype=dataset` | Indirme: kayitlarin ZIP'i (`download=P` islenmis, `R` ham) |

## Arama

Anahtar gerekmez. Sunucuda tarih, buyukluk, deprem kutusu veya dairesi
(`eminlat`.. / `elat,elon,erad`), Repi ve ag suzulur; yalniz serbest alan (zemin)
istasyonlari (`sttype=G`) istenir. Rjb, Rrup, Vs30, PGA/PGV/PGD ve mekanizma
yanit geldikten sonra uygulanir. Ne ham ne islenmis verisi olan kayitlar atilir.

CESMD Rjb/Rrup vermez; `fault_dist` (en yakin fay parcasina uzaklik) varsa
`RJB`/`RRUP` olarak kullanilir, yoksa Repi (nokta kaynak yaklasimi). PGA
`g`'den cm/s^2'ye cevrilir (islenmis yatay, yoksa ham).

## Indirme ve e-posta

Servis indirme icin kayitli bir e-posta ister (ucretsiz kayit:
<https://strongmotioncenter.org/cgi-bin/CESMD/register.pl>). E-posta
`CESMD_EMAIL` ortam degiskeniyle veya provider'a `email=` ile verilir; verilmezse
indirme aciklayici bir hatayla durur.

`FILE_NAME_H1` anahtari `AG.ISTASYON~REPI` bicimindedir (ornek `CI.CCC~34.5`):
servis istasyon koduyla suzmedigi icin ag (`netid`) ve Repi penceresi
(+-0.06 km) sunucuya, istasyon kodu ZIP icerigine uygulanir.

Indirme 2019 Ridgecrest depremi icin CI, AZ, BK, CE, NC, NN, NP, PG, WR aglarinda
canli dogrulandi. Yanit, `metadata.json` (istek adresi kayitli e-postayi icerir) ve
asil kayitlari tasiyan ic ZIP'ten olusur. Kutuphane ic ZIP'i acar, yalniz ivme
dosyalarini tutar (hiz/yer degistirme `.vel`/`.dis`, spektrum `V3` ve
`metadata.json` silinir) ve su dosyalari birakir:

- CSMIP/CGS/SCSN/NCSN/UNR aglari: tek dosyada tum kanallar, `CIADO.V2` (islenmis) veya
  `CIADO.V1` (ham).
- USGS NSMP/BK/AZ/NP/WR ve benzeri: kanal basina COSMOS dosyasi, `*.acc.V2c`
  (islenmis) veya `*.V1c` (ham).

```python
import os
from selection_service.core.earthquake_api import EarthquakeAPI
from selection_service.enums.enums import ProviderName
from selection_service.processing.criteria import SearchCriteria

os.environ["CESMD_EMAIL"] = "kayitli@eposta.org"
api = EarthquakeAPI([ProviderName.CESMD], [])
criteria = SearchCriteria(start_date="2019-07-01", end_date="2019-07-31",
                          min_magnitude=6.5, max_Repi=40)
```
