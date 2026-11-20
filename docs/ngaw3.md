# NGA-West3 Saglayicisi

`ProviderName.NGAW3` (2.5.0), NGA-West3 kuvvetli yer hareketi veritabanini
([nga-gm.org](https://nga-gm.org)) baglar. Veritabani kayit basina olay, istasyon,
saha (Vs30), mesafe (Rjb, Rrup) ve RotD50 siddet olculerini ile ivme zaman serilerini
REST API ile sunar. API belgesi: <https://nga-gm.org/pages/api_documentation>.

| Uc nokta | Kullanim |
| --- | --- |
| `GET /users/login` (Basic Auth) | 24 saat gecerli Bearer token |
| `GET /flatfile` | Arama: olay + istasyon + saha + yol + RotD50 PGA/PGV (`where`, `select`, `limit`, `page`) |
| `POST /unscaled-motions` (`format=json`) | Ivme zaman serileri (g), bilesen basina |

## Hesap

API her istekte token ister; hesap ucretsiz acilir (nga-gm.org > login/register).
Kullanici adi ve sifre su yollarla verilir (sifre diske yazilmaz, cache anahtarina
girmez):

```python
from selection_service.core.earthquake_api import EarthquakeAPI
from selection_service.enums.enums import ProviderName
from selection_service.processing.criteria import SearchCriteria

# Ortam degiskenleri: NGAGM_USERNAME, NGAGM_PASSWORD   (veya credentials= / username=,password=)
# Birden fazla provider'da her birinin hesabi ayri verilir:
#   credentials={ProviderName.CESMD: {"email": ...}, ProviderName.NGAW3: {"username": ..., "password": ...}}
api = EarthquakeAPI([ProviderName.NGAW3], [],
                    credentials={ProviderName.NGAW3: {"username": "kullanici", "password": "sifre"}})
criteria = SearchCriteria(start_date="1990-01-01", end_date="2023-12-31",
                          min_magnitude=6.5, max_Rjb=30, min_vs30=200, max_vs30=800)
```

## Arama

Sunucuya yalniz dogrulanmis sayisal alanlar gider: `magnitude`, `hypocenter_depth`,
`vs30`, `rjb`, `rrup`, `epicentral_distance`, hiposantre kutusu ve
`public_time_series=1` (yalniz indirilebilir kayitlar). Tarih araligi, mekanizma, daire
ve PGA/PGV sinirlari yanit geldikten sonra uygulanir. Sonuc ``max_rows`` satirda
(varsayilan 20000) kesilir; kesilirse uyari yazilir.

Eslestirme: PGA `g`'den cm/s^2'ye cevrilir (RotD50); PGV cm/s; PGD flatfile'da
yoktur (bos). Mekanizma `mechanism_based_on_Rake` (0 SS, 1 N, 2 R, 3 R/O, 4 N/O),
NGA-West2 ile ayni. `LOWFREQ(Hz)` yatay bilesenlerin en buyuk yuksek-geciren
kose frekansidir. `EVENT` ComCat kimligi, yoksa `NGAW3-<event_id>`.

## Indirme

`FILE_NAME_H1` anahtari `NGAW3_MID<motion_id>` bicimindedir. Zaman serileri JSON olarak
cekilir (dokumana gore aylik 500 hareketlik ZIP kotasina sayilmaz) ve NGA-West3 ZIP'lerindeki
bicimle (`NGAwest3_MID<id>_<azimut>.AT2`, dusey icin `_UP.AT2`, g biriminde) olay
klasorune yazilir.

## Lisans ve atif

NGA-gm lisansi ([nga-gm.org/pages/license](https://nga-gm.org/pages/license)) veriyi
arastirma, egitim ve danismanlik projelerinde kullanmaya izin verir; verinin ucuncu
taraflarca satilmasini, indirme limitlerini asmak icin birden fazla hesap acilmasini
yasaklar. Kutuphane NGA-West3 verisini paketlemez; her kullanici kendi hesabiyla ceker.
Atif onerilir: Buckreis, T.E. vd. (2023) Web portal for the global earthquake ground
motion relational database, DOI 10.34948/G4RP4K. Turev urunler verinin NGA projesince
onaylanmadigini belirtmelidir.

## Dogrulama durumu

Alan adlari nga-gm.org/schema sayfasindan, yanit bicimi API belgesinden alinmistir.
Testler sahte oturumla calisir; **gercek hesapla canli arama ve indirme henuz denenmedi**
(dogrulanmadi). Ilk canli denemede `select` listesindeki bir alan reddedilirse sunucu
HTTP 200 ile hata metni dondurur; kutuphane bunu `ProviderError` olarak bildirir.
