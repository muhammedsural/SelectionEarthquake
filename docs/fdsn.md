# FDSN Saglayicisi

`ProviderName.FDSN` FDSN standardindaki web servislerini dogrudan HTTP ile
kullanir; 2.3.0'dan itibaren ObsPy gerekmez.

| Servis | Kullanim |
| --- | --- |
| `fdsnws/event/1` | Deprem katalogu (`format=text`) |
| `fdsnws/station/1` | Istasyon, kanal ve duyarlilik bilgisi (`format=text`) |
| `fdsnws/dataselect/1` | Ham miniSEED dalga formu |

Olay katalogu (`service`/`base_url`) ile veri merkezi (`data_service`/
`data_base_url`) ayri dugumler olabilir. Bilinen dugum adlari: USGS, IRIS
(EarthScope), EMSC, ISC, GFZ, INGV, ORFEUS, KOERI, RESIF, ETH, NOA, GEONET,
NCEDC, SCEDC, RASPISHAKE. Diger dugumler icin kok adres verilir.

## Olay ve kayit aramasi

Varsayilan olarak yalniz olay katalogu dondurulur (deprem basina bir satir).
`record_search=True` verildiginde her deprem icin cevresindeki ivme olcer
istasyonlari aranir ve kayit basina bir satir dondurulur:

```python
api = EarthquakeAPI(
    [ProviderName.FDSN], [strategy],
    service="USGS", data_service="KOERI", record_search=True,
)
criteria = SearchCriteria(start_date="2023-02-06", end_date="2023-02-07",
                          min_magnitude=7.0, max_Repi=200)
result = api.run_sync(criteria, strategy.get_name())
```

| Parametre | Varsayilan | Aciklama |
| --- | --- | --- |
| `record_search` | `False` | Kayit basina satir dondur |
| `channels` | `("HN", "HG", "EN", "BN")` | Ivme olcer bantlari, oncelik sirasiyla; istasyon basina ilk bulunan bant secilir |
| `max_events` | `20` | Istasyon aranacak en buyuk deprem sayisi |
| `default_radius_km` | `150` | Mesafe siniri verilmezse arama yaricapi |
| `pre_event_s` | `30` | Indirmede deprem oncesi sure |

Kayit satirlarinda:

- `FILE_NAME_H1` `AG.ISTASYON.KONUM.BANT` bicimindedir (ornegin `TK.4614..HN`);
  uc bilesen birlikte iner.
- `RJB(km)` merkez ussu, `RRUP(km)` hiposantr mesafesidir (nokta kaynak
  yaklasimi). Sonlu fay mesafesi FDSN servislerinde yoktur.
- Vs30, PGA/PGV/PGD ve faylanma turu bostur; bu alanlara dayanan filtreler
  FDSN satirlarina uygulanmaz. Mesafe sinirlari (Rjb, Rrup, Repi, Rhyp) ve
  daire filtresi istemci tarafinda uygulanir.

## Indirme

```python
download = api.download_single_waveform(
    filename="TK.4614..HN", event_id="us6000jllz",
    station_code="FDSN.TK.4614", export_type="asc2", file_status="Acc",
)
```

- Zaman penceresi depremden `pre_event_s` once baslar, `120 s + mesafe/3.5`
  (en az 180 s, mesafe bilinmiyorsa 300 s) sonra biter.
- `export_type="mseed"` ham miniSEED (sayim) yazar.
- `asc2`/`ascii` her bilesen icin ESM benzeri bir ASCII dosyasi yazar. Sayimlar
  istasyon servisindeki kanal duyarliligina (`Scale`) bolunur; birim m/s^2 ise
  cm/s^2'ye cevrilir. Bu, ivme olcerlerin duz bandinda alet tepkisinin
  giderilmesine esdegerdir; tam kutup-sifir duzeltmesi yapilmaz.
- Yalniz ivme (`RawAcc`, `Acc`) istenebilir.

## miniSEED okuyucu

`selection_service.waveform.read_mseed` miniSEED 2 verisini numpy ile cozer
(16/32 bit tamsayi, 32/64 bit kayan nokta, Steim1, Steim2). Kanal basina bir
`Trace` doner; kucuk bosluklar interpolasyonla doldurulur, cakisan ornekler
atilir. miniSEED 3 ve Blockette 1000 olmayan kayitlar desteklenmez.

```python
from selection_service.waveform import read_mseed, write_ascii

traces = read_mseed(open("kayit.mseed", "rb").read())
write_ascii(traces, "cikti", "kayit")
```
