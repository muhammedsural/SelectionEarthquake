# CLI

Kurulumdan sonra `earthquake-selection-example` komutu kullanilabilir.

## Lokal PEER secimi

```bash
earthquake-selection-example --providers peer --num-records 11 --report-path selection_report.json --selected-csv selected_records.csv
```

Bu komut:

1. PEER flatfile verisini okur.
2. TBDY 2018 Gaussian stratejisini calistirir.
3. Secilen kayitlari CSV'ye yazar.
4. Raporu JSON olarak yazar.

## AFAD ile arama

```bash
earthquake-selection-example --providers afad --start-date 2023-02-06 --end-date 2023-02-07 --min-magnitude 6.0
```

AFAD API ag erisimi gerektirir.

## Waveform indirme

```bash
earthquake-selection-example --providers afad --download-waveforms --export-type mseed
```

`--download-waveforms` sadece download destekleyen provider'larda calisir.
PEER download desteklemez ve indirme adiminda atlanir.

## Parametreler

| Parametre | Varsayilan | Aciklama |
| --- | --- | --- |
| `--providers` | `peer` | `peer`, `afad` veya ikisi birden |
| `--start-date` | `2000-01-01` | Baslangic tarihi |
| `--end-date` | `2025-09-05` | Bitis tarihi |
| `--min-magnitude` | `7.0` | Minimum moment buyuklugu |
| `--max-magnitude` | `8.0` | Maksimum moment buyuklugu |
| `--min-vs30` | `300.0` | Minimum Vs30 |
| `--max-vs30` | `400.0` | Maksimum Vs30 |
| `--mechanism` | `StrikeSlip` | Birden fazla kez verilebilir |
| `--num-records` | `11` | Secilecek maksimum kayit sayisi |
| `--min-score` | `55.0` | Minimum kabul skoru |
| `--scoring-preset` | `tbdy_2018_record_selection` | Hazir agirlik seti |
| `--report-path` | `selection_report.json` | JSON rapor yolu |
| `--selected-csv` | `selected_records.csv` | CSV cikti yolu |
| `--download-waveforms` | kapali | Secilen kayitlar icin indirme baslatir |
| `--export-type` | `mseed` | `mseed`, `asc2`, `asd` |
