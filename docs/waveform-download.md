# Waveform Indirme

Waveform indirme provider yetenegine baglidir. Mevcut durumda AFAD download
destekler, PEER desteklemez.

## Toplu indirme

```python
download = api.download_waveforms(
    result.value.selected_df,
    export_type="mseed",
    batch_size=10,
)

if not download.success:
    print(download.error)
```

Download desteklemeyen provider'lar atlanir.

## Tekil indirme

```python
download = api.download_single_waveform(
    filename="record.mseed",
    event_id="12345",
    station_code="AFAD.TK.KND",
    export_type="mseed",
)
```

`station_code` ilk segmenti provider adi olarak yorumlanir. Ornek:
`AFAD.TK.KND` -> `AFAD`.

## AFAD download parametreleri

| Parametre | Varsayilan | Aciklama |
| --- | --- | --- |
| `batch_size` | `10` | Batch basina dosya sayisi, en fazla 10 |
| `file_type` | `ap` | Dosya tipi |
| `file_status` | `Acc` | RawAcc, Acc, Vel, Disp vb. |
| `export_type` | `mseed` | `mseed`, `asc2`, `asd` |
| `user_name` | `GuestUser` | AFAD guest kullanici adi |

## Cikti konumu

AFAD provider dosyalari varsayilan olarak `Afad_events` altina kaydeder.
Bu klasor `.gitignore` icinde tutulur.
