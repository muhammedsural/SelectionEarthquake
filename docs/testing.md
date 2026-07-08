# Test ve Kalite

## Test kosusu

```bash
pytest
```

Mevcut pytest ayarlari `pyproject.toml` icindedir.

## Kapsam

Test paketi su davranislari kapsar:

- AFAD API client hata yonetimi
- AFAD provider retry ve download akislari
- PEER filtreleme
- kolon mapper'lari
- pipeline adimlari
- `SearchCriteria` validasyonlari
- scoring motoru
- TBDY selection rules
- `Result` ve hata hiyerarsisi

## Yeni davranis eklerken

Kural:

- Public API degisiyorsa test ekle.
- Provider davranisi degisiyorsa mock/fixture tabanli test ekle.
- Scoring veya secim kurali degisiyorsa beklenen `SCORE`, `SELECTION_REASON`
  ve limit davranisini test et.
- Dokuman ornegi degisiyorsa import ve alan adlarini gercek API ile dogrula.

## Uyari politikasi

Test kosusu uyarilari gizlemek yerine temizlemeyi hedefler. Mevcut ayarlarda
pytest cache provider devre disidir; bunun nedeni Windows izinli calisma
ortaminda `.pytest_cache` yazma uyarilarini onlemektir.

## Yerel kalite komutlari

```bash
python -m compileall src
pytest
```

Dokuman icin:

```bash
mkdocs build --strict
```
