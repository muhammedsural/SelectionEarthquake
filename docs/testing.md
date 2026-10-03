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
- provider'lar arasi tekrar tespiti (`processing.dedup`)
- eksik veri ve TBDY uygunluk raporu

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
flake8 src --select=E9,F63,F7,F82
pytest --cov=src/selection_service --cov-fail-under=80
```

Dokuman icin:

```bash
mkdocs build --strict
```
