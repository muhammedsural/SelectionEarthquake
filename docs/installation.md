# Kurulum

## Desteklenen Python surumleri

Proje `pyproject.toml` icinde `Python >= 3.10` ister. Test matrisi Python
3.10, 3.11 ve 3.12 icin calisir.

## PyPI kurulumu

```bash
pip install earthquake-selection
```

Kurulumdan sonra import paketi `selection_service` olur:

```python
from selection_service.core.EarthquakeApi import EarthquakeAPI
```

## Yerel gelistirme kurulumu

```bash
git clone https://github.com/muhammedsural/SelectionEarthquake.git
cd SelectionEarthquake
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

## Dokuman kurulumu

```bash
pip install -r requirements-docs.txt
```

Yerel dokuman sunucusu:

```bash
mkdocs serve
```

Statik dokuman build:

```bash
mkdocs build --strict
```

## Test

```bash
pytest
```

Pytest ayarlari tek kaynak olarak `pyproject.toml` icindedir. Ayrica
`pytest.ini` tutulmaz.

## Paketleme

```bash
python -m build
twine check dist/*
```

Paket adi `earthquake-selection`, import namespace'i `selection_service` olarak
kalmalidir.
