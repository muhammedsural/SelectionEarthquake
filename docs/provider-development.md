# Provider Gelistirme

Yeni provider eklerken amac, provider'in ham verisini `STANDARD_COLUMNS`
semasina donusturmek ve pipeline'a `IDataFetcher` olarak sunmaktir.

## 1. Provider enum'una ekle

`src/selection_service/enums/Enums.py`:

```python
class ProviderName(str, Enum):
    AFAD = "AFAD"
    PEER = "PEER"
    NEW_PROVIDER = "NEW_PROVIDER"
```

## 2. Mapper yaz

`BaseColumnMapper` sinifini kullanarak ham kolonlari standart kolonlara esle.

```python
from selection_service.processing.Mappers import BaseColumnMapper

class NewProviderColumnMapper(BaseColumnMapper):
    def __init__(self, **kwargs):
        super().__init__({
            "raw_event": "EVENT",
            "raw_mag": "MAGNITUDE",
            "raw_station": "STATION",
        })
```

Mapper sonunda DataFrame `STANDARD_COLUMNS` sirasi ile donmelidir.

## 3. Provider yaz

Provider `IDataFetcher` sozlesmesini karsilamalidir.

```python
from selection_service.providers.interfaces import IDataFetcher
from selection_service.processing.ResultHandle import Result

class NewProvider(IDataFetcher):
    def get_name(self) -> str:
        return "NEW_PROVIDER"

    def map_criteria(self, criteria):
        return {}

    def fetch_data_sync(self, criteria):
        df = ...
        return Result.ok(df)

    async def fetch_data_async(self, criteria):
        df = ...
        return Result.ok(df)
```

## 4. Factory kaydi ekle

`ProviderFactory.create_provider()` icinde yeni provider'i olustur.

`ColumnMapperFactory` icin mapper kaydi ekle veya runtime'da
`ColumnMapperFactory.register(...)` kullan.

## 5. Test ekle

En az su testler eklenmelidir:

- mapper standart kolonlari uretiyor
- provider kriterleri dogru map ediyor
- sync fetch basarili durumda DataFrame donduruyor
- hata durumunda `Result.fail` veya `ProviderError` akisi korunuyor
- pipeline coklu provider ile kolon kaybi yasamiyor

## 6. Dokuman guncelle

Yeni provider:

- `docs/concepts.md`
- `docs/provider-development.md`
- `README.md`
- API referans sayfalari

icinde listelenmelidir.

## FDSN notu

Depoda FDSN icin taslak provider ve mapper bulunur, ancak enum/factory
entegrasyonu tamamlanmadigi surece public destek olarak dokumante edilmemelidir.
