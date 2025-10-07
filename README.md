# SelectionEarthquake

Deprem kayıtlarının karakteristik özelliklerinin bilgilerini farklı veri sağlayıcılardan (AFAD,PEER) çekip normalize eden, ardından belirlenen kriterlere göre puanlayan ve strateji tabanlı seçim yapan Python kütüphanesi.
Böylece araştırmacılar ve mühendisler, bina özelinde uygun deprem kayıtlarını hızlı ve güvenilir şekilde elde edebilir.

---

## 🚀 Özellikler

- 🌐 Çoklu veri sağlayıcı desteği (AFAD, PEER)
- 🔎 Esnek arama kriterleri (`magnitude`, `depth`, `distance`, `Vs30`, vb.)
- 🧩 Pipeline tabanlı mimari
- 📂 Çıktılar: CSV, XLSX, MiniSeed, Pandas DataFrame
- ⚡ Asenkron (async) sorgular ile hızlı veri çekme
- 🏆 Puanlama sistemi ve strateji tabanlı kayıt seçimi (örn. TBDY 2018’e göre seçim)
- 🧪 Test altyapısı (pytest) ve kolay genişletilebilir provider mimarisi

---

## 📦 Kurulum

```bash
# PyPI'den yükleme
pip install earthquake-selection

# Yerel geliştirme için
git clone https://github.com/kullanici/SelectionEarthquake.git
cd SelectionEarthquake
pip install -e .

```

## ⚡ Hızlı Başlangıç

```py
import asyncio
from selection_service.enums.Enums import DesignCode, ProviderName
from selection_service.core.Pipeline import EarthquakeAPI
from selection_service.processing.Selection import (SelectionConfig,
                                                    SearchCriteria,
                                                    TBDYSelectionStrategy,
                                                    TargetParameters)
from selection_service.core.LoggingConfig import setup_logging

setup_logging()

async def example_usage():
    # Seçim stratejisi oluşturma
    con = SelectionConfig(design_code=DesignCode.TBDY_2018,
                          num_records=22,
                          max_per_station=3,
                          max_per_event=3,
                          min_score=55)
    strategy = TBDYSelectionStrategy(config=con)

    #Arama kriterleri
    search_criteria = SearchCriteria(
        start_date="2000-01-01",
        end_date="2025-09-05",
        min_magnitude=7.0,
        max_magnitude=10.0,
        min_vs30=300,
        max_vs30=400
        # mechanisms=["StrikeSlip"]
        )
    
    # Hedef parametreler
    target_params = TargetParameters(
        magnitude=7.0,
        distance=30.0,
        vs30=400.0,
        pga=200,
        mechanism=["StrikeSlip"]
    )
    
    # API
    api = EarthquakeAPI(providerNames= [ProviderName.AFAD, 
                                   ProviderName.PEER],
                        strategies= [strategy])

    # Asenkron arama
    result = await api.run_async(criteria=search_criteria,
                                 target=target_params,
                                 strategy_name=strategy.get_name())
    
    # Senkron arama
    # result = api.run_sync(criteria=search_criteria,
    # target=target_params,
    # strategy_name=strategy.get_name())
    
    
    if result.success:
        print(result.value.selected_df[['PROVIDER','RSN','EVENT','YEAR','MAGNITUDE','STATION','VS30(m/s)','RRUP(km)','MECHANISM','PGA(cm2/sec)','PGV(cm/sec)','SCORE']].head(7))
        return result.value
    else:
        print(f"[ERROR]: {result.error}")
        return None
    
if __name__ == "__main__":
    df = asyncio.run(example_usage())
```

PROVIDER | RSN      | EVENT         | YEAR  | MAGNITUDE |           STATION            | VS30(m/s) | RRUP(km)   |  MECHANISM  | PGA(cm2/sec) | PGV(cm/sec) | SCORE  
---------|----------|---------------|------ |---------- |------------------------------|-----------|----------  | ----------- |-----------   |-----------  |-------------
PEER     |  900     |  Landers      |  1992 |    7.28   |  Yermo Fire Station          |    353.63 |  23.620000 |  StrikeSlip |  217.776277  |  40.263000  |  100.000000
PEER     |  3753    |  Landers      |  1992 |    7.28   |  Fun Valley                  |    388.63 |  25.020000 |  StrikeSlip |  206.125976  |  19.963000  |  100.000000
PEER     |  1615    |  Duzce, Turkey|  1999 |    7.14   |  Lamont 1062                 |    338.00 |  9.140000  |  StrikeSlip |  202.664229  |  14.630000  |  100.000000
PEER     |  881     |  Landers      |  1992 |    7.28   |  Morongo Valley Fire Station |    396.41 |  17.360000 |  StrikeSlip |  188.768206  |  24.317000  |  100.000000
PEER     |  1762    |  Hector Mine  |  1999 |    7.13   |  Amboy                       |    382.93 |  43.050000 |  StrikeSlip |  182.933249  |  23.776000  |  100.000000
AFAD     |  327943  |  17966        |  2023 |    7.70   |  DSİ, Musa Şahin Bulvarı     |    350.00 |  27.110381 |  StrikeSlip |  185.737903  |  29.642165  |  91.304348
AFAD     |  327943  |  17966        |  2023 |    7.70   |  DSİ, Musa Şahin Bulvarı     |    350.00 |  27.110381 |  StrikeSlip |  185.737903  |  29.642165  |  91.304348


## 🛠 Mimari

```bash
selection_service/
│
├── providers/          # Veri sağlayıcılar (AFAD, FDSN, PEER…)
├── core/               # Pipeline ve API
├── processing/         # SearchCriteria, Result, vs.
├── utility/            # Yardımcı fonksiyonlar
├── enums/              # ProviderName gibi enumlar
├── data/               # Kullanılan csv ve excel dosyaları

tests/              # pytest testleri

```

## 🤝 Provider Ekleme Adımları

- enums.Enums.ProviderName kısmına ismini ekle

- Yeni provider eklemek için providers/ altına python dosyasını aç.

- Provider sınıfı mutlaka IDataProvider'ı miras almalı.

- Provider a özel BaseColumnMapper sınıfını miras alan mapping sınıfını yaz ve ColumnMapperFactory e ekle

- ProviderFactory de create methoduna ekle

- Unit test yazmayı unutma.


## 📌 Yol Haritası

- [ ] Yeni provider: FDSN


## 📜 Lisans

MIT License
