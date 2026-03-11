from abc import ABC, abstractmethod
from datetime import datetime
import math
from typing import Any, Dict, List, Optional, Protocol, Tuple
# from obspy import UTCDateTime
import pandas as pd
from pydantic import BaseModel, Field, model_validator
from ..enums.Enums import DesignCode
from ..core.Config import MECHANISM_MAP,REVERSE_MECHANISM_MAP, SCORING_MAP, get_mechanism_numeric

class ScoringWeights(BaseModel):
    """
    Kullanıcı arayüzünden gelen ağırlıklar. 
    Eğer kullanıcı belirtmezse Config.py'deki varsayılanları kullanır.
    """
    # Config'deki her anahtar için dinamik alan oluşturuyoruz
    # (Burayı manuel de yazabilirsiniz ama Pydantic ile dinamik de yönetilebilir)
    magnitude: float = SCORING_MAP['magnitude']['weight']
    rjb: float = SCORING_MAP['rjb']['weight']
    rrup: float = SCORING_MAP['rrup']['weight']
    repi: float = SCORING_MAP['repi']['weight']
    vs30: float = SCORING_MAP['vs30']['weight']
    pga: float = SCORING_MAP['pga']['weight']
    pgv: float = SCORING_MAP['pgv']['weight']
    pgd: float = SCORING_MAP['pgd']['weight']
    t90: float = SCORING_MAP['t90']['weight']
    arias: float = SCORING_MAP['arias']['weight']
    depth: float = SCORING_MAP['depth']['weight']
    mechanism: float = SCORING_MAP['mechanism']['weight']

    def get_weight(self, key: str) -> float:
        return getattr(self, key, 0.0)
class SelectionConfig(BaseModel):
    """Seçim konfigürasyonu.

    Adım 4 — Pydantic BaseModel'e dönüştürme:
      Önceki kod @dataclass + Pydantic Field() karışımı kullanıyordu.
      Field(default_factory=...) yalnızca Pydantic modelleri için geçerlidir;
      @dataclass ile birlikte kullanıldığında runtime'da sessizce yanlış
      davranış üretir (Field nesnesi liste yerine field descriptor olarak kalır).

    Pydantic BaseModel kullanmanın ek faydaları:
      - Alan doğrulaması kolayca eklenebilir.
      - .model_dump() / .model_copy() hazır gelir.
      - ScoringWeights ve SearchCriteria ile tutarlı tip sistemi.
    """

    design_code         : DesignCode
    num_records         : int       = 22
    max_per_station     : int       = 3
    max_per_event       : int       = 3
    min_score           : float     = 50.0
    required_components : List[str] = Field(default_factory=list)

class SearchCriteria(BaseModel):
    """Arama kriterleri - Tüm sağlayıcılar için ortak kriterler"""
    start_date: str                          # from_date: Başlangıç tarihi (ISO format: "2023-02-06T01:16:00.000Z")  
    end_date: str                            # to_date: Bitiş tarihi (ISO format: "2023-02-06T01:18:41.000Z")
    min_magnitude: Optional[float] = None    # from_mw: Minimum Mw büyüklüğü
    max_magnitude: Optional[float] = None    # to_mw: Maksimum Mw büyüklüğü
    min_depth: Optional[float] = None        # min_depth: Minimum derinlik
    max_depth: Optional[float] = None        # max_depth: Maksimum derinlik
    station_code: Optional[str] = None       # station_code: İstasyon kodu
    network: Optional[str] = None            # network: Ağ bilgisi
    country: Optional[str] = None            # Ülke
    province: Optional[str] = None           # İl
    district: Optional[str] = None           # İlçe
    neighborhood: Optional[str] = None       # Mahalle
    min_latitude: Optional[float] = None     # Minimum enlem for box search
    max_latitude: Optional[float] = None     # Maksimum enlem for box search
    min_longitude: Optional[float] = None    # Minimum boylam for box search
    max_longitude: Optional[float] = None    # Maksimum boylam for box search
    circleLatitude: Optional[float] = None   # circleLatitude: for circle search
    circleLongitude: Optional[float] = None  # circleLongitude: for circle search
    circleRadius: Optional[float] = None     # circleRadius: for circle search
    min_pga: Optional[float] = None          # Minimum PGA değeri
    max_pga: Optional[float] = None          # Maksimum PGA değeri
    min_pgv: Optional[float] = None          # Minimum PGV değeri
    max_pgv: Optional[float] = None          # Maksimum PGV değeri
    min_pgd: Optional[float] = None          # Minimum PGD değeri
    max_pgd: Optional[float] = None          # Maksimum PGD değeri
    fault_type: Optional[str] = None         # Fay tipi
    event_name: Optional[str] = None         # Event ismi
    min_Repi: Optional[float] = None         # Minimum Repi değeri Repicentral distance (Deprem merkez üssüne olan uzaklık) 
    max_Repi: Optional[float] = None         # Maksimum Repi değeri Repicentral distance (Deprem merkez üssüne olan uzaklık)
    min_Rhyp: Optional[float] = None         # Minimum Rhyp değeri Hypocentral distance (Deprem hiposantrına olan uzaklık)
    max_Rhyp: Optional[float] = None         # Maksimum Rhyp değeri Hypocentral distance (Deprem hiposantrına olan uzaklık)
    min_Rjb: Optional[float] = None          # Minimum Rjb değeri Joyner-Boore distance (Yüzeye izdüşüm uzaklığı)
    max_Rjb: Optional[float] = None          # Maksimum Rjb değeri Joyner-Boore distance (Yüzeye izdüşüm uzaklığı)
    min_Rrup: Optional[float] = None         # Minimum Rrup değeri Rupture distance (Kırılma uzaklığı)
    max_Rrup: Optional[float] = None         # Maksimum Rrup değeri Rupture distance (Kırılma uzaklığı)
    min_vs30: Optional[float] = None         # Minimum Vs30 değeri
    max_vs30: Optional[float] = None         # Maksimum Vs30 değeri
    mechanisms: Optional[List[str]] = Field(default_factory=list) # Fay mekanizması (ör: StrikeSlip, Normal, Reverse, Oblique)
    region: Optional[str] = None       # Bölge adı (örn: "Marmara", "Ege", "Doğu Anadolu" gibi AFAD'ın bölge tanımlarından biri)
    bbox: Optional[Tuple[float, float, float, float]] = Field(default_factory=tuple) # BBox formatı: (min_lat, max_lat, min_lon, max_lon)

    # Kullanıcı boş bırakırsa, sistem (min+max)/2 formülünü kullanır.
    # Kullanıcı bunları girerse puanlamaya dahil olur, girmezse ELİMİNE olur.
    target_magnitude: Optional[float] = None
    target_rjb: Optional[float] = None
    target_rrup: Optional[float] = None
    target_repi: Optional[float] = None
    target_vs30: Optional[float] = None
    target_pga: Optional[float] = None
    target_pgv: Optional[float] = None
    target_pgd: Optional[float] = None
    target_t90: Optional[float] = None
    target_arias: Optional[float] = None
    target_depth: Optional[float] = None

    # -- Dinamik Ağırlıklar --
    weights: ScoringWeights = Field(default_factory=ScoringWeights)

    # --- Yardımcı Metodlar ---
    def get_effective_target(self, key: str) -> Optional[float]:
        """
        Belirli bir parametre için hedef değeri döndürür.
        1. target_X var mı? Varsa döndür.
        2. Yoksa min_X ve max_X ortalamasını al.
        3. O da yoksa None döndür (Puanlamadan düş)
        """
        # Explicit target kontrolü
        explicit = getattr(self, f"target_{key}", None)
        if explicit is not None:
            return explicit
        
        # Aralık ortalaması kontrolü
        min_val = getattr(self, f"min_{key}", None)
        max_val = getattr(self, f"max_{key}", None)
        
        # Sadece aralık verildiyse ve target yoksa, aralık ortasını hedef al
        if min_val is not None and max_val is not None:
            return (min_val + max_val) / 2.0
            
        return min_val if min_val is not None else max_val

    def get_sigma(self, key: str) -> float:
        """Config dosyasından o parametre için belirlenen katılık (strictness) değerini kullanarak sigma hesaplar."""
        config = SCORING_MAP.get(key, {})
        strictness = config.get('sigma_strictness', 4.0)
        
        # Eğer kullanıcının bir aralığı varsa, aralığı baz al
        min_val = getattr(self, f"min_{key}", None)
        max_val = getattr(self, f"max_{key}", None)
        
        if min_val is not None and max_val is not None:
            diff = max_val - min_val
            return diff / strictness if diff > 0 else 1.0
            
        # Aralık yoksa, hedef değerin %10'u kadar bir sigma uydur (Fallback)
        target = self.get_effective_target(key)
        return (target * 0.1) if target else 1.0
    
    def to_afad_params(self) -> Dict[str, Any]:
        """AFAD API'sine özel parametre dönüşümü"""
        params = {
            "startDate"     : f"{self.start_date}T00:00:00.000Z" if self.start_date else None,
            "endDate"       : f"{self.end_date}T23:59:59.999Z" if self.end_date else None,
            
            "fromLatitude"  : self.min_latitude,
            "toLatitude"    : self.max_latitude,
            "fromLongitude" : self.min_longitude,
            "toLongitude"   : self.max_longitude,
            
            "fromMagnitude" : self.min_magnitude,
            "toMagnitude"   : self.max_magnitude,
            
            "from_depth"    : self.min_depth,  
            "to_depth"      : self.max_depth, 
            "fromRepi"      : self.min_Repi,
            "toRepi"        : self.max_Repi,
            "fromRhyp"      : self.min_Rhyp,
            "toRhyp"        : self.max_Rhyp,
            "fromRjb"       : self.min_Rjb,
            "toRjb"         : self.max_Rjb,
            "fromRrup"      : self.min_Rrup,
            "toRrup"        : self.max_Rrup,
            "fromVs30"      : self.min_vs30,
            "toVs30"        : self.max_vs30,
            "fromPGA"       : self.min_pga,
            "toPGA"         : self.max_pga,
            "fromPGV"       : self.min_pgv,
            "toPGV"         : self.max_pgv,
            "fromPgd"       : self.min_pgd,
            "toPgd"         : self.max_pgd,
            
            
            "fromT90"       : None,            
            "country"       : self.country,  
            "province"      : self.province,  
            "district"      : self.district,  
        }
        
        # if self.region:
        #     params["region"] = self.region
            
        if self.mechanisms:
            # AFAD fay mekanizması parametrelerine dönüşüm
            mechanism_map = {
                "StrikeSlip": "SS",
                "Reverse": "R",
                "Normal": "N",
                "Oblique": "T"
            }
            mechParams = [mechanism_map.get(m, m) for m in self.mechanisms]
            params["faultType"] = mechParams[0]
        params = {k: v for k, v in params.items() if v is not None}
        return params
    
    def to_peer_params(self) -> Dict[str, Any]:
        """PEER veritabanına özel parametre dönüşümü"""
        params = {
            "year_start": int(self.start_date[:4]),
            "year_end": int(self.end_date[:4]),
            "min_magnitude": self.min_magnitude,
            "max_magnitude": self.max_magnitude,
            "min_vs30": self.min_vs30,
            "max_vs30": self.max_vs30,
            'min_Rjb': self.min_Rjb,
            'max_Rjb': self.max_Rjb,
            'min_Rrup':self.min_Rrup ,
            'max_Rrup':self.max_Rrup,
            'min_depth': self.min_depth,
            'max_depth': self.max_depth,
            'min_pga': self.min_pga,
            'max_pga': self.max_pga,
            'min_pgv': self.min_pgv,
            'max_pgv': self.max_pgv,
            'min_pgd': self.min_pgd,
            'max_pgd': self.max_pgd,
            'mechanisms': self.mechanisms
        }
        
        if self.mechanisms:
            params["mechanisms"] = [get_mechanism_numeric(m) for m in self.mechanisms if m in REVERSE_MECHANISM_MAP]
            
        return params
    
    def to_fdsn_params(self) -> Dict[str, Any]:
        """FDSN standardına özel parametre dönüşümü
            starttime: Any | None = None,
            endtime: Any | None = None,
            minlatitude: Any | None = None,
            maxlatitude: Any | None = None,
            minlongitude: Any | None = None,
            maxlongitude: Any | None = None,
            latitude: Any | None = None,
            longitude: Any | None = None,
            minradius: Any | None = None,
            maxradius: Any | None = None,
            mindepth: Any | None = None,
            maxdepth: Any | None = None,
            minmagnitude: Any | None = None,
            maxmagnitude: Any | None = None,
            magnitudetype: Any | None = None,
            eventtype: Any | None = None,
            includeallorigins: Any | None = None,
            includeallmagnitudes: Any | None = None,
            includearrivals: Any | None = None,
            eventid: Any | None = None,
            limit: Any | None = None,
            offset: Any | None = None,
            orderby: Any | None = None,
            catalog: Any | None = None,
            contributor: Any | None = None,
            updatedafter: Any | None = None,
            filename: Any | None = None,
            **kwargs
        """
        params = {
            "starttime": f"{self.start_date}T00:00:00.000Z" if self.start_date else None,
            "endtime": f"{self.end_date}T23:59:59.999Z" if self.end_date else None,
            "minmagnitude": self.min_magnitude,
            "maxmagnitude": self.max_magnitude,
            "latitude": self.min_latitude,
            "longitude": self.min_longitude,
            # "maxradius": criteria.max_radius_deg,
        }
        
        if self.bbox:
            params["minlatitude"], params["maxlatitude"], params["minlongitude"], params["maxlongitude"] = self.bbox
            
        return params

    @model_validator(mode='after')
    def check_magnitudes(self):
        if self.min_magnitude is not None and self.max_magnitude is not None:
            if self.min_magnitude > self.max_magnitude:
                raise ValueError("Minimum büyüklük maksimum büyüklükten büyük olamaz.")
            if self.min_magnitude < 0 or self.max_magnitude > 10:
                raise ValueError("Büyüklük değerleri 0-10 aralığında olmalıdır.")
        return self

    @model_validator(mode='after')
    def check_dates(self):
        try:
            start = datetime.fromisoformat(self.start_date.replace('Z', '+00:00'))
            end = datetime.fromisoformat(self.end_date.replace('Z', '+00:00'))
            if start > end:
                raise ValueError("Başlangıç tarihi bitiş tarihinden sonra olamaz.")
        except ValueError as e:
            raise ValueError(f"Geçersiz tarih formatı: {e}")
        return self

    @model_validator(mode='after')
    def check_bbox(self):
        if self.bbox:
            min_lat, max_lat, min_lon, max_lon = self.bbox
            if not (-90 <= min_lat <= 90) or not (-90 <= max_lat <= 90):
                raise ValueError("Enlem değerleri -90 ile 90 arasında olmalıdır.")
            if not (-180 <= min_lon <= 180) or not (-180 <= max_lon <= 180):
                raise ValueError("Boylam değerleri -180 ile 180 arasında olmalıdır.")
            if min_lat > max_lat or min_lon > max_lon:
                raise ValueError("Bbox koordinatları doğru sırada olmalıdır (min_lat, max_lat, min_lon, max_lon).")
        return self

    @model_validator(mode='after')
    def check_vs30(self):
        if self.min_vs30 is not None and self.max_vs30 is not None:
            if self.min_vs30 > self.max_vs30:
                raise ValueError("Minimum VS30 maksimum VS30'dan büyük olamaz.")
            if self.min_vs30 < 0 or self.max_vs30 > 3000:
                raise ValueError("VS30 değerleri 0-3000 m/s aralığında olmalıdır.")
        return self

    @model_validator(mode='after')
    def check_mechanisms(self):
        if self.mechanisms:
            valid_mechanisms = set(MECHANISM_MAP.values())
            for mechanism in self.mechanisms:
                if mechanism not in valid_mechanisms:
                    raise ValueError(f"Geçersiz mekanizma: {mechanism}. Geçerli mekanizmalar: {list(valid_mechanisms)}")
        return self

    @model_validator(mode='after')
    def check_distances(self):
        distance_fields = [
            ('min_Repi', 'max_Repi'), ('min_Rhyp', 'max_Rhyp'),
            ('min_Rjb', 'max_Rjb'), ('min_Rrup', 'max_Rrup')
        ]
        
        for min_field, max_field in distance_fields:
            min_val = getattr(self, min_field, None)
            max_val = getattr(self, max_field, None)
            
            if min_val is not None and max_val is not None and min_val > max_val:
                raise ValueError(f"{min_field} {max_field}'den büyük olamaz.")
            if min_val is not None and min_val < 0:
                raise ValueError(f"{min_field} negatif olamaz.")
        return self

    @model_validator(mode='after')
    def check_depths(self):
        if self.min_depth is not None and self.max_depth is not None:
            if self.min_depth > self.max_depth:
                raise ValueError("Minimum derinlik maksimum derinlikten büyük olamaz.")
            if self.min_depth < 0 or self.max_depth > 700:
                raise ValueError("Derinlik değerleri 0-700 km aralığında olmalıdır.")
        return self

    @model_validator(mode='after')
    def check_pga_pgv_pgd(self):
        if self.min_pga is not None and self.max_pga is not None:
            if self.min_pga > self.max_pga:
                raise ValueError("Minimum PGA maksimum PGA'dan büyük olamaz.")
            if self.min_pga < 0 or self.max_pga > 10000:
                raise ValueError("PGA değerleri 0-10000 cm/s² aralığında olmalıdır.")
        
        if self.min_pgv is not None and self.max_pgv is not None:
            if self.min_pgv > self.max_pgv:
                raise ValueError("Minimum PGV maksimum PGV'den büyük olamaz.")
            if self.min_pgv < 0 or self.max_pgv > 1000:
                raise ValueError("PGV değerleri 0-1000 cm/s aralığında olmalıdır.")
        
        if self.min_pgd is not None and self.max_pgd is not None:
            if self.min_pgd > self.max_pgd:
                raise ValueError("Minimum PGD maksimum PGD'den büyük olamaz.")
            if self.min_pgd < 0 or self.max_pgd > 1000:
                raise ValueError("PGD değerleri 0-1000 cm aralığında olmalıdır.")
        return self

    @model_validator(mode='after')
    def check_circle_search(self):
        if (self.circleLatitude is not None or self.circleLongitude is not None or self.circleRadius is not None):
            if self.circleLatitude is None or self.circleLongitude is None or self.circleRadius is None:
                raise ValueError("Dairesel arama için circleLatitude, circleLongitude ve circleRadius birlikte sağlanmalıdır.")
            if not (-90 <= self.circleLatitude <= 90):
                raise ValueError("circleLatitude -90 ile 90 arasında olmalıdır.")
            if not (-180 <= self.circleLongitude <= 180):
                raise ValueError("circleLongitude -180 ile 180 arasında olmalıdır.")
            if self.circleRadius < 0:
                raise ValueError("circleRadius negatif olamaz.")
        return self

class ISelectionStrategy(Protocol):
    """Seçim stratejisi interface'i"""
    
    def select_and_score(self, df: pd.DataFrame, criteria: SearchCriteria) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Kayıtları seç ve puanla"""
        ...
        
    def get_name(self) -> str:
        """Strateji adı"""
        ...

class BaseSelectionStrategy(ISelectionStrategy, ABC):
    """Temel seçim stratejisi"""
    
    def __init__(self, config: SelectionConfig):
        self.config = config

    def _gaussian_score(self, value: float, target: float, sigma: float) -> float:
        """Çan Eğrisi (Gaussian) Puanlama Fonksiyonu. Hedef değere tam isabet = 1.0 puan. Uzaklaştıkça puan yumuşak bir şekilde düşer.
            Gaussian Formülü: e^(-(x-u)^2 / (2*sigma^2))
        Args:
            value (float): _description_
            target (float): _description_
            sigma (float): _description_

        Returns:
            float: _description_
        """
        if value is None or target is None or pd.isna(value):
            return 0.0
        # Çan eğrisi formülü
        diff = value - target
        return math.exp(- (diff * diff) / (2 * sigma * sigma))

    def _categorical_score(self, record_val: str, target_list: list) -> float:
        """Metinsel eşleşme puanı (Mekanizma vb için)"""
        if not record_val or not target_list:
            return 0.0
        
        record_val_str = str(record_val)
        # Tam eşleşme
        if any(t == record_val_str for t in target_list):
            return 1.0
        # Kısmi eşleşme (Örn: "Reverse" arıyoruz, kayıt "Reverse-Oblique")
        if any(t in record_val_str for t in target_list):
            return 0.7
        return 0.0

    def _calculate_total_score(self, record: pd.Series, criteria: SearchCriteria) -> float:
        """
        DİNAMİK PUANLAMA MOTORU
        Config'deki tüm parametreleri tarar, kullanıcı ne girdiyse ona göre puanlar.
        """
        total_weighted_score = 0.0
        total_active_weight = 0.0
        
        # Config'deki tüm parametreler üzerinde dönüyoruz (Magnitude, Rjb, Rrup, Vs30...)
        for key, config in SCORING_MAP.items():
            
            # 1. Bu parametre için bir hedef (Target) var mı?
            # Kullanıcı target girmediyse veya min-max aralığı vermediyse bu parametreyi ELİMİNE ET.
            if key == 'mechanism':
                # Mekanizma özel durumu: liste boşsa geç
                if not criteria.mechanisms:
                    continue
                target_val = criteria.mechanisms
            else:
                target_val = criteria.get_effective_target(key)
                if target_val is None:
                    continue

            # 2. DataFrame'de bu veri var mı?
            col_name = config['column']
            if col_name not in record or pd.isna(record[col_name]):
                # Kullanıcı hedef istemiş ama veri setinde (örneğin PEER'de) bu kolon yoksa puanlamaya katma
                continue

            # 3. Ağırlığı al
            weight = criteria.weights.get_weight(key)
            if weight <= 0:
                continue

            # 4. Puanı Hesapla
            score = 0.0
            if config['type'] == 'numeric':
                sigma = criteria.get_sigma(key)
                score = self._gaussian_score(record[col_name], target_val, sigma)
            
            elif config['type'] == 'categorical':
                score = self._categorical_score(record[col_name], target_val)

            # 5. Toplama Ekle
            total_weighted_score += score * weight
            total_active_weight += weight

        # 6. Normalizasyon (0-100 arası)
        # Eğer hiçbir kriter girilmediyse 0 döndür
        if total_active_weight == 0:
            return 0.0
            
        return (total_weighted_score / total_active_weight) * 100.0
    
    def select_and_score(self, df: pd.DataFrame, criteria: SearchCriteria) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """ Kayıtları puanla ve seç. 

        Args:
            df (pd.DataFrame): Puanlanacak veri seti
            criteria (SearchCriteria): Kullanıcının girdiği arama kriterleri ve ağırlıklar

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Seçilen kayıtlar ve tüm kayıtların puanlı hali
        """
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()
        
        scored_df = df.copy()
        
        # Vektörize işlem yerine apply kullanıyoruz (karmaşık mantık için daha güvenli)
        # Performans gerekirse numpy ile vektörize edilebilir.
        scored_df['SCORE'] = scored_df.apply(
            lambda row: self._calculate_total_score(row, criteria), axis=1
        )
        
        selected_df = self._apply_selection_rules(scored_df)
        return selected_df, scored_df
    
    def _apply_selection_rules(self, df_scored: pd.DataFrame) -> pd.DataFrame:
        """Seçim kurallarını uygula"""
        filtered_df = df_scored[df_scored['SCORE'] >= self.config.min_score]
        if filtered_df.empty:
            return pd.DataFrame()
        
        sorted_df = filtered_df.sort_values('SCORE', ascending=False)
        selected_records = []
        station_counts = {}
        event_counts = {}
        
        for _, record in sorted_df.iterrows():
            if len(selected_records) >= self.config.num_records:
                break
            
            station = record.get('STATION', '')
            event = record.get('EVENT', '')
            
            if (station_counts.get(station, 0) >= self.config.max_per_station or 
                event_counts.get(event, 0) >= self.config.max_per_event):
                continue
            
            selected_records.append(record)
            station_counts[station] = station_counts.get(station, 0) + 1
            event_counts[event] = event_counts.get(event, 0) + 1
        
        return pd.DataFrame(selected_records)
        
    def get_name(self) -> str:
        return str(self.config.design_code.value)
class TBDYSelectionStrategy(BaseSelectionStrategy):
    """TBDY 2018 seçim stratejisi"""
    def get_name(self) -> str:
        return "TBDY_2018_Gaussian"
class EurocodeSelectionStrategy(BaseSelectionStrategy):
    """Eurocode 8 seçim stratejisi"""
    
    def _calculate_score(self, record: pd.Series, target_params: Dict[str, Any]) -> float:
        """Eurocode 8'e göre puan hesapla"""
        # Eurocode spesifik implementasyon
        return 0.0  # Implementasyon