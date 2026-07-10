from abc import ABC, abstractmethod
from datetime import datetime
import math
from typing import Any, Dict, List, Optional, Protocol, Tuple
# from obspy import UTCDateTime
import pandas as pd
from pydantic import BaseModel, Field, model_validator
from ..enums.Enums import DesignCode
from ..core.Config import (
    MECHANISM_MAP,
    REVERSE_MECHANISM_MAP,
    SCORING_MAP,
    SCORING_PRESETS,
    get_mechanism_numeric,
)

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

    @classmethod
    def from_preset(cls, name: str) -> "ScoringWeights":
        """Documented scoring presets for repeatable selection profiles."""
        try:
            preset = SCORING_PRESETS[name]
        except KeyError as exc:
            available = ", ".join(sorted(SCORING_PRESETS))
            raise ValueError(f"Unknown scoring preset '{name}'. Available: {available}") from exc
        return cls(**preset["weights"])

    @classmethod
    def preset_descriptions(cls) -> Dict[str, str]:
        """Return user-facing descriptions for all built-in scoring presets."""
        return {
            name: preset["description"]
            for name, preset in SCORING_PRESETS.items()
        }
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
    def _get_range_value(self, prefix: str, key: str) -> Optional[float]:
        """Return min/max field values while preserving legacy distance casing."""
        aliases = {
            "rjb": "Rjb",
            "rrup": "Rrup",
            "repi": "Repi",
            "rhyp": "Rhyp",
        }
        value = getattr(self, f"{prefix}_{key}", None)
        if value is not None:
            return value
        alias = aliases.get(key)
        if alias is None:
            return None
        return getattr(self, f"{prefix}_{alias}", None)

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
        min_val = self._get_range_value("min", key)
        max_val = self._get_range_value("max", key)
        
        # Sadece aralık verildiyse ve target yoksa, aralık ortasını hedef al
        if min_val is not None and max_val is not None:
            return (min_val + max_val) / 2.0
            
        return min_val if min_val is not None else max_val

    def get_sigma(self, key: str) -> float:
        """Config dosyasından o parametre için belirlenen katılık (strictness) değerini kullanarak sigma hesaplar."""
        config = SCORING_MAP.get(key, {})
        strictness = config.get('sigma_strictness', 4.0)
        
        # Eğer kullanıcının bir aralığı varsa, aralığı baz al
        min_val = self._get_range_value("min", key)
        max_val = self._get_range_value("max", key)
        
        if min_val is not None and max_val is not None:
            diff = max_val - min_val
            return diff / strictness if diff > 0 else 1.0
            
        # Aralık yoksa, hedef değerin %10'u kadar bir sigma uydur (Fallback)
        target = self.get_effective_target(key)
        return (target * 0.1) if target else 1.0

    def scoring_preset_docs(self) -> Dict[str, str]:
        """Built-in scoring preset descriptions for UI/CLI documentation."""
        return ScoringWeights.preset_descriptions()

    def get_mechanism_targets(self) -> List[str]:
        """Return all requested fault mechanism labels from both public fields."""
        targets: List[str] = []
        if self.mechanisms:
            targets.extend(self.mechanisms)
        if self.fault_type:
            targets.append(self.fault_type)
        return list(dict.fromkeys(targets))
    
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
            
        mechanism_targets = self.get_mechanism_targets()
        if mechanism_targets:
            # AFAD fay mekanizması parametrelerine dönüşüm
            mechanism_map = {
                "StrikeSlip": "SS",
                "Reverse": "R",
                "Normal": "N",
                "Oblique": "T"
            }
            mechParams = [mechanism_map.get(m, m) for m in mechanism_targets]
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
            'mechanisms': self.get_mechanism_targets()
        }
        
        mechanism_targets = self.get_mechanism_targets()
        if mechanism_targets:
            params["mechanisms"] = [
                get_mechanism_numeric(m)
                for m in mechanism_targets
                if m in REVERSE_MECHANISM_MAP
            ]
            
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
        def fdsn_time(value: str, end_of_day: bool = False) -> str:
            """Keep timestamps intact and expand date-only values to UTC days."""
            if "T" in value:
                return value
            suffix = "T23:59:59.999Z" if end_of_day else "T00:00:00.000Z"
            return f"{value}{suffix}"

        params = {
            "starttime": fdsn_time(self.start_date),
            "endtime": fdsn_time(self.end_date, end_of_day=True),
            "minmagnitude": self.min_magnitude,
            "maxmagnitude": self.max_magnitude,
            "mindepth": self.min_depth,
            "maxdepth": self.max_depth,
        }
        
        if self.bbox:
            params["minlatitude"], params["maxlatitude"], params["minlongitude"], params["maxlongitude"] = self.bbox
            
        return {key: value for key, value in params.items() if value is not None}

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
        valid_mechanisms = set(MECHANISM_MAP.values())
        for mechanism in self.get_mechanism_targets():
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
        score, _ = self._calculate_score_breakdown(record, criteria)
        return score

    def _calculate_score_breakdown(
        self, record: pd.Series, criteria: SearchCriteria
    ) -> Tuple[float, List[Dict[str, Any]]]:
        """Return total score and criterion-level contribution details."""
        total_weighted_score = 0.0
        total_active_weight = 0.0
        breakdown: List[Dict[str, Any]] = []
        
        # Config'deki tüm parametreler üzerinde dönüyoruz (Magnitude, Rjb, Rrup, Vs30...)
        for key, config in SCORING_MAP.items():
            
            # 1. Bu parametre için bir hedef (Target) var mı?
            # Kullanıcı target girmediyse veya min-max aralığı vermediyse bu parametreyi ELİMİNE ET.
            if key == 'mechanism':
                # Mekanizma özel durumu: liste boşsa geç
                mechanism_targets = criteria.get_mechanism_targets()
                if not mechanism_targets:
                    continue
                target_val = mechanism_targets
            else:
                target_val = criteria.get_effective_target(key)
                if target_val is None:
                    continue

            # 2. DataFrame'de bu veri var mı?
            col_name = config['column']
            if col_name not in record or pd.isna(record[col_name]):
                # Kullanıcı hedef istemiş ama veri setinde (örneğin PEER'de) bu kolon yoksa puanlamaya katma
                breakdown.append({
                    "criterion": key,
                    "column": col_name,
                    "status": "missing",
                    "target": target_val,
                    "value": None,
                    "weight": criteria.weights.get_weight(key),
                    "raw_score": 0.0,
                    "weighted_score": 0.0,
                })
                continue

            # 3. Ağırlığı al
            weight = criteria.weights.get_weight(key)
            if weight <= 0:
                breakdown.append({
                    "criterion": key,
                    "column": col_name,
                    "status": "inactive_weight",
                    "target": target_val,
                    "value": record[col_name],
                    "weight": weight,
                    "raw_score": 0.0,
                    "weighted_score": 0.0,
                })
                continue

            # 4. Puanı Hesapla
            score = 0.0
            sigma = None
            if config['type'] == 'numeric':
                sigma = criteria.get_sigma(key)
                score = self._gaussian_score(record[col_name], target_val, sigma)
            
            elif config['type'] == 'categorical':
                score = self._categorical_score(record[col_name], target_val)

            # 5. Toplama Ekle
            total_weighted_score += score * weight
            total_active_weight += weight
            breakdown.append({
                "criterion": key,
                "column": col_name,
                "status": "active",
                "target": target_val,
                "value": record[col_name],
                "weight": weight,
                "sigma": sigma,
                "raw_score": score,
                "weighted_score": score * weight,
            })

        # 6. Normalizasyon (0-100 arası)
        # Eğer hiçbir kriter girilmediyse 0 döndür
        if total_active_weight == 0:
            return 0.0, breakdown
            
        return (total_weighted_score / total_active_weight) * 100.0, breakdown
    
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
        score_results = scored_df.apply(
            lambda row: self._calculate_score_breakdown(row, criteria), axis=1
        )
        scored_df['SCORE'] = score_results.apply(lambda item: item[0])
        scored_df['SCORE_BREAKDOWN'] = score_results.apply(lambda item: item[1])
        
        selected_df, scored_df = self._apply_selection_rules_with_reasons(scored_df)
        return selected_df, scored_df
    
    def _apply_selection_rules(self, df_scored: pd.DataFrame) -> pd.DataFrame:
        """Seçim kurallarını uygula"""
        selected, _ = self._apply_selection_rules_with_reasons(df_scored)
        return selected

    def _apply_selection_rules_with_reasons(
        self, df_scored: pd.DataFrame
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Apply TBDY selection limits and annotate every record with a reason."""
        df_scored = df_scored.copy()
        df_scored["SELECTION_STATUS"] = "not_evaluated"
        df_scored["SELECTION_REASON"] = ""

        filtered_df = df_scored[df_scored['SCORE'] >= self.config.min_score]
        if filtered_df.empty:
            df_scored.loc[:, "SELECTION_STATUS"] = "rejected"
            df_scored.loc[:, "SELECTION_REASON"] = (
                f"score_below_min_score:{self.config.min_score}"
            )
            return pd.DataFrame(), df_scored

        below_min_mask = df_scored["SCORE"] < self.config.min_score
        df_scored.loc[below_min_mask, "SELECTION_STATUS"] = "rejected"
        df_scored.loc[below_min_mask, "SELECTION_REASON"] = (
            f"score_below_min_score:{self.config.min_score}"
        )
        
        sorted_df = filtered_df.sort_values('SCORE', ascending=False)
        selected_records = []
        selected_indices = []
        station_counts = {}
        event_counts = {}
        
        for idx, record in sorted_df.iterrows():
            if len(selected_records) >= self.config.num_records:
                df_scored.at[idx, "SELECTION_STATUS"] = "rejected"
                df_scored.at[idx, "SELECTION_REASON"] = (
                    f"num_records_limit:{self.config.num_records}"
                )
                break
            
            station = record.get('STATION', '')
            event = record.get('EVENT', '')
            
            if station_counts.get(station, 0) >= self.config.max_per_station:
                df_scored.at[idx, "SELECTION_STATUS"] = "rejected"
                df_scored.at[idx, "SELECTION_REASON"] = (
                    f"max_per_station:{self.config.max_per_station}"
                )
                continue

            if event_counts.get(event, 0) >= self.config.max_per_event:
                df_scored.at[idx, "SELECTION_STATUS"] = "rejected"
                df_scored.at[idx, "SELECTION_REASON"] = (
                    f"max_per_event:{self.config.max_per_event}"
                )
                continue
            
            selected_records.append(record)
            selected_indices.append(idx)
            station_counts[station] = station_counts.get(station, 0) + 1
            event_counts[event] = event_counts.get(event, 0) + 1
        
        df_scored.loc[selected_indices, "SELECTION_STATUS"] = "selected"
        df_scored.loc[selected_indices, "SELECTION_REASON"] = "selected"

        remaining_mask = df_scored["SELECTION_STATUS"].eq("not_evaluated")
        df_scored.loc[remaining_mask, "SELECTION_STATUS"] = "rejected"
        df_scored.loc[remaining_mask, "SELECTION_REASON"] = (
            f"num_records_limit:{self.config.num_records}"
        )

        selected_df = df_scored.loc[selected_indices].copy()
        return selected_df, df_scored
        
    def get_name(self) -> str:
        return str(self.config.design_code.value)
class TBDYSelectionStrategy(BaseSelectionStrategy):
    """TBDY 2018 seçim stratejisi"""
    def get_name(self) -> str:
        return "TBDY_2018_Gaussian"

class TBDY2018ConstraintStrategy(BaseSelectionStrategy):
    """Constraint-first selection with explicit error metrics and diversity limits."""

    def get_name(self) -> str:
        return "TBDY_2018_Constraint"

    def select_and_score(
        self, df: pd.DataFrame, criteria: SearchCriteria
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()

        scored_df = df.copy()
        evaluation = scored_df.apply(
            lambda row: self._evaluate_record(row, criteria), axis=1
        )
        scored_df["HARD_FILTERS"] = evaluation.apply(lambda item: item["hard_filters"])
        scored_df["ERROR_METRICS"] = evaluation.apply(lambda item: item["error_metrics"])
        scored_df["ERROR_TOTAL"] = evaluation.apply(lambda item: item["error_total"])
        scored_df["SCORE"] = evaluation.apply(lambda item: item["fit_score"])
        scored_df["SCORE_BREAKDOWN"] = evaluation.apply(lambda item: item["error_metrics"])
        scored_df["SELECTION_STATUS"] = "not_evaluated"
        scored_df["SELECTION_REASON"] = ""
        self._add_strategy_columns(scored_df)

        failed_mask = scored_df["HARD_FILTERS"].apply(
            lambda filters: any(item["status"] == "failed" for item in filters)
        )
        scored_df.loc[failed_mask, "SELECTION_STATUS"] = "rejected"
        scored_df.loc[failed_mask, "SELECTION_REASON"] = scored_df.loc[
            failed_mask, "HARD_FILTERS"
        ].apply(self._format_filter_reasons)

        candidate_df = self._candidate_order(scored_df.loc[~failed_mask])
        selected_indices = self._select_diverse_candidates(candidate_df, scored_df)

        scored_df.loc[selected_indices, "SELECTION_STATUS"] = "selected"
        scored_df.loc[selected_indices, "SELECTION_REASON"] = "selected"

        remaining_mask = scored_df["SELECTION_STATUS"].eq("not_evaluated")
        scored_df.loc[remaining_mask, "SELECTION_STATUS"] = "rejected"
        scored_df.loc[remaining_mask, "SELECTION_REASON"] = (
            f"num_records_limit:{self.config.num_records}"
        )

        selected_df = scored_df.loc[selected_indices].copy()
        return selected_df, scored_df

    def _add_strategy_columns(self, scored_df: pd.DataFrame) -> None:
        """Hook for strategy-specific ranking columns."""

    def _candidate_order(self, candidate_df: pd.DataFrame) -> pd.DataFrame:
        """Return candidates in preferred selection order."""
        return candidate_df.sort_values(["ERROR_TOTAL", "SCORE"], ascending=[True, False])

    def _evaluate_record(
        self, record: pd.Series, criteria: SearchCriteria
    ) -> Dict[str, Any]:
        hard_filters = self._hard_filter_results(record, criteria)
        error_metrics = self._error_metrics(record, criteria)
        active_errors = [
            item["normalized_error"]
            for item in error_metrics
            if item["status"] in ("active", "missing")
        ]
        error_total = (
            sum(active_errors) / len(active_errors)
            if active_errors
            else float("inf")
        )
        fit_score = 0.0 if math.isinf(error_total) else 100.0 / (1.0 + error_total)
        return {
            "hard_filters": hard_filters,
            "error_metrics": error_metrics,
            "error_total": error_total,
            "fit_score": fit_score,
        }

    def _hard_filter_results(
        self, record: pd.Series, criteria: SearchCriteria
    ) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for key, config in SCORING_MAP.items():
            column = config["column"]
            if config["type"] == "numeric":
                min_val = criteria._get_range_value("min", key)
                max_val = criteria._get_range_value("max", key)
                if min_val is None and max_val is None:
                    continue
                value = record.get(column)
                status = "passed"
                reason = ""
                if value is None or pd.isna(value):
                    status = "failed"
                    reason = f"missing:{column}"
                elif min_val is not None and value < min_val:
                    status = "failed"
                    reason = f"{key}_below_min:{min_val}"
                elif max_val is not None and value > max_val:
                    status = "failed"
                    reason = f"{key}_above_max:{max_val}"
                results.append({
                    "criterion": key,
                    "column": column,
                    "status": status,
                    "value": None if value is None or pd.isna(value) else value,
                    "min": min_val,
                    "max": max_val,
                    "reason": reason,
                })
            elif key == "mechanism":
                targets = criteria.get_mechanism_targets()
                if not targets:
                    continue
                value = record.get(column)
                match = self._categorical_score(value, targets)
                status = "passed" if match > 0 else "failed"
                results.append({
                    "criterion": key,
                    "column": column,
                    "status": status,
                    "value": value,
                    "target": targets,
                    "reason": "" if status == "passed" else "mechanism_mismatch",
                })
        return results

    def _error_metrics(
        self, record: pd.Series, criteria: SearchCriteria
    ) -> List[Dict[str, Any]]:
        metrics: List[Dict[str, Any]] = []
        for key, config in SCORING_MAP.items():
            column = config["column"]
            if key == "mechanism":
                targets = criteria.get_mechanism_targets()
                if not targets:
                    continue
                value = record.get(column)
                match = self._categorical_score(value, targets)
                metrics.append({
                    "criterion": key,
                    "column": column,
                    "status": "active",
                    "target": targets,
                    "value": value,
                    "absolute_error": 0.0 if match > 0 else 1.0,
                    "normalized_error": 0.0 if match == 1.0 else 0.3 if match > 0 else 1.0,
                    "match": match,
                })
                continue

            target = criteria.get_effective_target(key)
            if target is None:
                continue
            value = record.get(column)
            if value is None or pd.isna(value):
                metrics.append({
                    "criterion": key,
                    "column": column,
                    "status": "missing",
                    "target": target,
                    "value": None,
                    "absolute_error": None,
                    "normalized_error": 1.0,
                })
                continue

            absolute_error = abs(float(value) - float(target))
            scale = self._error_scale(criteria, key, target)
            metrics.append({
                "criterion": key,
                "column": column,
                "status": "active",
                "target": target,
                "value": value,
                "absolute_error": absolute_error,
                "normalized_error": absolute_error / scale,
                "scale": scale,
            })
        return metrics

    def _error_scale(
        self, criteria: SearchCriteria, key: str, target: float
    ) -> float:
        min_val = criteria._get_range_value("min", key)
        max_val = criteria._get_range_value("max", key)
        if min_val is not None and max_val is not None and max_val > min_val:
            return max_val - min_val
        return max(abs(float(target)) * 0.1, 1.0)

    def _format_filter_reasons(self, filters: List[Dict[str, Any]]) -> str:
        reasons = [
            item["reason"]
            for item in filters
            if item["status"] == "failed" and item.get("reason")
        ]
        return ";".join(reasons) if reasons else "hard_filter_failed"

    def _select_diverse_candidates(
        self, candidate_df: pd.DataFrame, scored_df: pd.DataFrame
    ) -> List[Any]:
        selected_indices: List[Any] = []
        station_counts: Dict[Any, int] = {}
        event_counts: Dict[Any, int] = {}

        for idx, record in candidate_df.iterrows():
            if len(selected_indices) >= self.config.num_records:
                scored_df.at[idx, "SELECTION_STATUS"] = "rejected"
                scored_df.at[idx, "SELECTION_REASON"] = (
                    f"num_records_limit:{self.config.num_records}"
                )
                continue

            station = record.get("STATION", "")
            event = record.get("EVENT", "")
            if station_counts.get(station, 0) >= self.config.max_per_station:
                scored_df.at[idx, "SELECTION_STATUS"] = "rejected"
                scored_df.at[idx, "SELECTION_REASON"] = (
                    f"max_per_station:{self.config.max_per_station}"
                )
                continue
            if event_counts.get(event, 0) >= self.config.max_per_event:
                scored_df.at[idx, "SELECTION_STATUS"] = "rejected"
                scored_df.at[idx, "SELECTION_REASON"] = (
                    f"max_per_event:{self.config.max_per_event}"
                )
                continue

            selected_indices.append(idx)
            station_counts[station] = station_counts.get(station, 0) + 1
            event_counts[event] = event_counts.get(event, 0) + 1

        return selected_indices


class ConstraintSelectionStrategy(TBDY2018ConstraintStrategy):
    """Backward-compatible name for TBDY2018ConstraintStrategy."""


class ParetoSelectionStrategy(TBDY2018ConstraintStrategy):
    """Select nondominated records before applying diversity limits."""

    def get_name(self) -> str:
        return "Pareto_Selection"

    def _add_strategy_columns(self, scored_df: pd.DataFrame) -> None:
        ranks = self._pareto_ranks(scored_df)
        scored_df["PARETO_RANK"] = scored_df.index.map(ranks)
        scored_df["PARETO_FRONT"] = scored_df["PARETO_RANK"].eq(0)

    def _candidate_order(self, candidate_df: pd.DataFrame) -> pd.DataFrame:
        return candidate_df.sort_values(
            ["PARETO_RANK", "ERROR_TOTAL", "SCORE"],
            ascending=[True, True, False],
        )

    def _pareto_ranks(self, df: pd.DataFrame) -> Dict[Any, int]:
        remaining = list(df.index)
        ranks: Dict[Any, int] = {}
        rank = 0
        while remaining:
            front = []
            for idx in remaining:
                row = df.loc[idx]
                dominated = any(
                    self._dominates(df.loc[other], row)
                    for other in remaining
                    if other != idx
                )
                if not dominated:
                    front.append(idx)
            for idx in front:
                ranks[idx] = rank
            remaining = [idx for idx in remaining if idx not in front]
            rank += 1
        return ranks

    def _dominates(self, left: pd.Series, right: pd.Series) -> bool:
        left_metrics = self._metric_vector(left)
        right_metrics = self._metric_vector(right)
        keys = set(left_metrics) | set(right_metrics)
        if not keys:
            return False
        left_values = [left_metrics.get(key, 1.0) for key in keys]
        right_values = [right_metrics.get(key, 1.0) for key in keys]
        return all(l <= r for l, r in zip(left_values, right_values)) and any(
            l < r for l, r in zip(left_values, right_values)
        )

    def _metric_vector(self, record: pd.Series) -> Dict[str, float]:
        metrics = record.get("ERROR_METRICS", [])
        return {
            item["criterion"]: float(item.get("normalized_error", 1.0))
            for item in metrics
            if item.get("status") in ("active", "missing")
        }


class SpectrumMatchStrategy(TBDY2018ConstraintStrategy):
    """Prioritize spectral/intensity proxy metrics when response spectra are absent."""

    spectral_criteria = ("pga", "pgv", "pgd", "arias", "t90")

    def get_name(self) -> str:
        return "Spectrum_Match"

    def _evaluate_record(
        self, record: pd.Series, criteria: SearchCriteria
    ) -> Dict[str, Any]:
        result = super()._evaluate_record(record, criteria)
        spectral_errors = [
            item["normalized_error"]
            for item in result["error_metrics"]
            if item.get("criterion") in self.spectral_criteria
            and item.get("status") in ("active", "missing")
        ]
        if spectral_errors:
            spectrum_error = sum(spectral_errors) / len(spectral_errors)
            result["spectrum_error"] = spectrum_error
            result["error_total"] = spectrum_error
            result["fit_score"] = 100.0 / (1.0 + spectrum_error)
        else:
            result["spectrum_error"] = result["error_total"]
        return result

    def _add_strategy_columns(self, scored_df: pd.DataFrame) -> None:
        scored_df["SPECTRUM_ERROR"] = scored_df["ERROR_METRICS"].apply(
            self._spectrum_error_from_metrics
        )

    def _candidate_order(self, candidate_df: pd.DataFrame) -> pd.DataFrame:
        return candidate_df.sort_values(
            ["SPECTRUM_ERROR", "ERROR_TOTAL", "SCORE"],
            ascending=[True, True, False],
        )

    def _spectrum_error_from_metrics(self, metrics: List[Dict[str, Any]]) -> float:
        errors = [
            item["normalized_error"]
            for item in metrics
            if item.get("criterion") in self.spectral_criteria
            and item.get("status") in ("active", "missing")
        ]
        return sum(errors) / len(errors) if errors else float("inf")
class EurocodeSelectionStrategy(BaseSelectionStrategy):
    """Eurocode 8 seçim stratejisi"""
    
    def _calculate_score(self, record: pd.Series, target_params: Dict[str, Any]) -> float:
        """Eurocode 8'e göre puan hesapla"""
        # Eurocode spesifik implementasyon
        return 0.0  # Implementasyon
