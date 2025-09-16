from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Tuple
import pandas as pd
from ..enums.Enums import DesignCode
from ..core.Config import MECHANISM_MAP,REVERSE_MECHANISM_MAP, SCORE_RANGES_AND_WEIGHTS, get_mechanism_numeric



@dataclass
class SelectionConfig:
    """Seçim konfigürasyonu"""
    design_code: DesignCode
    num_records: int = 22
    max_per_station: int = 3
    max_per_event: int = 3
    min_score: float = 50.0
    required_components: List[str] = None

@dataclass  
class SearchCriteria:
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
    min_Repi: Optional[float] = None
    max_Repi: Optional[float] = None
    min_Rhyp: Optional[float] = None
    max_Rhyp: Optional[float] = None
    min_Rjb: Optional[float] = None
    max_Rjb: Optional[float] = None
    min_Rrup: Optional[float] = None
    max_Rrup: Optional[float] = None
    min_vs30: Optional[float] = None
    max_vs30: Optional[float] = None
    mechanisms: Optional[List[str]] = None
    bbox: Optional[Tuple[float, float, float, float]] = None
    region: Optional[str] = None

    def validate(self) -> None:
        """Kriterleri validate et"""
        CriteriaValidator.validate_search_criteria(self)
        
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
            "fromPgv"       : self.min_pgv,
            "toPgv"         : self.max_pgv,
            
            
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
        """FDSN standardına özel parametre dönüşümü"""
        params = {
            "starttime": self.start_date,
            "endtime": self.end_date,
            "minmagnitude": self.min_magnitude,
            "maxmagnitude": self.max_magnitude,
            "minradius": self.min_distance,
            "maxradius": self.max_distance,
            "format": "text"
        }
        
        if self.bbox:
            params["minlatitude"], params["maxlatitude"], params["minlongitude"], params["maxlongitude"] = self.bbox
            
        return params
    
@dataclass
class TargetParameters:
    """Hedef parametreler"""
    magnitude: float
    distance: float  
    vs30: float
    mechanism: Optional[str] = None
    pga: Optional[str] = None
    pgv: Optional[str] = None
    t90: Optional[str] = None
    

    def validate(self) -> None:
        """Parametreleri validate et"""
        CriteriaValidator.validate_target_parameters(self)

class ValidationError(Exception):
    """Özel validasyon hatası sınıfı"""
    pass

class CriteriaValidator:
    """Arama kriterleri validasyon sınıfı"""
    
    @staticmethod
    def validate_search_criteria(criteria: SearchCriteria) -> None:
        """SearchCriteria nesnesini validate et"""
        errors = []
        
        # Tarih validasyonu
        try:
            start_date = datetime.fromisoformat(criteria.start_date.replace('Z', '+00:00'))
            end_date = datetime.fromisoformat(criteria.end_date.replace('Z', '+00:00'))
            if start_date > end_date:
                errors.append("Başlangıç tarihi bitiş tarihinden sonra olamaz")
        except ValueError:
            errors.append("Geçersiz tarih formatı. ISO format (YYYY-MM-DD) kullanın")
        
        # Büyüklük validasyonu
        if criteria.min_magnitude is not None and criteria.max_magnitude is not None:
            if criteria.min_magnitude > criteria.max_magnitude:
                errors.append("Minimum büyüklük maksimum büyüklükten büyük olamaz")
            if criteria.min_magnitude < 0 or criteria.max_magnitude > 10:
                errors.append("Büyüklük değerleri 0-10 aralığında olmalıdır")
        
        # Mesafe validasyonu
        distance_fields = [
            ('min_Repi', 'max_Repi'), ('min_Rhyp', 'max_Rhyp'),
            ('min_Rjb', 'max_Rjb'), ('min_Rrup', 'max_Rrup')
        ]
        
        for min_field, max_field in distance_fields:
            min_val = getattr(criteria, min_field, None)
            max_val = getattr(criteria, max_field, None)
            
            if min_val is not None and max_val is not None and min_val > max_val:
                errors.append(f"{min_field} {max_field}'den büyük olamaz")
            if min_val is not None and min_val < 0:
                errors.append(f"{min_field} negatif olamaz")
        
        # Bbox validasyonu
        if criteria.bbox:
            min_lat, max_lat, min_lon, max_lon = criteria.bbox
            if not (-90 <= min_lat <= 90) or not (-90 <= max_lat <= 90):
                errors.append("Enlem değerleri -90 ile 90 arasında olmalıdır")
            if not (-180 <= min_lon <= 180) or not (-180 <= max_lon <= 180):
                errors.append("Boylam değerleri -180 ile 180 arasında olmalıdır")
            if min_lat > max_lat or min_lon > max_lon:
                errors.append("Bbox koordinatları doğru sırada olmalıdır (min_lat, max_lat, min_lon, max_lon)")
        
        # VS30 validasyonu
        if criteria.min_vs30 is not None and criteria.max_vs30 is not None:
            if criteria.min_vs30 > criteria.max_vs30:
                errors.append("Minimum VS30 maksimum VS30'dan büyük olamaz")
            if criteria.min_vs30 < 0 or criteria.max_vs30 > 3000:
                errors.append("VS30 değerleri 0-3000 m/s aralığında olmalıdır")
        
        # Mekanizma validasyonu
        if criteria.mechanisms:
            valid_mechanisms = set(MECHANISM_MAP.values())
            for mechanism in criteria.mechanisms:
                if mechanism not in valid_mechanisms:
                    errors.append(f"Geçersiz mekanizma: {mechanism}. Geçerli mekanizmalar: {list(valid_mechanisms)}")
        
        if errors:
            raise ValidationError(f"Validasyon hataları:\n" + "\n".join(f"- {error}" for error in errors))
    
    @staticmethod
    def validate_target_parameters(params: TargetParameters) -> None:
        """TargetParameters nesnesini validate et"""
        errors = []
        
        if params.magnitude <= 0 or params.magnitude > 10:
            errors.append("Hedef büyüklük 0-10 aralığında olmalıdır")
        
        if params.distance <= 0:
            errors.append("Hedef mesafe pozitif olmalıdır")
        
        if params.vs30 <= 0 or params.vs30 > 3000:
            errors.append("Hedef VS30 0-3000 m/s aralığında olmalıdır")
        
        # if params.mechanism and params.mechanism not in MECHANISM_MAP.values():
        #     errors.append(f"Geçersiz hedef mekanizma: {params.mechanism}")
            
        if params.mechanism:
            for count,value in enumerate(MECHANISM_MAP.values(),start=1) :
                if value in params.mechanism:
                    break
                if count == len(MECHANISM_MAP.values()):
                    errors.append(f"Geçersiz hedef mekanizma: {params.mechanism}")
        
        if errors:
            raise ValidationError(f"Hedef parametre validasyon hataları:\n" + "\n".join(f"- {error}" for error in errors))

class ISelectionStrategy(Protocol):
    """Seçim stratejisi interface'i"""
    
    def select_and_score(self, df: pd.DataFrame, target_params: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Kayıtları seç ve puanla"""
        ...
    
    def get_weights(self) -> Dict[str, float]:
        """Ağırlık katsayılarını getir"""
        ...
    
    def get_name(self) -> str:
        """Strateji adı"""
        ...

class BaseSelectionStrategy(ISelectionStrategy, ABC):
    """Temel seçim stratejisi"""
    
    def __init__(self, config: SelectionConfig):
        self.config = config
        self.parameters = SCORE_RANGES_AND_WEIGHTS
    
    @abstractmethod
    def _calculate_score(self, record: pd.Series, target_params: Dict[str, Any]) -> float:
        """Kayıt için puan hesapla"""
        pass
    
    def select_and_score(self, df: pd.DataFrame, target_params: Dict[str, Any]) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Kayıtları seç ve puanla"""
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()
        
        df_scored = df.copy()
        df_scored['SCORE'] = df_scored.apply(lambda row: self._calculate_score(row, target_params),axis=1)
        
        selected_df = self._apply_selection_rules(df_scored)
        return selected_df, df_scored
    
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
    
    def get_weights(self) -> Dict[str, float]:
        return self.parameters['weights']
    
    def get_name(self) -> str:
        return str(self.config.design_code.value)

    def _mechanism_score(self, record_mechanism: str, target_mechanism: str) -> float:
        """
        Mekanizma puanlaması:
        - Tam eşleşme: 1.0
        - Karma mekanizma ve hedef mekanizma içinde geçiyorsa: 0.7
        - Hiçbiri değilse: 0.0
        """
        if not record_mechanism or not target_mechanism:
            return 0.0
        if record_mechanism == target_mechanism:
            return 1.0
        if "-" in record_mechanism and target_mechanism in record_mechanism.split("-"):
            return 0.7
        return 0.0

    def _calculate_range_score(self, ratio: float, ranges: Dict[str, Tuple[float, float]]) -> float:
        """Aralıklara göre puan hesapla"""
        if ranges['very_good'][0] <= ratio <= ranges['very_good'][1]:
            return 1.0
        elif ranges['good'][0] <= ratio <= ranges['good'][1]:
            return 0.8
        elif ranges['acceptable'][0] <= ratio <= ranges['acceptable'][1]:
            return 0.6
        return 0.0
    
class TBDYSelectionStrategy(BaseSelectionStrategy):
    """TBDY 2018 seçim stratejisi"""
    # def __init__(self):
    #     config = SelectionConfig(design_code=DesignCode.TBDY_2018,
    #                       num_records=22,
    #                       max_per_station=3,
    #                       max_per_event=3,
    #                       min_score=55)
    #     super().__init__(config)
    def _calculate_score(self, record: pd.Series, target_params: Dict[str, Any]) -> float:
        """TBDY'ye göre puan hesapla"""
        score = 0.0
        total_weight = 0
        weights = self.get_weights() #ağırlıklandırma puanı çarpanlarını getirir.

        # Mechanism puanı
        if 'mechanism' in target_params and 'MECHANISM' in record:
            mech_score = 0
            for mech in target_params['mechanism']:
                dummy_score = self._mechanism_score(record['MECHANISM'], mech)
                mech_score = dummy_score if dummy_score > mech_score else mech_score
                
            score += mech_score * weights['mechanism_match']
            total_weight += weights['mechanism_match']
        
        # Magnitude puanı
        if 'magnitude' in target_params and 'MAGNITUDE' in record:
            mag_ratio = record['MAGNITUDE'] / target_params['magnitude']
            mag_score = self._calculate_range_score(mag_ratio, self.parameters['ranges']['magnitude'])
            score += mag_score * weights['magnitude_match']
            total_weight += weights['magnitude_match']
        
        # Mesafe puanı
        if 'distance' in target_params and 'RJB' in record:
            dist_ratio = record['RJB'] / target_params['distance']
            dist_score = self._calculate_range_score(dist_ratio, self.parameters['ranges']['distance'])
            score += dist_score * weights['distance_match']
            total_weight += weights['distance_match']
        
        # VS30 puanı
        if 'vs30' in target_params and 'VS30' in record:
            if target_params['vs30'] != None and record['VS30'] != None:
                vs30_ratio = record['VS30'] / target_params['vs30']
                vs30_score = self._calculate_range_score(vs30_ratio, self.parameters['ranges']['vs30'])
                score += vs30_score * weights['vs30_match']
                total_weight += weights['vs30_match']
            
        # kayıt puanı PGA(cm2/sec),PGV(cm/sec),T90_avg(sec)
        if 'pga' in target_params and 'PGA(cm2/sec)' in record:
            if target_params['pga'] != None and record['PGA(cm2/sec)'] != None:
                pga_ratio = record['PGA(cm2/sec)'] / target_params['pga']
                pga_score = self._calculate_range_score(pga_ratio, self.parameters['ranges']['pga'])
                score += pga_score * weights['pga_match']
                total_weight += weights['pga_match']
            
        if 'pgv' in target_params and 'PGV(cm/sec)' in record:
            if target_params['pgv'] != None and record['PGV(cm/sec)'] != None:
                pgv_ratio = record['PGV(cm/sec)'] / target_params['pgv']
                pgv_score = self._calculate_range_score(pgv_ratio, self.parameters['ranges']['pgv'])
                score += pgv_score * weights['pgv_match']
                total_weight += weights['pgv_match']
            
        if 't90' in target_params and 'T90_avg(sec)' in record:
            if target_params['t90'] != None and record['T90_avg(sec)'] != None:
                t90_ratio = record['T90_avg(sec)'] / target_params['t90']
                t90_score = self._calculate_range_score(t90_ratio, self.parameters['ranges']['t90'])
                score += t90_score * weights['t90_match']
                total_weight += weights['t90_match']
        
        return (score / total_weight) * 100 if total_weight > 0 else 0

class EurocodeSelectionStrategy(BaseSelectionStrategy):
    """Eurocode 8 seçim stratejisi"""
    
    def _calculate_score(self, record: pd.Series, target_params: Dict[str, Any]) -> float:
        """Eurocode 8'e göre puan hesapla"""
        # Eurocode spesifik implementasyon
        return 0.0  # Implementasyon
