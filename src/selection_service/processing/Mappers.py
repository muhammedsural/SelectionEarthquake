from abc import ABC
from math import atan2, cos, radians, sin, sqrt
from typing import Dict, Protocol, Type
import pandas as pd

from ..utility.path_utils import load_excel
from ..enums.Enums import ProviderName
from ..core.Config import STANDARD_COLUMNS, MECHANISM_MAP


class IColumnMapper(Protocol):
    """Kolon eşleme interface'i"""
    
    def map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Maps the columns of the given DataFrame to a standardized format.

        Args:
            df (pd.DataFrame): The input DataFrame whose columns need to be mapped.

        Returns:
            pd.DataFrame: A DataFrame with columns mapped to the standard format.
        """
        pass


class BaseColumnMapper(IColumnMapper, ABC):
    """Temel kolon eşleyici sınıfı"""

    def __init__(self, column_mappings: Dict[str, str], **kwargs):
        #
        self.column_mappings = column_mappings

    def map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Temel eşleme işlemi"""
        df = df.copy()
        df = df.rename(columns=self.column_mappings)
        return self._ensure_standard_columns(df)

    def _ensure_standard_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Eksik standart kolonları ekle"""
        for col in STANDARD_COLUMNS:
            if col not in df.columns:
                df[col] = None
        return df[STANDARD_COLUMNS]

# ==================== PROVIDER-SPECIFIC MAPPERS ====================


class AFADColumnMapper(BaseColumnMapper):
    """AFAD mapper"""
    
    def __init__(self, **kwargs):
        mappings = {
            "waveformId"                : "RSN"           ,
            "eventId"                   : "EVENT"         ,
            "mvalue"                    : "MAGNITUDE"     ,
            "mtype"                     : "MAGNITUDE_TYPE",
            "rjb"                       : "RJB(km)"       , 
            "rrup"                      : "RRUP(km)"      ,
            "repi"                      : "REPI(km)"      , 
            "rhyp"                      : "RHYP(km)"      ,   
            "relatedEarthquakeLatitude" : "HYPO_LAT"      ,   
            "relatedEarthquakeLongitude": "HYPO_LON"      ,
            "stationId"                 : "STATION_ID"    ,  
            "relatedStationLatitude"    : "STATION_LAT"   ,   
            "relatedStationLongitude"   : "STATION_LON"   ,    
            "pga"                       : "PGA(cm2/sec)"  ,    
            "pgv"                       : "PGV(cm/sec)"   ,    
            "pgd"                       : "PGD(cm)"       ,        
            "relatedStrike1"            : "STRIKE1"       ,      
            "relatedDip1"               : "DIP1"          ,     
            "relatedRake1"              : "RAKE1"         ,  
            "relatedStrike2"            : "STRIKE2"       ,   
            "relatedDip2"               : "DIP2"          ,
            "relatedRake2"              : "RAKE2"         ,
            "t90e"                      : "T90_E",
            "t90n"                      : "T90_N", 
            "t90u"                      : "T90_U"
        }
         
            # None                        :      "HYPO_DEPTH(km)",   
            # None                        :      "FAULT_NAME"    ,   
            # None                        :      "SLIP_RATE"     , 
            # None,                       :      "LOWFREQ(Hz)"   ,
            # "t90e"                      :      "T90_avg(sec)"  ,
            # "stationId"                 :      "SSN"           , 
            
        super().__init__(mappings)
        self.station_df = self._build_station_info_df()
    
    def map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """AFAD'a özel ek işlemler"""
        df = df.copy()
        
        # 1. YEAR dönüşümü
        if "eventDate" in df.columns:
            df["YEAR"] = df["eventDate"].str[:4].astype(int)
        
        # 2. Özel alanlar için işlemler
        df = self._handle_record_filenames(df)
        df = self._handle_station_infos(df)
        df = self._handle_mechanisms(df)
        df = self._handle_t90_duration(df)
        
        df = super().map_columns(df) #hali hazırdaki kolonların ismi değiştirilecek ve standart kolonlar sadece alınacak   
        return df

    def _handle_record_filenames(self, df: pd.DataFrame) -> pd.DataFrame:
        """Record filename'leri işle"""
        if "recordFilename" in df.columns:
            df["FILE_NAME_H1"] = df["recordFilename"]
            df["FILE_NAME_H2"] = df["recordFilename"] 
            df["FILE_NAME_V"] = df["recordFilename"]
        return df

    def _handle_station_infos(self, df: pd.DataFrame) -> pd.DataFrame:
        """İstasyon bilgilerini işle"""
        if "stationCode" in df.columns:
            # İstasyon kodlarını temizle
            df["stationCode"] = df["stationCode"].astype(str).str.strip()
            
            # Dict ile hızlı lookup
            vs30_map = dict(zip(self.station_df["Code"], self.station_df["Vs30"]))
            location_map = dict(zip(self.station_df["Code"], self.station_df["Location"]))
            # station_lat_map = dict(zip(self.station_df["Code"], self.station_df["Latitude"]))
            # station_lon_map = dict(zip(self.station_df["Code"], self.station_df["Longitude"]))
            # station_id_map  = dict(zip(self.station_df["Code"], self.station_df["Location"]))
            
            # Eşleme yap
            df["VS30(m/s)"] = df["stationCode"].map(vs30_map).fillna(0.0)
            df["STATION"] = df["stationCode"].map(location_map).fillna("")
            
            # SSN için stationId kullan
            # if "STATION_ID" in df.columns:
            #     df["SSN"] = df["STATION_ID"]
        
        return df

    def _handle_mechanisms(self, df: pd.DataFrame) -> pd.DataFrame:
        """Mekanizma bilgisini işle"""
        # Vektörize hesaplama için
        dip1 = df.get("relatedDip1", 0)
        rake1 = df.get("relatedRake1", 0) 
        dip2 = df.get("relatedDip2", 0)
        rake2 = df.get("relatedRake2", 0)
        
        # Her satır için mekanizma hesapla
        mechanisms = []
        for i in range(len(df)):
            mechanism = self._classify_fault_planes(
                dip1.iloc[i] if not pd.isna(dip1.iloc[i]) else 0,
                rake1.iloc[i] if not pd.isna(rake1.iloc[i]) else 0,
                dip2.iloc[i] if not pd.isna(dip2.iloc[i]) else 0, 
                rake2.iloc[i] if not pd.isna(rake2.iloc[i]) else 0
            )
            mechanisms.append(mechanism)
        
        df["MECHANISM"] = mechanisms
        return df

    def _handle_t90_duration(self, df: pd.DataFrame) -> pd.DataFrame:
        """T90 sürelerini işle"""
        # t90_cols = ["T90_E", "T90_N", "T90_U"]
        t90_cols = ["t90e", "t90n", "t90u"]
        if all(col in df.columns for col in t90_cols):
            # Ortalama hesapla
            df["T90_avg(sec)"] = df[t90_cols].mean(axis=1)
            # Opsiyonel: Individual kolonları temizle
            # df = df.drop(columns=t90_cols, errors='ignore')
        
        return df

    # ------- STATION UTILITY FUNCTIONS ------------
    def _haversine(self, lat1, lon1, lat2, lon2):
        """
        İki nokta arasındaki mesafeyi km cinsinden döndürür (Haversine formülü).
        """
        R = 6371.0  # Dünya yarıçapı (km)

        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))

        return R * c

    # AFADDataProvider'da istasyon eşleme iyileştirmesi
    def _build_station_info_df(self, max_distance_km: float = 30.0) -> pd.DataFrame:
        """Daha hızlı istasyon bilgisi yükleme"""
        try:
            df = load_excel("stations.xlsx")
            df["Code"] = df["Code"].astype(str).str.strip()
            
            # Eksik Vs30'ları doldurma - vektörize versiyon
            missing_mask = (df["Vs30"].isna()) | (df["Vs30"] == 0)
            if missing_mask.any():
                valid_stations = df[~missing_mask]
                if not valid_stations.empty:
                    # KDTree ile en yakın komşu arama (çok daha hızlı)
                    from scipy.spatial import KDTree
                    
                    valid_coords = valid_stations[['Latitude', 'Longitude']].values
                    tree = KDTree(valid_coords)
                    
                    missing_coords = df.loc[missing_mask, ['Latitude', 'Longitude']].values
                    distances, indices = tree.query(missing_coords, k=1)
                    
                    for i, (idx, distance) in enumerate(zip(missing_mask[missing_mask].index, distances)):
                        if distance <= max_distance_km:
                            df.loc[idx, 'Vs30'] = valid_stations.iloc[indices[i]]['Vs30']
                        else:
                            df.loc[idx, 'Vs30'] = 0.0
            return df
            
        except Exception as e:
            print(f"İstasyon dosyası yükleme hatası: {e}")
            return pd.DataFrame()    

    # ------- FAULT UTILITY FUNCTIONS ------------

    def _classify_fault_type(self, dip: float, rake: float) -> str:
        """
        Strike, dip ve rake değerlerine göre fay türünü sınıflandırır.
        Basit kurallar: 
        - Rake ~ 0° veya 180°  -> doğrultu atım (strike-slip)
        - Rake ~ +90°          -> ters fay (reverse)
        - Rake ~ -90°          -> normal fay (normal)
        - Aradaki açılar       -> oblik fay (oblique)
        - Eğer ±60 <= rake <= ±120 civarı ama dip < 30° → oblique olarak kabul ediyoruz.
        
        Args:
            ###strike (float): Fayın doğrultu açısı (0-360)
            dip (float): Fayın eğim açısı (0-90)
            rake (float): Fayın kayma açısı (-180, 180)
        
        Returns:
            str: "StrikeSlip" | "Normal" | "Reverse"| "Reverse/Oblique"| "Normal/Oblique" | "Oblique"
        """
        
        if pd.isna(dip) or pd.isna(rake):
            return "Unknown"
        
        rake_norm = ((rake + 180) % 360) - 180  # -180..180 normalize et
        dip = float(dip)

        if -30 <= rake_norm <= 30 or 150 <= abs(rake_norm) <= 180:
            return MECHANISM_MAP[0]
        elif 60 <= rake_norm <= 120:
            return MECHANISM_MAP[2] if dip >= 30 else MECHANISM_MAP[3]
        elif -120 <= rake_norm <= -60:
            return MECHANISM_MAP[1] if dip >= 30 else MECHANISM_MAP[4]
        else:
            return MECHANISM_MAP[5]

    def _classify_fault_planes(self, dip1, rake1, dip2, rake2) -> str:
        """
        İki fay düzlemi için sınıflandırma yapar.
        Eğer aynı türse tek değer, farklı türse birleştirilmiş değer döner.
        """
        f1 = self._classify_fault_type(dip1, rake1)
        f2 = self._classify_fault_type(dip2, rake2)

        return f1 if f1 == f2 else f"{f1}-{f2}"


class PEERColumnMapper(BaseColumnMapper):
    """PEER kolon eşleyici"""
    
    def __init__(self, **kwargs):
        mappings = {
            "Record Sequence Number"                    : "RSN",
            "Earthquake Name"                           : "EVENT",
            "YEAR"                                      : "YEAR", 
            "Earthquake Magnitude"                      : "MAGNITUDE",
            "Magnitude Type"                            : "MAGNITUDE_TYPE",
            "Station Name"                              : "STATION",
            "Station Sequence Number"                   : "SSN",
            "Station ID  No."                           : "STATION_ID",
            "Station Latitude"                          : "STATIN_LAT",
            "Station Longitude"                         : "STATIN_LON",
            "Vs30 (m/s) selected for analysis"          : "VS30(m/s)",
            "Strike (deg)"                              : "STRIKE1",
            "Dip (deg)"                                 : "DIP1",
            "Rake Angle (deg)"                          : "RAKE1",
            "Mechanism Based on Rake Angle"             : "MECHANISM",
            "EpiD (km)"                                 : "EPICENTER_DEPTH(km)",
            "HypD (km)"                                 : "HYPOCENTER_DEPTH(km)",
            "Joyner-Boore Dist. (km)"                   : "RJB(km)",
            "ClstD (km)"                                : "RRUP(km)",
            "Hypocenter Latitude (deg)"                 : "HYPO_LAT",
            "Hypocenter Longitude (deg)"                : "HYPO_LON",
            "Hypocenter Depth (km)"                     : "HYPO_DEPTH(km)",
            "Lowest Usable Freq - Ave. Component (Hz)"  : "LOWFREQ(Hz)",
            "File Name (Horizontal 1)"                  : "FILE_NAME_H1",
            "File Name (Horizontal 2)"                  : "FILE_NAME_H2",
            "File Name (Vertical)"                      : "FILE_NAME_V",
            "PGA(g)"                                    : "PGA(cm2/sec)",
            "PGV (cm/sec)"                              : "PGV(cm/sec)",
            "PGD (cm)"                                  : "PGD(cm)",
            "5-95%Duration(sec)"                        : "T90(sec)", 
            "AriasIntensity(m/sec)"                     : "ARIAS_INTENSITY(m/sec)"
        }
        super().__init__(mappings)
    
    def map_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """PEER'a özel ek işlemler"""
        df = df.copy()
        df = super().map_columns(df)
        
        # PGA birim dönüşümü (g → cm/s²)
        if "PGA(cm2/sec)" in df.columns:
            df["PGA(cm2/sec)"] = df["PGA(cm2/sec)"] * 980.665
        
        return df

 # ==================== MAPPER FACTORY ====================


class FDSNColumnMapper(BaseColumnMapper):
    """FDSN kolon eşleyici"""

    def __init__(self, **kwargs):
        mappings = {
            # FDSN spesifik kolon eşlemeleri buraya eklenebilir
            "Record Sequence Number"                    : "RSN",
            "Earthquake Name"                           : "EVENT",
            "YEAR"                                      : "YEAR", 
            "Earthquake Magnitude"                      : "MAGNITUDE",
            "Magnitude Type"                            : "MAGNITUDE_TYPE",
            "Station Name"                              : "STATION",
            "Station Sequence Number"                   : "SSN",
            "Station ID  No."                           : "STATION_ID",
            "Station Latitude"                          : "STATIN_LAT",
            "Station Longitude"                         : "STATIN_LON",
            "Vs30 (m/s) selected for analysis"          : "VS30(m/s)",
            "Strike (deg)"                              : "STRIKE1",
            "Dip (deg)"                                 : "DIP1",
            "Rake Angle (deg)"                          : "RAKE1",
            "Mechanism Based on Rake Angle"             : "MECHANISM",
            "EpiD (km)"                                 : "EPICENTER_DEPTH(km)",
            "HypD (km)"                                 : "HYPOCENTER_DEPTH(km)",
            "Joyner-Boore Dist. (km)"                   : "RJB(km)",
            "ClstD (km)"                                : "RRUP(km)",
            "Hypocenter Latitude (deg)"                 : "HYPO_LAT",
            "Hypocenter Longitude (deg)"                : "HYPO_LON",
            "Hypocenter Depth (km)"                     : "HYPO_DEPTH(km)",
            "Lowest Usable Freq - Ave. Component (Hz)"  : "LOWFREQ(Hz)",
            "File Name (Horizontal 1)"                  : "FILE_NAME_H1",
            "File Name (Horizontal 2)"                  : "FILE_NAME_H2",
            "File Name (Vertical)"                      : "FILE_NAME_V",
            "PGA(g)"                                    : "PGA(cm2/sec)",
            "PGV (cm/sec)"                              : "PGV(cm/sec)",
            "PGD (cm)"                                  : "PGD(cm)",
            "5-95%Duration(sec)"                        : "T90(sec)", 
            "AriasIntensity(m/sec)"                     : "ARIAS_INTENSITY(m/sec)"
        }
        super().__init__(mappings)


class ColumnMapperFactory:
    """Kolon eşleyici factory sınıfı"""
    
    _mappers = {
        ProviderName.AFAD: AFADColumnMapper,
        ProviderName.PEER: PEERColumnMapper,
        ProviderName.FDSN: BaseColumnMapper,
    }
    
    @classmethod
    def get_mapper(cls, provider: ProviderName) -> IColumnMapper:
        """Provider'a göre uygun eşleyiciyi döndür"""
        mapper_class = cls._mappers.get(provider, BaseColumnMapper)
        return mapper_class()
    
    @classmethod
    def register_mapper(cls, provider: ProviderName, mapper_class: Type[IColumnMapper]):
        """Yeni eşleyici kaydet"""
        cls._mappers[provider] = mapper_class

    @classmethod
    def create_mapper(cls, provider_Name, **kwargs) -> IColumnMapper:
        if provider_Name == ProviderName.AFAD:
            return AFADColumnMapper(**kwargs)
        if provider_Name == ProviderName.PEER:
            return PEERColumnMapper(**kwargs)
        else:
            return BaseColumnMapper(column_mappings={}, **kwargs)
