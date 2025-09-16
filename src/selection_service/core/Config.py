import pandas as pd

SCORE_RANGES_AND_WEIGHTS = {
                            'ranges': {
                                'magnitude' : {'very_good': (0.95, 1.05), 'good': (0.9, 1.1), 'acceptable': (0.85, 1.15)},
                                'distance'  : {'very_good': (0.85, 1.15)  , 'good': (0.8, 1.2), 'acceptable': (0.75, 1.25)},
                                'vs30'      : {'very_good': (0.9, 1.1)  , 'good': (0.8, 1.2), 'acceptable': (0.7, 1.3)},
                                'pga'       : {'very_good': (0.9, 1.1)  , 'good': (0.8, 1.2), 'acceptable': (0.7, 1.3)},
                                'pgv'       : {'very_good': (0.9, 1.1)  , 'good': (0.8, 1.2), 'acceptable': (0.7, 1.3)},
                                't90'       : {'very_good': (0.9, 1.1)  , 'good': (0.8, 1.2), 'acceptable': (0.7, 1.3)},                    
                            },
                            'weights': {
                                'magnitude_match': 5.0, 
                                'distance_match': 4.5, 
                                'vs30_match': 4.0,
                                'pga_match': 3.5, 
                                'pgv_match': 3.0, 
                                't90_match': 3.0,
                                'mechanism_match': 3.0
                            }
                        }
STANDARD_COLUMNS = ["PROVIDER","RSN","EVENT", "YEAR", "MAGNITUDE", "MAGNITUDE_TYPE", 
                    "STATION","SSN","STATION_ID","STATION_LAT","STATION_LON","VS30(m/s)",
                    "STRIKE1","DIP1","RAKE1","MECHANISM",
                    "EPICENTER_DEPTH(km)","HYPOCENTER_DEPTH(km)","RJB(km)","RRUP(km)","HYPO_LAT","HYPO_LON","HYPO_DEPTH(km)",
                    "T90_avg(sec)","ARIAS_INTENSITY(m/sec)","LOWFREQ(Hz)","FILE_NAME_H1","FILE_NAME_H2","FILE_NAME_V","PGA(cm2/sec)","PGV(cm/sec)","PGD(cm)"]

MECHANISM_MAP = {
    0: 'StrikeSlip',
    1: 'Normal', 
    2: 'Reverse',
    3: 'Reverse/Oblique',
    4: 'Normal/Oblique',
    5: 'Oblique',
    -999: 'Unknown'
}
REVERSE_MECHANISM_MAP = {v: k for k, v in MECHANISM_MAP.items()}

 # ------- MECHANISM UTILITY FUNCTIONS ------------

def convert_mechanism_to_text(df: pd.DataFrame, mechanism_col: str = 'MECHANISM') -> pd.DataFrame:
    """Mekanizma sütununu sayısal değerlerden metin karşılıklarına dönüştür"""
    df = df.copy()
    df[mechanism_col] = df[mechanism_col].map(MECHANISM_MAP).fillna("Unknown")
    return df

def convert_mechanism_to_numeric(df: pd.DataFrame, mechanism_col: str = 'MECHANISM') -> pd.DataFrame:
    """Mekanizma sütununu metin değerlerinden sayısal karşılıklarına dönüştür"""
    df = df.copy()
    df[mechanism_col] = df[mechanism_col].map(REVERSE_MECHANISM_MAP).fillna(-999).astype(int)
    return df

def get_mechanism_text(numeric_value: int) -> str:
    """Sayısal mekanizma değerini metin karşılığına çevir"""
    return MECHANISM_MAP.get(numeric_value, 'Unknown')

def get_mechanism_numeric(text_value: str) -> int:
    """Metin mekanizma değerini sayısal karşılığına çevir"""
    return REVERSE_MECHANISM_MAP.get(text_value, -999)

