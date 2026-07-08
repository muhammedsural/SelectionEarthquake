import pandas as pd
# Config.py

# Puanlama Parametreleri Kayıt Defteri
# key: Kod içinde kullanacağımız kısa ad
# column: DataFrame içindeki gerçek kolon adı
# weight: Varsayılan ağırlık
# sigma_strictness: Gaussian eğrisinin darlığı (Yüksek değer = Daha katı puanlama)
# type: 'numeric' veya 'categorical'

SCORING_MAP = {
    'magnitude': {
        'column': 'MAGNITUDE',
        'weight': 5.0,
        'sigma_strictness': 4.0,
        'type': 'numeric'
    },
    'rjb': {
        'column': 'RJB(km)',
        'weight': 4.5,
        'sigma_strictness': 3.0,
        'type': 'numeric'
    },
    'rrup': {
        'column': 'RRUP(km)',
        'weight': 4.5,
        'sigma_strictness': 3.0,
        'type': 'numeric'
    },
    'repi': {
        'column': 'REPI(km)',
        'weight': 4.0,
        'sigma_strictness': 3.0,
        'type': 'numeric'
    },
    'vs30': {
        'column': 'VS30(m/s)',
        'weight': 4.0,
        'sigma_strictness': 5.0,
        'type': 'numeric'
    },
    'pga': {
        'column': 'PGA(cm2/sec)',
        'weight': 3.5,
        'sigma_strictness': 4.0,
        'type': 'numeric'
    },
    'pgv': {
        'column': 'PGV(cm/sec)',
        'weight': 3.0,
        'sigma_strictness': 4.0,
        'type': 'numeric'
    },
    'pgd': {
        'column': 'PGD(cm)',
        'weight': 2.5,
        'sigma_strictness': 3.0,
        'type': 'numeric'
    },
    't90': {
        'column': 'T90_avg(sec)',
        'weight': 3.0,
        'sigma_strictness': 3.0,
        'type': 'numeric'
    },
    'arias': {
        'column': 'ARIAS_INTENSITY(m/sec)',
        'weight': 2.0,
        'sigma_strictness': 3.0,
        'type': 'numeric'
    },
    'depth': {
        'column': 'HYPO_DEPTH(km)',
        'weight': 2.0,
        'sigma_strictness': 2.0,
        'type': 'numeric'
    },
    'mechanism': {
        'column': 'MECHANISM',
        'weight': 3.0,
        'type': 'categorical' # Kategorik eşleşme
    }
}

SCORING_PRESETS = {
    "balanced": {
        "description": (
            "Default preset. Magnitude, distance, site class, intensity, "
            "duration, depth, and mechanism all contribute to the score."
        ),
        "weights": {
            key: config["weight"]
            for key, config in SCORING_MAP.items()
            if "weight" in config
        },
    },
    "tbdy_2018_record_selection": {
        "description": (
            "TBDY 2018-oriented preset. Emphasizes magnitude, source-to-site "
            "distance, Vs30, and fault mechanism for record selection."
        ),
        "weights": {
            "magnitude": 6.0,
            "rjb": 5.0,
            "rrup": 5.0,
            "repi": 3.0,
            "vs30": 5.0,
            "pga": 3.0,
            "pgv": 2.5,
            "pgd": 1.5,
            "t90": 2.0,
            "arias": 1.5,
            "depth": 1.0,
            "mechanism": 4.0,
        },
    },
    "site_response": {
        "description": (
            "Site-response preset. Gives more influence to Vs30, duration, "
            "and intensity measures while keeping magnitude and distance active."
        ),
        "weights": {
            "magnitude": 3.0,
            "rjb": 3.0,
            "rrup": 3.0,
            "repi": 2.0,
            "vs30": 6.0,
            "pga": 4.0,
            "pgv": 4.0,
            "pgd": 3.0,
            "t90": 5.0,
            "arias": 4.0,
            "depth": 1.0,
            "mechanism": 2.0,
        },
    },
}

#BUG Peer provider için ['T90_avg(sec)', 'ENDPOINTSOURCE'] başlıkları tanımlı değil hata veriyor.
STANDARD_COLUMNS = ["PROVIDER","RSN","EVENT", "YEAR", "MAGNITUDE", "MAGNITUDE_TYPE", 
                    "STATION","SSN","STATION_ID","STATION_LAT","STATION_LON","VS30(m/s)",
                    "STRIKE1","DIP1","RAKE1","MECHANISM",
                    "ENDPOINTSOURCE",
                    "EPICENTER_DEPTH(km)","HYPOCENTER_DEPTH(km)","RJB(km)","RRUP(km)","HYPO_LAT","HYPO_LON","HYPO_DEPTH(km)",
                    "T90_avg(sec)","ARIAS_INTENSITY(m/sec)","LOWFREQ(Hz)","FILE_NAME_H1","FILE_NAME_H2","FILE_NAME_V","PGA(cm2/sec)","PGV(cm/sec)","PGD(cm)",]

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

