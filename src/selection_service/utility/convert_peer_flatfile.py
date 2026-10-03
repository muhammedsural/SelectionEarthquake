# ==================== WEST2 FLATFİLE CONVERT TO SPECIAL FLATFILE ====================
from typing import Dict, Optional
import pandas as pd


MAPPINGS = {
    "Record Sequence Number"                    : "RSN",
    "Earthquake Name"                           : "EVENT",
    "YEAR"                                      : "YEAR",
    "Earthquake Magnitude"                      : "MAGNITUDE",
    "Magnitude Type"                            : "MAGNITUDE_TYPE",
    
    "Station Name"                              : "STATION",
    "Station Sequence Number"                   : "SSN",
    "Station ID  No."                           : "STATION_ID",
    "Station Latitude"                          : "STATION_LAT",
    "Station Longitude"                         : "STATION_LON",
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
    "PGA (g)"                                   : "PGA(cm2/sec)",
    "PGV (cm/sec)"                              : "PGV(cm/sec)",
    "PGD (cm)"                                  : "PGD(cm)"}

def type_changer(data : pd.DataFrame, date_columns : list[str], numeric_columns : list[str]) ->pd.DataFrame:
    # Tarih/saat dönüşümleri
    if date_columns != None:
        for col in date_columns:
            if col in data.columns:
                try:
                    data[col] = pd.to_datetime(data[col])
                except:
                    pass
    if date_columns != None:       
        # Numerik dönüşümler
        for col in numeric_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
    return data

def excel_to_csv(excel_path: str, save_path: str, mappings: Optional[Dict[str, str]] = None, numeric_columns : list[str] = None) -> pd.DataFrame:
    # Excel'i CSV'ye dönüştür
    df = pd.read_excel(excel_path)
    df = type_changer(df, date_columns=['DATE', 'TIME'], numeric_columns=numeric_columns)
    df.columns = [c.strip() for c in df.columns]
    
    if mappings:
        df = df.rename(columns=mappings)
    df = df[list(mappings.values())]
    # df.to_csv(save_path, index=False)
    return df
    
def convert_excel_to_csv() -> None:
    path = 'data'
    excel_path = path+'\\Updated_NGA_West2_Flatfile_RotD50_d030_public_version.xlsx'
    csv_path = path+'\\NGA-West2_flatfile.csv'
    peer_csv = path+'\\meta_data-R1.csv'
    not_numeric_columns = ['EVENT', 'STATION', 'MAGNITUDE_TYPE', 'FILE_NAME_H1', 'FILE_NAME_H2', 'FILE_NAME_V']
    numeric_columns = [v for k,v in MAPPINGS.items() if k not in not_numeric_columns]
    
    df2 = excel_to_csv(excel_path = excel_path, 
                 save_path=csv_path, 
                 mappings=MAPPINGS, 
                 numeric_columns=numeric_columns)

    df1 = pd.read_csv(peer_csv)

    #İlgili kolonlar birleştiriliyor
    df1['RecordSequenceNumber'] = df1['RecordSequenceNumber'].astype(str)
    df2['RSN'] = df2['RSN'].astype(str)

    # Gerekli kolonları seçip, RecordSequenceNumber'ı index yap
    meta = df1.set_index('RecordSequenceNumber')[["5-75%Duration(sec)", "5-95%Duration(sec)", "AriasIntensity(m/sec)"]]

    # df2'ye merge ile ekle
    df2 = df2.merge(
        meta,
        left_on='RSN',
        right_index=True,
        how='left'
    )
    df2.to_csv(csv_path, index=False)
    