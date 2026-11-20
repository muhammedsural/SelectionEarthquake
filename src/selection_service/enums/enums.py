from enum import Enum

class ProviderName(str, Enum):
    AFAD = "AFAD"
    PEER = "PEER"
    FDSN = "FDSN"
    ESM = "ESM"
    CESMD = "CESMD"
    NGAW3 = "NGAW3"

class DesignCode(str, Enum):
    TBDY_2018 = "TBDY_2018"
    # EUROCODE_8 = "EUROCODE_8"
    # ASCE_7_22 = "ASCE_7_22"
    # CUSTOM = "CUSTOM"
