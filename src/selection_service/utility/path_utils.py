import pandas as pd
import importlib.resources as pkg_resources
from selection_service import data  # paket içinde data klasörü

def load_csv(filename: str) -> pd.DataFrame:
    """
    Paket içindeki data klasöründen CSV dosyası oku.
    """
    with pkg_resources.files(data).joinpath(filename).open("rb") as f:
        return pd.read_csv(f)

def load_excel(filename: str) -> pd.DataFrame:
    """
    Belirtilen paket içindeki xlsx dosyasını DataFrame olarak yükler.

    Args:
        filename (str): Kaynak dosya adı (örn: 'NGA-West2_flatfile.xlsx')

    Returns:
        pd.DataFrame: Excel içeriği
    """
    with pkg_resources.files(data).joinpath(filename).open('rb') as f:
        df = pd.read_excel(f)
    return df