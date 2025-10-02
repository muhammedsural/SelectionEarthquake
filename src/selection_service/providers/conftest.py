import pytest
import pandas as pd
from unittest.mock import patch

from selection_service.processing.Selection import SearchCriteria

@pytest.fixture
def dummy_df():
    """Küçük sahte Peer flatfile DataFrame'i"""
    return pd.DataFrame({
        "MAGNITUDE": [5.5, 7.2, 6.0],
        "RJB(km)": [10, 30, 50],
        "RRUP(km)": [12, 40, 60],
        "VS30(m/s)": [200, 400, 800],
        "HYPO_DEPTH(km)": [5, 15, 25],
        "PGA(cm2/sec)": [100, 200, 300],
        "PGV(cm/sec)": [10, 20, 30],
        "PGD(cm)": [1, 2, 3],
        "MECHANISM": [1, 2, 3],
    })

@pytest.fixture
def dummy_mapper():
    """Kolon eşleyici sahte sınıf"""
    class DummyMapper:
        def map_columns(self, df):
            return df
    return DummyMapper()

@pytest.fixture
def empty_criteria():
    return SearchCriteria(start_date="2000-01-01", end_date="2025-01-01")

@pytest.fixture(autouse=True)
def mock_peer_csv(dummy_df):
    """
    Tüm testlerde PeerProvider içindeki load_csv fonksiyonunu mocklar.
    Böylece gerçek CSV okunmaz, hep dummy_df döner.
    """
    with patch("selection_service.providers.PeerProvider.load_csv", return_value=dummy_df.copy()):
        yield
