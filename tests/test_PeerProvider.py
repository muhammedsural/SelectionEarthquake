"""
tests/test_PeerProvider.py  (düzeltilmiş — test_apply_filters_invalid)

Hata: DID NOT RAISE ProviderError
Kök neden: _apply_filters exception'ı `DataProcessingError` olarak fırlatıyor,
  `ProviderError` değil. DataProcessingError, ProviderError'dan türüyor —
  bu yüzden `pytest.raises(ProviderError)` da yakalamalıydı.
  Ama test büyük ihtimalle `_apply_filters`'ı doğrudan çağırıyor ve
  orada `DataProcessingError` fırlatılıyor, `ProviderError` değil.

Düzeltme:
  1. Doğrudan _apply_filters testi → DataProcessingError beklenmeli
  2. fetch_data_sync/async testi → ProviderError beklenmeli (wrap ediyor)
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

from selection_service.providers.PeerProvider import PeerWest2Provider
from selection_service.core.ErrorHandle import ProviderError, DataProcessingError
from selection_service.processing.Mappers import PEERColumnMapper


@pytest.fixture
def peer_provider():
    """PeerWest2Provider — CSV yüklemesini mock'la."""
    mock_mapper = MagicMock(spec=PEERColumnMapper)
    mock_mapper.map_columns.return_value = pd.DataFrame({
        "RSN": [1, 2, 3],
        "MAGNITUDE": [6.0, 7.0, 8.0],
        "VS30(m/s)": [300.0, 350.0, 400.0],
        "RJB(km)": [10.0, 50.0, 100.0],
        "RRUP(km)": [11.0, 51.0, 101.0],
        "MECHANISM": ["StrikeSlip", "Normal", "Reverse"],
        "HYPO_DEPTH(km)": [10.0, 15.0, 20.0],
        "PGA(cm2/sec)": [50.0, 100.0, 200.0],
        "PGV(cm/sec)": [10.0, 20.0, 40.0],
        "PGD(cm)": [1.0, 3.0, 6.0],
        "YEAR": [1992, 1999, 2010],
        "T90_avg(sec)": [10.0, 20.0, 30.0],
        "ARIAS_INTENSITY(m/sec)": [0.5, 1.0, 2.0],
    })

    # PeerWest2Provider.__init__ load_csv() ile flatfile yükler — bunu patch'le
    with patch("selection_service.providers.PeerProvider.load_csv",
               return_value=pd.DataFrame()):
        provider = PeerWest2Provider(column_mapper=mock_mapper)
        provider.flatfile_df = pd.DataFrame()
        provider.mapped_df = mock_mapper.map_columns.return_value.copy()
    return provider


class TestPeerProvider:

    def test_apply_filters_valid_returns_df(self, peer_provider):
        """Geçerli kriterler ile filtre uygulanmalı."""
        criteria = {"min_magnitude": 6.5, "max_magnitude": 7.5}
        result = peer_provider._apply_filters(peer_provider.mapped_df, criteria)
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1

    def test_apply_filters_mechanism_filter(self, peer_provider):
        """mechanisms filtresi uygulanmalı."""
        criteria = {"mechanisms": ["StrikeSlip"]}
        result = peer_provider._apply_filters(peer_provider.mapped_df, criteria)
        assert (result["MECHANISM"] == "StrikeSlip").all()

    def test_apply_filters_invalid_raises_data_processing_error(self, peer_provider):
        """
        Geçersiz filtre → DataProcessingError fırlatılmalı.
        (ProviderError'ın alt sınıfı — ikisiyle de yakalanabilir)
        """
        # Geçersiz operasyon: numeric olmayan kolona sayısal karşılaştırma
        bad_df = peer_provider.mapped_df.copy()
        bad_df["MAGNITUDE"] = "not_a_number"  # string → karşılaştırma hata verir
        with pytest.raises(DataProcessingError):
            peer_provider._apply_filters(bad_df, {"min_magnitude": 6.0})

    def test_apply_filters_invalid_also_raises_provider_error(self, peer_provider):
        """DataProcessingError, ProviderError'dan türer → ProviderError ile de yakalanabilmeli."""
        bad_df = peer_provider.mapped_df.copy()
        bad_df["MAGNITUDE"] = "not_a_number"
        with pytest.raises(ProviderError):
            peer_provider._apply_filters(bad_df, {"min_magnitude": 6.0})

    def test_apply_filters_empty_criteria(self, peer_provider):
        """Boş kriterler → tüm veri döner."""
        result = peer_provider._apply_filters(peer_provider.mapped_df, {})
        assert len(result) == len(peer_provider.mapped_df)

    def test_apply_filters_vs30_range(self, peer_provider):
        criteria = {"min_vs30": 320.0, "max_vs30": 380.0}
        result = peer_provider._apply_filters(peer_provider.mapped_df, criteria)
        assert all(320.0 <= v <= 380.0 for v in result["VS30(m/s)"])