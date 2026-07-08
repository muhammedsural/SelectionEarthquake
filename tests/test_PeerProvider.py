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
from selection_service.processing.Selection import SearchCriteria


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

    def test_apply_filters_fault_type_filter(self, peer_provider):
        criteria = {"fault_type": "Reverse"}
        result = peer_provider._apply_filters(peer_provider.mapped_df, criteria)
        assert result["RSN"].tolist() == [3]

    def test_apply_filters_numeric_fault_type_matches_text_dataframe(self, peer_provider):
        criteria = {"mechanisms": [2]}
        result = peer_provider._apply_filters(peer_provider.mapped_df, criteria)
        assert result["RSN"].tolist() == [3]

    def test_apply_filters_text_fault_type_matches_numeric_dataframe(self, peer_provider):
        df = peer_provider.mapped_df.copy()
        df["MECHANISM"] = [0, 1, 2]
        criteria = {"fault_type": "Normal"}
        result = peer_provider._apply_filters(df, criteria)
        assert result["RSN"].tolist() == [2]

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

    @pytest.mark.parametrize(
        "criteria,expected_rsns",
        [
            (
                {"min_magnitude": 6.5, "max_magnitude": 7.5},
                [2],
            ),
            (
                {
                    "min_magnitude": 7.0,
                    "max_magnitude": 8.0,
                    "min_vs30": 390.0,
                    "max_vs30": 420.0,
                    "mechanisms": ["Reverse"],
                },
                [3],
            ),
            (
                {"max_Rjb": 50.0, "min_pga": 80.0},
                [2],
            ),
            (
                {"min_depth": 0.0, "max_depth": 15.0, "max_pgv": 20.0},
                [1, 2],
            ),
            (
                {"min_pgd": 5.0, "max_Rrup": 100.0},
                [],
            ),
        ],
    )
    def test_apply_filters_search_combinations(self, peer_provider, criteria, expected_rsns):
        result = peer_provider._apply_filters(peer_provider.mapped_df, criteria)
        assert result["RSN"].tolist() == expected_rsns

    def test_map_criteria_combines_searchcriteria_for_peer(self, peer_provider):
        criteria = SearchCriteria(
            start_date="1990-01-01",
            end_date="2010-12-31",
            min_magnitude=6.5,
            max_magnitude=8.0,
            min_vs30=300.0,
            max_vs30=400.0,
            min_Rjb=0.0,
            max_Rjb=100.0,
            mechanisms=["StrikeSlip", "Reverse"],
        )
        params = peer_provider.map_criteria(criteria)
        assert params["year_start"] == 1990
        assert params["year_end"] == 2010
        assert params["min_magnitude"] == 6.5
        assert params["max_Rjb"] == 100.0
        assert params["mechanisms"] == [0, 2]

    def test_map_criteria_uses_fault_type_for_peer(self, peer_provider):
        criteria = SearchCriteria(
            start_date="1990-01-01",
            end_date="2010-12-31",
            fault_type="Normal",
        )
        params = peer_provider.map_criteria(criteria)
        assert params["mechanisms"] == [1]

    def test_fetch_data_sync_with_combined_filters(self, peer_provider):
        peer_provider.flatfile_df = pd.DataFrame({"raw": [1, 2, 3]})
        criteria = {
            "min_magnitude": 6.5,
            "max_magnitude": 8.0,
            "min_vs30": 300.0,
            "max_vs30": 360.0,
            "mechanisms": ["Normal"],
        }
        result = peer_provider.fetch_data_sync(criteria)
        assert result.success
        assert result.value["RSN"].tolist() == [2]
        assert (result.value["PROVIDER"] == "PEER").all()
