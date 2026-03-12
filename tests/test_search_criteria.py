"""
tests/test_search_criteria.py

SearchCriteria — tüm validator'lar ve parametre dönüşümleri için %100 coverage.
"""

import pytest
from pydantic import ValidationError

from selection_service.processing.Selection import SearchCriteria, ScoringWeights


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def base_criteria():
    return SearchCriteria(start_date="2000-01-01", end_date="2025-01-01")


@pytest.fixture
def full_criteria():
    return SearchCriteria(
        start_date="2000-01-01",
        end_date="2025-01-01",
        min_magnitude=6.0,
        max_magnitude=8.0,
        min_vs30=200.0,
        max_vs30=500.0,
        min_Rjb=0.0,
        max_Rjb=200.0,
        min_Rrup=0.0,
        max_Rrup=200.0,
        min_depth=0.0,
        max_depth=50.0,
        mechanisms=["StrikeSlip"],
    )


# ─────────────────────────────────────────────────────────────────────────────
# check_magnitudes validator
# ─────────────────────────────────────────────────────────────────────────────

class TestMagnitudeValidator:

    def test_valid_magnitude_range(self, full_criteria):
        assert full_criteria.min_magnitude == 6.0

    def test_min_greater_than_max_raises(self):
        with pytest.raises(ValidationError, match="büyüklük"):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_magnitude=8.0, max_magnitude=6.0)

    def test_magnitude_below_zero_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_magnitude=-1.0, max_magnitude=8.0)

    def test_magnitude_above_ten_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_magnitude=5.0, max_magnitude=11.0)

    def test_only_min_magnitude_valid(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01", min_magnitude=6.0)
        assert c.min_magnitude == 6.0
        assert c.max_magnitude is None

    def test_only_max_magnitude_valid(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01", max_magnitude=8.0)
        assert c.max_magnitude == 8.0


# ─────────────────────────────────────────────────────────────────────────────
# check_dates validator
# ─────────────────────────────────────────────────────────────────────────────

class TestDateValidator:

    def test_valid_dates(self, base_criteria):
        assert base_criteria.start_date == "2000-01-01"

    def test_start_after_end_raises(self):
        with pytest.raises(ValidationError, match="tarih"):
            SearchCriteria(start_date="2025-01-01", end_date="2000-01-01")

    def test_same_date_valid(self):
        c = SearchCriteria(start_date="2023-01-01", end_date="2023-01-01")
        assert c.start_date == c.end_date


# ─────────────────────────────────────────────────────────────────────────────
# check_vs30 validator
# ─────────────────────────────────────────────────────────────────────────────

class TestVs30Validator:

    def test_valid_vs30(self, full_criteria):
        assert full_criteria.min_vs30 == 200.0

    def test_min_greater_than_max_raises(self):
        with pytest.raises(ValidationError, match="VS30"):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_vs30=500.0, max_vs30=200.0)

    def test_vs30_below_zero_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_vs30=-10.0, max_vs30=500.0)

    def test_vs30_above_3000_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_vs30=100.0, max_vs30=3500.0)


# ─────────────────────────────────────────────────────────────────────────────
# check_mechanisms validator
# ─────────────────────────────────────────────────────────────────────────────

class TestMechanismValidator:

    def test_valid_mechanism(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           mechanisms=["StrikeSlip"])
        assert c.mechanisms == ["StrikeSlip"]

    def test_all_valid_mechanisms(self):
        for m in ["StrikeSlip", "Normal", "Reverse", "Oblique",
                  "Reverse/Oblique", "Normal/Oblique"]:
            c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                               mechanisms=[m])
            assert m in c.mechanisms

    def test_invalid_mechanism_raises(self):
        with pytest.raises(ValidationError, match="Geçersiz mekanizma"):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           mechanisms=["InvalidType"])

    def test_empty_mechanisms_valid(self, base_criteria):
        assert base_criteria.mechanisms == []


# ─────────────────────────────────────────────────────────────────────────────
# check_distances validator
# ─────────────────────────────────────────────────────────────────────────────

class TestDistanceValidator:

    def test_valid_rjb(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_Rjb=0.0, max_Rjb=200.0)
        assert c.max_Rjb == 200.0

    def test_min_rjb_greater_than_max_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_Rjb=200.0, max_Rjb=100.0)

    def test_negative_rjb_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_Rjb=-5.0, max_Rjb=100.0)

    def test_rrup_invalid_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_Rrup=150.0, max_Rrup=50.0)


# ─────────────────────────────────────────────────────────────────────────────
# check_depths validator
# ─────────────────────────────────────────────────────────────────────────────

class TestDepthValidator:

    def test_valid_depth(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_depth=0.0, max_depth=100.0)
        assert c.max_depth == 100.0

    def test_min_depth_greater_than_max_raises(self):
        with pytest.raises(ValidationError, match="derinlik"):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_depth=200.0, max_depth=100.0)

    def test_depth_below_zero_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_depth=-1.0, max_depth=100.0)

    def test_depth_above_700_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_depth=0.0, max_depth=800.0)


# ─────────────────────────────────────────────────────────────────────────────
# check_pga_pgv_pgd validator
# ─────────────────────────────────────────────────────────────────────────────

class TestPgaValidator:

    def test_valid_pga(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_pga=0.0, max_pga=500.0)
        assert c.max_pga == 500.0

    def test_invalid_pga_order_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_pga=500.0, max_pga=100.0)

    def test_invalid_pgv_order_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_pgv=200.0, max_pgv=50.0)

    def test_invalid_pgd_order_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_pgd=100.0, max_pgd=10.0)

    def test_pga_above_max_raises(self):
        with pytest.raises(ValidationError):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_pga=0.0, max_pga=99999.0)


# ─────────────────────────────────────────────────────────────────────────────
# check_bbox validator
# ─────────────────────────────────────────────────────────────────────────────

class TestBboxValidator:

    def test_valid_bbox(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           bbox=(36.0, 42.0, 26.0, 45.0))
        assert c.bbox is not None

    def test_invalid_lat_raises(self):
        with pytest.raises(ValidationError, match="Enlem"):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           bbox=(-95.0, 42.0, 26.0, 45.0))

    def test_invalid_lon_raises(self):
        with pytest.raises(ValidationError, match="Boylam"):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           bbox=(36.0, 42.0, 26.0, 200.0))

    def test_bbox_inverted_lat_raises(self):
        with pytest.raises(ValidationError, match="Bbox"):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           bbox=(42.0, 36.0, 26.0, 45.0))


# ─────────────────────────────────────────────────────────────────────────────
# check_circle_search validator
# ─────────────────────────────────────────────────────────────────────────────

class TestCircleSearchValidator:

    def test_valid_circle_search(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           circleLatitude=37.0, circleLongitude=35.0, circleRadius=100.0)
        assert c.circleRadius == 100.0

    def test_partial_circle_raises(self):
        with pytest.raises(ValidationError, match="birlikte"):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           circleLatitude=37.0, circleLongitude=35.0)

    def test_invalid_circle_lat_raises(self):
        with pytest.raises(ValidationError, match="circleLatitude"):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           circleLatitude=95.0, circleLongitude=35.0, circleRadius=100.0)

    def test_invalid_circle_lon_raises(self):
        with pytest.raises(ValidationError, match="circleLongitude"):
            SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           circleLatitude=37.0, circleLongitude=190.0, circleRadius=100.0)


# ─────────────────────────────────────────────────────────────────────────────
# get_effective_target ve get_sigma
# ─────────────────────────────────────────────────────────────────────────────

class TestEffectiveTarget:

    def test_min_max_midpoint(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_magnitude=6.0, max_magnitude=8.0)
        assert c.get_effective_target("magnitude") == pytest.approx(7.0)

    def test_only_min_returns_min(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_magnitude=6.0)
        assert c.get_effective_target("magnitude") == pytest.approx(6.0)

    def test_only_max_returns_max(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           max_magnitude=8.0)
        assert c.get_effective_target("magnitude") == pytest.approx(8.0)

    def test_no_value_returns_none(self, base_criteria):
        assert base_criteria.get_effective_target("magnitude") is None

    def test_explicit_target_takes_priority(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_magnitude=6.0, max_magnitude=8.0,
                           target_magnitude=7.5)
        assert c.get_effective_target("magnitude") == pytest.approx(7.5)


class TestGetSigma:

    def test_sigma_from_range(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_magnitude=6.0, max_magnitude=8.0)
        sigma = c.get_sigma("magnitude")
        # diff=2.0, strictness=4.0 → sigma=0.5
        assert sigma == pytest.approx(0.5)

    def test_sigma_fallback_from_target(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_magnitude=7.0)
        sigma = c.get_sigma("magnitude")
        # target=7.0 → sigma=0.7 (7.0 * 0.1)
        assert sigma == pytest.approx(0.7)

    def test_sigma_zero_diff_returns_one(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           min_magnitude=7.0, max_magnitude=7.0)
        sigma = c.get_sigma("magnitude")
        assert sigma == pytest.approx(1.0)

    def test_sigma_no_value_returns_one(self, base_criteria):
        sigma = base_criteria.get_sigma("magnitude")
        assert sigma == pytest.approx(1.0)


# ─────────────────────────────────────────────────────────────────────────────
# Parametre dönüşümleri
# ─────────────────────────────────────────────────────────────────────────────

class TestParamConversions:

    def test_to_afad_params_basic(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-09-05",
                           min_magnitude=7.0, max_magnitude=10.0,
                           min_vs30=300.0, max_vs30=400.0,
                           mechanisms=["StrikeSlip"])
        params = c.to_afad_params()
        assert params["startDate"] == "2000-01-01T00:00:00.000Z"
        assert params["endDate"] == "2025-09-05T23:59:59.999Z"
        assert params["fromMagnitude"] == 7.0
        assert params["faultType"] == "SS"

    def test_to_afad_params_none_filtered(self, base_criteria):
        """None değerler AFAD params'tan çıkarılmalı."""
        params = base_criteria.to_afad_params()
        assert all(v is not None for v in params.values())

    def test_to_afad_mechanisms_mapping(self):
        for mech, code in [("StrikeSlip", "SS"), ("Reverse", "R"), ("Normal", "N")]:
            c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                               mechanisms=[mech])
            assert c.to_afad_params()["faultType"] == code

    def test_to_peer_params_basic(self, full_criteria):
        params = full_criteria.to_peer_params()
        assert params["min_magnitude"] == 6.0
        assert params["max_magnitude"] == 8.0
        assert params["min_vs30"] == 200.0

    def test_to_peer_params_mechanisms_numeric(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           mechanisms=["StrikeSlip"])
        params = c.to_peer_params()
        assert "mechanisms" in params

    def test_to_fdsn_params_basic(self, base_criteria):
        params = base_criteria.to_fdsn_params()
        assert "starttime" in params
        assert "endtime" in params

    def test_to_fdsn_params_with_bbox(self):
        c = SearchCriteria(start_date="2000-01-01", end_date="2025-01-01",
                           bbox=(36.0, 42.0, 26.0, 45.0))
        params = c.to_fdsn_params()
        assert "minlatitude" in params
        assert "maxlatitude" in params


# ─────────────────────────────────────────────────────────────────────────────
# ScoringWeights
# ─────────────────────────────────────────────────────────────────────────────

class TestScoringWeights:

    def test_get_weight_known_key(self):
        w = ScoringWeights()
        assert w.get_weight("magnitude") > 0

    def test_get_weight_unknown_key(self):
        w = ScoringWeights()
        assert w.get_weight("nonexistent_key") == 0.0

    def test_custom_weight(self):
        w = ScoringWeights(magnitude=10.0)
        assert w.get_weight("magnitude") == pytest.approx(10.0)
