"""Provider'lar arası tekrar eden deprem/kayıt tespiti."""

import pandas as pd
import pytest

from selection_service.core.pipeline import EarthquakePipeline, PipelineContext
from selection_service.enums.enums import DesignCode
from selection_service.processing.criteria import SearchCriteria, SelectionConfig
from selection_service.processing.dedup import (
    EVENT_GROUP_COLUMN,
    assign_event_groups,
    find_duplicate_records,
    haversine_km,
    valid_coordinates,
)
from selection_service.processing.result_handle import Result
from selection_service.processing.strategies import TBDY2018ConstraintStrategy
from unittest.mock import MagicMock


def frame(rows):
    cols = ["PROVIDER", "EVENT", "YEAR", "MAGNITUDE", "HYPO_LAT", "HYPO_LON",
            "STATION", "STATION_LAT", "STATION_LON"]
    return pd.DataFrame(rows, columns=cols)


KOCAELI = [
    ("PEER", "Kocaeli, Turkey", 1999, 7.51, 40.75, 29.99, "ARC", 40.82, 29.36),
    ("AFAD", "17-08-1999 Izmit", 1999, 7.40, 40.70, 29.95, "TK.ARC", 40.82, 29.36),
]


class TestHelpers:
    def test_haversine_one_degree_latitude(self):
        assert haversine_km(0, 0, 1, 0) == pytest.approx(111.2, abs=0.5)

    def test_valid_coordinates_rejects_placeholders(self):
        lat = pd.Series([40.0, -999.0, 0.0, None, 95.0])
        lon = pd.Series([29.0, -999.0, 0.0, 29.0, 29.0])
        assert valid_coordinates(lat, lon).tolist() == [True, False, False, False, False]


class TestEventGroups:
    def test_same_event_in_two_providers_is_grouped(self):
        groups = assign_event_groups(frame(KOCAELI))
        assert groups.nunique() == 1
        assert groups.iloc[0].startswith("G")

    def test_aftershock_in_same_provider_is_not_merged(self):
        rows = KOCAELI + [
            ("AFAD", "17-08-1999 aftershock", 1999, 5.0, 40.70, 29.90, "X", 41.0, 30.0)
        ]
        groups = assign_event_groups(frame(rows))
        assert groups.iloc[1] != groups.iloc[2]  # artçı ana şokla birleşmez
        assert groups.iloc[0] == groups.iloc[1]

    def test_different_year_or_far_events_stay_separate(self):
        rows = [
            ("PEER", "A", 1999, 7.0, 40.0, 29.0, "S", 40.0, 29.0),
            ("AFAD", "B", 2000, 7.0, 40.0, 29.0, "S", 40.0, 29.0),   # farklı yıl
            ("AFAD", "C", 1999, 7.0, 42.0, 35.0, "S", 40.0, 29.0),   # çok uzak
            ("AFAD", "D", 1999, 5.5, 40.0, 29.0, "S", 40.0, 29.0),   # büyüklük farkı
        ]
        groups = assign_event_groups(frame(rows))
        assert groups.nunique() == 4

    def test_events_without_valid_location_are_not_matched(self):
        rows = [
            ("PEER", "A", 1999, 7.0, -999.0, -999.0, "S", 40.0, 29.0),
            ("AFAD", "B", 1999, 7.0, 0.0, 0.0, "S", 40.0, 29.0),
        ]
        assert assign_event_groups(frame(rows)).nunique() == 2

    def test_single_provider_keeps_event_names(self):
        df = frame([KOCAELI[0]])
        assert assign_event_groups(df).iloc[0] == "PEER|Kocaeli, Turkey"


class TestDuplicateRecords:
    def test_same_station_in_other_provider_is_duplicate(self):
        df = frame(KOCAELI)
        df[EVENT_GROUP_COLUMN] = assign_event_groups(df)
        assert find_duplicate_records(df).tolist() == [False, True]  # ilk provider korunur

    def test_distant_station_is_kept(self):
        rows = [KOCAELI[0], KOCAELI[1][:7] + (41.5, 30.5)]
        df = frame(rows)
        df[EVENT_GROUP_COLUMN] = assign_event_groups(df)
        assert find_duplicate_records(df).tolist() == [False, False]

    def test_missing_station_coordinates_never_duplicate(self):
        rows = [KOCAELI[0][:7] + (-999.0, -999.0), KOCAELI[1][:7] + (-999.0, -999.0)]
        df = frame(rows)
        df[EVENT_GROUP_COLUMN] = assign_event_groups(df)
        assert not find_duplicate_records(df).any()


def _provider(name, df):
    p = MagicMock()
    p.get_name.return_value = name
    p.map_criteria.return_value = {}
    p.fetch_data_sync.return_value = Result.ok(df)
    return p


def _records(provider, event, n, lat, lon, base_station, **extra):
    rows = []
    for i in range(n):
        rows.append({
            "PROVIDER": provider, "RSN": i, "EVENT": event, "YEAR": 1999,
            "MAGNITUDE": 7.4, "HYPO_LAT": lat, "HYPO_LON": lon,
            "STATION": f"{base_station}{i}", "STATION_LAT": 40.0 + i * 0.1,
            "STATION_LON": 29.0 + i * 0.1, "VS30(m/s)": 350.0,
            "RJB(km)": 20.0, "MECHANISM": "StrikeSlip", **extra,
        })
    return pd.DataFrame(rows)


class TestPipelineIntegration:
    def _run(self, peer, afad, num_records=10):
        strategy = TBDY2018ConstraintStrategy(
            SelectionConfig(design_code=DesignCode.TBDY_2018, num_records=num_records,
                            max_per_event=3, max_per_station=10, min_score=0.0)
        )
        ctx = PipelineContext(
            providers=[_provider("PEER", peer), _provider("AFAD", afad)],
            strategy=strategy,
            search_criteria=SearchCriteria(
                start_date="1990-01-01", end_date="2025-01-01", target_magnitude=7.4
            ),
        )
        return EarthquakePipeline().execute_sync(ctx)

    def test_same_earthquake_from_two_providers_respects_event_limit(self):
        peer = _records("PEER", "Kocaeli, Turkey", 4, 40.75, 29.99, "P")
        afad = _records("AFAD", "17-08-1999 Izmit", 4, 40.70, 29.95, "A")
        # AFAD istasyonları farklı konumda: tekrar değil, ama aynı deprem
        afad["STATION_LAT"] += 0.5
        result = self._run(peer, afad)
        assert result.success
        selected = result.value.selected_df
        assert len(selected) == 3  # iki provider'a rağmen aynı depremden en fazla 3
        assert selected[EVENT_GROUP_COLUMN].nunique() == 1
        assert result.value.report["compliance"]["max_per_event_ok"] is True

    def test_duplicate_records_are_removed_from_selection(self):
        peer = _records("PEER", "Kocaeli, Turkey", 3, 40.75, 29.99, "P")
        afad = _records("AFAD", "17-08-1999 Izmit", 3, 40.70, 29.95, "A")  # aynı istasyon konumları
        result = self._run(peer, afad)
        value = result.value
        assert value.report["deduplication"]["duplicate_records_removed"] == 3
        assert set(value.selected_df["PROVIDER"]) == {"PEER"}
        assert any("Dedup" in line for line in value.logs)
        assert len(value.scored_df) == 3  # tekrarlar aday havuzuna hiç girmez


def _naive_duplicates(df, max_km=1.0):
    """Eski O(n²) uygulamanın referans hâli (eşdeğerlik testi için)."""
    dup = pd.Series(False, index=df.index)
    order = {p: n for n, p in enumerate(df["PROVIDER"].astype(str).unique())}
    for _, rows in df.groupby(EVENT_GROUP_COLUMN):
        rows = rows.assign(_r=rows["PROVIDER"].map(order)).sort_values("_r", kind="stable")
        kept = []
        for idx, row in rows.iterrows():
            is_dup = any(
                other["_r"] != row["_r"]
                and haversine_km(other["STATION_LAT"], other["STATION_LON"],
                                 row["STATION_LAT"], row["STATION_LON"]) <= max_km
                for other in kept
            )
            if is_dup:
                dup.at[idx] = True
            else:
                kept.append(row)
    return dup


class TestDuplicateRecordsVectorized:
    @pytest.mark.parametrize("seed", range(5))
    def test_matches_naive_implementation(self, seed):
        import numpy as np

        rng = np.random.default_rng(seed)
        frames = []
        for provider in ("PEER", "AFAD", "FDSN"):
            n = 40
            frames.append(pd.DataFrame({
                "PROVIDER": provider,
                # küçük bir alan: 1 km eşiği altında çok sayıda yakın istasyon
                "STATION_LAT": 40.0 + rng.random(n) * 0.05,
                "STATION_LON": 29.0 + rng.random(n) * 0.05,
            }))
        df = pd.concat(frames, ignore_index=True)
        df[EVENT_GROUP_COLUMN] = "G1"
        assert find_duplicate_records(df).tolist() == _naive_duplicates(df).tolist()

    def test_large_group_is_fast(self):
        import time

        import numpy as np

        rng = np.random.default_rng(0)
        n = 5000
        df = pd.concat([
            pd.DataFrame({"PROVIDER": p, "STATION_LAT": 40 + rng.random(n) * 3,
                          "STATION_LON": 28 + rng.random(n) * 3})
            for p in ("PEER", "AFAD")
        ], ignore_index=True)
        df[EVENT_GROUP_COLUMN] = "G1"
        start = time.perf_counter()
        find_duplicate_records(df)
        assert time.perf_counter() - start < 5.0  # eski uygulama ~20 sn


class TestEventGroupColumnAlwaysPresent:
    def test_single_provider_run_has_event_group(self):
        peer = _records("PEER", "Kocaeli, Turkey", 3, 40.75, 29.99, "P")
        strategy = TBDY2018ConstraintStrategy(
            SelectionConfig(design_code=DesignCode.TBDY_2018, num_records=5, min_score=0.0)
        )
        ctx = PipelineContext(
            providers=[_provider("PEER", peer)],
            strategy=strategy,
            search_criteria=SearchCriteria(start_date="1990-01-01", end_date="2025-01-01"),
        )
        result = EarthquakePipeline().execute_sync(ctx)
        assert set(result.value.selected_df[EVENT_GROUP_COLUMN]) == {"PEER|Kocaeli, Turkey"}
        assert result.value.report["deduplication"] == {
            "merged_event_groups": 0, "duplicate_records_removed": 0,
        }
