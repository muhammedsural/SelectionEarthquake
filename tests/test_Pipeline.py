import asyncio
import time
import pytest
import pandas as pd
from unittest.mock import MagicMock, AsyncMock

from selection_service.core.Pipeline import (
    EarthquakePipeline, PipelineContext, PipelineResult, PipelineReporter,
)
from selection_service.core.ErrorHandle import NoDataError, PipelineError
from selection_service.processing.ResultHandle import Result
from selection_service.core.Config import STANDARD_COLUMNS


# ─── helpers ────────────────────────────────────────────────────────────────

def make_df(rows=2, with_endpoint=False) -> pd.DataFrame:
    records = []
    for i in range(rows):
        r = {c: None for c in STANDARD_COLUMNS}
        r.update({"RSN": i+1, "PROVIDER": "PEER", "EVENT": f"EQ_{i}",
                  "YEAR": 2000, "MAGNITUDE": 7.0+i*0.1, "SSN": i+100,
                  "STATION": f"ST{i}", "VS30(m/s)": 350.0,
                  "RJB(km)": 50.0+i, "RRUP(km)": 51.0+i,
                  "MECHANISM": "StrikeSlip", "PGA(cm2/sec)": 100.0,
                  "PGV(cm/sec)": 20.0, "T90_avg(sec)": 15.0,
                  "ENDPOINTSOURCE": f"https://x.com/{i}" if with_endpoint else None,
                  "FILE_NAME_H1": f"F{i}.AT2"})
        records.append(r)
    return pd.DataFrame(records)


def make_provider(name, df=None, fail=False, empty=False, raise_exc=False):
    p = MagicMock()
    p.get_name.return_value = name
    p.map_criteria.return_value = {}
    if raise_exc:
        p.fetch_data_sync.side_effect = RuntimeError(f"{name} crashed")
        p.fetch_data_async = AsyncMock(side_effect=RuntimeError(f"{name} crashed"))
    elif fail:
        p.fetch_data_sync.return_value = Result.fail(Exception(f"{name} failed"))
        p.fetch_data_async = AsyncMock(return_value=Result.fail(Exception(f"{name} failed")))
    elif empty:
        p.fetch_data_sync.return_value = Result.ok(pd.DataFrame())
        p.fetch_data_async = AsyncMock(return_value=Result.ok(pd.DataFrame()))
    else:
        data = df if df is not None else make_df()
        p.fetch_data_sync.return_value = Result.ok(data)
        p.fetch_data_async = AsyncMock(return_value=Result.ok(data))
    return p


def make_strategy(num=5, score=80.0):
    s = MagicMock()
    s.get_name.return_value = "MockStrategy"
    def _select(df, criteria):
        scored = df.copy(); scored["SCORE"] = score
        return scored.head(num), scored
    s.select_and_score.side_effect = _select
    return s


def make_criteria():
    c = MagicMock()
    c.mechanisms = ["StrikeSlip"]
    c.model_dump.return_value = {}
    return c


def ctx(providers=None, strategy=None, data=None):
    return PipelineContext(
        providers=providers or [],
        strategy=strategy or make_strategy(),
        search_criteria=make_criteria(),
        data=data,
    )


def run(coro):
    return asyncio.run(coro)


# ─── PipelineResult ──────────────────────────────────────────────────────────

class TestPipelineResult:
    def test_default_lists(self):
        r = PipelineResult(selected_df=pd.DataFrame(), scored_df=pd.DataFrame(),
                           report={}, execution_time=1.0)
        assert r.failed_providers == []
        assert r.logs == []


# ─── PipelineReporter ────────────────────────────────────────────────────────

class TestPipelineReporter:

    def _ctx_with(self, rows=2, scored_rows=None):
        c = ctx(providers=[make_provider("PEER")])
        c.selected_df = make_df(rows); c.selected_df["SCORE"] = 80.0
        c.scored_df = make_df(scored_rows or rows); c.scored_df["SCORE"] = 80.0
        return c

    def test_warning_selected_none(self):
        c = ctx(); c.selected_df = None; c.scored_df = None
        assert PipelineReporter().generate_report(c)["status"] == "warning"

    def test_warning_selected_empty(self):
        c = ctx(); c.selected_df = pd.DataFrame(); c.scored_df = pd.DataFrame()
        assert PipelineReporter().generate_report(c)["status"] == "warning"

    def test_success_all_keys(self):
        r = PipelineReporter().generate_report(self._ctx_with())
        assert r["status"] == "success"
        for k in (
            "selected_count",
            "total_considered",
            "strategy",
            "providers",
            "records",
            "statistics",
            "search_criteria",
            "selection_summary",
            "score_breakdown",
            "error_metrics",
        ):
            assert k in r

    def test_selected_count(self):
        assert PipelineReporter().generate_report(self._ctx_with(3))["selected_count"] == 3

    def test_total_considered_from_scored(self):
        c = self._ctx_with(2, scored_rows=7)
        assert PipelineReporter().generate_report(c)["total_considered"] == 7

    def test_total_considered_scored_none(self):
        c = self._ctx_with(); c.scored_df = None
        assert PipelineReporter().generate_report(c)["total_considered"] == 0

    def test_calculate_statistics_magnitude_range(self):
        c = self._ctx_with(3); c.selected_df["MAGNITUDE"] = [5.0, 7.0, 8.0]
        stats = PipelineReporter().generate_report(c)["statistics"]
        assert stats["magnitude_range"] == (5.0, 8.0)

    def test_statistics_distance_when_present(self):
        c = self._ctx_with(2); c.selected_df["RJB(km)"] = [10.0, 30.0]
        assert "distance_range" in PipelineReporter().generate_report(c)["statistics"]

    def test_statistics_no_distance_when_absent(self):
        c = self._ctx_with()
        c.selected_df = pd.DataFrame({"MAGNITUDE": [7.0], "SCORE": [80.0]})
        c.scored_df = c.selected_df.copy()
        assert "distance_range" not in PipelineReporter().generate_report(c)["statistics"]

    def test_criteria_model_dump_used(self):
        r = PipelineReporter().generate_report(self._ctx_with())
        assert isinstance(r["search_criteria"], dict)

    def test_criteria_without_model_dump(self):
        c = self._ctx_with()
        c.search_criteria = {"raw": "criteria"}
        assert PipelineReporter().generate_report(c)["search_criteria"] == {"raw": "criteria"}

    def test_selection_summary_counts_rejections(self):
        c = self._ctx_with(1, scored_rows=3)
        c.scored_df["SELECTION_STATUS"] = ["selected", "rejected", "rejected"]
        c.scored_df["SELECTION_REASON"] = [
            "selected",
            "score_below_min_score:55.0",
            "max_per_event:3",
        ]
        summary = PipelineReporter().generate_report(c)["selection_summary"]
        assert summary["status_counts"]["rejected"] == 2
        assert summary["rejection_reasons"]["max_per_event:3"] == 1

    def test_score_breakdown_for_selected_records(self):
        c = self._ctx_with(1)
        c.selected_df["SCORE_BREAKDOWN"] = [[{"criterion": "magnitude"}]]
        c.selected_df["SELECTION_REASON"] = ["selected"]
        breakdown = PipelineReporter().generate_report(c)["score_breakdown"]
        assert breakdown[0]["criteria"][0]["criterion"] == "magnitude"

    def test_error_metrics_for_selected_records(self):
        c = self._ctx_with(1)
        c.selected_df["ERROR_TOTAL"] = [0.1]
        c.selected_df["ERROR_METRICS"] = [[{"criterion": "magnitude", "normalized_error": 0.0}]]
        c.selected_df["HARD_FILTERS"] = [[{"criterion": "magnitude", "status": "passed"}]]
        c.selected_df["SELECTION_REASON"] = ["selected"]
        metrics = PipelineReporter().generate_report(c)["error_metrics"]
        assert metrics[0]["metrics"][0]["criterion"] == "magnitude"
        assert metrics[0]["hard_filters"][0]["status"] == "passed"


# ─── _validate_inputs ────────────────────────────────────────────────────────

class TestValidateInputs:
    def test_ok_with_providers(self):
        result = EarthquakePipeline()._validate_inputs(ctx(providers=[make_provider("PEER")]))
        assert result.success

    def test_fail_no_providers(self):
        result = EarthquakePipeline()._validate_inputs(ctx(providers=[]))
        assert result.success is False
        assert isinstance(result.error, PipelineError)


# ─── _fetch_data_sync ────────────────────────────────────────────────────────

class TestFetchDataSync:
    def test_success(self):
        c = ctx(providers=[make_provider("PEER", make_df(3))])
        r = EarthquakePipeline()._fetch_data_sync(c)
        assert r.success
        assert len(r.value.data) == 1

    def test_fail_result(self):
        c = ctx(providers=[make_provider("AFAD", fail=True)])
        r = EarthquakePipeline()._fetch_data_sync(c)
        assert r.success is False
        assert isinstance(r.error, NoDataError)

    def test_empty_result(self):
        c = ctx(providers=[make_provider("AFAD", empty=True)])
        assert EarthquakePipeline()._fetch_data_sync(c).success is False

    def test_raise_exc(self):
        c = ctx(providers=[make_provider("AFAD", raise_exc=True)])
        r = EarthquakePipeline()._fetch_data_sync(c)
        assert r.success is False

    def test_one_good_one_bad(self):
        c = ctx(providers=[make_provider("PEER", make_df()), make_provider("AFAD", fail=True)])
        r = EarthquakePipeline()._fetch_data_sync(c)
        assert r.success
        assert "AFAD" in r.value.failed_providers

    def test_all_fail_no_data(self):
        c = ctx(providers=[make_provider("PEER", fail=True), make_provider("AFAD", empty=True)])
        r = EarthquakePipeline()._fetch_data_sync(c)
        assert isinstance(r.error, NoDataError)

    def test_ok_log(self):
        c = ctx(providers=[make_provider("PEER", make_df(3))])
        r = EarthquakePipeline()._fetch_data_sync(c)
        assert any("[OK] PEER" in log for log in r.value.logs)

    def test_error_log_on_exception(self):
        c = ctx(providers=[make_provider("PEER", make_df()), make_provider("AFAD", raise_exc=True)])
        r = EarthquakePipeline()._fetch_data_sync(c)
        assert any("[ERROR] AFAD" in log for log in r.value.logs)


# ─── _fetch_data_async ───────────────────────────────────────────────────────

class TestFetchDataAsync:
    def test_success(self):
        c = ctx(providers=[make_provider("PEER", make_df())])
        assert run(EarthquakePipeline()._fetch_data_async(c)).success

    def test_fail_result(self):
        c = ctx(providers=[make_provider("AFAD", fail=True)])
        assert run(EarthquakePipeline()._fetch_data_async(c)).success is False

    def test_empty_result(self):
        c = ctx(providers=[make_provider("AFAD", empty=True)])
        r = run(EarthquakePipeline()._fetch_data_async(c))
        assert r.success is False

    def test_raise_exc(self):
        c = ctx(providers=[make_provider("PEER", raise_exc=True)])
        assert run(EarthquakePipeline()._fetch_data_async(c)).success is False

    def test_one_good_one_bad(self):
        c = ctx(providers=[make_provider("PEER", make_df()), make_provider("AFAD", fail=True)])
        r = run(EarthquakePipeline()._fetch_data_async(c))
        assert r.success
        assert "AFAD" in r.value.failed_providers

    def test_all_fail_no_data(self):
        c = ctx(providers=[make_provider("PEER", fail=True)])
        r = run(EarthquakePipeline()._fetch_data_async(c))
        assert isinstance(r.error, NoDataError)

    def test_empty_data_log(self):
        c = ctx(providers=[make_provider("PEER", make_df()), make_provider("AFAD", empty=True)])
        r = run(EarthquakePipeline()._fetch_data_async(c))
        assert r.success
        assert any("AFAD" in log for log in r.value.logs)


# ─── _combine_data ───────────────────────────────────────────────────────────

class TestCombineData:
    def test_endpointsource_preserved(self):
        c = ctx(data=[make_df(2), make_df(1, with_endpoint=True)])
        r = EarthquakePipeline()._combine_data(c)
        assert r.success
        assert "ENDPOINTSOURCE" in r.value.combined_df.columns

    def test_afad_url_intact(self):
        c = ctx(data=[make_df(1, with_endpoint=True)])
        r = EarthquakePipeline()._combine_data(c)
        assert r.value.combined_df["ENDPOINTSOURCE"].iloc[0] == "https://x.com/0"

    def test_peer_endpointsource_none(self):
        c = ctx(data=[make_df(2)])
        r = EarthquakePipeline()._combine_data(c)
        assert r.value.combined_df["ENDPOINTSOURCE"].isna().all()

    def test_numeric_null_filled_zero(self):
        """
        Sayısal (float/int dtype) None → 0 ile doldurulmalı.
        Önemli: kolonu açıkça float dtype ile oluştur, yoksa pandas
        object olarak tutar ve numeric fillna etkilemez.
        """
        df = pd.DataFrame({
            "RSN": pd.array([1], dtype="Float64"),          # nullable Int
            "MAGNITUDE": pd.array([None], dtype="Float64"), # gerçekten sayısal null
            "ENDPOINTSOURCE": [None],
            "EVENT": ["test"],
        })
        c = ctx(data=[df])
        r = EarthquakePipeline()._combine_data(c)
        assert r.value.combined_df["MAGNITUDE"].iloc[0] == 0

    def test_string_null_filled_empty(self):
        df = pd.DataFrame({"EVENT": [None], "MECHANISM": [None], "ENDPOINTSOURCE": [None]})
        c = ctx(data=[df])
        r = EarthquakePipeline()._combine_data(c)
        assert r.value.combined_df["EVENT"].iloc[0] == ""

    def test_empty_data_list(self):
        c = ctx(data=[])
        r = EarthquakePipeline()._combine_data(c)
        assert r.success is False
        assert isinstance(r.error, NoDataError)

    def test_all_empty_dfs(self):
        c = ctx(data=[pd.DataFrame(), pd.DataFrame()])
        r = EarthquakePipeline()._combine_data(c)
        assert r.success is False

    def test_row_count(self):
        c = ctx(data=[make_df(2), make_df(3)])
        r = EarthquakePipeline()._combine_data(c)
        assert len(r.value.combined_df) == 5

    def test_combined_log(self):
        c = ctx(data=[make_df(2)])
        r = EarthquakePipeline()._combine_data(c)
        assert any("Combined total" in log for log in r.value.logs)


# ─── _apply_strategy ─────────────────────────────────────────────────────────

class TestApplyStrategy:
    def test_success(self):
        c = ctx(strategy=make_strategy()); c.combined_df = make_df(5)
        r = EarthquakePipeline()._apply_strategy(c)
        assert r.success
        assert "SCORE" in r.value.selected_df.columns

    def test_empty_combined_no_data(self):
        c = ctx(strategy=make_strategy()); c.combined_df = pd.DataFrame()
        r = EarthquakePipeline()._apply_strategy(c)
        assert r.success is False
        assert isinstance(r.error, NoDataError)

    def test_log_strategy_name(self):
        c = ctx(strategy=make_strategy()); c.combined_df = make_df(3)
        r = EarthquakePipeline()._apply_strategy(c)
        assert any("MockStrategy" in log for log in r.value.logs)


# ─── _finalize_result ────────────────────────────────────────────────────────

class TestFinalizeResult:
    def _ctx(self):
        c = ctx(providers=[make_provider("PEER")])
        c.selected_df = make_df(2); c.selected_df["SCORE"] = 80.0
        c.scored_df = c.selected_df.copy()
        c.start_time = time.time() - 0.1
        return c

    def test_returns_pipeline_result(self):
        r = EarthquakePipeline()._finalize_result(self._ctx())
        assert r.success
        assert isinstance(r.value, PipelineResult)

    def test_execution_time_positive(self):
        r = EarthquakePipeline()._finalize_result(self._ctx())
        assert r.value.execution_time > 0


# ─── _compose_sync / _compose_async ─────────────────────────────────────────

class TestComposers:
    def test_sync_passes_through(self):
        composed = EarthquakePipeline()._compose_sync(
            lambda c: Result.ok(c), lambda c: Result.ok(c)
        )
        assert composed(MagicMock()).success

    def test_sync_short_circuits(self):
        called = []
        composed = EarthquakePipeline()._compose_sync(
            lambda c: Result.fail(Exception("stop")),
            lambda c: called.append(True) or Result.ok(c),
        )
        composed(MagicMock())
        assert called == []

    def test_async_passes_through(self):
        composed = EarthquakePipeline()._compose_async(lambda c: Result.ok(c))
        assert run(composed(MagicMock())).success

    def test_async_short_circuits(self):
        called = []
        composed = EarthquakePipeline()._compose_async(
            lambda c: Result.fail(Exception("stop")),
            lambda c: called.append(True) or Result.ok(c),
        )
        run(composed(MagicMock()))
        assert called == []

    def test_async_awaits_coroutine(self):
        async def async_step(c): return Result.ok(c)
        composed = EarthquakePipeline()._compose_async(async_step)
        assert run(composed(MagicMock())).success


# ─── uçtan uca ───────────────────────────────────────────────────────────────

class TestEndToEnd:
    def test_sync_success(self):
        c = ctx(providers=[make_provider("PEER", make_df(5))], strategy=make_strategy())
        r = EarthquakePipeline().execute_sync(c)
        assert r.success
        assert isinstance(r.value, PipelineResult)

    def test_sync_no_providers(self):
        assert EarthquakePipeline().execute_sync(ctx(providers=[])).success is False

    def test_sync_all_fail(self):
        c = ctx(providers=[make_provider("PEER", fail=True)])
        assert EarthquakePipeline().execute_sync(c).success is False

    def test_async_success(self):
        c = ctx(providers=[make_provider("PEER", make_df(5))], strategy=make_strategy())
        r = run(EarthquakePipeline().execute_async(c))
        assert r.success

    def test_async_no_providers(self):
        r = run(EarthquakePipeline().execute_async(ctx(providers=[])))
        assert r.success is False

    def test_multi_provider(self):
        c = ctx(providers=[
            make_provider("PEER", make_df(3)),
            make_provider("AFAD", make_df(2, with_endpoint=True)),
        ], strategy=make_strategy(num=10))
        r = EarthquakePipeline().execute_sync(c)
        assert r.success
        assert len(r.value.selected_df) <= 10

    def test_failed_providers_propagated(self):
        c = ctx(providers=[make_provider("PEER", make_df()), make_provider("AFAD", fail=True)])
        r = EarthquakePipeline().execute_sync(c)
        assert r.success
        assert "AFAD" in r.value.failed_providers

    def test_start_time_reset(self):
        c = ctx(providers=[make_provider("PEER", make_df())])
        c.start_time = 0.0
        r = EarthquakePipeline().execute_sync(c)
        assert r.value.execution_time < 10
