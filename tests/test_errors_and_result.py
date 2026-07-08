"""
tests/test_errors_and_result.py

ErrorHandle ve ResultHandle modülleri — %100 coverage hedefi.
"""

import pytest
from selection_service.processing.ResultHandle import Result, result_decorator, async_result_decorator
from selection_service.core.ErrorHandle import (
    PipelineError, ValidationError, NoDataError, StrategyError,
    ProviderError, NetworkError, DataProcessingError,
)


# ─────────────────────────────────────────────────────────────────────────────
# Result pattern
# ─────────────────────────────────────────────────────────────────────────────

class TestResult:

    def test_ok_success_true(self):
        assert Result.ok(1).success is True

    def test_ok_value(self):
        assert Result.ok("hello").value == "hello"

    def test_ok_error_none(self):
        assert Result.ok(42).error is None

    def test_fail_success_false(self):
        assert Result.fail(ValueError("x")).success is False

    def test_fail_error(self):
        err = ValueError("boom")
        assert Result.fail(err).error is err

    def test_fail_value_none(self):
        assert Result.fail(ValueError()).value is None

    def test_unwrap_ok(self):
        assert Result.ok(99).unwrap() == 99

    def test_unwrap_fail_raises(self):
        with pytest.raises(RuntimeError, match="oops"):
            Result.fail(RuntimeError("oops")).unwrap()

    def test_repr_ok(self):
        r = Result.ok(42)
        assert "OK" in repr(r)

    def test_repr_fail(self):
        r = Result.fail(ValueError("x"))
        assert "FAIL" in repr(r)

    def test_ok_none_value(self):
        """None değer de geçerli ok sonucu olmalı."""
        r = Result.ok(None)
        assert r.success is True
        assert r.value is None

    def test_ok_dataframe_value(self):
        import pandas as pd
        df = pd.DataFrame({"a": [1]})
        r = Result.ok(df)
        assert r.success is True


class TestResultDecorator:

    def test_decorator_success(self):
        @result_decorator
        def add(self, a, b):
            return a + b

        result = add(None, 2, 3)
        assert result.success is True
        assert result.value == 5

    def test_decorator_failure(self):
        @result_decorator
        def fail_fn(self):
            raise ValueError("test error")

        result = fail_fn(None)
        assert result.success is False
        assert isinstance(result.error, ValueError)

    def test_async_decorator_success(self):
        import asyncio

        @async_result_decorator
        async def async_add(self, a, b):
            return a + b

        result = asyncio.run(async_add(None, 4, 5))
        assert result.success is True
        assert result.value == 9

    def test_async_decorator_failure(self):
        import asyncio

        @async_result_decorator
        async def async_fail(self):
            raise RuntimeError("async boom")

        result = asyncio.run(async_fail(None))
        assert result.success is False
        assert isinstance(result.error, RuntimeError)


# ─────────────────────────────────────────────────────────────────────────────
# ErrorHandle hiyerarşisi
# ─────────────────────────────────────────────────────────────────────────────

class TestErrorHierarchy:

    def test_pipeline_error_is_exception(self):
        with pytest.raises(PipelineError):
            raise PipelineError("base")

    def test_validation_error_is_pipeline_error(self):
        with pytest.raises(PipelineError):
            raise ValidationError("invalid")

    def test_no_data_error_is_pipeline_error(self):
        with pytest.raises(PipelineError):
            raise NoDataError("no data")

    def test_strategy_error_is_pipeline_error(self):
        with pytest.raises(PipelineError):
            raise StrategyError("strategy failed")

    def test_provider_error_message(self):
        err = ProviderError("AFAD", ValueError("conn refused"), "Download failed")
        assert err.provider_name == "AFAD"
        assert err.message == "Download failed"
        assert str(err) == "Download failed"

    def test_provider_error_default_message(self):
        inner = ValueError("timeout")
        err = ProviderError("PEER", inner)
        assert "PEER" in err.message
        assert "timeout" in err.message

    def test_provider_error_original_error(self):
        inner = ConnectionError("refused")
        err = ProviderError("AFAD", inner)
        assert err.original_error is inner

    def test_network_error_is_provider_error(self):
        with pytest.raises(ProviderError):
            raise NetworkError("NET", ConnectionError("down"))

    def test_data_processing_error_is_provider_error(self):
        with pytest.raises(ProviderError):
            raise DataProcessingError("PEER", ValueError("parse error"))

    def test_catch_by_base_class(self):
        """Tüm domain hataları PipelineError ile yakalanabilmeli."""
        errors = [
            ValidationError("v"),
            NoDataError("nd"),
            StrategyError("s"),
            ProviderError("P", Exception("e")),
            NetworkError("N", Exception("n")),
            DataProcessingError("D", Exception("d")),
        ]
        for err in errors:
            with pytest.raises(PipelineError):
                raise err
