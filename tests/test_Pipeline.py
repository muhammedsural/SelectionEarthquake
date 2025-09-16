import pytest
import pandas as pd

from selection_service.core.Pipeline import EarthquakePipeline
from selection_service.processing.ResultHandle import Result
from selection_service.core.ErrorHandle import ProviderError, StrategyError


# --------------------------
# Mock nesneler
# --------------------------
class MockProvider:
    def __init__(self, name, df=None, should_fail=False, empty=False):
        self._name = name
        self._df = df if df is not None else pd.DataFrame(
            {"MAGNITUDE": [5.0], "RJB(km)": [10.0], "SCORE": [0.9]}
        )
        self._should_fail = should_fail
        self._empty = empty

    def get_name(self):
        return self._name

    def map_criteria(self, criteria):
        return criteria

    async def fetch_data_async(self, criteria):
        if self._should_fail:
            return Result(success=False, value=None, error=Exception("Provider error"))
        if self._empty:
            return Result(success=True, value=pd.DataFrame())
        return Result(success=True, value=self._df)


class MockStrategy:
    def __init__(self, should_fail=False):
        self._should_fail = should_fail

    def get_name(self):
        return "mock_strategy"

    def select_and_score(self, df, target_params):
        if self._should_fail:
            raise Exception("Strategy failure")
        # sadece df’i seçilmiş + skorlanmış gibi döndür
        return df, df


class MockCriteria:
    def validate(self):
        pass


class MockTargetParams:
    def validate(self):
        pass


# --------------------------
# Testler
# --------------------------

@pytest.mark.asyncio
async def test_execute_async_success():
    providers = [MockProvider("p1"), MockProvider("p2")]
    strategy = MockStrategy()
    criteria = MockCriteria()
    target_params = MockTargetParams()
    pipeline = EarthquakePipeline()

    result = await pipeline.execute_async(providers, strategy, criteria, target_params)

    assert isinstance(result, Result)
    assert result.success
    assert result.value is not None
    # assert "combined_data" in result.value
    # assert not result.value["combined_data"].empty


# @pytest.mark.asyncio
# async def test_execute_async_no_data():
#     providers = [MockProvider("p1", empty=True)]
#     strategy = MockStrategy()
#     criteria = MockCriteria()
#     target_params = MockTargetParams()
#     pipeline = EarthquakePipeline()

#     result = await pipeline.execute_async(providers, strategy, criteria, target_params)

#     assert not result.success
#     assert isinstance(result.error, StrategyError)


# @pytest.mark.asyncio
# async def test_execute_async_provider_failure():
#     providers = [MockProvider("p1", should_fail=True), MockProvider("p2")]
#     strategy = MockStrategy()
#     criteria = MockCriteria()
#     target_params = MockTargetParams()
#     pipeline = EarthquakePipeline()

#     result = await pipeline.execute_async(providers, strategy, criteria, target_params)

#     assert result.success
#     assert "failed_providers" in result.value.__dataclass_fields__
#     assert "p1" in result.value.failed_providers


@pytest.mark.asyncio
async def test_execute_async_strategy_failure():
    providers = [MockProvider("p1")]
    strategy = MockStrategy(should_fail=True)
    criteria = MockCriteria()
    target_params = MockTargetParams()
    pipeline = EarthquakePipeline()

    result = await pipeline.execute_async(providers, strategy, criteria, target_params)

    assert not result.success
    assert isinstance(result.error, StrategyError)


@pytest.mark.asyncio
async def test_execute_async_multiple_providers():
    df1 = pd.DataFrame({"MAGNITUDE": [5.0], "RJB(km)": [10.0], "SCORE": [0.9]})
    df2 = pd.DataFrame({"MAGNITUDE": [6.0], "RJB(km)": [15.0], "SCORE": [0.8]})
    providers = [MockProvider("p1", df=df1), MockProvider("p2", df=df2)]
    strategy = MockStrategy()
    criteria = MockCriteria()
    target_params = MockTargetParams()
    pipeline = EarthquakePipeline()

    result = await pipeline.execute_async(providers, strategy, criteria, target_params)

    assert result.success
    # combined = result.value["combined_data"]
    # assert len(combined) == 2
    # assert set(combined["MAGNITUDE"]) == {5.0, 6.0}
