import pytest
import pandas as pd
import numpy as np
import time
from unittest.mock import MagicMock, AsyncMock, patch
from selection_service.core.Pipeline import (
    EarthquakePipeline,
    PipelineContext,
    PipelineResult,
    PipelineReporter
)
from selection_service.core.ErrorHandle import NoDataError, PipelineError, ProviderError
from selection_service.processing.ResultHandle import Result
from selection_service.processing.Selection import SearchCriteria

# --- Fixtures ---

@pytest.fixture
def mock_provider():
    """Başarılı bir provider mock'u"""
    provider = MagicMock()
    provider.get_name.return_value = "TestProvider"
    provider.map_criteria.return_value = {}
    
    # Basit bir dataframe dönen fetch metodları
    df = pd.DataFrame({
        "MAGNITUDE": [5.0, 6.0],
        "RJB(km)": [10, 20],
        "SCORE": [80, 90]
    })
    
    provider.fetch_data_sync.return_value = Result.ok(df)
    provider.fetch_data_async = AsyncMock(return_value=Result.ok(df))
    return provider

@pytest.fixture
def mock_strategy():
    """Seçim stratejisi mock'u"""
    strategy = MagicMock()
    strategy.get_name.return_value = "TestStrategy"
    
    def select_and_score_side_effect(df, criteria):
        # Skorlanmış ve seçilmiş dataframe simülasyonu
        scored = df.copy()
        scored["SCORE"] = [80, 90] # Basit skorlama
        selected = scored.iloc[[1]] # 2. satırı seç
        return selected, scored
        
    strategy.select_and_score.side_effect = select_and_score_side_effect
    return strategy

@pytest.fixture
def search_criteria():
    """Arama kriterleri mock'u"""
    return MagicMock(spec=SearchCriteria)

@pytest.fixture
def pipeline_context(mock_provider, mock_strategy, search_criteria):
    """Temel pipeline context'i"""
    return PipelineContext(
        providers=[mock_provider],
        strategy=mock_strategy,
        search_criteria=search_criteria
    )

@pytest.fixture
def pipeline():
    """EarthquakePipeline örneği"""
    return EarthquakePipeline()

# --- PipelineReporter Tests ---

class TestPipelineReporter:
    def test_generate_report_success(self, pipeline_context):
        reporter = PipelineReporter()
        
        # Context'i manuel dolduralım
        pipeline_context.selected_df = pd.DataFrame({
            "MAGNITUDE": [6.0], "RJB(km)": [20], "SCORE": [90]
        })
        pipeline_context.scored_df = pd.DataFrame({
            "MAGNITUDE": [5.0, 6.0], "RJB(km)": [10, 20], "SCORE": [80, 90]
        })
        
        report = reporter.generate_report(pipeline_context)
        
        assert report["status"] == "success"
        assert report["selected_count"] == 1
        assert report["total_considered"] == 2
        assert report["strategy"] == "TestStrategy"
        assert report["statistics"]["magnitude_range"] == (6.0, 6.0)
        assert report["statistics"]["score_range"] == (90, 90)

    def test_generate_report_no_selection(self, pipeline_context):
        reporter = PipelineReporter()
        pipeline_context.selected_df = pd.DataFrame() # Boş
        
        report = reporter.generate_report(pipeline_context)
        
        assert report["status"] == "warning"
        assert report["message"] == "No records selected"

# --- EarthquakePipeline Tests (Sync) ---

class TestEarthquakePipelineSync:

    def test_validate_inputs_success(self, pipeline, pipeline_context):
        # Decorator'ı atlayarak veya doğrudan çağırarak test edebiliriz
        # Ancak pipeline akışı içinde test etmek daha doğal
        result = pipeline.execute_sync(pipeline_context)
        assert result.success is True

    def test_validate_inputs_no_providers(self, pipeline, mock_strategy, search_criteria):
        # Provider listesi boş
        context = PipelineContext(
            providers=[], 
            strategy=mock_strategy, 
            search_criteria=search_criteria
        )
        result = pipeline.execute_sync(context)
        
        assert result.success is False
        assert isinstance(result.error, PipelineError)
        assert "No providers specified" in str(result.error)

    # def test_fetch_data_sync_success(self, pipeline, pipeline_context):
    #     result = pipeline.execute_sync(pipeline_context)
        
    #     assert result.success is True
    #     ctx = result.value
    #     assert len(ctx.scored_df) == 1
    #     assert len(ctx.combined_df) == 2
    #     assert "[OK] TestProvider fetched 2 records" in ctx.logs

    # def test_fetch_data_sync_failure(self, pipeline, pipeline_context, mock_provider):
    #     # Provider hata dönsün
    #     mock_provider.fetch_data_sync.return_value = Result.fail(ProviderError("TestProvider", "API Error"))
        
    #     result = pipeline.execute_sync(pipeline_context)
        
    #     assert result.success is False
    #     # NoDataError bekliyoruz çünkü tek provider vardı ve o da hata verdi
    #     assert isinstance(result.error, NoDataError)
    #     assert "TestProvider" in result.value.failed_providers

    def test_combine_data_logic(self, pipeline, pipeline_context):
        # 2 farklı provider verisi simülasyonu
        df1 = pd.DataFrame({"A": [1], "B": [2]})
        df2 = pd.DataFrame({"A": [3], "C": [4]}) # Farklı kolonlar
        
        pipeline_context.data = [df1, df2]
        
        # combine_data metodunu doğrudan test edelim (Result sarmalayıcısı ile)
        # Not: _combine_data private olduğu için name mangling veya public wrapper gerekebilir.
        # Python'da direkt erişebiliriz:
        res = pipeline._combine_data(pipeline_context)
        
        assert res.success is True
        combined = res.value.combined_df
        
        assert len(combined) == 2
        assert "C" in combined.columns # Birleşim kümesi
        assert combined.iloc[0]["C"] == 0 # Fillna(0) çalıştı mı? (Numerik varsayımı)

    def test_apply_strategy_success(self, pipeline, pipeline_context):
        result = pipeline.execute_sync(pipeline_context)
        
        assert result.success is True
        ctx = result.value
        assert ctx.selected_df is not None
        assert len(ctx.selected_df) == 1
        assert ctx.scored_df is not None

# --- EarthquakePipeline Tests (Async) ---

@pytest.mark.asyncio
class TestEarthquakePipelineAsync:

    async def test_execute_async_success(self, pipeline, pipeline_context):
        result = await pipeline.execute_async(pipeline_context)
        
        assert result.success is True
        pipeline_result = result.value
        assert isinstance(pipeline_result, PipelineResult)
        assert len(pipeline_result.selected_df) == 1
        assert pipeline_result.execution_time > 0

    async def test_fetch_data_async_partial_failure(self, pipeline, pipeline_context, mock_provider):
        # İki provider olsun: Biri başarılı, biri hatalı
        fail_provider = MagicMock()
        fail_provider.get_name.return_value = "FailProvider"
        fail_provider.map_criteria.return_value = {}
        fail_provider.fetch_data_async = AsyncMock(return_value=Result.fail(ProviderError("Fail", "Timeout")))
        
        pipeline_context.providers = [mock_provider, fail_provider]
        
        result = await pipeline.execute_async(pipeline_context)
        
        assert result.success is True # Bir provider başarılı olduğu için pipeline başarılı
        ctx = result.value.report # PipelineResult içindeki report üzerinden veya context'e erişerek
        
        # PipelineResult failed_providers alanını kontrol edelim
        assert "FailProvider" in result.value.failed_providers
        assert len(result.value.selected_df) > 0 # Başarılı provider'dan gelen veri

    async def test_fetch_data_async_all_failure(self, pipeline, pipeline_context):
        # Tek provider var ve o da hata veriyor (mock_provider'ı bozalım)
        pipeline_context.providers[0].fetch_data_async = AsyncMock(side_effect=Exception("Network Down"))
        
        result = await pipeline.execute_async(pipeline_context)
        
        assert result.success is False
        assert isinstance(result.error, NoDataError) or isinstance(result.error, ProviderError)

    # async def test_combine_data_empty_columns(self, pipeline, pipeline_context):
    #     """Tüm sütunları NaN olan verilerin temizlenmesi testi"""
    #     # Tamamen boş sütunlu bir DF
    #     df_empty_cols = pd.DataFrame({"A": [1, 2], "Empty": [None, None]})
    #     pipeline_context.providers[0].fetch_data_async = AsyncMock(return_value=Result.ok(df_empty_cols))
        
    #     result = await pipeline.execute_async(pipeline_context)
        
    #     assert result.success is True
    #     combined = result.value.scored_df # Scored df combined üzerinden gelir
    #     assert "Empty" not in combined.columns