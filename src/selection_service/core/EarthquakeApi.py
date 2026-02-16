from typing import Any, List
import pandas as pd

from selection_service.core.Pipeline import EarthquakePipeline, PipelineContext, PipelineResult, PipelineResult
from selection_service.core.Pipeline import EarthquakePipeline
from ..providers.ProvidersFactory import ProviderFactory
from ..enums.Enums import ProviderName
from ..providers.IProvider import IDataProvider
from ..processing.Selection import (ISelectionStrategy, SearchCriteria)
from ..core.ErrorHandle import (PipelineError, ProviderError, StrategyError)
from ..processing.ResultHandle import Result
import logging

# logger = logging.getLogger(__name__)
    
class EarthquakeAPI:
    """
    Main Entry Point (Facade).
    Kullanıcı sadece bu sınıfı ve SearchCriteria'yı bilmelidir.
    """

    def __init__(self, 
                 provider_names: List[ProviderName],
                 strategies: List[ISelectionStrategy],
                 use_cache: bool = True,
                 **kwargs: Any):
        
        self.factory = ProviderFactory()
        self.providers = [
            self.factory.create_provider(name, use_cache=use_cache, **kwargs) 
            for name in provider_names
        ]
        self.strategies = {s.get_name(): s for s in strategies}
        self.pipeline = EarthquakePipeline()

    async def run_async(self, criteria: SearchCriteria, strategy_name: str) -> Result[PipelineResult, PipelineError]:
        """Asenkron çalıştırma"""
        return await self._run_pipeline(criteria, strategy_name, is_async=True)

    def run_sync(self, criteria: SearchCriteria, strategy_name: str) -> Result[PipelineResult, PipelineError]:
        """Senkron çalıştırma"""
        return self._run_pipeline(criteria, strategy_name, is_async=False)

    def _run_pipeline(self, criteria: SearchCriteria, strategy_name: str, is_async: bool) -> Result:
        """Ortak çalıştırma mantığı"""
        if strategy_name not in self.strategies:
            return Result.fail(ValueError(f"Strategy '{strategy_name}' not found. Available: {list(self.strategies.keys())}"))
        
        strategy = self.strategies[strategy_name]
        context = PipelineContext(
            providers=self.providers,
            strategy=strategy,
            search_criteria=criteria
        )

        if is_async:
            return self.pipeline.execute_async(context) # Await çağıran yerde yapılacak
        return self.pipeline.execute_sync(context)

    # --- DOWNLOAD YÖNETİMİ ---
    
    def download_waveforms(self, result_df: pd.DataFrame) -> Result[bool, ProviderError]:
        """
        Sonuç DataFrame'indeki dosyaları ilgili provider'a indirir.
        Group-by kullanarak her provider'a toplu istek atar (Batch işlemi için daha uygun).
        """
        try:
            # Her provider için grupla
            for provider_name, group in result_df.groupby("PROVIDER"):
                provider = self._get_provider(provider_name)
                if not provider:
                    print(f"Warning: Provider {provider_name} not found active.")
                    continue
                
                # Eğer provider batch indirmeyi destekliyorsa onu kullan, yoksa tek tek
                # Burada IProvider interface'indeki mevcut metoda sadık kalıyoruz
                for _, row in group.iterrows():
                    provider.download_single_waveforms(
                        filename=row.get('FILE_NAME_H1'),
                        event_id=row.get('EVENT'),
                        station_code=row.get('SSN') # veya STATION_ID
                    )
            return Result.ok(True)
        except Exception as e:
            return Result.fail(ProviderError("API", e, "Bulk download failed"))

    def download_single_waveform(self, filename: str, event_id: str, station_code: str) -> Result[bool, ProviderError]:
        """Tek bir waveform indirme metodu"""
        try:
            provider = self._get_provider(station_code.split('.')[0]) # Station code'dan provider ismini çıkar
            if not provider:
                return Result.fail(ProviderError("API", ValueError(f"Provider not found for station {station_code}"), "Download failed"))
            provider.download_single_waveforms(filename=filename, event_id=event_id, station_code=station_code)
            return Result.ok(True)
        except Exception as e:
            return Result.fail(ProviderError("API", e, "Single waveform download failed"))

    def _get_provider(self, name: str) -> IDataProvider:
        return next((p for p in self.providers if p.get_name() == name), None)

    # --- HELPER (Opsiyonel) ---
    # Eski getter metodları yerine kullanıcıya Result objesini kullanması öğretilmelidir.
    # Ancak Re-selection mantığı API seviyesinde tutulabilir.
    
    def re_selection(self, df: pd.DataFrame, strategy_name: str, new_criteria: SearchCriteria) -> Result[PipelineResult, PipelineError]:
        """Varolan data üzerinde yeniden strateji uygula (Tekrar fetch etmeden)"""
        if strategy_name not in self.strategies:
            return Result.fail(ValueError(f"Strategy not found"))
        
        strategy = self.strategies[strategy_name]
        try:
            # Sadece logic çalıştır
            selected, scored = strategy.select_and_score(df, new_criteria)
            
            # Sahte bir context ile rapor üret
            dummy_ctx = PipelineContext([], strategy, new_criteria)
            dummy_ctx.selected_df = selected
            dummy_ctx.scored_df = scored
            
            report = self.pipeline.reporter.generate_report(dummy_ctx)
            
            return Result.ok(PipelineResult(
                selected_df=selected,
                scored_df=scored,
                report=report,
                execution_time=0.0
            ))
        except Exception as e:
            return Result.fail(StrategyError(f"Re-selection failed: {e}"))