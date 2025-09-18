from .AsyncPipeline import AsyncPipeline
from .SyncPipeline import SyncPipeline

class PipelineFactory:
    """Pipeline factory class"""
    
    @staticmethod
    def create_async_pipeline() -> AsyncPipeline:
        return AsyncPipeline()
    
    @staticmethod
    def create_sync_pipeline() -> SyncPipeline:
        return SyncPipeline()