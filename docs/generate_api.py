"""Generate mkdocstrings API reference pages for SelectionEarthquake."""

from __future__ import annotations

from pathlib import Path


API_PAGES = {
    "core.md": [
        "selection_service.core.earthquake_api.EarthquakeAPI",
        "selection_service.core.pipeline.PipelineResult",
        "selection_service.core.pipeline.PipelineContext",
        "selection_service.core.pipeline.PipelineReporter",
        "selection_service.core.pipeline.EarthquakePipeline",
        "selection_service.core.config",
        "selection_service.core.error_handle",
    ],
    "processing.md": [
        "selection_service.processing.criteria.ScoringWeights",
        "selection_service.processing.criteria.SelectionConfig",
        "selection_service.processing.criteria.SearchCriteria",
        "selection_service.processing.strategies.ISelectionStrategy",
        "selection_service.processing.strategies.BaseSelectionStrategy",
        "selection_service.processing.strategies.TBDYSelectionStrategy",
        "selection_service.processing.strategies.TBDY2018ConstraintStrategy",
        "selection_service.processing.strategies.ConstraintSelectionStrategy",
        "selection_service.processing.strategies.EurocodeSelectionStrategy",
        "selection_service.processing.mappers",
        "selection_service.processing.dedup",
        "selection_service.processing.result_handle",
    ],
    "providers.md": [
        "selection_service.providers.interfaces",
        "selection_service.providers.providers_factory.ProviderFactory",
        "selection_service.providers.providers_factory.CachedProviderProxy",
        "selection_service.providers.peer_provider.PeerWest2Provider",
        "selection_service.providers.afad_provider.AFADDataProvider",
        "selection_service.providers.afad.afad_api_client.AfadApiClient",
        "selection_service.providers.afad.afad_file_manager.AfadFileManager",
        "selection_service.providers.cache_manager.CacheManager",
    ],
    "services.md": [
        "selection_service.services.provider_registry.ProviderRegistry",
        "selection_service.services.earthquake_query_service.EarthquakeQueryService",
        "selection_service.services.waveform_download_service.WaveformDownloadService",
    ],
}


def generate_api_pages() -> None:
    """Generate API pages that match mkdocs.yml navigation."""
    api_path = Path("docs") / "api"
    api_path.mkdir(exist_ok=True)

    for filename, modules in API_PAGES.items():
        title = filename.removesuffix(".md").title()
        content = [f"# {title} API", ""]
        for module in modules:
            content.append(f"::: {module}")
            content.append("")
        (api_path / filename).write_text("\n".join(content), encoding="utf-8")


if __name__ == "__main__":
    generate_api_pages()
