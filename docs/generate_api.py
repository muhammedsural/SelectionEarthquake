"""Generate mkdocstrings API reference pages for SelectionEarthquake."""

from __future__ import annotations

from pathlib import Path


API_PAGES = {
    "core.md": [
        "selection_service.core.EarthquakeApi.EarthquakeAPI",
        "selection_service.core.Pipeline.PipelineResult",
        "selection_service.core.Pipeline.PipelineContext",
        "selection_service.core.Pipeline.PipelineReporter",
        "selection_service.core.Pipeline.EarthquakePipeline",
        "selection_service.core.Config",
        "selection_service.core.ErrorHandle",
    ],
    "processing.md": [
        "selection_service.processing.Selection.ScoringWeights",
        "selection_service.processing.Selection.SelectionConfig",
        "selection_service.processing.Selection.SearchCriteria",
        "selection_service.processing.Selection.ISelectionStrategy",
        "selection_service.processing.Selection.BaseSelectionStrategy",
        "selection_service.processing.Selection.TBDYSelectionStrategy",
        "selection_service.processing.Selection.EurocodeSelectionStrategy",
        "selection_service.processing.Mappers",
        "selection_service.processing.ResultHandle",
    ],
    "providers.md": [
        "selection_service.providers.interfaces",
        "selection_service.providers.ProvidersFactory.ProviderFactory",
        "selection_service.providers.ProvidersFactory.CachedProviderProxy",
        "selection_service.providers.PeerProvider.PeerWest2Provider",
        "selection_service.providers.AfadProvider.AFADDataProvider",
        "selection_service.providers.afad.AfadApiClient.AfadApiClient",
        "selection_service.providers.afad.AfadFileManager.AfadFileManager",
        "selection_service.providers.CacheManager.CacheManager",
    ],
    "services.md": [
        "selection_service.services.ProviderRegistry.ProviderRegistry",
        "selection_service.services.EarthquakeQueryService.EarthquakeQueryService",
        "selection_service.services.WaveformDownloadService.WaveformDownloadService",
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
