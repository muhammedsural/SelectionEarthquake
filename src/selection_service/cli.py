"""Command-line example for the full search, selection, report, download flow."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

from selection_service.core.EarthquakeApi import EarthquakeAPI
from selection_service.core.LoggingConfig import setup_logging
from selection_service.enums.Enums import DesignCode, ProviderName
from selection_service.processing.Selection import (
    ParetoSelectionStrategy,
    ScoringWeights,
    SearchCriteria,
    SelectionConfig,
    SpectrumMatchStrategy,
    TBDY2018ConstraintStrategy,
    TBDYSelectionStrategy,
)


def _provider_names(values: Iterable[str]) -> list[ProviderName]:
    mapping = {provider.value.lower(): provider for provider in ProviderName}
    providers = []
    for value in values:
        key = value.lower()
        if key not in mapping:
            available = ", ".join(sorted(mapping))
            raise argparse.ArgumentTypeError(
                f"Unknown provider '{value}'. Available: {available}"
            )
        providers.append(mapping[key])
    return providers


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run an earthquake record search, selection, report, and optional waveform download."
    )
    parser.add_argument("--providers", nargs="+", default=["peer"], help="Provider names: peer afad")
    parser.add_argument("--start-date", default="2000-01-01")
    parser.add_argument("--end-date", default="2025-09-05")
    parser.add_argument("--min-magnitude", type=float, default=7.0)
    parser.add_argument("--max-magnitude", type=float, default=8.0)
    parser.add_argument("--min-vs30", type=float, default=300.0)
    parser.add_argument("--max-vs30", type=float, default=400.0)
    parser.add_argument("--mechanism", action="append", default=["StrikeSlip"])
    parser.add_argument("--num-records", type=int, default=11)
    parser.add_argument("--min-score", type=float, default=55.0)
    parser.add_argument(
        "--strategy",
        default="gaussian",
        choices=["gaussian", "constraint", "pareto", "spectrum"],
        help="Selection strategy: gaussian, constraint, pareto, or spectrum",
    )
    parser.add_argument(
        "--scoring-preset",
        default="tbdy_2018_record_selection",
        choices=sorted(ScoringWeights.preset_descriptions()),
    )
    parser.add_argument("--report-path", default="selection_report.json")
    parser.add_argument("--selected-csv", default="selected_records.csv")
    parser.add_argument("--download-waveforms", action="store_true")
    parser.add_argument("--export-type", default="mseed", choices=["mseed", "asc2", "asd"])
    return parser


def main(argv: list[str] | None = None) -> int:
    setup_logging()
    parser = build_parser()
    args = parser.parse_args(argv)

    providers = _provider_names(args.providers)
    config = SelectionConfig(
        design_code=DesignCode.TBDY_2018,
        num_records=args.num_records,
        min_score=args.min_score,
    )
    criteria = SearchCriteria(
        start_date=args.start_date,
        end_date=args.end_date,
        min_magnitude=args.min_magnitude,
        max_magnitude=args.max_magnitude,
        min_vs30=args.min_vs30,
        max_vs30=args.max_vs30,
        mechanisms=args.mechanism,
        weights=ScoringWeights.from_preset(args.scoring_preset),
    )
    strategies = {
        "gaussian": TBDYSelectionStrategy,
        "constraint": TBDY2018ConstraintStrategy,
        "pareto": ParetoSelectionStrategy,
        "spectrum": SpectrumMatchStrategy,
    }
    strategy = strategies[args.strategy](config=config)
    api = EarthquakeAPI(provider_names=providers, strategies=[strategy], use_cache=True)

    result = api.run_sync(criteria=criteria, strategy_name=strategy.get_name())
    if not result.success:
        print(f"Selection failed: {result.error}")
        return 1

    pipeline_result = result.value
    selected_path = Path(args.selected_csv)
    report_path = Path(args.report_path)
    pipeline_result.selected_df.to_csv(selected_path, index=False)
    report_path.write_text(
        json.dumps(pipeline_result.report, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    print(f"Selected records: {len(pipeline_result.selected_df)}")
    print(f"Selected CSV: {selected_path}")
    print(f"Report JSON: {report_path}")

    if args.download_waveforms:
        download = api.download_waveforms(
            pipeline_result.selected_df,
            export_type=args.export_type,
        )
        if not download.success:
            print(f"Waveform download failed: {download.error}")
            return 2
        print("Waveform download completed or unsupported providers were skipped.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
