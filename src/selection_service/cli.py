"""Command-line example for the full search, selection, report, download flow."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable, Iterable

from selection_service.core.earthquake_api import EarthquakeAPI
from selection_service.core.logging_config import setup_logging
from selection_service.enums.enums import DesignCode, ProviderName
from selection_service.processing.criteria import ScoringWeights, SearchCriteria, SelectionConfig
from selection_service.processing.strategies import TBDY2018ConstraintStrategy, TBDYSelectionStrategy


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


def collect_credentials(
    providers: list[ProviderName],
    args: argparse.Namespace,
    *,
    need_download: bool,
    environ: dict[str, str] | None = None,
    interactive: bool | None = None,
    ask: Callable[[str], str] = input,
    ask_secret: Callable[[str], str] = getpass.getpass,
) -> dict[str, Any]:
    """Hesap gerektiren provider'lar icin eksik bilgiyi sorar.

    Oncelik: komut satiri argumani, ortam degiskeni, terminal sorusu. Parola yalniz gizli
    (``getpass``) sorulur; ortam degiskeni veya ``--ngagm-password`` verilmisse sorulmaz.
    Terminal etkilesimli degilse (boru, CI) sorulmaz; provider eksik bilgiyi kendi
    aciklayici hatasiyla bildirir.
    """
    env = os.environ if environ is None else environ
    interactive = sys.stdin.isatty() if interactive is None else interactive
    kwargs: dict[str, Any] = {}

    if ProviderName.CESMD in providers and need_download:
        # CESMD aramasi anahtarsiz calisir; kayitli e-posta yalniz indirme icin gerekir.
        email = args.cesmd_email or env.get("CESMD_EMAIL")
        if not email and interactive:
            email = ask("CESMD kayitli e-posta (https://strongmotioncenter.org kaydi): ").strip()
        if email:
            kwargs["email"] = email

    if ProviderName.NGAW3 in providers:
        username = args.ngagm_username or env.get("NGAGM_USERNAME")
        password = args.ngagm_password or env.get("NGAGM_PASSWORD")
        if not username and interactive:
            username = ask("NGA-West3 (nga-gm.org) kullanici adi: ").strip()
        if username and not password and interactive:
            password = ask_secret("NGA-West3 (nga-gm.org) parola: ")
        if username:
            kwargs["username"] = username
        if password:
            kwargs["password"] = password
    return kwargs


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run an earthquake record search, selection, report, and optional waveform download."
    )
    parser.add_argument(
        "--providers", nargs="+", default=["peer"], help="Provider names: peer afad fdsn esm cesmd ngaw3"
    )
    parser.add_argument(
        "--cesmd-email",
        help="CESMD registered e-mail (download only). Default: CESMD_EMAIL env var, else asked in the terminal.",
    )
    parser.add_argument(
        "--ngagm-username",
        help="nga-gm.org user name for NGAW3. Default: NGAGM_USERNAME env var, else asked in the terminal.",
    )
    parser.add_argument(
        "--ngagm-password",
        help="nga-gm.org password. Prefer the NGAGM_PASSWORD env var or the hidden prompt: "
        "command-line arguments are visible in the process list and shell history.",
    )
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
        choices=["gaussian", "constraint"],
        help="Selection strategy: gaussian or constraint",
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
    }
    strategy = strategies[args.strategy](config=config)
    credentials = collect_credentials(providers, args, need_download=args.download_waveforms)
    api = EarthquakeAPI(provider_names=providers, strategies=[strategy], use_cache=True, **credentials)

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
