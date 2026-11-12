"""Command-line entry point: run the same transform the Lambda runs, on demand.

    claims-transform --feed cclf --months 2023-01            # S3 state + S3 raw (like Lambda)
    claims-transform --all                                   # every feed, WARNs over all history
    claims-transform --all --db dbt/claims_lakehouse.duckdb  # local state file, reports in
                                                             # transform_runs/ (offline with
                                                             # --raw-root)

Without --db the S3 state is fetched and, on success, published back with the same
conditional upload the Lambda uses. Logs go to stderr; the run report goes to stdout.
Exit codes: 0 = passed/warned, 2 = stopped (a STOP test failed), 1 = error.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from claims_etl.ingestion.log import configure_logging
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.transform.config import load_transform_config
from claims_etl.transform.runner import Outcome, TransformRunError, new_run_id, run_transform
from claims_etl.transform.sinks import LocalRunSink, S3RunSink
from claims_etl.transform.state import LocalStateStore, S3StateStore

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_STOPPED = 2


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="dbt build of staging + intermediate.")
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument("--feed", action="append", help="dbt source to build (cclf, ma_payer)")
    which.add_argument("--all", action="store_true", help="build every feed")
    parser.add_argument(
        "--months", help="comma-separated YYYY-MM delivery months scoping the WARN tests "
        "(default: all history)"
    )  # fmt: skip
    parser.add_argument("--db", type=Path, help="local DuckDB state file instead of S3")
    parser.add_argument(
        "--raw-root", type=Path,
        help="local raw folder (<root>/cms/cclf/..., <root>/ma_payer/ma/...) instead of S3"
    )  # fmt: skip
    parser.add_argument("--runs-dir", type=Path, default=Path("transform_runs"),
                        help="report folder when --db is used")  # fmt: skip
    parser.add_argument("--config-dir", type=Path, help="config folder (default: repo config/)")
    parser.add_argument("--work-dir", type=Path, help="parent folder for temporary files")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)
    config = load_transform_config(args.config_dir)

    by_source = {source: feed for feed, source in config.feed_sources.items()}
    names = sorted(by_source) if args.all else args.feed
    unknown = sorted(set(names) - set(by_source))
    if unknown:
        print(f"unknown feed(s) {unknown}; choose from {sorted(by_source)}", file=sys.stderr)
        return EXIT_ERROR
    months = [m.strip() for m in args.months.split(",") if m.strip()] if args.months else []

    run_id = new_run_id()
    if args.db:
        state = LocalStateStore(args.db)
        sink = LocalRunSink(args.runs_dir, run_id)
    else:
        store = S3Store.for_region(config.region)
        state = S3StateStore(store.client, config.raw_bucket, config.state_key, config.backup_key)
        sink = S3RunSink(store, config.raw_bucket, config.runs_prefix, run_id)
    extra_vars = {}
    if args.raw_root:
        root = args.raw_root.resolve()
        extra_vars = {
            "cclf_raw_root": (root / "cms" / "cclf").as_posix(),
            "ma_payer_raw_root": (root / "ma_payer" / "ma").as_posix(),
        }

    try:
        report = run_transform(
            [], config=config, state=state, sink=sink, feeds=[by_source[n] for n in names],
            delivery_months=months, extra_vars=extra_vars, run_id=run_id,
            work_dir=args.work_dir,
        )  # fmt: skip
    except TransformRunError as exc:
        print(str(exc), file=sys.stderr)
        return EXIT_ERROR
    print(json.dumps(report.to_dict(), indent=2))
    return EXIT_STOPPED if report.outcome is Outcome.STOPPED else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
