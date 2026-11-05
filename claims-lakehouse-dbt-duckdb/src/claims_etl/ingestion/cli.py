"""Command-line entry point.

    python -m claims_etl.ingestion.cli --key cms/cclf/claims_header/CCLF1_202301.csv
    python -m claims_etl.ingestion.cli --key humana/ma/member_eligibility/HUM_ELIG_202301.csv
    python -m claims_etl.ingestion.cli --scan

Logs (JSON lines) go to stderr; a JSON summary of manifest records goes to stdout.
Exit codes: 0 = all success/skipped, 2 = at least one quarantined, 1 = at least one error.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from claims_etl.ingestion.config import IngestionConfig, load_config
from claims_etl.ingestion.log import configure_logging
from claims_etl.ingestion.models import ManifestRecord, Outcome
from claims_etl.ingestion.pipeline import process_file
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.ingestion.utils import new_batch_id

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_QUARANTINED = 2


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Ingest CCLF CSV files from landing to raw.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--key", help="landing key of one CSV file to process")
    target.add_argument(
        "--scan", action="store_true", help="process every CSV file under the landing prefix"
    )
    parser.add_argument("--config-dir", type=Path, help="config folder (default: repo config/)")
    parser.add_argument("--work-dir", type=Path, help="parent folder for temporary files")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def scan_keys(store: S3Store, config: IngestionConfig) -> list[str]:
    """Landing files waiting to be processed (processed ones are already archived)."""
    return [
        key
        for prefix in config.landing_prefixes
        for key in store.list_keys(config.landing_bucket, prefix)
        if config.is_landing_file(key)
    ]


def exit_code(records: Sequence[ManifestRecord]) -> int:
    """Map outcomes to a process exit code."""
    outcomes = {r.outcome for r in records}
    if Outcome.ERROR in outcomes:
        return EXIT_ERROR
    if Outcome.QUARANTINED in outcomes:
        return EXIT_QUARANTINED
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    """Run ingestion for one key or a scan, print a summary, return an exit code."""
    args = parse_args(argv)
    configure_logging(args.log_level)
    config = load_config(args.config_dir)
    store = S3Store.for_region(config.region)
    batch_id = new_batch_id()

    if args.key and not config.is_landing_file(args.key):
        prefixes = ", ".join(config.landing_prefixes)
        print(f"--key must be a .csv under one of: {prefixes}", file=sys.stderr)
        return EXIT_ERROR

    keys = [args.key] if args.key else scan_keys(store, config)
    records = [
        process_file(key, config=config, store=store, batch_id=batch_id, work_dir=args.work_dir)
        for key in keys
    ]
    print(json.dumps({"batch_id": batch_id, "records": [r.to_dict() for r in records]}, indent=2))
    return exit_code(records)


if __name__ == "__main__":
    sys.exit(main())
