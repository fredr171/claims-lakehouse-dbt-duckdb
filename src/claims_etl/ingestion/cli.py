"""Command-line entry point.

    python -m claims_etl.ingestion.cli --key cms/desynpuf/beneficiary_summary/<zip>
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
from claims_etl.ingestion.pipeline import process_zip
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.ingestion.utils import new_batch_id

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_QUARANTINED = 2


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Ingest DE-SynPUF zips from landing to raw.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--key", help="landing key of one zip to process")
    target.add_argument(
        "--scan", action="store_true", help="process every zip under the landing prefix"
    )
    parser.add_argument("--config-dir", type=Path, help="config folder (default: repo config/)")
    parser.add_argument("--work-dir", type=Path, help="parent folder for temporary files")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def scan_keys(store: S3Store, config: IngestionConfig) -> list[str]:
    """Landing zips waiting to be processed (processed ones are already archived)."""
    return [
        key
        for key in store.list_keys(config.landing_bucket, config.landing_prefix)
        if config.is_landing_zip(key)
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

    if args.key and not config.is_landing_zip(args.key):
        print(f"--key must be a .zip under {config.landing_prefix}", file=sys.stderr)
        return EXIT_ERROR

    keys = [args.key] if args.key else scan_keys(store, config)
    records = [
        process_zip(key, config=config, store=store, batch_id=batch_id, work_dir=args.work_dir)
        for key in keys
    ]
    print(json.dumps({"batch_id": batch_id, "records": [r.to_dict() for r in records]}, indent=2))
    return exit_code(records)


if __name__ == "__main__":
    sys.exit(main())
