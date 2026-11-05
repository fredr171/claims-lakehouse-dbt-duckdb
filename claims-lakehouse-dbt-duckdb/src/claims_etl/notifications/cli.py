"""Command-line entry point for quarantine notifications.

    python -m claims_etl.notifications.cli --error-key <quarantine/.../x.csv.error.json>
    python -m claims_etl.notifications.cli --scan

``--scan`` is the retry mechanism: it visits every .error.json under
quarantine/, and notify_quarantine skips the ones already ticketed.
Settings (GITHUB_TOKEN, GITHUB_REPO) are read from the environment, loaded
from a gitignored .env if present. Exit code: 0 all good, 1 any failure.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

from claims_etl.ingestion.log import configure_logging, log_event
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.notifications.config import NotificationConfig, load_notification_config
from claims_etl.notifications.github_client import GitHubClient
from claims_etl.notifications.notifier import notify_quarantine
from claims_etl.notifications.settings import MissingSettingError

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_FAILED = 1


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Open GitHub issues for quarantined files.")
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--error-key", help="landing key of one .error.json")
    target.add_argument(
        "--scan", action="store_true", help="notify every .error.json not yet ticketed"
    )
    parser.add_argument("--config-dir", type=Path, help="config folder (default: repo config/)")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def scan_error_keys(store: S3Store, config: NotificationConfig) -> list[str]:
    """Every quarantine report in the landing bucket."""
    return [
        key
        for key in store.list_keys(config.landing_bucket, config.quarantine_prefix)
        if config.is_error_json_key(key)
    ]


def main(argv: Sequence[str] | None = None) -> int:
    """Notify for one key or a scan; print a JSON summary; return an exit code."""
    args = parse_args(argv)
    load_dotenv(find_dotenv(usecwd=True))  # no-op if there is no .env
    configure_logging(args.log_level)
    config = load_notification_config(args.config_dir)

    if args.error_key and not config.is_error_json_key(args.error_key):
        print(
            f"--error-key must be a {config.error_json_suffix} key under "
            f"{config.quarantine_prefix}",
            file=sys.stderr,
        )
        return EXIT_FAILED
    try:
        client = GitHubClient.from_settings()
    except (MissingSettingError, ValueError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return EXIT_FAILED
    store = S3Store.for_region(config.region)

    keys = [args.error_key] if args.error_key else scan_error_keys(store, config)
    results, failures = [], []
    for key in keys:
        try:
            results.append(notify_quarantine(key, config=config, store=store, client=client))
        except Exception as exc:  # noqa: BLE001 - one failing ticket must not stop a scan
            failures.append({"error_key": key, "error": f"{type(exc).__name__}: {exc}"})
            log_event(
                logger,
                "notify_failed",
                level=logging.ERROR,
                key=key,
                error=f"{type(exc).__name__}: {exc}",
            )

    summary = {"results": [r.to_dict() for r in results], "failures": failures}
    print(json.dumps(summary, indent=2))
    return EXIT_FAILED if failures else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
