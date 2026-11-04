"""AWS Lambda entry point for quarantine notifications (stub, NOT deployed yet).

Intended trigger: S3 ObjectCreated on the landing bucket, prefix quarantine/,
suffix .error.json. Before deploying: switch settings._lookup to ssm_setting,
store GITHUB_TOKEN / GITHUB_REPO in SSM under /claims-etl/, and bundle
`requests` in the package (it is not in the Lambda runtime).
"""

from __future__ import annotations

import logging
from typing import Any

from claims_etl.ingestion.lambda_handler import parse_s3_event
from claims_etl.ingestion.log import configure_logging, log_event
from claims_etl.notifications.config import load_notification_config
from claims_etl.notifications.notifier import notify_quarantine

logger = logging.getLogger(__name__)


class NotificationFailedError(RuntimeError):
    """Raised so Lambda's async retry re-delivers the event."""


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Notify for every quarantine report in the event; ignore anything else."""
    configure_logging()
    config = load_notification_config()
    results, failed = [], []
    for bucket, key in parse_s3_event(event):
        if bucket != config.landing_bucket or not config.is_error_json_key(key):
            log_event(logger, "event_ignored", bucket=bucket, key=key)
            continue
        try:
            results.append(notify_quarantine(key, config=config).to_dict())
        except Exception as exc:  # noqa: BLE001 - attempt every record, then raise
            failed.append(key)
            log_event(
                logger,
                "notify_failed",
                level=logging.ERROR,
                key=key,
                error=f"{type(exc).__name__}: {exc}",
            )
    if failed:
        raise NotificationFailedError(f"notification failed (will retry) for: {failed}")
    return {"results": results}
