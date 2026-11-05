"""AWS Lambda entry point for quarantine notifications (deployed by infra/terraform).

Trigger: S3 ObjectCreated on the landing bucket, prefix quarantine/, suffix
.error.json. Settings come from SSM (/claims-etl/GITHUB_TOKEN, GITHUB_REPO)
because Terraform sets CLAIMS_ETL_SETTINGS_SOURCE=ssm. This module must not
import pyarrow: the notifier function has no pandas layer.
"""

from __future__ import annotations

import logging
from typing import Any

from claims_etl.ingestion.log import configure_logging, log_event
from claims_etl.notifications.config import load_notification_config
from claims_etl.notifications.notifier import notify_quarantine
from claims_etl.s3_events import parse_s3_event

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
