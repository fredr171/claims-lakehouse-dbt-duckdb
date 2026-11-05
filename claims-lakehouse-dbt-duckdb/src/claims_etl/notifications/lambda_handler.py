"""AWS Lambda entry point for claims-etl-notify (deployed by infra/terraform).

Three triggers, one function (same GitHub repo, token and rules):
- S3 ObjectCreated on the landing bucket, quarantine/*.error.json -> quarantine issue
  (notifier.notify_quarantine).
- S3 ObjectCreated on the raw bucket, _transform/runs/<run_id>/report.json -> transform
  alert (alerts.notify_transform_report).
- SNS from the claims-etl-alerts topic (CloudWatch alarm state changes) -> alarm alert
  (alerts.notify_alarm).

Settings come from SSM (/claims-etl/GITHUB_TOKEN, GITHUB_REPO) because Terraform sets
CLAIMS_ETL_SETTINGS_SOURCE=ssm. This module must not import pyarrow: the notifier
function has no pandas layer.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from claims_etl.ingestion.log import configure_logging, log_event
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.notifications.alert_config import AlertConfig, load_alert_config
from claims_etl.notifications.alerts import notify_alarm, notify_transform_report
from claims_etl.notifications.config import NotificationConfig, load_notification_config
from claims_etl.notifications.github_client import GitHubClient
from claims_etl.notifications.notifier import notify_quarantine
from claims_etl.s3_events import parse_s3_event

logger = logging.getLogger(__name__)


class NotificationFailedError(RuntimeError):
    """Raised so Lambda's async retry re-delivers the event."""


class _Context:
    """Configs, S3 and the GitHub client, created once per invocation and only if needed."""

    def __init__(self) -> None:
        self.notifications: NotificationConfig = load_notification_config()
        self.alerts: AlertConfig = load_alert_config()
        self._store: S3Store | None = None
        self._client: GitHubClient | None = None

    @property
    def store(self) -> S3Store:
        self._store = self._store or S3Store.for_region(self.alerts.region)
        return self._store

    @property
    def client(self) -> GitHubClient:
        self._client = self._client or GitHubClient.from_settings()
        return self._client


def _jobs(event: dict[str, Any], ctx: _Context) -> list[tuple[str, Any]]:
    """(description, zero-argument callable) per record we act on; others are logged."""
    jobs: list[tuple[str, Any]] = []
    for record in event.get("Records", []):
        if record.get("EventSource") == "aws:sns" or "Sns" in record:
            message = json.loads(record["Sns"]["Message"])
            name = str(message.get("AlarmName", "?"))
            jobs.append((f"alarm {name}", lambda m=message: notify_alarm(
                m, config=ctx.alerts, store=ctx.store, client=ctx.client)))  # fmt: skip
    for bucket, key in parse_s3_event(event):
        if bucket == ctx.notifications.landing_bucket and ctx.notifications.is_error_json_key(key):
            jobs.append((key, lambda k=key: notify_quarantine(k, config=ctx.notifications)))
        elif bucket == ctx.alerts.raw_bucket and ctx.alerts.is_report_key(key):
            jobs.append((key, lambda k=key: notify_transform_report(
                k, config=ctx.alerts, store=ctx.store, client=ctx.client)))  # fmt: skip
        else:
            log_event(logger, "event_ignored", bucket=bucket, key=key)
    return jobs


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Notify for every record we recognize; raise at the end if any failed (retry)."""
    configure_logging()
    ctx = _Context()
    results, failed = [], []
    for description, job in _jobs(event, ctx):
        try:
            results.append(job().to_dict())
        except Exception as exc:  # noqa: BLE001 - attempt every record, then raise
            failed.append(description)
            log_event(logger, "notify_failed", level=logging.ERROR, key=description,
                      error=f"{type(exc).__name__}: {exc}")  # fmt: skip
    if failed:
        raise NotificationFailedError(f"notification failed (will retry) for: {failed}")
    return {"results": results}
