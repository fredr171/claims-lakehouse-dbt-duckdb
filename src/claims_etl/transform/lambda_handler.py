"""AWS Lambda entry point for claims-etl-transform (infra/terraform/transform.tf).

Invoked by an SQS FIFO event source mapping. Each SQS message body is an EventBridge
event (source claims-etl.ingestion, detail-type FileIngested). All messages in the
batch go into ONE dbt build. The queue has a single message group, so builds never
overlap and the DuckDB state has one writer.

Return normally (messages deleted): passed, warned, stopped, or nothing to build.
Raise (the whole batch is retried, then dead-lettered): error outcomes, including a
state conflict and messages that cannot be parsed.

Retry backoff: a failed message would otherwise stay hidden for the queue's whole
visibility timeout (6 x the function timeout = 1 hour), stalling the single FIFO group.
Before raising, the handler shortens each message's visibility to
`retry_backoff_seconds` for its receive count (2, 5, then 10 minutes), so a passing
problem is retried within minutes and a persistent one reaches the DLQ in about 17.
The queue's own timeout stays the backstop when the function dies before it can
(timeout, out of memory).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from typing import Any

import boto3

from claims_etl.events import DETAIL_TYPE, EVENT_SOURCE, FileIngested
from claims_etl.ingestion.log import configure_logging, log_event
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.transform.config import (
    DEFAULT_RETRY_BACKOFF_SECONDS,
    TransformConfig,
    load_transform_config,
)
from claims_etl.transform.runner import new_run_id, run_transform
from claims_etl.transform.sinks import S3RunSink
from claims_etl.transform.state import S3StateStore

logger = logging.getLogger(__name__)
_SQS_BATCH_LIMIT = 10  # ChangeMessageVisibilityBatch entries per call


def parse_sqs_event(event: dict[str, Any]) -> list[FileIngested]:
    """FileIngested events from an SQS batch; other EventBridge events are ignored."""
    events = []
    for record in event.get("Records", []):
        body = json.loads(record["body"])
        if body.get("source") != EVENT_SOURCE or body.get("detail-type") != DETAIL_TYPE:
            log_event(logger, "message_ignored", message_id=record.get("messageId"),
                      source=body.get("source"), detail_type=body.get("detail-type"))  # fmt: skip
            continue
        events.append(FileIngested.from_detail(body["detail"]))
    return events


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Build the feeds named in the batch; raise on an error outcome (retry soon)."""
    configure_logging()
    backoff = DEFAULT_RETRY_BACKOFF_SECONDS
    try:
        file_events = parse_sqs_event(event)
        if not file_events:
            return {"outcome": "nothing_to_build"}
        config = load_transform_config()
        backoff = config.retry_backoff_seconds
        return _build(file_events, config)
    except Exception:
        retry_soon(event.get("Records", []), backoff)
        raise


def _build(file_events: list[FileIngested], config: TransformConfig) -> dict[str, Any]:
    """One run_transform against the S3 state; its report goes to the runs prefix."""
    store = S3Store.for_region(config.region)
    run_id = new_run_id()
    state = S3StateStore(store.client, config.raw_bucket, config.state_key, config.backup_key)
    sink = S3RunSink(store, config.raw_bucket, config.runs_prefix, run_id)
    report = run_transform(file_events, config=config, state=state, sink=sink, run_id=run_id)
    return {"run_id": report.run_id, "outcome": report.outcome.value, "counts": report.counts}


def backoff_seconds(receive_count: int, delays: Sequence[int]) -> int:
    """Delay before the next attempt after failed attempt `receive_count` (last repeats)."""
    return delays[min(max(receive_count, 1), len(delays)) - 1]


def queue_url_from_arn(arn: str) -> tuple[str, str]:
    """(region, queue URL) for arn:aws:sqs:<region>:<account>:<name>; no GetQueueUrl call."""
    _, _, service, region, account, name = arn.split(":")
    if service != "sqs":
        raise ValueError(f"not an SQS queue ARN: {arn!r}")
    return region, f"https://sqs.{region}.amazonaws.com/{account}/{name}"


def _sqs_client(region: str) -> Any:
    return boto3.session.Session().client("sqs", region_name=region)


def retry_soon(records: Sequence[dict[str, Any]], delays: Sequence[int]) -> None:
    """Make the failed batch visible again after the backoff delay. Never raises.

    If this fails, the messages simply keep the queue's full visibility timeout, so it
    is logged and the original error is what the caller re-raises.
    """
    by_queue: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        if record.get("eventSourceARN") and record.get("receiptHandle"):
            by_queue.setdefault(record["eventSourceARN"], []).append(record)
    for arn, queued in by_queue.items():
        try:
            region, url = queue_url_from_arn(arn)
            client = _sqs_client(region)
            failed = 0
            for start in range(0, len(queued), _SQS_BATCH_LIMIT):
                chunk = queued[start : start + _SQS_BATCH_LIMIT]
                entries = [
                    {
                        "Id": str(i),
                        "ReceiptHandle": r["receiptHandle"],
                        "VisibilityTimeout": backoff_seconds(
                            int(r.get("attributes", {}).get("ApproximateReceiveCount", 1)),
                            delays,
                        ),
                    }
                    for i, r in enumerate(chunk)
                ]
                response = client.change_message_visibility_batch(QueueUrl=url, Entries=entries)
                failed += len(response.get("Failed", []))
            log_event(logger, "retry_scheduled", queue=arn.rsplit(":", 1)[-1],
                      messages=len(queued), failed=failed,
                      delays=sorted({e["VisibilityTimeout"] for e in entries}))  # fmt: skip
        except Exception:  # noqa: BLE001 - the queue's own visibility timeout still applies
            logger.exception("retry_backoff_failed", extra={"fields": {"queue": arn}})
