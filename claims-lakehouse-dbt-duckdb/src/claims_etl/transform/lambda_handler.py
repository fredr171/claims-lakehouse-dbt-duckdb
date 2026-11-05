"""AWS Lambda entry point for claims-etl-transform (infra/terraform/transform.tf).

Invoked by an SQS FIFO event source mapping. Each SQS message body is an EventBridge
event (source claims-etl.ingestion, detail-type FileIngested). All messages in the
batch go into ONE dbt build. The queue has a single message group, so builds never
overlap and the DuckDB state has one writer.

Return normally (messages deleted): passed, warned, stopped, or nothing to build.
Raise (the whole batch is retried, then dead-lettered): error outcomes, including a
state conflict and messages that cannot be parsed.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from claims_etl.events import DETAIL_TYPE, EVENT_SOURCE, FileIngested
from claims_etl.ingestion.log import configure_logging, log_event
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.transform.config import load_transform_config
from claims_etl.transform.runner import new_run_id, run_transform
from claims_etl.transform.sinks import S3RunSink
from claims_etl.transform.state import S3StateStore

logger = logging.getLogger(__name__)


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
    """Build the feeds named in the batch; raise on an error outcome (retry)."""
    configure_logging()
    file_events = parse_sqs_event(event)
    if not file_events:
        return {"outcome": "nothing_to_build"}

    config = load_transform_config()
    store = S3Store.for_region(config.region)
    run_id = new_run_id()
    state = S3StateStore(store.client, config.raw_bucket, config.state_key, config.backup_key)
    sink = S3RunSink(store, config.raw_bucket, config.runs_prefix, run_id)
    report = run_transform(file_events, config=config, state=state, sink=sink, run_id=run_id)
    return {"run_id": report.run_id, "outcome": report.outcome.value, "counts": report.counts}
