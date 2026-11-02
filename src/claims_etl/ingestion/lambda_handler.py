"""AWS Lambda entry point (stub, not deployed yet).

Expects S3 ObjectCreated notifications for the landing bucket. Lambda's /tmp
is used as the working directory; credentials come from the execution role.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import unquote_plus

from claims_etl.ingestion.config import load_config
from claims_etl.ingestion.log import configure_logging, log_event
from claims_etl.ingestion.pipeline import process_zip
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.ingestion.utils import new_batch_id

logger = logging.getLogger(__name__)


def parse_s3_event(event: dict[str, Any]) -> list[tuple[str, str]]:
    """Extract (bucket, key) pairs from an S3 event. Keys arrive URL-encoded."""
    return [
        (rec["s3"]["bucket"]["name"], unquote_plus(rec["s3"]["object"]["key"]))
        for rec in event.get("Records", [])
        if "s3" in rec
    ]


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Process every landing zip named in the event; ignore anything else."""
    configure_logging()
    config = load_config()
    store = S3Store.for_region(config.region)
    batch_id = new_batch_id()

    results = []
    for bucket, key in parse_s3_event(event):
        if bucket != config.landing_bucket or not config.is_landing_zip(key):
            log_event(logger, "event_ignored", bucket=bucket, key=key)
            continue
        record = process_zip(key, config=config, store=store, batch_id=batch_id)
        results.append(record.to_dict())
    return {"batch_id": batch_id, "results": results}
