"""AWS Lambda entry point, deployed by infra/terraform.

Invoked asynchronously by S3 ObjectCreated notifications on the landing bucket
(prefix cms/desynpuf/, suffix .zip). Archive/quarantine moves land outside that
prefix, so the function never re-triggers itself. Lambda's /tmp is the working
directory; credentials come from the execution role.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import unquote_plus

from claims_etl.ingestion.config import load_config
from claims_etl.ingestion.log import configure_logging, log_event
from claims_etl.ingestion.models import Outcome
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


class IngestionRetryableError(RuntimeError):
    """Raised so Lambda's async retry re-delivers the event after an `error` outcome.

    `quarantined` is a final answer (bad data) and is not retried. `error` means
    S3/network trouble, and the zip is still in landing, so a retry can succeed.
    """


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Process every landing zip named in the event; ignore anything else.

    Raises IngestionRetryableError if any zip ended in `error`, after all
    zips in the event have been attempted.
    """
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

    failed = [r["source_key"] for r in results if r["outcome"] == Outcome.ERROR.value]
    if failed:
        raise IngestionRetryableError(f"ingestion error (will retry) for: {failed}")
    return {"batch_id": batch_id, "results": results}
