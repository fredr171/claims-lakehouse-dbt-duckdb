"""Parse S3 event notifications. Shared by every Lambda handler.

Kept dependency-free so handlers that do not need pyarrow (the notifier) can
use it without importing the ingestion pipeline.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote_plus


def parse_s3_event(event: dict[str, Any]) -> list[tuple[str, str]]:
    """Extract (bucket, key) pairs from an S3 event. Keys arrive URL-encoded."""
    return [
        (rec["s3"]["bucket"]["name"], unquote_plus(rec["s3"]["object"]["key"]))
        for rec in event.get("Records", [])
        if "s3" in rec
    ]
