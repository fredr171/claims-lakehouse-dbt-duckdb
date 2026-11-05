"""The FileIngested event: the contract between ingestion and the transform runner.

Ingestion publishes one event to the EventBridge default bus for every file that is
in raw: a fresh load, or a re-delivered duplicate (so an event lost to a failed
publish is sent again when the retry finds the file already loaded). An EventBridge
rule forwards these events to the transform queue (infra/terraform/transform.tf).

Kept dependency-free apart from boto3, so the transform package can import it
without pulling in pyarrow.
"""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Protocol

import boto3

EVENT_SOURCE = "claims-etl.ingestion"
DETAIL_TYPE = "FileIngested"
_MONTH_RE = re.compile(r"\d{4}-(0[1-9]|1[0-2])")


class EventPublishError(RuntimeError):
    """EventBridge did not accept the event."""


@dataclass(frozen=True)
class FileIngested:
    """One delivered file that is now in raw."""

    feed: str  # landing prefix, e.g. "cms/cclf/"
    file_type: str  # e.g. "claims_header"
    delivery_month: str  # "YYYY-MM", from the file name
    checksum: str
    raw_uri: str
    file_name: str
    batch_id: str
    redelivered: bool = False  # True when ingestion found it already loaded

    def __post_init__(self) -> None:
        if not self.feed.endswith("/") or not self.file_type:
            raise ValueError(f"invalid feed/file_type: {self.feed!r} {self.file_type!r}")
        if not _MONTH_RE.fullmatch(self.delivery_month):
            raise ValueError(f"delivery_month must be YYYY-MM, got {self.delivery_month!r}")

    def to_detail(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_detail(cls, detail: dict[str, Any]) -> FileIngested:
        return cls(**{name: detail[name] for name in cls.__dataclass_fields__ if name in detail})


class EventPublisher(Protocol):
    def publish(self, event: FileIngested) -> None: ...


class EventBridgePublisher:
    """Publishes FileIngested events to the default event bus."""

    def __init__(self, client: Any) -> None:
        self._client = client

    @classmethod
    def for_region(cls, region: str) -> EventBridgePublisher:
        return cls(boto3.session.Session().client("events", region_name=region))

    def publish(self, event: FileIngested) -> None:
        """Send one event; raise EventPublishError unless EventBridge accepted it."""
        response = self._client.put_events(
            Entries=[
                {
                    "Source": EVENT_SOURCE,
                    "DetailType": DETAIL_TYPE,
                    "Detail": json.dumps(event.to_detail()),
                }
            ]
        )
        if response.get("FailedEntryCount", 0):
            entry = response["Entries"][0]
            reason = f"{entry.get('ErrorCode')} {entry.get('ErrorMessage')}"
            raise EventPublishError(f"put_events rejected the event: {reason}")
