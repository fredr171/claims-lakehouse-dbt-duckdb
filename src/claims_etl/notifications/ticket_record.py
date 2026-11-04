"""Notifier-owned record of tickets per checksum: s3://<raw>/_tickets/<sha256>.json.

Kept apart from the ingestion manifest on purpose. Ingestion writes the
manifest *after* the .error.json, so a notifier write there could be
overwritten, and the manifest should keep a single writer.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from claims_etl.ingestion.s3_store import S3Store
from claims_etl.notifications.config import NotificationConfig


@dataclass
class TicketEvent:
    """One notification: which quarantine event (batch) produced which action."""

    batch_id: str
    action: str  # "created" | "commented"
    at: str
    failed_check: str | None
    ticket_number: int
    url: str  # issue URL for "created", comment URL for "commented"


@dataclass
class TicketRecord:
    """Ticket state for one checksum."""

    checksum: str
    tracker: str
    repo: str
    ticket_number: int
    ticket_url: str
    events: list[TicketEvent] = field(default_factory=list)

    @property
    def notified_batch_ids(self) -> set[str]:
        """Batch IDs already turned into an issue or comment."""
        return {event.batch_id for event in self.events}

    def to_dict(self) -> dict[str, Any]:
        """Serialize for S3."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TicketRecord:
        """Deserialize from S3."""
        events = [TicketEvent(**event) for event in data.get("events", [])]
        return cls(**{**data, "events": events})


def ticket_record_key(config: NotificationConfig, checksum: str) -> str:
    """S3 key of the record for a checksum."""
    return f"{config.ticket_prefix}{checksum}.json"


def load_ticket_record(
    store: S3Store, config: NotificationConfig, checksum: str
) -> TicketRecord | None:
    """The record for a checksum, or None if never ticketed."""
    data = store.get_json(config.raw_bucket, ticket_record_key(config, checksum))
    return TicketRecord.from_dict(data) if data else None


def save_ticket_record(store: S3Store, config: NotificationConfig, record: TicketRecord) -> str:
    """Write the record; return its S3 key."""
    key = ticket_record_key(config, record.checksum)
    store.put_json(config.raw_bucket, key, record.to_dict())
    return key
