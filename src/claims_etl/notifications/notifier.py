"""notify_quarantine: one .error.json -> a new ticket or a comment on the open one.

Decoupled from ingestion: it only reads the report ingestion already wrote,
so a ticketing failure can never affect ingestion or quarantine. Idempotent
per quarantine event: a batch ID already in the ticket record is skipped
without any API call, which makes ``--scan`` a safe retry.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

from claims_etl.ingestion.log import log_event
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.ingestion.utils import utc_now
from claims_etl.notifications import content
from claims_etl.notifications.config import LabelSpec, NotificationConfig, load_notification_config
from claims_etl.notifications.github_client import GitHubClient, Ticket
from claims_etl.notifications.report import QuarantineReport
from claims_etl.notifications.ticket_record import (
    TicketEvent,
    TicketRecord,
    load_ticket_record,
    save_ticket_record,
)

logger = logging.getLogger(__name__)


class TicketClient(Protocol):
    """What the notifier needs from a tracker (GitHub today; Jira later)."""

    repo: str

    def list_open_tickets(self, label: str) -> Iterable[Ticket]: ...
    def ensure_labels(self, labels: list[LabelSpec]) -> list[str]: ...
    def create_ticket(self, title: str, body: str, labels: list[str]) -> Ticket: ...
    def add_comment(self, ticket: Ticket, body: str) -> str: ...


class NotifyOutcome(StrEnum):
    """What notify_quarantine did."""

    CREATED = "created"
    COMMENTED = "commented"
    ALREADY_NOTIFIED = "already_notified"


@dataclass(frozen=True)
class NotifyResult:
    """Outcome of one notification."""

    error_key: str
    checksum: str
    outcome: NotifyOutcome
    ticket_number: int
    ticket_url: str
    comment_url: str | None = None

    def to_dict(self) -> dict[str, object]:
        """Serialize for the CLI summary."""
        return {**self.__dict__, "outcome": self.outcome.value}


def find_open_ticket(client: TicketClient, label: str, checksum: str) -> Ticket | None:
    """The open ticket whose body carries this checksum, if any."""
    for ticket in client.list_open_tickets(label):
        if content.find_checksum(ticket.body) == checksum:
            return ticket
    return None


def notify_quarantine(
    error_json_key: str,
    *,
    config: NotificationConfig | None = None,
    store: S3Store | None = None,
    client: TicketClient | None = None,
) -> NotifyResult:
    """Create or update the ticket for one quarantine report.

    Raises on failure (TicketingError for the tracker, botocore errors for
    S3); callers decide how to report it. Ingestion is never involved.
    """
    config = config or load_notification_config()
    if not config.is_error_json_key(error_json_key):
        raise ValueError(
            f"{error_json_key!r} is not a {config.error_json_suffix} key under "
            f"{config.quarantine_prefix!r}"
        )
    store = store or S3Store.for_region(config.region)
    client = client or GitHubClient.from_settings()

    data = store.get_json(config.landing_bucket, error_json_key)
    if data is None:
        raise FileNotFoundError(f"s3://{config.landing_bucket}/{error_json_key} not found")
    report = QuarantineReport.from_dict(error_json_key, data)
    log_event(
        logger,
        "notify_started",
        key=error_json_key,
        checksum=report.checksum,
        batch_id=report.batch_id,
        failed_check=report.failed_check,
    )

    record = load_ticket_record(store, config, report.checksum)
    if record and report.batch_id in record.notified_batch_ids:
        log_event(
            logger,
            "notify_skipped",
            key=error_json_key,
            reason="already_notified",
            ticket_url=record.ticket_url,
        )
        return NotifyResult(
            error_key=error_json_key,
            checksum=report.checksum,
            outcome=NotifyOutcome.ALREADY_NOTIFIED,
            ticket_number=record.ticket_number,
            ticket_url=record.ticket_url,
        )

    existing = find_open_ticket(client, config.match_label, report.checksum)
    if existing:
        comment_url = client.add_comment(existing, content.build_comment(report, config))
        ticket, outcome, event_url = existing, NotifyOutcome.COMMENTED, comment_url
    else:
        labels = config.labels_for(report.file_type or "unknown")
        client.ensure_labels(labels)
        ticket = client.create_ticket(
            content.build_title(report, config),
            content.build_body(report, config),
            [label.name for label in labels],
        )
        comment_url, outcome, event_url = None, NotifyOutcome.CREATED, ticket.url

    record = _updated_record(record, report, client.repo, ticket, outcome, event_url)
    save_ticket_record(store, config, record)
    log_event(
        logger,
        "notify_finished",
        key=error_json_key,
        outcome=outcome.value,
        ticket_number=ticket.number,
        ticket_url=ticket.url,
        comment_url=comment_url,
    )
    return NotifyResult(
        error_key=error_json_key,
        checksum=report.checksum,
        outcome=outcome,
        ticket_number=ticket.number,
        ticket_url=ticket.url,
        comment_url=comment_url,
    )


def _updated_record(
    record: TicketRecord | None,
    report: QuarantineReport,
    repo: str,
    ticket: Ticket,
    outcome: NotifyOutcome,
    event_url: str,
) -> TicketRecord:
    """Append this event; point the record at the current ticket (a new one if the old closed)."""
    record = record or TicketRecord(
        checksum=report.checksum,
        tracker="github",
        repo=repo,
        ticket_number=ticket.number,
        ticket_url=ticket.url,
    )
    record.ticket_number = ticket.number
    record.ticket_url = ticket.url
    record.events.append(
        TicketEvent(
            batch_id=report.batch_id,
            action=outcome.value,
            at=utc_now().isoformat(),
            failed_check=report.failed_check,
            ticket_number=ticket.number,
            url=event_url,
        )
    )
    return record
