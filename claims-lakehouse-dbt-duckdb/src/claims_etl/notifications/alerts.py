"""Pipeline alerts -> GitHub issues: transform run reports and CloudWatch alarms.

Same tracker, client and rules as quarantine notifications (notifier.py), for the two
kinds of failure ingestion's quarantine never sees:

- notify_transform_report(report_key): a transform run report
  (_transform/runs/<run_id>/report.json, written by claims_etl/transform/runner.py).
  `stopped` / `error` -> one OPEN issue per problem (a repeat adds a comment). `passed` /
  `warned` -> every open transform issue whose feeds this run built is resolved:
  comment + close. So an open transform issue always means "still broken".
- notify_alarm(message): a CloudWatch alarm state change via SNS, for failures that
  leave no report (undeliverable events, a killed Lambda, a dead-lettered batch,
  ingestion errors). ALARM -> issue (or comment); OK -> comment + close.

Issues are found by the body line ``Alert key: `<key>` `` among open issues with the
match label (Issues list endpoint, never search: search is indexed with a delay).
Each acted-on event leaves a record under alerts.record_prefix, so a re-delivered S3 or
SNS event is skipped without API calls.

The repo is PUBLIC. Reports already carry only test names and failure counts; on top
of that everything is whitelisted here: node/test names must look like identifiers,
file names like file names, the exception keeps only its class name, and dbt error
MESSAGES are never posted (a SQL error can quote a row value). The full report stays
in the private raw bucket and is linked.
"""

from __future__ import annotations

import hashlib
import logging
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from claims_etl.ingestion.log import log_event
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.ingestion.utils import utc_now
from claims_etl.notifications.alert_config import AlertConfig
from claims_etl.notifications.config import LabelSpec
from claims_etl.notifications.content import code, s3_console_url, truncate
from claims_etl.notifications.github_client import Ticket

logger = logging.getLogger(__name__)

ALERT_KEY_LINE = re.compile(r"^Alert key: `([A-Za-z0-9:+._-]+)`$", re.MULTILINE)
REDACTED = "<redacted>"
MAX_TITLE_CHARS = 200
MAX_LISTED = 20  # tests / nodes listed per issue; the rest are counted

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")  # dbt unique_ids, test names, classes
_FILE_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")
_MONTH = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_FEED_SELECTOR = re.compile(r"^source:([a-z0-9_]+)\+$")
_ALARM_NAME = re.compile(r"^[A-Za-z0-9._-]{1,255}$")
_RUN_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_TIMESTAMP = re.compile(r"^[0-9T:.+Z-]{10,40}$")


class AlertClient(Protocol):
    """What alerting needs from a tracker (GitHubClient)."""

    repo: str

    def list_open_tickets(self, label: str) -> Iterable[Ticket]: ...
    def ensure_labels(self, labels: list[LabelSpec]) -> list[str]: ...
    def create_ticket(self, title: str, body: str, labels: list[str]) -> Ticket: ...
    def add_comment(self, ticket: Ticket, body: str) -> str: ...
    def close_ticket(self, ticket: Ticket) -> None: ...


class AlertOutcome(StrEnum):
    CREATED = "created"
    COMMENTED = "commented"
    CLOSED = "closed"
    NOTHING_TO_DO = "nothing_to_do"
    ALREADY_NOTIFIED = "already_notified"


@dataclass(frozen=True)
class AlertResult:
    event_id: str
    outcome: AlertOutcome
    ticket_numbers: tuple[int, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {"event_id": self.event_id, "outcome": self.outcome.value,
                "ticket_numbers": list(self.ticket_numbers)}  # fmt: skip


# --- sanitizing ---------------------------------------------------------------------------


def _safe(value: Any, pattern: re.Pattern[str]) -> str:
    text = str(value) if value is not None else ""
    return text if pattern.fullmatch(text) else REDACTED


def find_alert_key(body: str | None) -> str | None:
    match = ALERT_KEY_LINE.search(body or "")
    return match.group(1) if match else None


def alert_key_line(key: str) -> str:
    return f"Alert key: `{key}`"


def _digest(parts: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()[:12]


# --- transform run reports ------------------------------------------------------------------


@dataclass(frozen=True)
class TransformRun:
    """The public-safe view of a run report."""

    run_id: str
    outcome: str
    feeds: tuple[str, ...]  # what the run BUILT (its selectors)
    problem_feeds: tuple[str, ...]  # what the failing tests/nodes READ (the issue's feeds)
    months: tuple[str, ...]
    files: tuple[str, ...]
    stops: tuple[tuple[str, int | None], ...]  # (test name, failing rows)
    warn_count: int
    errors: tuple[str, ...]  # node unique_ids only
    exception_type: str | None
    counts: dict[str, int]
    started_at: str
    finished_at: str
    published: bool
    report_uri: str
    run_results_uri: str | None

    @classmethod
    def from_report(cls, data: dict[str, Any], report_uri: str) -> TransformRun:
        feeds = sorted({
            _safe(m.group(1), _IDENTIFIER)
            for s in data.get("selectors") or [] if (m := _FEED_SELECTOR.fullmatch(str(s)))
        })  # fmt: skip
        failing = data.get("stops") if data.get("outcome") == "stopped" else data.get("errors")
        node_feeds = {
            _safe(f, _IDENTIFIER) for node in failing or [] for f in node.get("feeds") or []
        }
        # Reports from before the runner recorded node feeds (or a crash with no failing
        # node) fall back to the feeds the run built.
        problem_feeds = sorted(node_feeds & set(feeds)) or feeds
        exception = str(data.get("exception") or "")
        exception_type = exception.split(":", 1)[0].strip() if exception else None
        artifacts = data.get("artifacts") or {}
        return cls(
            run_id=_safe(data.get("run_id"), _RUN_ID),
            outcome=str(data.get("outcome") or ""),
            feeds=tuple(feeds),
            problem_feeds=tuple(problem_feeds),
            months=tuple(sorted({_safe(m, _MONTH) for m in data.get("delivery_months") or []})),
            files=tuple(
                sorted({_safe(t.get("file_name"), _FILE_NAME) for t in data.get("triggers") or []})
            ),  # fmt: skip
            stops=tuple(
                sorted(
                    (
                        _safe(s.get("name"), _IDENTIFIER),
                        s.get("failures") if isinstance(s.get("failures"), int) else None,
                    )
                    for s in data.get("stops") or []
                )
            ),  # fmt: skip
            warn_count=len(data.get("warns") or []),
            errors=tuple(
                sorted({_safe(e.get("unique_id"), _IDENTIFIER) for e in data.get("errors") or []})
            ),  # fmt: skip
            exception_type=_safe(exception_type, _IDENTIFIER) if exception_type else None,
            counts={
                str(k): int(v)
                for k, v in (data.get("counts") or {}).items()
                if _IDENTIFIER.fullmatch(str(k)) and isinstance(v, int)
            },  # fmt: skip
            started_at=_safe(data.get("started_at"), _TIMESTAMP),
            finished_at=_safe(data.get("finished_at"), _TIMESTAMP),
            published=bool(data.get("published")),
            report_uri=report_uri,
            run_results_uri=artifacts.get("run_results")
            if str(artifacts.get("run_results", "")).startswith("s3://")
            else None,
        )

    @property
    def feed_label(self) -> str:
        """The feeds of the PROBLEM: a run can build cclf+humana and stop on a humana test."""
        return "+".join(self.problem_feeds) or "unknown"

    @property
    def alert_key(self) -> str:
        """Same problem -> same key: the failing nodes' feeds plus what failed (never the run
        or month). Keyed by the problem's feeds, so any later good run that builds those
        feeds resolves it, even if it builds fewer feeds than the run that failed."""
        if self.outcome == "stopped":
            what = [name for name, _ in self.stops]
        else:
            what = list(self.errors) or [f"exception:{self.exception_type or 'unknown'}"]
        return f"transform-{self.outcome}:{self.feed_label}:{_digest(what)}"


def feeds_of_key(key: str) -> set[str] | None:
    """Feeds named in a transform alert key; None for any other key."""
    parts = key.split(":")
    if len(parts) != 3 or not parts[0].startswith("transform-"):
        return None
    return set(parts[1].split("+"))


def _node_name(unique_id: str) -> str:
    """model.claims_lakehouse.int_x -> int_x (tests carry a hash suffix: drop it)."""
    parts = unique_id.split(".")
    return parts[2] if len(parts) >= 3 else unique_id


def transform_title(run: TransformRun) -> str:
    if run.outcome == "stopped":
        first = run.stops[0][0] if run.stops else "unknown test"
        more = f" (+{len(run.stops) - 1} more)" if len(run.stops) > 1 else ""
        title = f"[Transform stopped] {run.feed_label}: {first}{more}"
    elif run.errors:
        more = f" (+{len(run.errors) - 1} more)" if len(run.errors) > 1 else ""
        title = f"[Transform error] {run.feed_label}: {_node_name(run.errors[0])} failed{more}"
    else:
        title = f"[Transform error] {run.feed_label}: build crashed ({run.exception_type})"
    return truncate(title, MAX_TITLE_CHARS)


def _links(run: TransformRun, region: str) -> list[str]:
    lines = [f"- Run report: {code(run.report_uri)} "
             f"([console]({s3_console_url(run.report_uri, region)}))"]  # fmt: skip
    if run.run_results_uri:
        lines.append(f"- dbt run_results.json: {code(run.run_results_uri)} "
                     f"([console]({s3_console_url(run.run_results_uri, region)}))")  # fmt: skip
    return lines


def _run_summary(run: TransformRun) -> list[str]:
    counts = ", ".join(code(f"{k} {v}") for k, v in sorted(run.counts.items())) or "`-`"
    return [
        "| | |",
        "|---|---|",
        f"| Run | {code(run.run_id, in_table=True)} |",
        f"| Outcome | {code(run.outcome, in_table=True)} |",
        f"| Feeds built | {', '.join(code(f, in_table=True) for f in run.feeds) or '`-`'} |",
        f"| Problem in | {', '.join(code(f, in_table=True) for f in run.problem_feeds) or '`-`'} |",
        f"| Delivery months | {', '.join(code(m, in_table=True) for m in run.months) or '`-`'} |",
        f"| Triggering files | {', '.join(code(f, in_table=True) for f in run.files) or '`-`'} |",
        f"| Started / finished (UTC) | {code(run.started_at, in_table=True)} / "
        f"{code(run.finished_at, in_table=True)} |",
        f"| Results | {counts} |",
        f"| State published | {'yes' if run.published else 'no (the previous state was kept)'} |",
    ]


def transform_body(run: TransformRun, config: AlertConfig) -> str:
    lines = [f"Transform run {code(run.run_id)} ended {code(run.outcome)}.", "", *_run_summary(run)]
    if run.outcome == "stopped":
        lines += ["", "### Stopping tests", "", "| Test | Failing rows |", "|---|---|"]
        lines += [f"| {code(name, in_table=True)} | {failures if failures is not None else '-'} |"
                  for name, failures in run.stops[:MAX_LISTED]]  # fmt: skip
        if len(run.stops) > MAX_LISTED:
            lines.append(f"| ... {len(run.stops) - MAX_LISTED} more | |")
        if run.warn_count:
            lines += ["", f"{run.warn_count} WARN test(s) also flagged rows (see the report)."]
        next_step = config.stopped_next_step
    else:
        lines += ["", "### What failed", ""]
        if run.errors:
            lines += [f"- {code(uid)}" for uid in run.errors[:MAX_LISTED]]
            if len(run.errors) > MAX_LISTED:
                lines.append(f"- ... {len(run.errors) - MAX_LISTED} more")
        if run.exception_type:
            lines.append(f"- Build exception: {code(run.exception_type)}")
        lines += ["", "Error messages are not posted here (they can quote data values); "
                      "read them in the run report."]  # fmt: skip
        next_step = config.error_next_step
    lines += [
        "",
        "### Next steps",
        "",
        next_step,
        "",
        "### Details",
        "",
        *_links(run, config.region),
    ]
    lines += ["", alert_key_line(run.alert_key), ""]
    return "\n".join(lines)


def transform_comment(run: TransformRun, config: AlertConfig) -> str:
    months = ", ".join(code(m) for m in run.months) or "`-`"
    files = ", ".join(code(f) for f in run.files) or "`-`"
    return "\n".join([
        f"Seen again: run {code(run.run_id)} ended {code(run.outcome)} "
        f"({code(run.finished_at)} UTC), months {months}, files {files}.",
        "", *_links(run, config.region), "",
    ])  # fmt: skip


def resolved_comment(run: TransformRun, config: AlertConfig) -> str:
    months = ", ".join(code(m) for m in run.months) or "`-`"
    return "\n".join([
        f"Resolved: run {code(run.run_id)} ended {code(run.outcome)} for "
        f"{', '.join(code(f) for f in run.feeds)} (months {months}); the intermediate "
        "models were built. Closing.",
        "", *_links(run, config.region), "",
    ])  # fmt: skip


# --- shared create / comment / close ----------------------------------------------------------


def _open_alerts(client: AlertClient, config: AlertConfig) -> list[tuple[Ticket, str]]:
    return [(t, key) for t in client.list_open_tickets(config.match_label)
            if (key := find_alert_key(t.body))]  # fmt: skip


def _record_key(config: AlertConfig, event_id: str) -> str:
    return f"{config.record_prefix}{event_id}.json"


def _already_notified(store: S3Store, config: AlertConfig, event_id: str) -> bool:
    return store.get_json(config.raw_bucket, _record_key(config, event_id)) is not None


def _save_record(store: S3Store, config: AlertConfig, result: AlertResult, key: str) -> None:
    record = {**result.to_dict(), "alert_key": key, "at": utc_now().isoformat()}
    store.put_json(config.raw_bucket, _record_key(config, result.event_id), record)


def _raise_or_comment(
    client: AlertClient, config: AlertConfig, key: str, title: str, body: str, comment: str,
    labels: Sequence[str], event_id: str,
) -> AlertResult:  # fmt: skip
    existing = next((t for t, k in _open_alerts(client, config) if k == key), None)
    if existing:
        client.add_comment(existing, comment)
        return AlertResult(event_id, AlertOutcome.COMMENTED, (existing.number,))
    specs = config.labels_for(*labels)
    client.ensure_labels(specs)
    ticket = client.create_ticket(title, body, [s.name for s in specs])
    return AlertResult(event_id, AlertOutcome.CREATED, (ticket.number,))


# --- entry points ---------------------------------------------------------------------------


def notify_transform_report(
    report_key: str, *, config: AlertConfig, store: S3Store, client: AlertClient
) -> AlertResult:
    """Open/update an issue for a stopped or error run; close what a good run resolved."""
    if not config.is_report_key(report_key):
        raise ValueError(f"{report_key!r} is not a run report under {config.runs_prefix!r}")
    data = store.get_json(config.raw_bucket, report_key)
    if data is None:
        raise FileNotFoundError(f"s3://{config.raw_bucket}/{report_key} not found")
    run = TransformRun.from_report(data, f"s3://{config.raw_bucket}/{report_key}")
    event_id = f"run-{run.run_id}"
    log_event(logger, "alert_started", event_id=event_id, outcome=run.outcome, feeds=run.feeds)
    if _already_notified(store, config, event_id):
        return _log_result(AlertResult(event_id, AlertOutcome.ALREADY_NOTIFIED))

    if run.outcome in ("stopped", "error"):
        result = _raise_or_comment(
            client, config, run.alert_key, transform_title(run), transform_body(run, config),
            transform_comment(run, config), ("transform", run.outcome), event_id,
        )  # fmt: skip
        _save_record(store, config, result, run.alert_key)
    elif run.outcome in ("passed", "warned") and run.feeds:
        resolved = [
            t for t, key in _open_alerts(client, config)
            if (feeds := feeds_of_key(key)) is not None and feeds <= set(run.feeds)
        ]  # fmt: skip
        for ticket in resolved:
            client.add_comment(ticket, resolved_comment(run, config))
            client.close_ticket(ticket)
        result = AlertResult(event_id, AlertOutcome.CLOSED if resolved
                             else AlertOutcome.NOTHING_TO_DO,
                             tuple(t.number for t in resolved))  # fmt: skip
        if resolved:  # a no-op is safe to repeat; no record needed
            _save_record(store, config, result, "transform-resolved")
    else:
        result = AlertResult(event_id, AlertOutcome.NOTHING_TO_DO)
    return _log_result(result)


@dataclass(frozen=True)
class AlarmChange:
    """The public-safe view of a CloudWatch alarm state change (SNS message)."""

    name: str
    state: str
    time: str
    reason: str

    @classmethod
    def from_message(cls, message: dict[str, Any]) -> AlarmChange:
        return cls(
            name=_safe(message.get("AlarmName"), _ALARM_NAME),
            state=str(message.get("NewStateValue") or ""),
            time=_safe(message.get("StateChangeTime"), _TIMESTAMP),
            reason=truncate(" ".join(str(message.get("NewStateReason") or "").split()), 300),
        )

    @property
    def event_id(self) -> str:
        return f"alarm-{_digest([self.name, self.state, self.time])}"

    @property
    def alert_key(self) -> str:
        return f"alarm:{self.name}"


def alarm_title(alarm: AlarmChange) -> str:
    return truncate(f"[Alarm] {alarm.name}", MAX_TITLE_CHARS)


def alarm_body(alarm: AlarmChange, config: AlertConfig) -> str:
    console = (f"https://{config.region}.console.aws.amazon.com/cloudwatch/home"
               f"?region={config.region}#alarmsV2:alarm/{alarm.name}")  # fmt: skip
    return "\n".join([
        f"CloudWatch alarm {code(alarm.name)} went to {code('ALARM')} at {code(alarm.time)} UTC.",
        "", f"Reason: {code(alarm.reason)}",
        "", "### Next steps", "", config.alarm_next_step(alarm.name),
        "", f"Alarm: [console]({console}). This issue closes itself when the alarm returns "
        "to OK.",
        "", alert_key_line(alarm.alert_key), "",
    ])  # fmt: skip


def notify_alarm(
    message: dict[str, Any], *, config: AlertConfig, store: S3Store, client: AlertClient
) -> AlertResult:
    """ALARM -> open/update the alarm's issue; OK -> comment + close it."""
    alarm = AlarmChange.from_message(message)
    log_event(logger, "alert_started", event_id=alarm.event_id, alarm=alarm.name,
              state=alarm.state)  # fmt: skip
    if alarm.name == REDACTED or alarm.state not in ("ALARM", "OK"):
        return _log_result(AlertResult(alarm.event_id, AlertOutcome.NOTHING_TO_DO))
    if _already_notified(store, config, alarm.event_id):
        return _log_result(AlertResult(alarm.event_id, AlertOutcome.ALREADY_NOTIFIED))

    if alarm.state == "ALARM":
        comment = (f"Alarm {code(alarm.name)} went to {code('ALARM')} again at "
                   f"{code(alarm.time)} UTC. Reason: {code(alarm.reason)}")  # fmt: skip
        result = _raise_or_comment(client, config, alarm.alert_key, alarm_title(alarm),
                                   alarm_body(alarm, config), comment, ("alarm",),
                                   alarm.event_id)  # fmt: skip
    else:
        existing = [t for t, key in _open_alerts(client, config) if key == alarm.alert_key]
        for ticket in existing:
            client.add_comment(ticket, f"Alarm {code(alarm.name)} is back to {code('OK')} at "
                                       f"{code(alarm.time)} UTC. Closing.")  # fmt: skip
            client.close_ticket(ticket)
        result = AlertResult(alarm.event_id,
                             AlertOutcome.CLOSED if existing else AlertOutcome.NOTHING_TO_DO,
                             tuple(t.number for t in existing))  # fmt: skip
        if not existing:
            return _log_result(result)
    _save_record(store, config, result, alarm.alert_key)
    return _log_result(result)


def _log_result(result: AlertResult) -> AlertResult:
    log_event(logger, "alert_finished", **result.to_dict())
    return result
