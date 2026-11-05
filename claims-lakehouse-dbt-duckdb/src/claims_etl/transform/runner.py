"""run_transform: one dbt build of staging + intermediate for a batch of FileIngested events.

1. Map each event's feed to its dbt source and select the WHOLE feed (`source:cclf+`),
   not just the delivered file type. Every STOP test of the feed then runs on every
   build, so a file that failed a stop earlier keeps blocking intermediate even when
   a different file type of the same feed arrives next.
2. Fetch the DuckDB state into a working folder; run `dbt build` against the copy, with
   `delivery_months` set to the events' months (scopes the WARN tests only). dbt runs
   in a subprocess: its exit closes (and checkpoints) the DuckDB file before it is
   published, and a warm Lambda keeps no dbt state between invocations.
3. Classify the outcome from run_results.json:
     passed  - everything passed
     warned  - WARN tests found issues; intermediate was built
     stopped - a STOP test failed; dbt skipped the intermediate models below it
     error   - a model or test could not run (SQL error, S3 trouble) or dbt crashed
4. passed / warned / stopped: publish the working copy as the new state (dbt only built
   what was safe; a stopped feed stays pending, because its raw checksum is not in
   intermediate yet), then write the run report. These are final answers.
   error: keep the old state, write the report, raise TransformRunError so the queue
   retries the batch (and finally dead-letters it).

The report holds file metadata, test names and failure COUNTS only; never row values.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import tempfile
import uuid
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from claims_etl.events import FileIngested
from claims_etl.ingestion.log import log_event
from claims_etl.transform.config import TransformConfig
from claims_etl.transform.state import StateStore

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR_ENV_VAR = "CLAIMS_DBT_PROJECT_DIR"
_REPO_DBT_DIR = Path(__file__).resolve().parents[3] / "dbt"
_MAX_MESSAGE_CHARS = 500
_DBT_CRASHED = 2  # dbt exit codes: 0 success, 1 a node failed, 2 dbt itself failed


class Outcome(StrEnum):
    PASSED = "passed"
    WARNED = "warned"
    STOPPED = "stopped"
    ERROR = "error"


class TransformRunError(RuntimeError):
    """The build could not run (not a data problem); the batch should be retried."""


class ArtifactSink(Protocol):
    """Where run reports and run_results.json go (S3 in Lambda, a folder locally)."""

    def put_json(self, name: str, data: dict[str, Any]) -> str: ...

    def put_file(self, name: str, path: Path) -> str: ...


@dataclass
class TransformReport:
    run_id: str
    started_at: str
    target: str
    state_uri: str
    selectors: list[str]
    delivery_months: list[str] | None
    triggers: list[dict[str, Any]]
    outcome: Outcome = Outcome.ERROR
    published: bool = False
    counts: dict[str, int] = field(default_factory=dict)
    stops: list[dict[str, Any]] = field(default_factory=list)
    warns: list[dict[str, Any]] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    exception: str | None = None
    artifacts: dict[str, str] = field(default_factory=dict)
    finished_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = dict(self.__dict__)
        data["outcome"] = self.outcome.value
        return data


def new_run_id() -> str:
    return f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def selectors_for(feeds: Sequence[str], config: TransformConfig) -> list[str]:
    """`source:<dbt source>+` for each distinct feed, in a stable order."""
    return sorted({f"source:{config.source_for(feed)}+" for feed in feeds})


def dbt_project_dir() -> Path:
    return Path(os.environ.get(DBT_PROJECT_DIR_ENV_VAR, _REPO_DBT_DIR))


def run_transform(
    events: Sequence[FileIngested],
    *,
    config: TransformConfig,
    state: StateStore,
    sink: ArtifactSink,
    feeds: Sequence[str] = (),
    delivery_months: Sequence[str] | None = None,
    extra_vars: dict[str, Any] | None = None,
    run_id: str | None = None,
    work_dir: Path | None = None,
) -> TransformReport:
    """Build the feeds named by the events (plus `feeds`), publish, report.

    delivery_months defaults to the events' months; pass [] to run every WARN test
    over all history. Raises TransformRunError on an `error` outcome, after the
    report is written.
    """
    all_feeds = [e.feed for e in events] + list(feeds)
    if not all_feeds:
        raise ValueError("nothing to build: no events and no feeds")
    if delivery_months is None:
        delivery_months = [e.delivery_month for e in events]
    months = sorted(set(delivery_months)) or None
    report = TransformReport(
        run_id=run_id or new_run_id(),
        started_at=datetime.now(UTC).isoformat(timespec="seconds"),
        target=config.dbt_target,
        state_uri=state.describe(),
        selectors=selectors_for(all_feeds, config),
        delivery_months=months,
        triggers=[_trigger(e) for e in events],
    )
    log_event(logger, "transform_started", run_id=report.run_id, selectors=report.selectors,
              delivery_months=months, triggers=len(events))  # fmt: skip

    with tempfile.TemporaryDirectory(prefix="claims_transform_", dir=work_dir) as tmp:
        work = Path(tmp)
        db = work / "claims_lakehouse.duckdb"
        try:
            state.fetch(db)
            run_results = _dbt_build(report, db, work, extra_vars or {})
            _classify(report, run_results)
            if report.outcome is not Outcome.ERROR:
                state.publish(db)
                report.published = True
        except Exception as exc:  # noqa: BLE001 - reported, then re-raised as retryable
            report.outcome = Outcome.ERROR
            report.exception = f"{type(exc).__name__}: {exc}"[:_MAX_MESSAGE_CHARS]
            logger.exception("transform_crashed", extra={"fields": {"run_id": report.run_id}})
        results_file = work / "target" / "run_results.json"
        if results_file.is_file():
            report.artifacts["run_results"] = sink.put_file("run_results.json", results_file)
        report.finished_at = datetime.now(UTC).isoformat(timespec="seconds")
        report.artifacts["report"] = sink.put_json("report.json", report.to_dict())

    log_event(logger, "transform_finished", run_id=report.run_id, outcome=report.outcome.value,
              published=report.published, counts=report.counts,
              stops=[s["name"] for s in report.stops], warns=[w["name"] for w in report.warns],
              errors=[e["unique_id"] for e in report.errors])  # fmt: skip
    if report.outcome is Outcome.ERROR:
        raise TransformRunError(
            f"transform run {report.run_id} failed: "
            f"{report.exception or [e['unique_id'] for e in report.errors]}"
        )
    return report


def _trigger(event: FileIngested) -> dict[str, Any]:
    keys = ("feed", "file_type", "delivery_month", "file_name", "checksum", "batch_id",
            "redelivered")  # fmt: skip
    return {k: getattr(event, k) for k in keys}


def _dbt_build(
    report: TransformReport, db: Path, work: Path, extra_vars: dict[str, Any]
) -> dict[str, Any]:
    """Run `dbt build` in a subprocess; return run_results.json (raises if dbt crashed)."""
    project = dbt_project_dir()
    dbt_vars = dict(extra_vars)
    if report.delivery_months:
        dbt_vars["delivery_months"] = report.delivery_months
    command = [
        sys.executable, "-m", "dbt.cli.main", "build",
        "--select", *report.selectors,
        "--project-dir", str(project),
        "--profiles-dir", str(project),
        "--target", report.target,
        "--target-path", str(work / "target"),
        "--log-path", str(work / "logs"),
        "--vars", json.dumps(dbt_vars),
    ]  # fmt: skip
    env = {
        **os.environ,
        "CLAIMS_DUCKDB_PATH": str(db),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "PYTHONIOENCODING": "utf-8",
    }
    result = subprocess.run(
        command, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace"
    )
    log_event(logger, "dbt_finished", run_id=report.run_id, returncode=result.returncode)
    results_file = work / "target" / "run_results.json"
    if result.returncode == _DBT_CRASHED or not results_file.is_file():
        # Console output only here: test failure output elsewhere is counts, never rows.
        tail = result.stdout[-2000:] + result.stderr[-2000:]
        raise TransformRunError(f"dbt exited {result.returncode}: {tail}")
    return json.loads(results_file.read_text(encoding="utf-8"))


def _classify(report: TransformReport, run_results: dict[str, Any]) -> None:
    """Fill counts/stops/warns/errors/skipped and the outcome from run_results.json."""
    results = run_results.get("results", [])
    report.counts = dict(sorted(Counter(r["status"] for r in results).items()))
    for r in results:
        uid, status = r["unique_id"], r["status"]
        if status == "skipped":
            report.skipped.append(uid)
        elif status == "error":
            message = (r.get("message") or "")[:_MAX_MESSAGE_CHARS]
            report.errors.append({"unique_id": uid, "message": message})
        elif status in ("fail", "warn") and uid.startswith("test."):
            entry = {"unique_id": uid, "name": uid.split(".")[2], "failures": r.get("failures")}
            (report.stops if status == "fail" else report.warns).append(entry)
    report.skipped.sort()
    if report.errors:
        report.outcome = Outcome.ERROR
    elif report.stops:
        report.outcome = Outcome.STOPPED
    elif report.warns:
        report.outcome = Outcome.WARNED
    else:
        report.outcome = Outcome.PASSED
