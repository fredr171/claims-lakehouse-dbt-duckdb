"""Transform runner wiring without dbt: events, ingestion publishing, state, outcomes.

dbt itself is replaced by a stub returning a canned run_results.json; the real dbt
runs are in test_transform_dbt.py. S3 and EventBridge are moto; never touches AWS.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import boto3
import pytest

from claims_etl.events import (
    DETAIL_TYPE,
    EVENT_SOURCE,
    EventBridgePublisher,
    EventPublishError,
    FileIngested,
)
from claims_etl.ingestion.config import ConfigError
from claims_etl.ingestion.models import Outcome as IngestOutcome
from claims_etl.ingestion.pipeline import process_file
from claims_etl.transform import lambda_handler, runner
from claims_etl.transform.config import load_transform_config
from claims_etl.transform.runner import Outcome, TransformRunError, run_transform
from claims_etl.transform.sinks import LocalRunSink, S3RunSink
from claims_etl.transform.state import LocalStateStore, S3StateStore, StateConflictError
from tests.conftest import LANDING_BUCKET, LANDING_KEY, RAW_BUCKET, REGION

STATE_KEY = "_transform/state/claims_lakehouse.duckdb"
BACKUP_KEY = "_transform/state/claims_lakehouse.previous.duckdb"


def _event(feed: str = "cms/cclf/", month: str = "2023-01", **kw: Any) -> FileIngested:
    fields = {
        "feed": feed,
        "file_type": "claims_header" if feed == "cms/cclf/" else "medical_claim_lines",
        "delivery_month": month,
        "checksum": "ab" * 32,
        "raw_uri": "s3://raw/x",
        "file_name": "CCLF1_202301.csv",
        "batch_id": "batch-1",
    }
    return FileIngested(**{**fields, **kw})


class RecordingPublisher:
    def __init__(self, fail: bool = False) -> None:
        self.events: list[FileIngested] = []
        self.fail = fail

    def publish(self, event: FileIngested) -> None:
        if self.fail:
            raise EventPublishError("EventBridge unavailable")
        self.events.append(event)


# --- FileIngested ---------------------------------------------------------------------


def test_event_round_trips_and_validates() -> None:
    event = _event(redelivered=True)
    assert FileIngested.from_detail(json.loads(json.dumps(event.to_detail()))) == event
    with pytest.raises(ValueError, match="YYYY-MM"):
        _event(month="2023-13")
    with pytest.raises(ValueError, match="feed"):
        _event(feed="cms/cclf")


def test_eventbridge_publisher_raises_on_rejected_entry() -> None:
    class Rejecting:
        def put_events(self, Entries):  # noqa: N803 - boto3 parameter name
            return {"FailedEntryCount": 1, "Entries": [{"ErrorCode": "Throttled"}]}

    with pytest.raises(EventPublishError, match="Throttled"):
        EventBridgePublisher(Rejecting()).publish(_event())


def test_eventbridge_publisher_sends_source_and_detail_type(s3) -> None:
    client = boto3.client("events", region_name=REGION)
    sent = []
    client.meta.events.register(
        "provide-client-params.events.PutEvents", lambda params, **_: sent.append(params)
    )
    EventBridgePublisher(client).publish(_event())
    [entry] = sent[0]["Entries"]
    assert (entry["Source"], entry["DetailType"]) == (EVENT_SOURCE, DETAIL_TYPE)
    assert json.loads(entry["Detail"])["delivery_month"] == "2023-01"


# --- ingestion publishes ------------------------------------------------------------------


def test_success_and_duplicate_publish_quarantine_does_not(s3, config, valid_csv, tmp_path) -> None:
    publisher = RecordingPublisher()
    s3.upload(valid_csv, LANDING_BUCKET, LANDING_KEY)
    assert process_file(LANDING_KEY, config=config, store=s3, publisher=publisher).outcome is (
        IngestOutcome.SUCCESS
    )
    s3.upload(valid_csv, LANDING_BUCKET, LANDING_KEY)  # same file again
    assert process_file(LANDING_KEY, config=config, store=s3, publisher=publisher).outcome is (
        IngestOutcome.SKIPPED_DUPLICATE
    )
    first, again = publisher.events
    assert (first.feed, first.file_type, first.delivery_month) == (
        "cms/cclf/", "claims_header", "2023-01"
    )  # fmt: skip
    assert first.raw_uri.endswith("delivery_month=2023-01/part-0.parquet")
    assert (first.redelivered, again.redelivered) == (False, True)
    assert again.checksum == first.checksum

    bad = tmp_path / "bad.csv"
    bad.write_bytes(b"WRONG,HEADER\n1,2\n")
    s3.upload(bad, LANDING_BUCKET, LANDING_KEY)
    assert process_file(LANDING_KEY, config=config, store=s3, publisher=publisher).outcome is (
        IngestOutcome.QUARANTINED
    )
    assert len(publisher.events) == 2


def test_failed_publish_leaves_file_in_landing_and_retry_publishes(s3, config, valid_csv) -> None:
    s3.upload(valid_csv, LANDING_BUCKET, LANDING_KEY)
    failed = process_file(LANDING_KEY, config=config, store=s3, publisher=RecordingPublisher(True))
    assert failed.outcome is IngestOutcome.ERROR
    assert s3.exists(LANDING_BUCKET, LANDING_KEY)  # not archived: the retry sees it again

    publisher = RecordingPublisher()
    retry = process_file(LANDING_KEY, config=config, store=s3, publisher=publisher)
    assert retry.outcome is IngestOutcome.SKIPPED_DUPLICATE
    assert [e.redelivered for e in publisher.events] == [True]
    assert not s3.exists(LANDING_BUCKET, LANDING_KEY)


def test_publish_events_flag_must_be_boolean(config_dir: Path) -> None:
    path = config_dir / "ingestion.yml"
    path.write_text(path.read_text(encoding="utf-8") + "publish_events: 'yes'\n", encoding="utf-8")
    with pytest.raises(ConfigError, match="publish_events"):
        load_transform_config(config_dir)


# --- config, selectors, SQS parsing -------------------------------------------------------


def test_transform_config_maps_feeds_to_dbt_sources(config_dir: Path) -> None:
    config = load_transform_config(config_dir)
    assert config.feed_sources == {"cms/cclf/": "cclf", "humana/ma/": "humana"}
    assert config.raw_bucket == RAW_BUCKET
    events = [_event(), _event(month="2023-02"), _event("humana/ma/")]
    # The whole feed, never one file type: every STOP test of the feed runs each time.
    assert runner.selectors_for([e.feed for e in events], config) == [
        "source:cclf+", "source:humana+"
    ]  # fmt: skip


def test_transform_config_rejects_unmapped_feed(config_dir: Path) -> None:
    path = config_dir / "transform.yml"
    path.write_text(path.read_text(encoding="utf-8").replace("  humana/ma/: humana\n", ""),
                    encoding="utf-8")  # fmt: skip
    with pytest.raises(ConfigError, match="must map exactly"):
        load_transform_config(config_dir)


def _sqs_record(body: dict) -> dict:
    return {"messageId": "m", "body": json.dumps(body)}


def test_parse_sqs_event_keeps_only_file_ingested() -> None:
    event = _event()
    records = [
        _sqs_record({"source": EVENT_SOURCE, "detail-type": DETAIL_TYPE,
                     "detail": event.to_detail()}),
        _sqs_record({"source": "aws.s3", "detail-type": "Object Created", "detail": {}}),
    ]  # fmt: skip
    assert lambda_handler.parse_sqs_event({"Records": records}) == [event]


def test_handler_with_nothing_to_build_does_not_run() -> None:
    body = {"source": "other", "detail-type": "x", "detail": {}}
    assert lambda_handler.handler({"Records": [_sqs_record(body)]}, None) == {
        "outcome": "nothing_to_build"
    }


# --- outcomes (dbt stubbed) -------------------------------------------------------------------


def _results(*rows: tuple[str, str]) -> dict:
    return {"results": [{"unique_id": u, "status": s, "failures": 1 if s in ("fail", "warn")
                         else 0, "message": "Conversion Error"} for u, s in rows]}  # fmt: skip


MODEL = "model.claims_lakehouse.int_cclf__claim_families"
STOP = "test.claims_lakehouse.unique_stg_cclf__claims_header_claim_id.abc"
WARN = "test.claims_lakehouse.not_null_stg_cclf__claims_header_facility_ccn.def"


@pytest.fixture
def stub_dbt(monkeypatch: pytest.MonkeyPatch):
    """Replace the dbt subprocess: write `results` as run_results.json and the db file."""
    calls: list[dict] = []

    def install(results: dict | Exception):
        def fake(report, db: Path, work: Path, extra_vars: dict) -> dict:
            calls.append({"selectors": report.selectors, "months": report.delivery_months,
                          "vars": extra_vars})  # fmt: skip
            if isinstance(results, Exception):
                raise results
            db.write_bytes(b"built-" + report.run_id.encode())
            (work / "target").mkdir()
            (work / "target" / "run_results.json").write_text(json.dumps(results))
            return results

        monkeypatch.setattr(runner, "_dbt_build", fake)
        return calls

    return install


@pytest.mark.parametrize(
    ("rows", "outcome"),
    [
        ([(MODEL, "success"), (STOP, "pass")], Outcome.PASSED),
        ([(MODEL, "success"), (WARN, "warn")], Outcome.WARNED),
        ([(MODEL, "skipped"), (STOP, "fail"), (WARN, "warn")], Outcome.STOPPED),
    ],
)
def test_data_outcomes_publish_state_and_report(stub_dbt, config, tmp_path, rows, outcome) -> None:
    calls = stub_dbt(_results(*rows))
    state = LocalStateStore(tmp_path / "state.duckdb")
    report = run_transform(
        [_event(month="2023-02"), _event(month="2023-01")], config=_tconfig(tmp_path),
        state=state, sink=LocalRunSink(tmp_path / "runs", "run-1"), run_id="run-1",
    )  # fmt: skip
    assert report.outcome is outcome
    assert report.published and state.path.read_bytes() == b"built-run-1"
    assert calls[0]["selectors"] == ["source:cclf+"]
    assert calls[0]["months"] == ["2023-01", "2023-02"]
    saved = json.loads((tmp_path / "runs/run-1/report.json").read_text())
    assert saved["outcome"] == outcome.value
    assert (tmp_path / "runs/run-1/run_results.json").is_file()
    if outcome is Outcome.STOPPED:
        assert [s["name"] for s in saved["stops"]] == ["unique_stg_cclf__claims_header_claim_id"]
        assert saved["skipped"] == [MODEL]


def test_model_error_keeps_old_state_writes_report_and_raises(stub_dbt, tmp_path) -> None:
    stub_dbt(_results((MODEL, "error"), (STOP, "pass")))
    state = LocalStateStore(tmp_path / "state.duckdb")
    state.path.write_bytes(b"old")
    with pytest.raises(TransformRunError, match="int_cclf__claim_families"):
        run_transform([_event()], config=_tconfig(tmp_path), state=state,
                      sink=LocalRunSink(tmp_path / "runs", "r"), run_id="r")  # fmt: skip
    assert state.path.read_bytes() == b"old"
    saved = json.loads((tmp_path / "runs/r/report.json").read_text())
    assert (saved["outcome"], saved["published"]) == ("error", False)


def test_dbt_crash_is_reported_and_raised(stub_dbt, tmp_path) -> None:
    stub_dbt(TransformRunError("dbt exited 2: profile not found"))
    with pytest.raises(TransformRunError, match="profile not found"):
        run_transform([_event()], config=_tconfig(tmp_path),
                      state=LocalStateStore(tmp_path / "s.duckdb"),
                      sink=LocalRunSink(tmp_path / "runs", "r"), run_id="r")  # fmt: skip
    saved = json.loads((tmp_path / "runs/r/report.json").read_text())
    assert "profile not found" in saved["exception"]
    assert not (tmp_path / "s.duckdb").exists()


def test_cli_feeds_and_unscoped_months(stub_dbt, config_dir, tmp_path, capsys) -> None:
    from claims_etl.transform import cli

    calls = stub_dbt(_results((MODEL, "skipped"), (STOP, "fail")))
    code = cli.main([
        "--all", "--db", str(tmp_path / "s.duckdb"), "--config-dir", str(config_dir),
        "--runs-dir", str(tmp_path / "runs"), "--raw-root", str(tmp_path),
    ])  # fmt: skip
    assert code == cli.EXIT_STOPPED
    assert calls[0]["selectors"] == ["source:cclf+", "source:humana+"]
    assert calls[0]["months"] is None  # no --months: WARN tests cover all history
    assert calls[0]["vars"]["cclf_raw_root"].endswith("/cms/cclf")
    assert json.loads(capsys.readouterr().out)["outcome"] == "stopped"
    assert cli.main(["--feed", "aetna", "--config-dir", str(config_dir)]) == cli.EXIT_ERROR


def _tconfig(tmp_path: Path):
    from tests.conftest import write_test_config

    return load_transform_config(write_test_config(tmp_path / "tconfig"))


# --- S3 state: conditional upload -------------------------------------------------------------


def test_s3_state_first_publish_backup_and_conflict(s3, tmp_path) -> None:
    def store() -> S3StateStore:
        return S3StateStore(s3.client, RAW_BUCKET, STATE_KEY, BACKUP_KEY)

    work = tmp_path / "work.duckdb"
    first = store()
    assert first.fetch(work) is False
    work.write_bytes(b"v1")
    first.publish(work)

    a, b = store(), store()
    assert a.fetch(tmp_path / "a") and b.fetch(tmp_path / "b")
    (tmp_path / "a").write_bytes(b"v2")
    a.publish(tmp_path / "a")
    assert s3.client.get_object(Bucket=RAW_BUCKET, Key=BACKUP_KEY)["Body"].read() == b"v1"

    (tmp_path / "b").write_bytes(b"v2-from-b")
    with pytest.raises(StateConflictError):  # b started from v1; a already replaced it
        b.publish(tmp_path / "b")
    assert s3.client.get_object(Bucket=RAW_BUCKET, Key=STATE_KEY)["Body"].read() == b"v2"

    late = store()  # nobody fetched: "no state yet" must not overwrite an existing state
    late._fetched, late._etag = True, None
    with pytest.raises(StateConflictError):
        late.publish(tmp_path / "b")


def test_s3_run_sink_writes_under_run_prefix(s3, tmp_path) -> None:
    sink = S3RunSink(s3, RAW_BUCKET, "_transform/runs/", "run-9")
    assert sink.put_json("report.json", {"ok": 1}) == (
        f"s3://{RAW_BUCKET}/_transform/runs/run-9/report.json"
    )
    assert s3.get_json(RAW_BUCKET, "_transform/runs/run-9/report.json") == {"ok": 1}


# --- retry backoff (lambda_handler.retry_soon) ------------------------------------------------


def test_backoff_seconds_by_receive_count() -> None:
    delays = (120, 300, 600)
    assert [lambda_handler.backoff_seconds(n, delays) for n in (0, 1, 2, 3, 7)] == [
        120, 120, 300, 600, 600,
    ]  # fmt: skip


def test_queue_url_from_arn() -> None:
    arn = "arn:aws:sqs:us-east-2:123456789012:claims-etl-transform.fifo"
    assert lambda_handler.queue_url_from_arn(arn) == (
        "us-east-2",
        "https://sqs.us-east-2.amazonaws.com/123456789012/claims-etl-transform.fifo",
    )
    with pytest.raises(ValueError, match="not an SQS"):
        lambda_handler.queue_url_from_arn("arn:aws:sns:us-east-2:123456789012:topic")


class RecordingSqs:
    """Wraps the moto SQS client and records change_message_visibility_batch calls."""

    def __init__(self, client: Any, fail: bool = False) -> None:
        self._client, self.fail, self.calls = client, fail, []

    def change_message_visibility_batch(self, **kwargs: Any) -> dict:
        self.calls.append(kwargs)
        if self.fail:
            raise RuntimeError("SQS unavailable")
        return self._client.change_message_visibility_batch(**kwargs)


def _receive(sqs: Any, url: str) -> list[dict]:
    response = sqs.receive_message(
        QueueUrl=url, MaxNumberOfMessages=10, AttributeNames=["ApproximateReceiveCount"]
    )
    return response["Messages"]


def _received_batch(receives: int) -> tuple[list[dict], Any]:
    """Two real FIFO messages, received `receives` times before; Lambda-shaped records."""
    sqs = boto3.client("sqs", region_name=REGION)
    url = sqs.create_queue(
        QueueName="claims-etl-transform.fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )["QueueUrl"]
    arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    body = {"source": EVENT_SOURCE, "detail-type": DETAIL_TYPE, "detail": _event().to_detail()}
    for month in ("2023-01", "2023-02"):
        sqs.send_message(QueueUrl=url, MessageGroupId="transform",
                         MessageBody=json.dumps({**body, "id": month}))  # fmt: skip
    for _ in range(receives):
        messages = _receive(sqs, url)
        # Visible again at once, as after an expired visibility timeout.
        sqs.change_message_visibility_batch(
            QueueUrl=url,
            Entries=[
                {"Id": str(i), "ReceiptHandle": m["ReceiptHandle"], "VisibilityTimeout": 0}
                for i, m in enumerate(messages)
            ],
        )
    messages = _receive(sqs, url)
    records = [
        {"messageId": m["MessageId"], "receiptHandle": m["ReceiptHandle"], "body": m["Body"],
         "attributes": m["Attributes"], "eventSourceARN": arn, "awsRegion": REGION}
        for m in messages
    ]  # fmt: skip
    return records, sqs


@pytest.mark.parametrize(("earlier_receives", "delay"), [(0, 120), (1, 300), (2, 600)])
def test_failed_build_is_retried_after_backoff(s3, monkeypatch, earlier_receives, delay) -> None:
    records, sqs = _received_batch(earlier_receives)
    assert {r["attributes"]["ApproximateReceiveCount"] for r in records} == {
        str(earlier_receives + 1)
    }
    recorder = RecordingSqs(sqs)
    monkeypatch.setattr(lambda_handler, "_sqs_client", lambda region: recorder)

    def failing_build(*args: Any, **kwargs: Any) -> None:
        raise TransformRunError("dbt exited 2: crash")

    monkeypatch.setattr(lambda_handler, "run_transform", failing_build)
    with pytest.raises(TransformRunError, match="crash"):  # still raised: SQS retries it
        lambda_handler.handler({"Records": records}, None)
    [call] = recorder.calls
    assert call["QueueUrl"].endswith("/claims-etl-transform.fifo")
    assert sorted(e["ReceiptHandle"] for e in call["Entries"]) == sorted(
        r["receiptHandle"] for r in records
    )
    assert {e["VisibilityTimeout"] for e in call["Entries"]} == {delay}


def test_unparseable_batch_is_also_retried_after_backoff(s3, monkeypatch) -> None:
    records, sqs = _received_batch(0)
    records[0]["body"] = "not json"
    recorder = RecordingSqs(sqs)
    monkeypatch.setattr(lambda_handler, "_sqs_client", lambda region: recorder)
    with pytest.raises(json.JSONDecodeError):
        lambda_handler.handler({"Records": records}, None)
    assert {e["VisibilityTimeout"] for e in recorder.calls[0]["Entries"]} == {120}


@pytest.mark.parametrize("outcome", [Outcome.PASSED, Outcome.WARNED, Outcome.STOPPED])
def test_final_outcomes_leave_visibility_alone(s3, monkeypatch, outcome) -> None:
    records, sqs = _received_batch(0)
    recorder = RecordingSqs(sqs)
    monkeypatch.setattr(lambda_handler, "_sqs_client", lambda region: recorder)

    class Report:
        run_id, counts = "r", {}

    Report.outcome = outcome
    monkeypatch.setattr(lambda_handler, "run_transform", lambda *a, **k: Report())
    assert lambda_handler.handler({"Records": records}, None)["outcome"] == outcome.value
    assert recorder.calls == []


def test_backoff_failure_never_hides_the_build_error(s3, monkeypatch) -> None:
    records, sqs = _received_batch(0)
    recorder = RecordingSqs(sqs, fail=True)
    monkeypatch.setattr(lambda_handler, "_sqs_client", lambda region: recorder)

    def failing_build(*args: Any, **kwargs: Any) -> None:
        raise TransformRunError("state conflict")

    monkeypatch.setattr(lambda_handler, "run_transform", failing_build)
    with pytest.raises(TransformRunError, match="state conflict"):
        lambda_handler.handler({"Records": records}, None)
    assert len(recorder.calls) == 1


@pytest.mark.parametrize("value", [[], [120, "5"], [-1], [43201], [True], "120"])
def test_transform_config_rejects_bad_backoff(config_dir: Path, value: Any) -> None:
    path = config_dir / "transform.yml"
    setting = f"retry_backoff_seconds: {json.dumps(value)}\n"  # the last key wins in YAML
    path.write_text(path.read_text(encoding="utf-8") + setting, encoding="utf-8")
    with pytest.raises(ConfigError, match="retry_backoff_seconds"):
        load_transform_config(config_dir)


def test_transform_config_reads_backoff(config_dir: Path) -> None:
    assert load_transform_config(config_dir).retry_backoff_seconds == (120, 300, 600)
