"""Pipeline alerts -> GitHub issues: transform run reports and CloudWatch alarms.

moto S3 (reports, alert records) + responses (GitHub). responses fails any request the
test did not register, so "no API calls" is enforced, not assumed.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import responses

from claims_etl.ingestion.config import ConfigError
from claims_etl.notifications import lambda_handler
from claims_etl.notifications.alert_config import load_alert_config
from claims_etl.notifications.alerts import (
    AlertOutcome,
    TransformRun,
    alert_key_line,
    feeds_of_key,
    notify_alarm,
    notify_transform_report,
)
from claims_etl.notifications.github_client import API_ROOT, GitHubClient, Ticket
from tests.conftest import RAW_BUCKET

TOKEN = "github_pat_TEST_SECRET_DO_NOT_LOG_789"
REPO = "owner/repo"
ISSUES = f"{API_ROOT}/repos/{REPO}/issues"
LABELS = f"{API_ROOT}/repos/{REPO}/labels"
STOP_TEST = "unique_stg_cclf__claims_header_claim_id"
STOP_UID = f"test.claims_lakehouse.{STOP_TEST}.abc123"
MODEL_UID = "model.claims_lakehouse.int_cclf__claim_families"


@pytest.fixture
def acfg(config_dir: Path):
    return load_alert_config(config_dir)


@pytest.fixture
def client() -> GitHubClient:
    return GitHubClient(TOKEN, REPO, jitter=False, sleep=lambda _s: None)


@pytest.fixture
def api():
    with responses.RequestsMock() as rsps:
        yield rsps


def _report(run_id: str = "20261105T202804Z-b009e1f2", outcome: str = "stopped",
            selectors=("source:cclf+", "source:humana+"), **extra: Any) -> dict:  # fmt: skip
    data = {
        "run_id": run_id,
        "started_at": "2026-11-05T20:28:04+00:00",
        "finished_at": "2026-11-05T20:28:23+00:00",
        "target": "lambda",
        "state_uri": "s3://raw/_transform/state/claims_lakehouse.duckdb",
        "selectors": list(selectors),
        "delivery_months": ["2023-04"],
        "triggers": [{"feed": "cms/cclf/", "file_type": "claims_header",
                      "delivery_month": "2023-04", "file_name": "CCLF1_202304.csv",
                      "checksum": "ab" * 32, "batch_id": "b-1", "redelivered": False}],
        "outcome": outcome,
        "published": outcome != "error",
        "counts": {"pass": 140, "fail": 1, "skipped": 2} if outcome == "stopped" else {"pass": 143},
        "stops": [{"unique_id": STOP_UID, "name": STOP_TEST, "failures": 3}]
        if outcome == "stopped" else [],
        "warns": [],
        "errors": [],
        "skipped": [],
        "exception": None,
        "artifacts": {"run_results": f"s3://{RAW_BUCKET}/_transform/runs/{run_id}/run_results.json"},
    }  # fmt: skip
    data.update(extra)
    return data


def _put_report(s3, data: dict) -> str:
    key = f"_transform/runs/{data['run_id']}/report.json"
    s3.put_json(RAW_BUCKET, key, data)
    return key


def _issue(number: int, key: str) -> dict:
    return {"number": number, "html_url": f"https://gh/{number}",
            "body": f"some text\n\n{alert_key_line(key)}\n"}  # fmt: skip


def _mock_create(api, number: int = 50, open_issues: list | None = None,
                 new_labels: int = 2) -> None:  # fmt: skip
    """No matching open issue; `new_labels` labels to create; then the new issue."""
    api.get(ISSUES, json=open_issues or [])
    api.get(LABELS, json=[{"name": "pipeline-alert"}])
    for _ in range(new_labels):
        api.post(LABELS, status=201, json={})
    api.post(ISSUES, status=201, json={"number": number, "html_url": f"https://gh/{number}",
                                       "body": ""})  # fmt: skip


def _posted(api, url: str, method: str = "POST") -> list[dict]:
    return [json.loads(c.request.body) for c in api.calls
            if c.request.method == method and c.request.url.split("?")[0] == url]  # fmt: skip


# --- transform reports ------------------------------------------------------------------------


def test_stopped_run_opens_issue_with_tests_counts_and_key(s3, acfg, client, api) -> None:
    key = _put_report(s3, _report())
    _mock_create(api)

    result = notify_transform_report(key, config=acfg, store=s3, client=client)

    assert (result.outcome, result.ticket_numbers) == (AlertOutcome.CREATED, (50,))
    [issue] = _posted(api, ISSUES)
    assert issue["title"] == f"[Transform stopped] cclf+humana: {STOP_TEST}"
    assert issue["labels"] == ["pipeline-alert", "transform", "stopped"]
    body = issue["body"]
    assert f"| `{STOP_TEST}` | 3 |" in body
    assert "`CCLF1_202304.csv`" in body and "`2023-04`" in body
    assert f"_transform/runs/{_report()['run_id']}/report.json" in body
    run = TransformRun.from_report(_report(), "s3://x")
    assert alert_key_line(run.alert_key) in body
    assert [label["name"] for label in _posted(api, LABELS)] == ["transform", "stopped"]


def test_same_run_twice_makes_no_api_calls(s3, acfg, client, api) -> None:
    key = _put_report(s3, _report())
    _mock_create(api)
    notify_transform_report(key, config=acfg, store=s3, client=client)
    calls = len(api.calls)

    again = notify_transform_report(key, config=acfg, store=s3, client=client)

    assert again.outcome is AlertOutcome.ALREADY_NOTIFIED
    assert len(api.calls) == calls


def test_same_problem_in_a_later_run_comments_on_the_open_issue(s3, acfg, client, api) -> None:
    alert_key = TransformRun.from_report(_report(), "s3://x").alert_key
    key = _put_report(s3, _report(run_id="20261106T000000Z-later001"))
    api.get(ISSUES, json=[_issue(7, "alarm:other"), _issue(8, alert_key)])
    api.post(f"{ISSUES}/8/comments", status=201, json={"html_url": "https://gh/8#c"})

    result = notify_transform_report(key, config=acfg, store=s3, client=client)

    assert (result.outcome, result.ticket_numbers) == (AlertOutcome.COMMENTED, (8,))
    [comment] = _posted(api, f"{ISSUES}/8/comments")
    assert "20261106T000000Z-later001" in comment["body"]


def test_error_run_never_posts_messages_or_exception_text(s3, acfg, client, api) -> None:
    secret = "SMITH"  # a data value quoted by a SQL error
    data = _report(
        outcome="error",
        errors=[{"unique_id": MODEL_UID,
                 "message": f"Conversion Error: Could not convert string '{secret}' to INT32"}],
        exception=f"TransformRunError: dbt exited 2: Could not convert '{secret}'",
        triggers=[{"file_name": "CCLF1_202304.csv"}, {"file_name": "x`@evil [link](http://a)"}],
    )  # fmt: skip
    key = _put_report(s3, data)
    _mock_create(api)

    notify_transform_report(key, config=acfg, store=s3, client=client)

    [issue] = _posted(api, ISSUES)
    text = issue["title"] + issue["body"]
    assert issue["title"] == "[Transform error] cclf+humana: int_cclf__claim_families failed"
    assert issue["labels"] == ["pipeline-alert", "transform", "error"]
    for leaked in (secret, "Conversion", "dbt exited", "@evil", "http://a"):
        assert leaked not in text
    assert f"`{MODEL_UID}`" in text and "`TransformRunError`" in text and "<redacted>" in text


def test_crash_without_node_errors_is_keyed_by_exception_type(s3, acfg, client, api) -> None:
    data = _report(outcome="error", exception="TransformRunError: dbt exited 2: boom")
    key = _put_report(s3, data)
    _mock_create(api)

    notify_transform_report(key, config=acfg, store=s3, client=client)

    [issue] = _posted(api, ISSUES)
    assert issue["title"] == "[Transform error] cclf+humana: build crashed (TransformRunError)"


def test_good_run_closes_issues_for_the_feeds_it_built(s3, acfg, client, api) -> None:
    key = _put_report(s3, _report(run_id="20261106T000000Z-pass0001", outcome="passed"))
    open_issues = [
        _issue(1, "transform-stopped:cclf:aaaaaaaaaaaa"),
        _issue(2, "transform-error:cclf+humana:bbbbbbbbbbbb"),
        _issue(3, "alarm:claims-etl-ingest-errors"),  # alarms close only on OK
        _issue(4, "transform-stopped:cclf+other:cccccccccccc"),  # feed not built here
    ]
    api.get(ISSUES, json=open_issues)
    for number in (1, 2):
        api.post(f"{ISSUES}/{number}/comments", status=201, json={"html_url": "https://gh/c"})
        api.patch(f"{ISSUES}/{number}", json={})

    result = notify_transform_report(key, config=acfg, store=s3, client=client)

    assert (result.outcome, result.ticket_numbers) == (AlertOutcome.CLOSED, (1, 2))
    assert _posted(api, f"{ISSUES}/1", "PATCH") == [
        {"state": "closed", "state_reason": "completed"}
    ]
    assert "Resolved" in _posted(api, f"{ISSUES}/2/comments")[0]["body"]


def test_good_run_for_one_feed_leaves_a_two_feed_issue_open(s3, acfg, client, api) -> None:
    key = _put_report(s3, _report(outcome="warned", selectors=["source:cclf+"]))
    api.get(ISSUES, json=[_issue(2, "transform-error:cclf+humana:bbbbbbbbbbbb")])

    result = notify_transform_report(key, config=acfg, store=s3, client=client)

    assert result.outcome is AlertOutcome.NOTHING_TO_DO
    assert not _posted(api, f"{ISSUES}/2", "PATCH")
    assert s3.get_json(RAW_BUCKET, f"{acfg.record_prefix}run-{_report()['run_id']}.json") is None


def test_issue_is_keyed_by_the_failing_tests_feed_and_a_one_feed_run_closes_it(
    s3, acfg, client, api
) -> None:
    """Issue #12: a run built cclf+humana and stopped on a Humana-only test; the fix run
    built only Humana. The issue must name Humana alone, so that run resolves it."""
    stop = {"unique_id": "test.claims_lakehouse.not_null_stg_humana__x_mbi.1",
            "name": "not_null_stg_humana__x_mbi", "failures": 1, "feeds": ["humana"]}  # fmt: skip
    key = _put_report(s3, _report(stops=[stop]))
    _mock_create(api)
    notify_transform_report(key, config=acfg, store=s3, client=client)
    [issue] = _posted(api, ISSUES)
    alert_key = TransformRun.from_report(_report(stops=[stop]), "s3://x").alert_key
    assert alert_key.startswith("transform-stopped:humana:")
    assert issue["title"] == "[Transform stopped] humana: not_null_stg_humana__x_mbi"
    assert "| Feeds built | `cclf`, `humana` |" in issue["body"]
    assert "| Problem in | `humana` |" in issue["body"]

    api.reset()
    fix = _put_report(s3, _report(run_id="20261106T193914Z-5ade588b", outcome="passed",
                                  selectors=["source:humana+"]))  # fmt: skip
    api.get(ISSUES, json=[_issue(12, alert_key)])
    api.post(f"{ISSUES}/12/comments", status=201, json={"html_url": "https://gh/12#c"})
    api.patch(f"{ISSUES}/12", json={})
    result = notify_transform_report(fix, config=acfg, store=s3, client=client)
    assert (result.outcome, result.ticket_numbers) == (AlertOutcome.CLOSED, (12,))


def test_problem_feeds_fall_back_to_the_built_feeds() -> None:
    # Reports written before the runner recorded node feeds, and unknown feed names.
    old = TransformRun.from_report(_report(), "s3://x")
    assert old.problem_feeds == ("cclf", "humana")
    stray = {"name": STOP_TEST, "failures": 1, "feeds": ["aetna"]}  # not a feed the run built
    assert TransformRun.from_report(_report(stops=[stray]), "s3://x").problem_feeds == (
        "cclf", "humana")  # fmt: skip
    error = _report(outcome="error", errors=[{"unique_id": MODEL_UID, "feeds": ["cclf"]}])
    assert TransformRun.from_report(error, "s3://x").alert_key.startswith("transform-error:cclf:")


def test_feeds_of_key() -> None:
    assert feeds_of_key("transform-stopped:cclf+humana:abc") == {"cclf", "humana"}
    assert feeds_of_key("alarm:claims-etl-ingest-errors") is None


# --- CloudWatch alarms ------------------------------------------------------------------------


def _alarm(state: str = "ALARM", at: str = "2026-11-05T21:00:00.000+0000",
           name: str = "claims-etl-transform-dlq-not-empty") -> dict:  # fmt: skip
    return {"AlarmName": name, "NewStateValue": state, "OldStateValue": "OK",
            "NewStateReason": "Threshold Crossed: 1 datapoint [1.0] was greater than 0.0",
            "StateChangeTime": at}  # fmt: skip


def test_alarm_opens_issue_then_comments_then_closes_on_ok(s3, acfg, client, api) -> None:
    _mock_create(api, number=60, new_labels=1)
    first = notify_alarm(_alarm(), config=acfg, store=s3, client=client)
    [issue] = _posted(api, ISSUES)
    assert issue["title"] == "[Alarm] claims-etl-transform-dlq-not-empty"
    assert issue["labels"] == ["pipeline-alert", "alarm"]
    assert "redrive" in issue["body"]  # the alarm's own next step from notifications.yml
    assert first.outcome is AlertOutcome.CREATED

    open_issue = _issue(60, "alarm:claims-etl-transform-dlq-not-empty")
    api.get(ISSUES, json=[open_issue])
    api.post(f"{ISSUES}/60/comments", status=201, json={"html_url": "https://gh/60#c"})
    again = notify_alarm(_alarm(at="2026-11-05T22:00:00.000+0000"), config=acfg, store=s3,
                         client=client)  # fmt: skip
    assert again.outcome is AlertOutcome.COMMENTED

    api.patch(f"{ISSUES}/60", json={})
    ok = notify_alarm(_alarm("OK", at="2026-11-05T22:10:00.000+0000"), config=acfg, store=s3,
                      client=client)  # fmt: skip
    assert (ok.outcome, ok.ticket_numbers) == (AlertOutcome.CLOSED, (60,))
    assert _posted(api, f"{ISSUES}/60", "PATCH") == [
        {"state": "closed", "state_reason": "completed"}
    ]


def test_alarm_redelivery_and_irrelevant_states_make_no_api_calls(s3, acfg, client, api) -> None:
    _mock_create(api, new_labels=1)
    notify_alarm(_alarm(), config=acfg, store=s3, client=client)
    calls = len(api.calls)

    assert notify_alarm(_alarm(), config=acfg, store=s3,
                        client=client).outcome is AlertOutcome.ALREADY_NOTIFIED  # fmt: skip
    assert notify_alarm(_alarm("INSUFFICIENT_DATA"), config=acfg, store=s3,
                        client=client).outcome is AlertOutcome.NOTHING_TO_DO  # fmt: skip
    assert notify_alarm(_alarm(name="bad name; @x"), config=acfg, store=s3,
                        client=client).outcome is AlertOutcome.NOTHING_TO_DO  # fmt: skip
    assert len(api.calls) == calls


def test_ok_without_an_open_issue_does_nothing(s3, acfg, client, api) -> None:
    api.get(ISSUES, json=[])
    result = notify_alarm(_alarm("OK"), config=acfg, store=s3, client=client)
    assert result.outcome is AlertOutcome.NOTHING_TO_DO


# --- handler routing + config + client ---------------------------------------------------------


def test_handler_routes_reports_and_alarms_and_ignores_the_rest(
    s3, config_dir, client, api, monkeypatch
) -> None:  # fmt: skip
    monkeypatch.setenv("CLAIMS_ETL_CONFIG_DIR", str(config_dir))
    monkeypatch.setattr(GitHubClient, "from_settings", classmethod(lambda cls: client))
    key = _put_report(s3, _report())
    api.get(ISSUES, json=[])
    api.get(LABELS, json=[{"name": "pipeline-alert"}, {"name": "transform"},
                          {"name": "stopped"}, {"name": "alarm"}])  # fmt: skip
    api.post(ISSUES, status=201, json={"number": 70, "html_url": "https://gh/70", "body": ""})
    api.post(ISSUES, status=201, json={"number": 71, "html_url": "https://gh/71", "body": ""})
    event = {"Records": [
        {"s3": {"bucket": {"name": RAW_BUCKET}, "object": {"key": key}}},
        {"s3": {"bucket": {"name": RAW_BUCKET},
                "object": {"key": "_transform/runs/x/run_results.json"}}},
        {"EventSource": "aws:sns", "Sns": {"Message": json.dumps(_alarm())}},
    ]}  # fmt: skip

    result = lambda_handler.handler(event, None)

    assert sorted(r["outcome"] for r in result["results"]) == ["created", "created"]
    assert sorted(i["title"].split("]")[0] for i in _posted(api, ISSUES)) == [
        "[Alarm", "[Transform stopped",
    ]  # fmt: skip


def test_handler_raises_after_trying_every_record(s3, config_dir, client, api, monkeypatch) -> None:
    monkeypatch.setenv("CLAIMS_ETL_CONFIG_DIR", str(config_dir))
    monkeypatch.setattr(GitHubClient, "from_settings", classmethod(lambda cls: client))
    missing = "_transform/runs/missing-run/report.json"
    event = {"Records": [{"s3": {"bucket": {"name": RAW_BUCKET}, "object": {"key": missing}}}]}
    with pytest.raises(lambda_handler.NotificationFailedError, match="missing-run"):
        lambda_handler.handler(event, None)


def test_is_report_key(acfg) -> None:
    assert acfg.is_report_key("_transform/runs/20261105T202804Z-b009e1f2/report.json")
    for key in (
        "_transform/runs/a/b/report.json",
        "_transform/runs//report.json",
        "_transform/state/report.json",
        "_transform/runs/a/run_results.json",
    ):
        assert not acfg.is_report_key(key), key  # fmt: skip


def test_alert_config_requires_its_labels(config_dir: Path) -> None:
    path = config_dir / "notifications.yml"
    text = path.read_text(encoding="utf-8").replace("    stopped: {color", "    stoped: {color")
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigError, match="stopped"):
        load_alert_config(config_dir)


def test_close_ticket_patches_the_issue(client, api) -> None:
    api.patch(f"{ISSUES}/9", json={})
    client.close_ticket(Ticket(number=9, url="https://gh/9", body=""))
    assert _posted(api, f"{ISSUES}/9", "PATCH") == [
        {"state": "closed", "state_reason": "completed"}
    ]
