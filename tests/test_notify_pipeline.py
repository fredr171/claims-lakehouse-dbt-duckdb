"""notify_quarantine end to end: real ingestion quarantines a fixture zip in moto S3,
then the notifier talks to a mocked GitHub API. No real S3 or GitHub."""

from __future__ import annotations

import json
from pathlib import Path

import boto3
import pytest
import responses
from moto import mock_aws

from claims_etl.ingestion.pipeline import process_zip
from claims_etl.notifications import cli, lambda_handler, settings
from claims_etl.notifications.config import load_notification_config
from claims_etl.notifications.github_client import API_ROOT, GitHubClient, TicketingError
from claims_etl.notifications.notifier import NotifyOutcome, notify_quarantine
from claims_etl.notifications.ticket_record import load_ticket_record
from tests.conftest import (
    COLUMNS,
    CSV_NAME,
    LANDING_BUCKET,
    LANDING_KEY,
    RAW_BUCKET,
    VALID_ROWS,
    csv_bytes,
    make_zip,
)

TOKEN = "github_pat_TEST_SECRET_DO_NOT_LOG_456"
REPO = "owner/repo"
ISSUES = f"{API_ROOT}/repos/{REPO}/issues"
LABELS = f"{API_ROOT}/repos/{REPO}/labels"
ERROR_KEY = f"quarantine/{LANDING_KEY}.error.json"


@pytest.fixture
def ncfg(config_dir: Path):
    return load_notification_config(config_dir)


@pytest.fixture
def client() -> GitHubClient:
    return GitHubClient(TOKEN, REPO, jitter=False, sleep=lambda _s: None)


@pytest.fixture
def api():
    with responses.RequestsMock() as rsps:
        yield rsps


def _quarantine(s3, config, tmp_path: Path, rows=None, header=COLUMNS) -> dict:
    """Run real ingestion on a broken zip so it writes a genuine .error.json."""
    data = csv_bytes(VALID_ROWS if rows is None else rows, header=header)
    path = make_zip(tmp_path / f"bad_{len(list(tmp_path.iterdir()))}.zip", {CSV_NAME: data})
    s3.upload(path, LANDING_BUCKET, LANDING_KEY)
    record = process_zip(LANDING_KEY, config=config, store=s3)
    assert record.outcome.value == "quarantined"
    return record.to_dict()


def _mock_create(api, number: int = 41) -> None:
    api.get(ISSUES, json=[])
    api.get(LABELS, json=[{"name": "quarantine"}])
    api.post(LABELS, status=201, json={})  # data-quality
    api.post(LABELS, status=201, json={})  # beneficiary_summary
    api.post(
        ISSUES, status=201, json={"number": number, "html_url": f"https://gh/{number}", "body": ""}
    )


def _posted_issue(api) -> dict:
    [call] = [c for c in api.calls if c.request.method == "POST" and c.request.url == ISSUES]
    return json.loads(call.request.body)


def test_new_issue_created_with_title_body_labels(s3, config, ncfg, client, api, tmp_path) -> None:
    report = _quarantine(s3, config, tmp_path, rows=[VALID_ROWS[0], ["", "19430101", "39"]])
    _mock_create(api)

    result = notify_quarantine(ERROR_KEY, config=ncfg, store=s3, client=client)

    assert result.outcome is NotifyOutcome.CREATED
    assert (result.ticket_number, result.ticket_url) == (41, "https://gh/41")
    issue = _posted_issue(api)
    assert issue["title"] == "[Quarantine] beneficiary_summary 2008 sample 1: csv_key_columns"
    assert issue["labels"] == ["quarantine", "data-quality", "beneficiary_summary"]
    assert f"Checksum: `{report['checksum_sha256']}`" in issue["body"]
    assert report["batch_id"] in issue["body"]
    assert "line 3: DESYNPUF_ID" in issue["body"]


def test_duplicate_checksum_adds_comment_not_issue(s3, config, ncfg, client, api, tmp_path) -> None:
    report = _quarantine(s3, config, tmp_path, rows=[])
    body = f"old ticket\n\nChecksum: `{report['checksum_sha256']}`\n"
    page2 = f"{ISSUES}?page=2"
    api.get(
        ISSUES,
        json=[{"number": 5, "html_url": "https://gh/5", "body": "Checksum: `" + "0" * 64 + "`"}],
        headers={"Link": f'<{page2}>; rel="next"'},
    )
    api.get(page2, json=[{"number": 6, "html_url": "https://gh/6", "body": body}])
    api.post(f"{ISSUES}/6/comments", status=201, json={"html_url": "https://gh/6#c1"})

    result = notify_quarantine(ERROR_KEY, config=ncfg, store=s3, client=client)

    assert result.outcome is NotifyOutcome.COMMENTED
    assert (result.ticket_number, result.comment_url) == (6, "https://gh/6#c1")
    assert not [c for c in api.calls if c.request.url == ISSUES and c.request.method == "POST"]
    comment = json.loads(api.calls[-1].request.body)["body"]
    assert "Quarantined again" in comment and report["batch_id"] in comment


def test_ticket_record_written_and_manifest_untouched(
    s3, config, ncfg, client, api, tmp_path
) -> None:
    report = _quarantine(s3, config, tmp_path, rows=[])
    manifest_key = f"_manifests/{report['checksum_sha256']}.json"
    manifest_before = s3.get_json(RAW_BUCKET, manifest_key)
    _mock_create(api)

    notify_quarantine(ERROR_KEY, config=ncfg, store=s3, client=client)

    record = load_ticket_record(s3, ncfg, report["checksum_sha256"])
    assert record is not None
    assert (record.ticket_url, record.ticket_number) == ("https://gh/41", 41)
    assert record.notified_batch_ids == {report["batch_id"]}
    assert record.events[0].action == "created"
    assert s3.get_json(RAW_BUCKET, manifest_key) == manifest_before  # single writer


def test_same_event_twice_makes_no_api_calls(s3, config, ncfg, client, api, tmp_path) -> None:
    _quarantine(s3, config, tmp_path, rows=[])
    _mock_create(api)
    notify_quarantine(ERROR_KEY, config=ncfg, store=s3, client=client)
    calls_after_first = len(api.calls)

    again = notify_quarantine(ERROR_KEY, config=ncfg, store=s3, client=client)

    assert again.outcome is NotifyOutcome.ALREADY_NOTIFIED
    assert again.ticket_url == "https://gh/41"
    assert len(api.calls) == calls_after_first


def test_requarantine_of_same_file_comments_on_its_issue(
    s3, config, ncfg, client, api, tmp_path
) -> None:
    """Same bytes delivered twice -> 1 issue, then 1 comment (the live-test scenario)."""
    report = _quarantine(s3, config, tmp_path, rows=[])
    _mock_create(api)
    notify_quarantine(ERROR_KEY, config=ncfg, store=s3, client=client)

    # Re-deliver identical bytes: same checksum, new batch.
    zip_path = sorted(tmp_path.glob("bad_*.zip"))[0]
    s3.upload(zip_path, LANDING_BUCKET, LANDING_KEY)
    second = process_zip(LANDING_KEY, config=config, store=s3)
    assert second.checksum == report["checksum_sha256"] and second.batch_id != report["batch_id"]

    api.reset()  # GitHub now has the issue created above
    body = f"Checksum: `{report['checksum_sha256']}`"
    api.get(ISSUES, json=[{"number": 41, "html_url": "https://gh/41", "body": body}])
    api.post(f"{ISSUES}/41/comments", status=201, json={"html_url": "https://gh/41#c2"})
    result = notify_quarantine(ERROR_KEY, config=ncfg, store=s3, client=client)

    assert result.outcome is NotifyOutcome.COMMENTED and result.ticket_number == 41
    record = load_ticket_record(s3, ncfg, report["checksum_sha256"])
    assert [e.action for e in record.events] == ["created", "commented"]


def test_headerless_file_issue_contains_no_row_values(
    s3, config, ncfg, client, api, tmp_path
) -> None:
    # No header row: the first data row is parsed as the header and reported as columns.
    _quarantine(s3, config, tmp_path, rows=VALID_ROWS[1:], header=VALID_ROWS[0])
    _mock_create(api)
    notify_quarantine(ERROR_KEY, config=ncfg, store=s3, client=client)

    issue = _posted_issue(api)
    assert issue["title"].endswith(": csv_header")  # failed where we expect, not elsewhere
    error_json = s3.get_json(LANDING_BUCKET, ERROR_KEY)
    assert VALID_ROWS[0][0] in error_json["failure_details"]  # the private report HAS the value
    for value in VALID_ROWS[0]:
        assert value not in issue["body"]  # ...but the public issue does not
    assert "3 non-identifier value(s) redacted" in issue["body"]


def test_github_outage_fails_cleanly_and_ingestion_is_unaffected(
    s3, config, ncfg, client, api, tmp_path
) -> None:
    report = _quarantine(s3, config, tmp_path, rows=[])
    for _ in range(4):
        api.get(ISSUES, status=502)

    with pytest.raises(TicketingError):
        notify_quarantine(ERROR_KEY, config=ncfg, store=s3, client=client)

    # Quarantine and manifest are exactly as ingestion left them; nothing recorded.
    assert s3.exists(LANDING_BUCKET, f"quarantine/{LANDING_KEY}")
    assert s3.exists(LANDING_BUCKET, ERROR_KEY)
    manifest = s3.get_json(RAW_BUCKET, f"_manifests/{report['checksum_sha256']}.json")
    assert manifest["outcome"] == "quarantined"
    assert load_ticket_record(s3, ncfg, report["checksum_sha256"]) is None


def test_rejects_keys_that_are_not_error_reports(s3, ncfg, client) -> None:
    with pytest.raises(ValueError, match="error.json"):
        notify_quarantine(LANDING_KEY, config=ncfg, store=s3, client=client)


# --- CLI ------------------------------------------------------------------------


@pytest.fixture
def gh_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_TOKEN", TOKEN)
    monkeypatch.setenv("GITHUB_REPO", REPO)
    monkeypatch.setattr(GitHubClient, "_backoff", lambda self, attempt: 0.0)


def test_cli_scan_notifies_only_unticketed(
    s3, config, config_dir, gh_env, api, tmp_path, capsys
) -> None:
    _quarantine(s3, config, tmp_path, rows=[])
    _mock_create(api)
    assert cli.main(["--scan", "--config-dir", str(config_dir)]) == cli.EXIT_OK
    first = json.loads(capsys.readouterr().out)
    assert [r["outcome"] for r in first["results"]] == ["created"]

    calls = len(api.calls)
    assert cli.main(["--scan", "--config-dir", str(config_dir)]) == cli.EXIT_OK
    second = json.loads(capsys.readouterr().out)
    assert [r["outcome"] for r in second["results"]] == ["already_notified"]
    assert len(api.calls) == calls  # retry scan made no GitHub calls


def test_cli_exits_nonzero_on_github_failure(
    s3, config, config_dir, gh_env, api, tmp_path, capsys
) -> None:
    _quarantine(s3, config, tmp_path, rows=[])
    for _ in range(4):
        api.get(ISSUES, status=503)
    assert cli.main(["--error-key", ERROR_KEY, "--config-dir", str(config_dir)]) == cli.EXIT_FAILED
    out = capsys.readouterr()
    assert json.loads(out.out)["failures"][0]["error_key"] == ERROR_KEY
    assert TOKEN not in out.out + out.err


def test_cli_reports_missing_token(s3, config_dir, monkeypatch, capsys) -> None:
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setenv("GITHUB_REPO", REPO)
    monkeypatch.setattr(cli, "load_dotenv", lambda *_a, **_k: False)  # ignore a real .env
    assert cli.main(["--scan", "--config-dir", str(config_dir)]) == cli.EXIT_FAILED
    assert "GITHUB_TOKEN is not set" in capsys.readouterr().err


# --- Lambda stub + settings ------------------------------------------------------------


def test_lambda_stub_ignores_non_report_keys(config_dir, monkeypatch) -> None:
    monkeypatch.setenv("CLAIMS_ETL_CONFIG_DIR", str(config_dir))
    event = {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": LANDING_BUCKET},
                    "object": {"key": f"quarantine/{LANDING_KEY}"},
                }
            },
            {"s3": {"bucket": {"name": "other"}, "object": {"key": ERROR_KEY}}},
        ]
    }
    assert lambda_handler.handler(event, None) == {"results": []}


def test_get_setting_reads_env_and_names_missing(monkeypatch) -> None:
    monkeypatch.setenv("SOME_SETTING", "value")
    assert settings.get_setting("SOME_SETTING") == "value"
    monkeypatch.delenv("SOME_SETTING")
    with pytest.raises(settings.MissingSettingError, match="SOME_SETTING"):
        settings.get_setting("SOME_SETTING")


def test_ssm_setting_reads_secure_string(monkeypatch) -> None:
    for var, value in {
        "AWS_ACCESS_KEY_ID": "t",
        "AWS_SECRET_ACCESS_KEY": "t",
        "AWS_DEFAULT_REGION": "us-east-2",
    }.items():
        monkeypatch.setenv(var, value)
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    settings._ssm_client.cache_clear()
    with mock_aws():
        boto3.client("ssm").put_parameter(
            Name="/claims-etl/GITHUB_REPO", Value=REPO, Type="SecureString"
        )
        assert settings.ssm_setting("GITHUB_REPO") == REPO
        assert settings.ssm_setting("NOT_THERE") is None
    settings._ssm_client.cache_clear()
