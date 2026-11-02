"""CLI and Lambda handler wiring (moto; never touches AWS)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from claims_etl.ingestion import cli, lambda_handler
from claims_etl.ingestion.config import CONFIG_DIR_ENV_VAR
from claims_etl.ingestion.models import ManifestRecord, Outcome
from tests.conftest import LANDING_BUCKET, LANDING_KEY


def _record(outcome: Outcome) -> ManifestRecord:
    return ManifestRecord(
        source_uri="s3://b/k",
        source_key="k",
        zip_name="k",
        batch_id="b",
        started_at=datetime.now(UTC),
        outcome=outcome,
    )


@pytest.mark.parametrize(
    ("outcomes", "code"),
    [
        ([Outcome.SUCCESS, Outcome.SKIPPED_DUPLICATE], cli.EXIT_OK),
        ([Outcome.SUCCESS, Outcome.QUARANTINED], cli.EXIT_QUARANTINED),
        ([Outcome.QUARANTINED, Outcome.ERROR], cli.EXIT_ERROR),
        ([], cli.EXIT_OK),
    ],
)
def test_exit_code(outcomes: list[Outcome], code: int) -> None:
    assert cli.exit_code([_record(o) for o in outcomes]) == code


def test_scan_processes_only_landing_zips(s3, config, config_dir: Path, valid_zip, capsys) -> None:
    s3.upload(valid_zip, LANDING_BUCKET, LANDING_KEY)
    s3.put_json(LANDING_BUCKET, "cms/desynpuf/beneficiary_summary/notes.json", {})
    assert cli.scan_keys(s3, config) == [LANDING_KEY]

    code = cli.main(["--scan", "--config-dir", str(config_dir)])
    summary = json.loads(capsys.readouterr().out)
    assert code == cli.EXIT_OK
    assert [r["outcome"] for r in summary["records"]] == ["success"]


def test_cli_rejects_key_outside_prefix(s3, config_dir: Path) -> None:
    assert cli.main(["--key", "elsewhere/x.zip", "--config-dir", str(config_dir)]) == cli.EXIT_ERROR


def test_parse_s3_event_decodes_keys() -> None:
    event = {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "a/my+file%3D1.zip"}}}]}
    assert lambda_handler.parse_s3_event(event) == [("b", "a/my file=1.zip")]


def test_handler_processes_landing_zip_and_ignores_others(
    s3, config_dir: Path, valid_zip, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(CONFIG_DIR_ENV_VAR, str(config_dir))
    s3.upload(valid_zip, LANDING_BUCKET, LANDING_KEY)
    event = {
        "Records": [
            {"s3": {"bucket": {"name": LANDING_BUCKET}, "object": {"key": LANDING_KEY}}},
            {"s3": {"bucket": {"name": "other-bucket"}, "object": {"key": LANDING_KEY}}},
            {"s3": {"bucket": {"name": LANDING_BUCKET}, "object": {"key": "archive/x.zip"}}},
        ]
    }
    result = lambda_handler.handler(event, context=None)
    assert [r["outcome"] for r in result["results"]] == ["success"]
