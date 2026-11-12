"""CLI and Lambda handler wiring (moto; never touches AWS)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from claims_etl.ingestion import cli, lambda_handler
from claims_etl.ingestion.config import CONFIG_DIR_ENV_VAR
from claims_etl.ingestion.models import ManifestRecord, Outcome
from tests.conftest import (
    LANDING_BUCKET,
    LANDING_KEY,
    MAP_COLUMNS,
    MAP_FILE_NAME,
    MAP_LANDING_KEY,
    MAP_ROWS,
    csv_bytes,
    write_csv,
)


def _s3_event(bucket: str, key: str) -> dict:
    """Minimal S3 ObjectCreated event with one record."""
    return {"Records": [{"s3": {"bucket": {"name": bucket}, "object": {"key": key}}}]}


def _record(outcome: Outcome) -> ManifestRecord:
    return ManifestRecord(
        source_uri="s3://b/k",
        source_key="k",
        file_name="k",
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


def test_scan_processes_only_landing_csvs_in_every_feed(
    s3, config, config_dir: Path, valid_csv, tmp_path: Path, capsys
) -> None:
    ma = write_csv(tmp_path / MAP_FILE_NAME, csv_bytes(MAP_ROWS, header=MAP_COLUMNS))
    s3.upload(valid_csv, LANDING_BUCKET, LANDING_KEY)
    s3.upload(ma, LANDING_BUCKET, MAP_LANDING_KEY)
    s3.put_json(LANDING_BUCKET, "cms/cclf/claims_header/notes.json", {})
    s3.upload(ma, LANDING_BUCKET, f"elsewhere/{MAP_FILE_NAME}")  # not a feed prefix
    assert cli.scan_keys(s3, config) == [LANDING_KEY, MAP_LANDING_KEY]

    code = cli.main(["--scan", "--config-dir", str(config_dir)])
    summary = json.loads(capsys.readouterr().out)
    assert code == cli.EXIT_OK
    assert [r["outcome"] for r in summary["records"]] == ["success", "success"]


def test_cli_rejects_key_outside_prefix(s3, config_dir: Path) -> None:
    assert cli.main(["--key", "elsewhere/x.csv", "--config-dir", str(config_dir)]) == cli.EXIT_ERROR


def test_parse_s3_event_decodes_keys() -> None:
    event = {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "a/my+file%3D1.csv"}}}]}
    assert lambda_handler.parse_s3_event(event) == [("b", "a/my file=1.csv")]


def test_handler_processes_landing_csv_and_ignores_others(
    s3, config_dir: Path, valid_csv, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(CONFIG_DIR_ENV_VAR, str(config_dir))
    s3.upload(valid_csv, LANDING_BUCKET, LANDING_KEY)
    event = {
        "Records": [
            {"s3": {"bucket": {"name": LANDING_BUCKET}, "object": {"key": LANDING_KEY}}},
            {"s3": {"bucket": {"name": "other-bucket"}, "object": {"key": LANDING_KEY}}},
            {"s3": {"bucket": {"name": LANDING_BUCKET}, "object": {"key": "archive/x.csv"}}},
        ]
    }
    result = lambda_handler.handler(event, context=None)
    assert [r["outcome"] for r in result["results"]] == ["success"]


def test_handler_raises_on_error_so_lambda_retries(
    s3, config_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(CONFIG_DIR_ENV_VAR, str(config_dir))
    # Object was never uploaded: download fails -> `error` outcome -> must raise.
    event = _s3_event(LANDING_BUCKET, LANDING_KEY)
    with pytest.raises(lambda_handler.IngestionRetryableError, match="will retry"):
        lambda_handler.handler(event, context=None)


def test_handler_does_not_raise_on_quarantine(
    s3, config_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(CONFIG_DIR_ENV_VAR, str(config_dir))
    bad = write_csv(tmp_path / "bad.csv", csv_bytes([]))
    s3.upload(bad, LANDING_BUCKET, LANDING_KEY)
    event = _s3_event(LANDING_BUCKET, LANDING_KEY)
    result = lambda_handler.handler(event, context=None)
    assert [r["outcome"] for r in result["results"]] == ["quarantined"]
