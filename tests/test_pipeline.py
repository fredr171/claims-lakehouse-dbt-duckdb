"""End-to-end process_zip against in-memory S3 (moto). Never touches AWS."""

from __future__ import annotations

import io
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from claims_etl.ingestion import pipeline
from claims_etl.ingestion.models import Outcome
from claims_etl.ingestion.pipeline import process_zip
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.ingestion.utils import sha256_file
from tests.conftest import (
    CSV_NAME,
    LANDING_BUCKET,
    LANDING_KEY,
    RAW_BUCKET,
    VALID_ROWS,
    csv_bytes,
    make_zip,
)

RAW_KEY = "cms/desynpuf/beneficiary_summary/year=2008/sample=1/part-0.parquet"
ARCHIVE_KEY = f"archive/{LANDING_KEY}"
QUARANTINE_KEY = f"quarantine/{LANDING_KEY}"


def _land(s3: S3Store, zip_path: Path, key: str = LANDING_KEY) -> None:
    s3.upload(zip_path, LANDING_BUCKET, key)


def _read_raw(s3: S3Store):
    body = s3._client.get_object(Bucket=RAW_BUCKET, Key=RAW_KEY)["Body"].read()
    return pq.read_table(io.BytesIO(body))


def test_success_publishes_parquet_manifest_and_archives(s3, config, valid_zip) -> None:
    _land(s3, valid_zip)
    record = process_zip(LANDING_KEY, config=config, store=s3, batch_id="batch-1")

    assert record.outcome is Outcome.SUCCESS
    assert record.csv_row_count == record.parquet_row_count == 3
    assert record.output_uri == f"s3://{RAW_BUCKET}/{RAW_KEY}"
    assert (record.year, record.sample) == ("2008", "1")

    table = _read_raw(s3)
    assert table.num_rows == 3
    assert set(table.column("_batch_id").to_pylist()) == {"batch-1"}
    assert set(table.column("_source_checksum").to_pylist()) == {sha256_file(valid_zip)}

    manifest = s3.get_json(RAW_BUCKET, f"_manifests/{record.checksum}.json")
    assert manifest["outcome"] == "success"
    assert manifest["parquet_row_count"] == 3
    assert manifest["checks"][-1]["name"] == "row_count_reconciliation"

    assert not s3.exists(LANDING_BUCKET, LANDING_KEY)
    assert s3.exists(LANDING_BUCKET, ARCHIVE_KEY)


def test_same_checksum_is_skipped_and_archived(s3, config, valid_zip) -> None:
    _land(s3, valid_zip)
    first = process_zip(LANDING_KEY, config=config, store=s3)
    _land(s3, valid_zip)  # same bytes delivered again
    second = process_zip(LANDING_KEY, config=config, store=s3)

    assert second.outcome is Outcome.SKIPPED_DUPLICATE
    assert second.output_uri == first.output_uri
    assert not s3.exists(LANDING_BUCKET, LANDING_KEY)
    # the original success record must survive, or a third copy would be reloaded
    manifest = s3.get_json(RAW_BUCKET, f"_manifests/{first.checksum}.json")
    assert manifest["outcome"] == "success"
    assert manifest["batch_id"] == first.batch_id


def test_invalid_zip_is_quarantined_with_error_json(s3, config, tmp_path) -> None:
    bad = make_zip(tmp_path / "bad.zip", {CSV_NAME: csv_bytes(VALID_ROWS, header=["A", "B", "C"])})
    _land(s3, bad)
    record = process_zip(LANDING_KEY, config=config, store=s3)

    assert record.outcome is Outcome.QUARANTINED
    assert record.failed_check == "csv_header"
    assert not s3.exists(LANDING_BUCKET, LANDING_KEY)
    assert s3.exists(LANDING_BUCKET, QUARANTINE_KEY)
    error = s3.get_json(LANDING_BUCKET, f"{QUARANTINE_KEY}.error.json")
    assert error["failed_check"] == "csv_header"
    assert s3.get_json(RAW_BUCKET, f"_manifests/{record.checksum}.json")["outcome"] == "quarantined"
    assert not s3.exists(RAW_BUCKET, RAW_KEY)


def test_quarantined_file_can_be_fixed_and_redelivered(s3, config, tmp_path, valid_zip) -> None:
    bad = make_zip(tmp_path / "bad.zip", {CSV_NAME: csv_bytes([])})  # header only
    _land(s3, bad)
    assert process_zip(LANDING_KEY, config=config, store=s3).outcome is Outcome.QUARANTINED
    _land(s3, valid_zip)
    assert process_zip(LANDING_KEY, config=config, store=s3).outcome is Outcome.SUCCESS


def test_reconciliation_mismatch_quarantines_without_upload(
    s3, config, valid_zip, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pipeline, "parquet_row_count", lambda _path: 2)
    _land(s3, valid_zip)
    record = process_zip(LANDING_KEY, config=config, store=s3)

    assert record.outcome is Outcome.QUARANTINED
    assert record.failed_check == "row_count_reconciliation"
    assert not s3.exists(RAW_BUCKET, RAW_KEY)
    assert s3.exists(LANDING_BUCKET, QUARANTINE_KEY)


def test_infrastructure_failure_is_error_and_leaves_nothing_moved(s3, config) -> None:
    # The object was never uploaded, so the download fails: an S3 problem, not bad data.
    record = process_zip(LANDING_KEY, config=config, store=s3)
    assert record.outcome is Outcome.ERROR
    assert "ClientError" in (record.failure_details or "")
    assert not s3.exists(LANDING_BUCKET, QUARANTINE_KEY)


def test_rejects_keys_outside_landing_prefix(s3, config) -> None:
    with pytest.raises(ValueError, match="not a .zip under"):
        process_zip("random/file.zip", config=config, store=s3)


def test_move_copies_verifies_and_deletes(s3, valid_zip) -> None:
    _land(s3, valid_zip)
    s3.move(LANDING_BUCKET, LANDING_KEY, ARCHIVE_KEY)
    assert not s3.exists(LANDING_BUCKET, LANDING_KEY)
    assert s3.head(LANDING_BUCKET, ARCHIVE_KEY)["ContentLength"] == valid_zip.stat().st_size


def test_move_of_missing_object_raises(s3) -> None:
    with pytest.raises(FileNotFoundError):
        s3.move(LANDING_BUCKET, "missing.zip", "archive/missing.zip")
