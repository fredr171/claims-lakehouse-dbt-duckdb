"""End-to-end process_file against in-memory S3 (moto). Never touches AWS."""

from __future__ import annotations

import io
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from claims_etl.ingestion import pipeline
from claims_etl.ingestion.models import Outcome
from claims_etl.ingestion.pipeline import process_file
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.ingestion.utils import sha256_file
from tests.conftest import (
    FILE_NAME,
    HUM_COLUMNS,
    HUM_FILE_NAME,
    HUM_LANDING_KEY,
    HUM_RAW_KEY,
    HUM_ROWS,
    LANDING_BUCKET,
    LANDING_KEY,
    RAW_BUCKET,
    RAW_KEY,
    VALID_ROWS,
    csv_bytes,
    write_csv,
)

ARCHIVE_KEY = f"archive/{LANDING_KEY}"
QUARANTINE_KEY = f"quarantine/{LANDING_KEY}"


def _land(s3: S3Store, path: Path, key: str = LANDING_KEY) -> None:
    s3.upload(path, LANDING_BUCKET, key)


def _read_raw(s3: S3Store, key: str = RAW_KEY):
    body = s3._client.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body))


def test_success_publishes_parquet_manifest_and_archives(s3, config, valid_csv) -> None:
    _land(s3, valid_csv)
    record = process_file(LANDING_KEY, config=config, store=s3, batch_id="batch-1")

    assert record.outcome is Outcome.SUCCESS
    assert record.csv_row_count == record.parquet_row_count == 3
    assert record.output_uri == f"s3://{RAW_BUCKET}/{RAW_KEY}"
    assert record.name_fields == {"year": "2023", "month": "01"}

    table = _read_raw(s3)
    assert table.num_rows == 3
    assert set(table.column("_batch_id").to_pylist()) == {"batch-1"}
    assert set(table.column("_source_file").to_pylist()) == {FILE_NAME}
    assert set(table.column("_source_checksum").to_pylist()) == {sha256_file(valid_csv)}
    assert table.column("PRVDR_FAC_FIPS_ST_CD").to_pylist() == ["39", "05", ""]

    manifest = s3.get_json(RAW_BUCKET, f"_manifests/{record.checksum}.json")
    assert manifest["outcome"] == "success"
    assert manifest["file_name"] == FILE_NAME
    assert manifest["name_fields"] == {"year": "2023", "month": "01"}
    assert manifest["parquet_row_count"] == 3
    assert manifest["checks"][-1]["name"] == "row_count_reconciliation"

    assert not s3.exists(LANDING_BUCKET, LANDING_KEY)
    assert s3.exists(LANDING_BUCKET, ARCHIVE_KEY)


def test_next_month_lands_in_its_own_partition(s3, config, valid_csv, tmp_path) -> None:
    feb_key = "cms/cclf/claims_header/CCLF1_202302.csv"
    feb = write_csv(tmp_path / "CCLF1_202302.csv", csv_bytes(VALID_ROWS[:1]))
    _land(s3, valid_csv)
    _land(s3, feb, feb_key)
    jan_record = process_file(LANDING_KEY, config=config, store=s3)
    feb_record = process_file(feb_key, config=config, store=s3)

    assert jan_record.outcome is feb_record.outcome is Outcome.SUCCESS
    feb_raw = "cms/cclf/claims_header/delivery_month=2023-02/part-0.parquet"
    assert feb_record.output_uri == f"s3://{RAW_BUCKET}/{feb_raw}"
    assert _read_raw(s3).num_rows == 3  # January untouched
    assert _read_raw(s3, feb_raw).num_rows == 1


def test_humana_feed_lands_in_its_own_raw_prefix(s3, config, tmp_path) -> None:
    hum = write_csv(tmp_path / HUM_FILE_NAME, csv_bytes(HUM_ROWS, header=HUM_COLUMNS))
    _land(s3, hum, HUM_LANDING_KEY)
    record = process_file(HUM_LANDING_KEY, config=config, store=s3, batch_id="batch-h")

    assert record.outcome is Outcome.SUCCESS
    assert record.file_type == "member_eligibility"
    assert record.output_uri == f"s3://{RAW_BUCKET}/{HUM_RAW_KEY}"
    table = _read_raw(s3, HUM_RAW_KEY)
    assert table.column("COUNTY_FIPS").to_pylist() == ["39099", "05001"]  # leading zero kept
    assert set(table.column("_source_file").to_pylist()) == {HUM_FILE_NAME}
    assert s3.exists(LANDING_BUCKET, f"archive/{HUM_LANDING_KEY}")
    assert not s3.exists(LANDING_BUCKET, HUM_LANDING_KEY)


def test_cclf_file_under_the_humana_prefix_is_quarantined(s3, config, valid_csv) -> None:
    wrong_feed_key = f"humana/ma/claims_header/{FILE_NAME}"
    _land(s3, valid_csv, wrong_feed_key)
    record = process_file(wrong_feed_key, config=config, store=s3)

    assert record.outcome is Outcome.QUARANTINED
    assert record.failed_check == "known_file_type"
    assert s3.exists(LANDING_BUCKET, f"quarantine/{wrong_feed_key}")


def test_same_checksum_is_skipped_and_archived(s3, config, valid_csv) -> None:
    _land(s3, valid_csv)
    first = process_file(LANDING_KEY, config=config, store=s3)
    _land(s3, valid_csv)  # same bytes delivered again
    second = process_file(LANDING_KEY, config=config, store=s3)

    assert second.outcome is Outcome.SKIPPED_DUPLICATE
    assert second.output_uri == first.output_uri
    assert not s3.exists(LANDING_BUCKET, LANDING_KEY)
    # the original success record must survive, or a third copy would be reloaded
    manifest = s3.get_json(RAW_BUCKET, f"_manifests/{first.checksum}.json")
    assert manifest["outcome"] == "success"
    assert manifest["batch_id"] == first.batch_id


def test_invalid_file_is_quarantined_with_error_json(s3, config, tmp_path) -> None:
    bad = write_csv(tmp_path / "bad.csv", csv_bytes(VALID_ROWS, header=["A", "B", "C"]))
    _land(s3, bad)
    record = process_file(LANDING_KEY, config=config, store=s3)

    assert record.outcome is Outcome.QUARANTINED
    assert record.failed_check == "csv_header"
    assert not s3.exists(LANDING_BUCKET, LANDING_KEY)
    assert s3.exists(LANDING_BUCKET, QUARANTINE_KEY)
    error = s3.get_json(LANDING_BUCKET, f"{QUARANTINE_KEY}.error.json")
    assert error["failed_check"] == "csv_header"
    assert s3.get_json(RAW_BUCKET, f"_manifests/{record.checksum}.json")["outcome"] == "quarantined"
    assert not s3.exists(RAW_BUCKET, RAW_KEY)


def test_quarantined_file_can_be_fixed_and_redelivered(s3, config, tmp_path, valid_csv) -> None:
    bad = write_csv(tmp_path / "bad.csv", csv_bytes([]))  # header only
    _land(s3, bad)
    assert process_file(LANDING_KEY, config=config, store=s3).outcome is Outcome.QUARANTINED
    _land(s3, valid_csv)
    assert process_file(LANDING_KEY, config=config, store=s3).outcome is Outcome.SUCCESS


def test_reconciliation_mismatch_quarantines_without_upload(
    s3, config, valid_csv, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(pipeline, "parquet_row_count", lambda _path: 2)
    _land(s3, valid_csv)
    record = process_file(LANDING_KEY, config=config, store=s3)

    assert record.outcome is Outcome.QUARANTINED
    assert record.failed_check == "row_count_reconciliation"
    assert not s3.exists(RAW_BUCKET, RAW_KEY)
    assert s3.exists(LANDING_BUCKET, QUARANTINE_KEY)


def test_infrastructure_failure_is_error_and_leaves_nothing_moved(s3, config) -> None:
    # The object was never uploaded, so the download fails: an S3 problem, not bad data.
    record = process_file(LANDING_KEY, config=config, store=s3)
    assert record.outcome is Outcome.ERROR
    assert "ClientError" in (record.failure_details or "")
    assert not s3.exists(LANDING_BUCKET, QUARANTINE_KEY)


def test_rejects_keys_outside_landing_prefix(s3, config) -> None:
    with pytest.raises(ValueError, match="not a .csv under"):
        process_file("random/file.csv", config=config, store=s3)


def test_move_copies_verifies_and_deletes(s3, valid_csv) -> None:
    _land(s3, valid_csv)
    s3.move(LANDING_BUCKET, LANDING_KEY, ARCHIVE_KEY)
    assert not s3.exists(LANDING_BUCKET, LANDING_KEY)
    assert s3.head(LANDING_BUCKET, ARCHIVE_KEY)["ContentLength"] == valid_csv.stat().st_size


def test_move_of_missing_object_raises(s3) -> None:
    with pytest.raises(FileNotFoundError):
        s3.move(LANDING_BUCKET, "missing.csv", "archive/missing.csv")
