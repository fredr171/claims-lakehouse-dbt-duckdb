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
    LANDING_BUCKET,
    LANDING_KEY,
    MAP_COLUMNS,
    MAP_FILE_NAME,
    MAP_LANDING_KEY,
    MAP_RAW_PARTITION,
    MAP_ROWS,
    RAW_BUCKET,
    VALID_ROWS,
    csv_bytes,
    delivery_key,
    raw_key,
    write_csv,
)


def archive_key(path: Path, key: str = LANDING_KEY) -> str:
    return delivery_key("archive/", key, sha256_file(path))


def quarantine_key(path: Path, key: str = LANDING_KEY) -> str:
    return delivery_key("quarantine/", key, sha256_file(path))


def _land(s3: S3Store, path: Path, key: str = LANDING_KEY) -> None:
    s3.upload(path, LANDING_BUCKET, key)


def _read_raw(s3: S3Store, key: str):
    body = s3._client.get_object(Bucket=RAW_BUCKET, Key=key)["Body"].read()
    return pq.read_table(io.BytesIO(body))


def test_success_publishes_parquet_manifest_and_archives(s3, config, valid_csv) -> None:
    _land(s3, valid_csv)
    record = process_file(LANDING_KEY, config=config, store=s3, batch_id="batch-1")

    assert record.outcome is Outcome.SUCCESS
    assert record.csv_row_count == record.parquet_row_count == 3
    assert record.output_uri == f"s3://{RAW_BUCKET}/{raw_key(record.checksum)}"
    assert record.name_fields == {"year": "2023", "month": "01"}

    table = _read_raw(s3, raw_key(record.checksum))
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
    assert s3.exists(LANDING_BUCKET, archive_key(valid_csv))


def test_corrected_resend_lands_next_to_the_original(s3, config, valid_csv, tmp_path) -> None:
    """Immutable bronze: a re-send of the same file name never replaces the first delivery."""
    _land(s3, valid_csv)
    first = process_file(LANDING_KEY, config=config, store=s3)
    corrected = write_csv(tmp_path / "corrected.csv", csv_bytes(VALID_ROWS[:2]))
    _land(s3, corrected)
    second = process_file(LANDING_KEY, config=config, store=s3)

    assert first.outcome is second.outcome is Outcome.SUCCESS
    assert first.checksum != second.checksum and first.output_uri != second.output_uri
    assert _read_raw(s3, raw_key(first.checksum)).num_rows == 3  # the original survives
    assert _read_raw(s3, raw_key(second.checksum)).num_rows == 2
    assert s3.exists(LANDING_BUCKET, archive_key(valid_csv))  # both deliveries archived
    assert s3.exists(LANDING_BUCKET, archive_key(corrected))
    for record in (first, second):
        assert s3.get_json(RAW_BUCKET, f"_manifests/{record.checksum}.json")["outcome"] == "success"


def test_next_month_lands_in_its_own_partition(s3, config, valid_csv, tmp_path) -> None:
    feb_key = "cms/cclf/claims_header/CCLF1_202302.csv"
    feb = write_csv(tmp_path / "CCLF1_202302.csv", csv_bytes(VALID_ROWS[:1]))
    _land(s3, valid_csv)
    _land(s3, feb, feb_key)
    jan_record = process_file(LANDING_KEY, config=config, store=s3)
    feb_record = process_file(feb_key, config=config, store=s3)

    assert jan_record.outcome is feb_record.outcome is Outcome.SUCCESS
    feb_raw = raw_key(feb_record.checksum, "cms/cclf/claims_header/delivery_month=2023-02")
    assert feb_record.output_uri == f"s3://{RAW_BUCKET}/{feb_raw}"
    assert _read_raw(s3, raw_key(jan_record.checksum)).num_rows == 3  # January untouched
    assert _read_raw(s3, feb_raw).num_rows == 1


def test_ma_payer_feed_lands_in_its_own_raw_prefix(s3, config, tmp_path) -> None:
    ma = write_csv(tmp_path / MAP_FILE_NAME, csv_bytes(MAP_ROWS, header=MAP_COLUMNS))
    _land(s3, ma, MAP_LANDING_KEY)
    record = process_file(MAP_LANDING_KEY, config=config, store=s3, batch_id="batch-h")

    assert record.outcome is Outcome.SUCCESS
    assert record.file_type == "member_eligibility"
    map_raw = raw_key(record.checksum, MAP_RAW_PARTITION)
    assert record.output_uri == f"s3://{RAW_BUCKET}/{map_raw}"
    table = _read_raw(s3, map_raw)
    assert table.column("COUNTY_FIPS").to_pylist() == ["39099", "05001"]  # leading zero kept
    assert set(table.column("_source_file").to_pylist()) == {MAP_FILE_NAME}
    assert s3.exists(LANDING_BUCKET, archive_key(ma, MAP_LANDING_KEY))
    assert not s3.exists(LANDING_BUCKET, MAP_LANDING_KEY)


def test_cclf_file_under_the_ma_payer_prefix_is_quarantined(s3, config, valid_csv) -> None:
    wrong_feed_key = f"ma_payer/ma/claims_header/{FILE_NAME}"
    _land(s3, valid_csv, wrong_feed_key)
    record = process_file(wrong_feed_key, config=config, store=s3)

    assert record.outcome is Outcome.QUARANTINED
    assert record.failed_check == "known_file_type"
    assert s3.exists(LANDING_BUCKET, quarantine_key(valid_csv, wrong_feed_key))


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
    assert s3.exists(LANDING_BUCKET, quarantine_key(bad))
    error = s3.get_json(LANDING_BUCKET, f"{quarantine_key(bad)}.error.json")
    assert error["failed_check"] == "csv_header"
    assert s3.get_json(RAW_BUCKET, f"_manifests/{record.checksum}.json")["outcome"] == "quarantined"
    assert not list(s3.list_keys(RAW_BUCKET, "cms/"))


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
    assert not list(s3.list_keys(RAW_BUCKET, "cms/"))
    assert s3.exists(LANDING_BUCKET, quarantine_key(valid_csv))


def test_infrastructure_failure_is_error_and_leaves_nothing_moved(s3, config) -> None:
    # The object was never uploaded, so the download fails: an S3 problem, not bad data.
    record = process_file(LANDING_KEY, config=config, store=s3)
    assert record.outcome is Outcome.ERROR
    assert "ClientError" in (record.failure_details or "")
    assert not list(s3.list_keys(LANDING_BUCKET, "quarantine/"))


def test_rejects_keys_outside_landing_prefix(s3, config) -> None:
    with pytest.raises(ValueError, match="not a .csv under"):
        process_file("random/file.csv", config=config, store=s3)


def test_move_copies_verifies_and_deletes(s3, valid_csv) -> None:
    _land(s3, valid_csv)
    s3.move(LANDING_BUCKET, LANDING_KEY, archive_key(valid_csv))
    assert not s3.exists(LANDING_BUCKET, LANDING_KEY)
    assert s3.head(LANDING_BUCKET, archive_key(valid_csv))["ContentLength"] == (
        valid_csv.stat().st_size
    )


def test_move_of_missing_object_raises(s3) -> None:
    with pytest.raises(FileNotFoundError):
        s3.move(LANDING_BUCKET, "missing.csv", "archive/missing.csv")
