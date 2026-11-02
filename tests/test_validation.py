"""validate_zip: check ordering, stop-at-first-failure, and facts passed downstream."""

from __future__ import annotations

from pathlib import Path

from claims_etl.ingestion.validation import validate_zip
from tests.conftest import CSV_NAME, LANDING_KEY, VALID_ROWS, csv_bytes, make_zip

EXPECTED_ORDER = [
    "known_file_type",
    "file_name",
    "file_size",
    "zip_opens",
    "member_paths",
    "expected_member",
    "uncompressed_size",
    "compression_ratio",
    "zip_crc",
    "csv_utf8",
    "csv_parseable",
    "csv_header",
    "csv_field_counts",
    "csv_row_count",
    "csv_key_columns",
]


def test_valid_zip_runs_every_check_in_order(valid_zip: Path, config) -> None:
    result = validate_zip(valid_zip, LANDING_KEY, config)
    assert result.passed
    assert [c.name for c in result.checks] == EXPECTED_ORDER
    assert result.name_groups == {"year": "2008", "sample": "1"}
    assert result.csv_member == CSV_NAME
    assert result.csv_row_count == 3


def test_stops_at_first_failure(tmp_path: Path, config) -> None:
    path = make_zip(tmp_path / "t.zip", {CSV_NAME: csv_bytes(VALID_ROWS), "../evil.csv": b"x"})
    result = validate_zip(path, LANDING_KEY, config)
    assert not result.passed
    assert result.failure is not None and result.failure.name == "member_paths"
    assert result.checks[-1].name == "member_paths"  # nothing after the failure ran
    assert result.csv_row_count is None


def test_bad_name_in_key_fails_before_opening_zip(valid_zip: Path, config) -> None:
    key = "cms/desynpuf/beneficiary_summary/176541_DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip"
    result = validate_zip(valid_zip, key, config)
    assert result.failure is not None and result.failure.name == "file_name"


def test_unknown_file_type_fails_first(valid_zip: Path, config) -> None:
    result = validate_zip(valid_zip, "cms/desynpuf/pharmacy/x.zip", config)
    assert [c.name for c in result.checks] == ["known_file_type"]
    assert not result.passed


def test_corrupt_zip_fails_zip_opens(tmp_path: Path, config) -> None:
    path = tmp_path / "corrupt.zip"
    path.write_bytes(b"PK\x03\x04 not really a zip")
    result = validate_zip(path, LANDING_KEY, config)
    assert result.failure is not None and result.failure.name == "zip_opens"


def test_empty_file_fails_size(tmp_path: Path, config) -> None:
    path = tmp_path / "empty.zip"
    path.write_bytes(b"")
    result = validate_zip(path, LANDING_KEY, config)
    assert result.failure is not None and result.failure.name == "file_size"


def test_missing_key_fails_last_check(tmp_path: Path, config) -> None:
    rows = [VALID_ROWS[0], ["", "19430101", "39"]]
    path = make_zip(tmp_path / "k.zip", {CSV_NAME: csv_bytes(rows)})
    result = validate_zip(path, LANDING_KEY, config)
    assert result.failure is not None and result.failure.name == "csv_key_columns"
