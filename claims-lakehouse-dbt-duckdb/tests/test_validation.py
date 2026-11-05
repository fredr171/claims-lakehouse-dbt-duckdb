"""validate_file: check ordering, stop-at-first-failure, and facts passed downstream."""

from __future__ import annotations

from pathlib import Path

from claims_etl.ingestion.validation import validate_file
from tests.conftest import LANDING_KEY, VALID_ROWS, csv_bytes, write_csv

EXPECTED_ORDER = [
    "known_file_type",
    "file_name",
    "file_size",
    "csv_utf8",
    "csv_parseable",
    "csv_header",
    "csv_field_counts",
    "csv_row_count",
    "csv_key_columns",
]


def test_valid_file_runs_every_check_in_order(valid_csv: Path, config) -> None:
    result = validate_file(valid_csv, LANDING_KEY, config)
    assert result.passed
    assert [c.name for c in result.checks] == EXPECTED_ORDER
    assert result.name_groups == {"year": "2023", "month": "01"}
    assert result.csv_row_count == 3


def test_stops_at_first_failure(tmp_path: Path, config) -> None:
    path = write_csv(tmp_path / "t.csv", csv_bytes(VALID_ROWS, header=["A", "B", "C"]))
    result = validate_file(path, LANDING_KEY, config)
    assert not result.passed
    assert result.failure is not None and result.failure.name == "csv_header"
    assert result.checks[-1].name == "csv_header"  # nothing after the failure ran
    assert result.csv_row_count is None


def test_bad_name_in_key_fails_before_reading_csv(valid_csv: Path, config) -> None:
    key = "cms/cclf/claims_header/CCLF1_202301_v2.csv"
    result = validate_file(valid_csv, key, config)
    assert result.failure is not None and result.failure.name == "file_name"
    assert result.checks[-1].name == "file_name"


def test_unknown_file_type_fails_first(valid_csv: Path, config) -> None:
    result = validate_file(valid_csv, "cms/cclf/pharmacy/CCLF7_202301.csv", config)
    assert [c.name for c in result.checks] == ["known_file_type"]
    assert not result.passed


def test_empty_file_fails_size(tmp_path: Path, config) -> None:
    path = write_csv(tmp_path / "empty.csv", b"")
    result = validate_file(path, LANDING_KEY, config)
    assert result.failure is not None and result.failure.name == "file_size"


def test_missing_key_fails_last_check(tmp_path: Path, config) -> None:
    rows = [VALID_ROWS[0], ["1000085822479", "", "05"]]
    path = write_csv(tmp_path / "k.csv", csv_bytes(rows))
    result = validate_file(path, LANDING_KEY, config)
    assert result.failure is not None and result.failure.name == "csv_key_columns"
