"""CSV-level checks, driven through profile_csv on in-memory bytes."""

from __future__ import annotations

import io

from claims_etl.ingestion import checks
from claims_etl.ingestion.csv_inspect import profile_csv
from claims_etl.ingestion.models import CsvProfile
from tests.conftest import COLUMNS, VALID_ROWS, csv_bytes

KEYS = ("CUR_CLM_UNIQ_ID", "BENE_MBI_ID")


def _profile(data: bytes) -> CsvProfile:
    return profile_csv(io.BytesIO(data), KEYS)


def _all_csv_checks(profile: CsvProfile) -> list:
    return [
        checks.check_csv_utf8(profile),
        checks.check_csv_parseable(profile),
        checks.check_csv_header(profile.header, COLUMNS),
        checks.check_csv_field_counts(profile),
        checks.check_csv_row_count(profile),
        checks.check_csv_key_columns(profile, KEYS),
    ]


def test_valid_csv_passes_every_check() -> None:
    profile = _profile(csv_bytes(VALID_ROWS))
    assert profile.row_count == 3
    assert all(r.passed for r in _all_csv_checks(profile))


def test_crlf_line_endings_also_pass() -> None:
    assert all(r.passed for r in _all_csv_checks(_profile(csv_bytes(VALID_ROWS, eol="\r\n"))))


def test_quoted_header_also_passes() -> None:
    quoted = b'"CUR_CLM_UNIQ_ID","BENE_MBI_ID","PRVDR_FAC_FIPS_ST_CD"\n' + b"1,2,3\n"
    assert all(r.passed for r in _all_csv_checks(_profile(quoted)))


# --- csv_utf8 ------------------------------------------------------------------


def test_invalid_utf8_fails() -> None:
    data = csv_bytes(VALID_ROWS).replace(b"9GD7PX4RK74", b"9GD7PX\xe94RK74")  # Latin-1 byte
    result = checks.check_csv_utf8(_profile(data))
    assert not result.passed
    assert "UTF-8" in result.details


# --- csv_parseable -----------------------------------------------------------------


def test_malformed_quoting_fails() -> None:
    data = csv_bytes([["1000085822452", '"6AA3QX1NP94"x', "39"]])
    result = checks.check_csv_parseable(_profile(data))
    assert not result.passed
    assert "line 2" in result.details


# --- csv_header ------------------------------------------------------------------


def test_empty_csv_fails_header() -> None:
    result = checks.check_csv_header(_profile(b"").header, COLUMNS)
    assert not result.passed
    assert "no header" in result.details


def test_renamed_column_fails_header() -> None:
    header = ["CUR_CLM_UNIQ_ID", "BENE_MBI", "PRVDR_FAC_FIPS_ST_CD"]
    result = checks.check_csv_header(_profile(csv_bytes(VALID_ROWS, header=header)).header, COLUMNS)
    assert not result.passed
    assert "missing ['BENE_MBI_ID']" in result.details
    assert "unexpected ['BENE_MBI']" in result.details


def test_reordered_columns_fail_header() -> None:
    header = ["BENE_MBI_ID", "CUR_CLM_UNIQ_ID", "PRVDR_FAC_FIPS_ST_CD"]
    result = checks.check_csv_header(_profile(csv_bytes(VALID_ROWS, header=header)).header, COLUMNS)
    assert not result.passed
    assert "order differs at column 1" in result.details


def test_missing_column_fails_header() -> None:
    header = ["CUR_CLM_UNIQ_ID", "BENE_MBI_ID"]
    rows = [r[:2] for r in VALID_ROWS]
    result = checks.check_csv_header(_profile(csv_bytes(rows, header=header)).header, COLUMNS)
    assert not result.passed
    assert "PRVDR_FAC_FIPS_ST_CD" in result.details


def test_duplicate_column_fails_header() -> None:
    header = [*COLUMNS, "PRVDR_FAC_FIPS_ST_CD"]
    rows = [[*r, "x"] for r in VALID_ROWS]
    result = checks.check_csv_header(_profile(csv_bytes(rows, header=header)).header, COLUMNS)
    assert not result.passed
    assert "got 4 columns, expected 3" in result.details


def test_utf8_bom_fails_header() -> None:
    result = checks.check_csv_header(
        _profile(b"\xef\xbb\xbf" + csv_bytes(VALID_ROWS)).header, COLUMNS
    )
    assert not result.passed


# --- csv_field_counts --------------------------------------------------------------


def test_ragged_row_fails_with_line_number() -> None:
    rows = [VALID_ROWS[0], ["1000085822479", "9GD7PX4RK74"], VALID_ROWS[2]]
    profile = _profile(csv_bytes(rows))
    result = checks.check_csv_field_counts(profile)
    assert not result.passed
    assert "line 3: 2 fields" in result.details
    assert profile.row_count == 3  # ragged rows are still counted


def test_blank_line_counts_as_ragged() -> None:
    data = csv_bytes(VALID_ROWS) + b"\n"
    assert not checks.check_csv_field_counts(_profile(data)).passed


# --- csv_row_count -------------------------------------------------------------------


def test_header_only_fails_row_count() -> None:
    result = checks.check_csv_row_count(_profile(csv_bytes([])))
    assert not result.passed
    assert "no data rows" in result.details


# --- csv_key_columns -------------------------------------------------------------------


def test_empty_key_fails() -> None:
    rows = [VALID_ROWS[0], ["1000085822479", "", "05"]]
    result = checks.check_csv_key_columns(_profile(csv_bytes(rows)), KEYS)
    assert not result.passed
    assert "line 3: BENE_MBI_ID" in result.details


def test_whitespace_only_key_fails() -> None:
    rows = [["   ", "6AA3QX1NP94", "39"]]
    assert not checks.check_csv_key_columns(_profile(csv_bytes(rows)), KEYS).passed


def test_empty_non_key_column_is_allowed() -> None:
    # VALID_ROWS[2] has an empty PRVDR_FAC_FIPS_ST_CD: that is a dbt concern, not ingestion.
    assert checks.check_csv_key_columns(_profile(csv_bytes(VALID_ROWS)), KEYS).passed


# --- row_count_reconciliation --------------------------------------------------------------


def test_reconciliation_match_passes() -> None:
    assert checks.check_row_count_reconciliation(48, 48).passed


def test_reconciliation_mismatch_fails() -> None:
    result = checks.check_row_count_reconciliation(48, 47)
    assert not result.passed
    assert "48" in result.details
