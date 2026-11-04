"""Ticket text: title, body, comment, sanitizing. Pure functions, no network."""

from __future__ import annotations

from pathlib import Path

import pytest

from claims_etl.notifications import content
from claims_etl.notifications.config import load_notification_config
from claims_etl.notifications.report import CheckSummary, InvalidReportError, QuarantineReport

SHA = "ab" * 32
ZIP = "DE1_0_2009_Beneficiary_Summary_File_Sample_7.zip"
KEY = f"quarantine/cms/desynpuf/beneficiary_summary/{ZIP}.error.json"


def _report(**overrides) -> QuarantineReport:
    values = dict(
        error_key=KEY,
        source_uri="s3://test-landing/cms/desynpuf/beneficiary_summary/DE1_0_2009_Beneficiary_Summary_File_Sample_7.zip",
        zip_name="DE1_0_2009_Beneficiary_Summary_File_Sample_7.zip",
        checksum=SHA,
        batch_id="batch-1",
        file_type="beneficiary_summary",
        year="2009",
        sample="7",
        failed_check="csv_field_counts",
        failure_details="1 row(s) without 32 fields; e.g. line 1378: 31 fields",
        quarantine_uri="s3://test-landing/quarantine/cms/desynpuf/beneficiary_summary/x.zip",
        quarantined_at="2026-11-04T15:56:35+00:00",
        checks=[CheckSummary("csv_header", True), CheckSummary("csv_field_counts", False)],
    )
    values.update(overrides)
    return QuarantineReport(**values)


@pytest.fixture
def ncfg(config_dir: Path):
    return load_notification_config(config_dir)


# --- title ---------------------------------------------------------------------


def test_title_with_partition(ncfg) -> None:
    assert content.build_title(_report(), ncfg) == (
        "[Quarantine] beneficiary_summary 2009 sample 7: csv_field_counts"
    )


def test_title_falls_back_to_zip_name_when_name_did_not_parse(ncfg) -> None:
    report = _report(
        year=None, sample=None, failed_check="file_name", zip_name="176541_DE1_0_2009_x.zip"
    )
    assert content.build_title(report, ncfg) == (
        "[Quarantine] beneficiary_summary 176541_DE1_0_2009_x.zip: file_name"
    )


def test_title_with_unknown_file_type(ncfg) -> None:
    report = _report(file_type=None, year=None, sample=None, failed_check="known_file_type")
    assert content.build_title(report, ncfg).startswith("[Quarantine] unknown ")


# --- body ----------------------------------------------------------------------


def test_body_has_required_sections(ncfg) -> None:
    body = content.build_body(_report(), ncfg)
    assert f"\nChecksum: `{SHA}`\n" in body  # own line, exact format
    assert content.find_checksum(body) == SHA
    assert "## Failed check: `csv_field_counts`" in body
    assert "line 1378: 31 fields" in body
    assert "`batch-1`" in body
    assert "`2026-11-04T15:56:35+00:00`" in body
    assert "Inspect the reported line numbers" in body  # next step for this check
    assert f"s3://test-landing/{KEY}" in body  # path to the .error.json
    assert "console.aws.amazon.com/s3/object/test-landing" in body


def test_next_step_mentions_file_type_config(ncfg) -> None:
    body = content.build_body(_report(failed_check="csv_header", failure_details="x"), ncfg)
    assert "config/file_types/beneficiary_summary.yml" in body


def test_unknown_check_gets_default_next_step(ncfg) -> None:
    body = content.build_body(_report(failed_check="brand_new_check"), ncfg)
    assert "Review the .error.json report" in body


def test_comment_mentions_batch_and_check(ncfg) -> None:
    comment = content.build_comment(_report(batch_id="batch-2"), ncfg)
    assert "Quarantined again" in comment
    assert "`batch-2`" in comment
    assert "`csv_field_counts`" in comment


# --- sanitizing ------------------------------------------------------------------


def test_long_details_are_truncated(ncfg) -> None:
    long_details = "line 2: 31 fields, " * 200
    body = content.build_body(_report(failure_details=long_details), ncfg)
    sanitized = content.sanitize_details("csv_field_counts", long_details, ncfg.max_details_chars)
    assert len(sanitized) <= ncfg.max_details_chars
    assert "truncated" in sanitized
    assert long_details not in body


def test_headerless_file_does_not_leak_row_values() -> None:
    # A file with no header row: the first DATA row is read as the header.
    details = (
        "missing ['DESYNPUF_ID', 'BENE_BIRTH_DT']; "
        "unexpected ['00013D2EFD8E45D1', '19230501', '', '0.00']"
    )
    safe = content.sanitize_details("csv_header", details, 500)
    for value in ("00013D2EFD8E45D1", "19230501", "0.00"):
        assert value not in safe
    assert "'DESYNPUF_ID'" in safe  # column names are kept
    assert "4 non-identifier value(s) redacted" in safe


def test_utf8_byte_values_are_redacted() -> None:
    details = "not valid UTF-8 after line 2: invalid continuation byte (b'\\xe9')"
    safe = content.sanitize_details("csv_utf8", details, 500)
    assert "xe9" not in safe
    assert "after line 2" in safe


def test_csv_parse_punctuation_is_kept() -> None:
    details = "malformed CSV at line 2: ',' expected after '\"'"
    assert content.sanitize_details("csv_parseable", details, 500) == details


def test_file_level_details_keep_names() -> None:
    details = "'176541_DE1_0_2009_x.zip' does not match ^DE1_0_.*$"
    assert content.sanitize_details("file_name", details, 500) == details


def test_code_span_neutralizes_mentions_and_markdown() -> None:
    rendered = content.code("hi @octocat `x` | [link](http://evil)\nnext", in_table=True)
    assert rendered.startswith("`") and rendered.endswith("`")
    assert rendered.count("`") == 2  # inner backticks replaced: cannot escape the span
    assert "\\|" in rendered and "\n" not in rendered


def test_find_checksum_requires_exact_line() -> None:
    assert content.find_checksum(f"text\nChecksum: `{SHA}`\nmore") == SHA
    assert content.find_checksum(f"see Checksum: `{SHA}` inline") is None
    assert content.find_checksum(None) is None


# --- report parsing ----------------------------------------------------------------


def test_report_rejects_non_quarantine_outcome() -> None:
    with pytest.raises(InvalidReportError, match="outcome"):
        QuarantineReport.from_dict(KEY, {"outcome": "success", "checksum_sha256": SHA})


def test_report_rejects_malformed_checksum() -> None:
    data = {"outcome": "quarantined", "checksum_sha256": "abc`\nChecksum: `x"}
    with pytest.raises(InvalidReportError, match="checksum"):
        QuarantineReport.from_dict(KEY, data)
