"""File-level checks: known file type, name, size."""

from __future__ import annotations

import re

import pytest

from claims_etl.ingestion import checks

BENE_REGEX = re.compile(
    r"^DE1_0_(?P<year>\d{4})_Beneficiary_Summary_File_Sample_(?P<sample>\d+)\.zip$"
)


def test_known_file_type_passes() -> None:
    assert checks.check_known_file_type("beneficiary_summary", {"beneficiary_summary"}).passed


def test_unknown_file_type_fails() -> None:
    result = checks.check_known_file_type("pharmacy", {"beneficiary_summary"})
    assert not result.passed
    assert "pharmacy" in result.details


def test_unparseable_key_fails_file_type() -> None:
    assert not checks.check_known_file_type(None, {"beneficiary_summary"}).passed


def test_file_name_matches() -> None:
    assert checks.check_file_name(
        "DE1_0_2010_Beneficiary_Summary_File_Sample_20.zip", BENE_REGEX
    ).passed


@pytest.mark.parametrize(
    "bad_name",
    [
        "176541_DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip",  # download-ID prefix seen in S3
        "DE1_0_2008_Beneficiary_Summary_File_Sample_1.ZIP",
        "DE1_0_08_Beneficiary_Summary_File_Sample_1.zip",
        "DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip.bak",
        "DE1_0_2008_Inpatient_Claims_Sample_1.zip",
    ],
)
def test_bad_file_name_fails(bad_name: str) -> None:
    result = checks.check_file_name(bad_name, BENE_REGEX)
    assert not result.passed
    assert bad_name in result.details


def test_empty_file_fails_size() -> None:
    result = checks.check_file_size(0, max_bytes=100)
    assert not result.passed
    assert "empty" in result.details


def test_oversized_file_fails_size() -> None:
    assert not checks.check_file_size(101, max_bytes=100).passed


def test_size_at_limit_passes() -> None:
    assert checks.check_file_size(100, max_bytes=100).passed
