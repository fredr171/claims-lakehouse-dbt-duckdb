"""File-level checks: known file type, name, size."""

from __future__ import annotations

import pytest

from claims_etl.ingestion import checks
from claims_etl.ingestion.config import load_config

# The real rule from config/file_types/beneficiary_summary.yml, not a copy, so
# these names are tested against exactly what ingestion enforces.
BENE_REGEX = load_config().file_types["beneficiary_summary"].zip_name_regex


def test_known_file_type_passes() -> None:
    assert checks.check_known_file_type("beneficiary_summary", {"beneficiary_summary"}).passed


def test_unknown_file_type_fails() -> None:
    result = checks.check_known_file_type("pharmacy", {"beneficiary_summary"})
    assert not result.passed
    assert "pharmacy" in result.details


def test_unparseable_key_fails_file_type() -> None:
    assert not checks.check_known_file_type(None, {"beneficiary_summary"}).passed


@pytest.mark.parametrize(
    "good_name",
    [
        "DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip",
        "DE1_0_2010_Beneficiary_Summary_File_Sample_20.zip",
    ],
)
def test_file_name_matches(good_name: str) -> None:
    assert checks.check_file_name(good_name, BENE_REGEX).passed


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
