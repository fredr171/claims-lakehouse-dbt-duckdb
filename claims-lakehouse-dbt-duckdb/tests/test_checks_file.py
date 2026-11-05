"""File-level checks: known file type, name, size."""

from __future__ import annotations

import pytest

from claims_etl.ingestion import checks
from claims_etl.ingestion.config import load_config

# The real rule from config/file_types/claims_header.yml, not a copy, so these
# names are tested against exactly what ingestion enforces.
CCLF1_REGEX = load_config().file_types["claims_header"].file_name_regex


def test_known_file_type_passes() -> None:
    assert checks.check_known_file_type("claims_header", {"claims_header"}).passed


def test_unknown_file_type_fails() -> None:
    result = checks.check_known_file_type("pharmacy", {"claims_header"})
    assert not result.passed
    assert "pharmacy" in result.details


def test_unparseable_key_fails_file_type() -> None:
    assert not checks.check_known_file_type(None, {"claims_header"}).passed


@pytest.mark.parametrize("good_name", ["CCLF1_202301.csv", "CCLF1_202512.csv", "CCLF1_203010.csv"])
def test_file_name_matches(good_name: str) -> None:
    assert checks.check_file_name(good_name, CCLF1_REGEX).passed


@pytest.mark.parametrize(
    "bad_name",
    [
        "CCLF1_202313.csv",  # no month 13
        "CCLF1_202300.csv",  # no month 00
        "CCLF1_2023011.csv",
        "CCLF1_202301.CSV",
        "CCLF1_202301.csv.bak",
        "CCLF1_202301 (1).csv",  # browser re-download suffix
        "CCLF4_202301.csv",  # a diagnosis file in the claims_header folder
        "cclf1_202301.csv",
    ],
)
def test_bad_file_name_fails(bad_name: str) -> None:
    result = checks.check_file_name(bad_name, CCLF1_REGEX)
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
