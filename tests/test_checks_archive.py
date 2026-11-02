"""Archive-level checks: valid zip, CRC, member safety, member set, size/ratio."""

from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from claims_etl.ingestion import checks
from tests.conftest import CSV_NAME, VALID_ROWS, csv_bytes, make_zip


def _infos(path: Path) -> list[zipfile.ZipInfo]:
    with zipfile.ZipFile(path) as zf:
        return zf.infolist()


# --- zip_opens ---------------------------------------------------------------


def test_valid_zip_opens(valid_zip: Path) -> None:
    assert checks.check_zip_opens(valid_zip).passed


def test_non_zip_bytes_fail(tmp_path: Path) -> None:
    path = tmp_path / "fake.zip"
    path.write_bytes(b"this is a CSV pretending to be a zip\r\n")
    result = checks.check_zip_opens(path)
    assert not result.passed
    assert "not a valid zip" in result.details


def test_zero_byte_zip_fails(tmp_path: Path) -> None:
    path = tmp_path / "empty.zip"
    path.write_bytes(b"")
    assert not checks.check_zip_opens(path).passed


def test_truncated_zip_fails(valid_zip: Path, tmp_path: Path) -> None:
    truncated = tmp_path / "truncated.zip"
    truncated.write_bytes(valid_zip.read_bytes()[:-30])  # cut into the central directory
    assert not checks.check_zip_opens(truncated).passed


# --- zip_crc -----------------------------------------------------------------


def test_crc_passes_on_intact_zip(valid_zip: Path) -> None:
    with zipfile.ZipFile(valid_zip) as zf:
        assert checks.check_zip_crc(zf).passed


def test_crc_mismatch_fails(tmp_path: Path) -> None:
    payload = csv_bytes(VALID_ROWS)
    path = make_zip(tmp_path / "stored.zip", {CSV_NAME: payload}, compression=zipfile.ZIP_STORED)
    # Stored (uncompressed) data appears verbatim; flip one byte of it.
    data = bytearray(path.read_bytes())
    offset = data.index(b"00013D2EFD8E45D1")
    data[offset] = ord("9")
    path.write_bytes(bytes(data))

    with zipfile.ZipFile(path) as zf:
        result = checks.check_zip_crc(zf)
    assert not result.passed
    assert CSV_NAME in result.details


# --- member_paths -------------------------------------------------------------


@pytest.mark.parametrize(
    ("member", "reason"),
    [
        ("../evil.csv", "path traversal"),
        ("data/../../evil.csv", "path traversal"),
        ("..\\evil.csv", "path traversal"),
        ("/etc/evil.csv", "absolute path"),
        ("C:/Windows/evil.csv", "absolute path"),
        ("inner.zip", "nested archive"),
        ("inner.tar.gz", "nested archive"),
    ],
)
def test_unsafe_member_fails(tmp_path: Path, member: str, reason: str) -> None:
    path = make_zip(tmp_path / "bad.zip", {CSV_NAME: csv_bytes(VALID_ROWS), member: b"x"})
    result = checks.check_member_paths(_infos(path))
    assert not result.passed
    assert reason in result.details


def test_safe_members_pass(valid_zip: Path) -> None:
    assert checks.check_member_paths(_infos(valid_zip)).passed


# --- expected_member ------------------------------------------------------------


def test_exact_expected_member_passes(valid_zip: Path) -> None:
    assert checks.check_expected_member(_infos(valid_zip), CSV_NAME).passed


def test_directory_entries_are_ignored(tmp_path: Path) -> None:
    path = make_zip(tmp_path / "d.zip", {"folder/": b"", CSV_NAME: csv_bytes(VALID_ROWS)})
    # "folder/" is a directory; the only *file* is still the expected CSV.
    assert checks.check_expected_member(_infos(path), CSV_NAME).passed


def test_wrong_member_name_fails(tmp_path: Path) -> None:
    path = make_zip(tmp_path / "w.zip", {"other.csv": csv_bytes(VALID_ROWS)})
    result = checks.check_expected_member(_infos(path), CSV_NAME)
    assert not result.passed
    assert "other.csv" in result.details


def test_extra_member_fails(tmp_path: Path) -> None:
    path = make_zip(tmp_path / "x.zip", {CSV_NAME: csv_bytes(VALID_ROWS), "readme.txt": b"hi"})
    assert not checks.check_expected_member(_infos(path), CSV_NAME).passed


def test_empty_archive_fails_expected_member(tmp_path: Path) -> None:
    path = make_zip(tmp_path / "none.zip", {})
    assert not checks.check_expected_member(_infos(path), CSV_NAME).passed


# --- uncompressed_size / compression_ratio ---------------------------------------------


def test_uncompressed_size_over_limit_fails(valid_zip: Path) -> None:
    assert not checks.check_uncompressed_size(_infos(valid_zip), max_bytes=10).passed


def test_uncompressed_size_within_limit_passes(valid_zip: Path) -> None:
    assert checks.check_uncompressed_size(_infos(valid_zip), max_bytes=1_000_000).passed


def test_high_compression_ratio_fails(tmp_path: Path) -> None:
    # 1 MB of one repeated byte deflates ~1000:1, the shape of a zip bomb.
    path = make_zip(tmp_path / "bomb.zip", {CSV_NAME: b"0" * 1_000_000})
    result = checks.check_compression_ratio(_infos(path), max_ratio=100)
    assert not result.passed
    assert "exceeds" in result.details


def test_normal_compression_ratio_passes(valid_zip: Path) -> None:
    assert checks.check_compression_ratio(_infos(valid_zip), max_ratio=100).passed
