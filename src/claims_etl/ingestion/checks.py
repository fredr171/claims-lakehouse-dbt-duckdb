"""Validation checks: each one is a small function returning a CheckResult.

Checks never raise for bad *data*; they report it. Row-level data quality
(code formats, dates, amounts) is deliberately out of scope; that is the job
of dbt staging tests.
"""

from __future__ import annotations

import re
import zipfile
from collections.abc import Collection, Sequence
from pathlib import Path

from claims_etl.ingestion.models import CheckResult, CsvProfile
from claims_etl.ingestion.zip_inspect import unsafe_member_reason

# --- file level --------------------------------------------------------------


def check_known_file_type(file_type: str | None, known: Collection[str]) -> CheckResult:
    """The key's <file_type> folder has a config."""
    name = "known_file_type"
    if file_type is None:
        return CheckResult.fail(name, "key is not of the form <landing_prefix><file_type>/<zip>")
    if file_type not in known:
        return CheckResult.fail(name, f"no config for file type {file_type!r}")
    return CheckResult.ok(name, file_type)


def check_file_name(zip_name: str, pattern: re.Pattern[str]) -> CheckResult:
    """The zip name matches the file type's naming regex."""
    name = "file_name"
    if pattern.fullmatch(zip_name):
        return CheckResult.ok(name, zip_name)
    return CheckResult.fail(name, f"{zip_name!r} does not match {pattern.pattern}")


def check_file_size(size_bytes: int, max_bytes: int) -> CheckResult:
    """The zip is non-empty and within the configured maximum size."""
    name = "file_size"
    if size_bytes == 0:
        return CheckResult.fail(name, "zip is empty (0 bytes)")
    if size_bytes > max_bytes:
        return CheckResult.fail(name, f"{size_bytes} bytes exceeds max {max_bytes}")
    return CheckResult.ok(name, f"{size_bytes} bytes")


# --- archive level -------------------------------------------------------------


def check_zip_opens(path: Path) -> CheckResult:
    """The file is a readable zip archive (central directory parses)."""
    name = "zip_opens"
    try:
        with zipfile.ZipFile(path) as zf:
            count = len(zf.infolist())
    except (zipfile.BadZipFile, OSError) as exc:
        return CheckResult.fail(name, f"not a valid zip: {exc}")
    return CheckResult.ok(name, f"{count} entries")


def check_member_paths(infos: Sequence[zipfile.ZipInfo]) -> CheckResult:
    """No entry has an absolute path, '..' traversal, or is a nested archive."""
    name = "member_paths"
    problems = [
        f"{info.filename!r} ({reason})"
        for info in infos
        if (reason := unsafe_member_reason(info.filename))
    ]
    if problems:
        return CheckResult.fail(name, "unsafe entries: " + ", ".join(problems))
    return CheckResult.ok(name)


def check_expected_member(infos: Sequence[zipfile.ZipInfo], expected_name: str) -> CheckResult:
    """The archive contains exactly one file, named as expected (directories ignored)."""
    name = "expected_member"
    files = [info.filename for info in infos if not info.is_dir()]
    if files == [expected_name]:
        return CheckResult.ok(name, expected_name)
    return CheckResult.fail(name, f"expected exactly [{expected_name!r}], found {files}")


def check_uncompressed_size(infos: Sequence[zipfile.ZipInfo], max_bytes: int) -> CheckResult:
    """Total declared uncompressed size is within the configured maximum."""
    name = "uncompressed_size"
    total = sum(info.file_size for info in infos)
    if total > max_bytes:
        return CheckResult.fail(name, f"{total} bytes uncompressed exceeds max {max_bytes}")
    return CheckResult.ok(name, f"{total} bytes")


def check_compression_ratio(infos: Sequence[zipfile.ZipInfo], max_ratio: float) -> CheckResult:
    """Uncompressed/compressed ratio is within limits (zip-bomb guard)."""
    name = "compression_ratio"
    uncompressed = sum(info.file_size for info in infos)
    compressed = sum(info.compress_size for info in infos)
    if compressed == 0:
        ratio = 0.0 if uncompressed == 0 else float("inf")
    else:
        ratio = uncompressed / compressed
    if ratio > max_ratio:
        return CheckResult.fail(name, f"ratio {ratio:.1f} exceeds max {max_ratio}")
    return CheckResult.ok(name, f"ratio {ratio:.1f}")


def check_zip_crc(zf: zipfile.ZipFile) -> CheckResult:
    """Every member's data matches its CRC-32 (ZipFile.testzip)."""
    name = "zip_crc"
    try:
        bad_member = zf.testzip()
    except (zipfile.BadZipFile, OSError, EOFError) as exc:
        return CheckResult.fail(name, f"archive data unreadable: {exc}")
    if bad_member is not None:
        return CheckResult.fail(name, f"CRC mismatch in {bad_member!r}")
    return CheckResult.ok(name)


# --- CSV level -----------------------------------------------------------------


def check_csv_utf8(profile: CsvProfile) -> CheckResult:
    """The CSV decodes as UTF-8 (ASCII is a subset)."""
    name = "csv_utf8"
    if profile.decode_error:
        return CheckResult.fail(name, f"not valid UTF-8 {profile.decode_error}")
    return CheckResult.ok(name)


def check_csv_parseable(profile: CsvProfile) -> CheckResult:
    """The CSV parses under strict quoting rules."""
    name = "csv_parseable"
    if profile.parse_error:
        return CheckResult.fail(name, f"malformed CSV at {profile.parse_error}")
    return CheckResult.ok(name)


def check_csv_header(header: Sequence[str] | None, expected: Sequence[str]) -> CheckResult:
    """The header matches the expected columns exactly: same names, same order."""
    name = "csv_header"
    if header is None:
        return CheckResult.fail(name, "CSV is empty: no header row")
    if list(header) == list(expected):
        return CheckResult.ok(name, f"{len(header)} columns")

    problems: list[str] = []
    if missing := [c for c in expected if c not in header]:
        problems.append(f"missing {missing}")
    if unexpected := [c for c in header if c not in expected]:
        problems.append(f"unexpected {unexpected}")
    if not problems:
        position = next(
            (i for i, (got, want) in enumerate(zip(header, expected, strict=False)) if got != want),
            None,
        )
        if position is not None:
            problems.append(
                f"order differs at column {position + 1}: "
                f"got {header[position]!r}, expected {expected[position]!r}"
            )
        else:
            problems.append(f"got {len(header)} columns, expected {len(expected)}")
    return CheckResult.fail(name, "; ".join(problems))


def check_csv_field_counts(profile: CsvProfile) -> CheckResult:
    """Every data row has as many fields as the header."""
    name = "csv_field_counts"
    if profile.ragged_row_count:
        examples = ", ".join(profile.ragged_row_examples)
        width = len(profile.header or [])
        return CheckResult.fail(
            name, f"{profile.ragged_row_count} row(s) without {width} fields; e.g. {examples}"
        )
    return CheckResult.ok(name)


def check_csv_row_count(profile: CsvProfile) -> CheckResult:
    """There is at least one data row after the header."""
    name = "csv_row_count"
    if profile.row_count == 0:
        return CheckResult.fail(name, "header present but no data rows")
    return CheckResult.ok(name, f"{profile.row_count} data rows")


def check_csv_key_columns(profile: CsvProfile, key_columns: Sequence[str]) -> CheckResult:
    """Key columns are never empty or whitespace-only."""
    name = "csv_key_columns"
    if profile.empty_key_count:
        examples = ", ".join(profile.empty_key_examples)
        return CheckResult.fail(
            name,
            f"{profile.empty_key_count} empty key value(s) in {list(key_columns)}; e.g. {examples}",
        )
    return CheckResult.ok(name, ", ".join(key_columns))


# --- post-write ----------------------------------------------------------------


def check_row_count_reconciliation(csv_rows: int, parquet_rows: int) -> CheckResult:
    """Parquet row count equals CSV data row count."""
    name = "row_count_reconciliation"
    if csv_rows != parquet_rows:
        return CheckResult.fail(name, f"CSV has {csv_rows} data rows, Parquet has {parquet_rows}")
    return CheckResult.ok(name, f"{csv_rows} rows")
