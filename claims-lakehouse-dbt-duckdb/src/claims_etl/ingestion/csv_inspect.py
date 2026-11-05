"""Read delivered CSV files: one strict reader shared by validation and the Parquet writer."""

from __future__ import annotations

import csv
import io
from collections.abc import Iterator
from typing import IO

from claims_etl.ingestion.models import CsvProfile


def open_csv_reader(binary: IO[bytes]) -> Iterator[list[str]]:
    """Wrap a binary stream in a strict UTF-8 CSV reader.

    Validation and the Parquet writer both use this one function, so they
    always agree on how rows are split and counted. ``reader.line_num`` gives
    the physical line number of the most recent record.
    """
    text = io.TextIOWrapper(binary, encoding="utf-8", errors="strict", newline="")
    return csv.reader(text, strict=True)


def profile_csv(
    binary: IO[bytes], key_columns: tuple[str, ...], max_examples: int = 5
) -> CsvProfile:
    """Stream a CSV once, collecting what the CSV checks need.

    Records the header, data row count, rows whose field count differs from
    the header, rows with an empty key column, and any decode/parse error
    (which stops the scan). Up to ``max_examples`` offending line numbers are kept.
    """
    profile = CsvProfile()
    reader = open_csv_reader(binary)
    try:
        header = next(reader, None)
        if header is None:
            return profile
        profile.header = header
        width = len(header)
        key_indexes = {col: header.index(col) for col in key_columns if col in header}

        for row in reader:
            profile.row_count += 1
            if len(row) != width:
                profile.ragged_row_count += 1
                if len(profile.ragged_row_examples) < max_examples:
                    profile.ragged_row_examples.append(f"line {reader.line_num}: {len(row)} fields")
                continue
            for col, idx in key_indexes.items():
                if not row[idx].strip():
                    profile.empty_key_count += 1
                    if len(profile.empty_key_examples) < max_examples:
                        profile.empty_key_examples.append(f"line {reader.line_num}: {col}")
    except UnicodeDecodeError as exc:
        profile.decode_error = (
            f"after line {reader.line_num}: {exc.reason} ({exc.object[exc.start : exc.end]!r})"
        )
    except csv.Error as exc:
        profile.parse_error = f"line {reader.line_num}: {exc}"
    return profile
