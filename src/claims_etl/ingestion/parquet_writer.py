"""Convert a validated CSV stream to Parquet: every source column as string, plus metadata.

Raw is a faithful copy of the source: no renaming, trimming, type inference,
or empty-string-to-NULL conversion. That all happens in dbt staging.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import IO

import pyarrow as pa
import pyarrow.parquet as pq

from claims_etl.ingestion.csv_inspect import open_csv_reader

_TIMESTAMP_TYPE = pa.timestamp("us", tz="UTC")


@dataclass(frozen=True)
class SourceMetadata:
    """Values for the metadata columns, constant for one delivered file."""

    source_uri: str
    source_file: str
    source_checksum: str
    ingested_at: datetime  # must be timezone-aware UTC
    batch_id: str


def build_schema(columns: Sequence[str]) -> pa.Schema:
    """All source columns as nullable string, followed by the metadata columns."""
    source_fields = [pa.field(col, pa.string()) for col in columns]
    metadata_fields = [
        pa.field("_source_uri", pa.string(), nullable=False),
        pa.field("_source_file", pa.string(), nullable=False),
        pa.field("_source_checksum", pa.string(), nullable=False),
        pa.field("_source_row_number", pa.int64(), nullable=False),
        pa.field("_ingested_at", _TIMESTAMP_TYPE, nullable=False),
        pa.field("_batch_id", pa.string(), nullable=False),
    ]
    return pa.schema(source_fields + metadata_fields)


def write_parquet(
    csv_binary: IO[bytes],
    out_path: Path,
    columns: Sequence[str],
    metadata: SourceMetadata,
    batch_rows: int = 50_000,
) -> int:
    """Stream CSV rows into a Parquet file in fixed-size record batches.

    ``_source_row_number`` is the physical line number in the CSV (header is
    line 1, so the first data row is 2). Returns the number of rows written.
    Raises ValueError if the stream does not match ``columns``; call this only
    after validation has passed.
    """
    if metadata.ingested_at.tzinfo is None:
        raise ValueError("ingested_at must be timezone-aware (UTC)")

    schema = build_schema(columns)
    reader = open_csv_reader(csv_binary)
    header = next(reader, None)
    if header != list(columns):
        raise ValueError("CSV header does not match expected columns; validate before writing")

    rows: list[list[str]] = []
    line_numbers: list[int] = []
    written = 0
    with pq.ParquetWriter(out_path, schema, compression="snappy") as writer:
        for row in reader:
            if len(row) != len(columns):
                raise ValueError(
                    f"line {reader.line_num}: {len(row)} fields, expected {len(columns)}"
                )
            rows.append(row)
            line_numbers.append(reader.line_num)
            if len(rows) >= batch_rows:
                writer.write_batch(_to_record_batch(rows, line_numbers, schema, metadata))
                written += len(rows)
                rows.clear()
                line_numbers.clear()
        if rows:
            writer.write_batch(_to_record_batch(rows, line_numbers, schema, metadata))
            written += len(rows)
    return written


def _to_record_batch(
    rows: list[list[str]], line_numbers: list[int], schema: pa.Schema, metadata: SourceMetadata
) -> pa.RecordBatch:
    """Transpose row-major CSV rows into an Arrow record batch with metadata columns."""
    n = len(rows)
    source_arrays = [pa.array(column, type=pa.string()) for column in zip(*rows, strict=True)]
    metadata_arrays = [
        pa.array([metadata.source_uri] * n, type=pa.string()),
        pa.array([metadata.source_file] * n, type=pa.string()),
        pa.array([metadata.source_checksum] * n, type=pa.string()),
        pa.array(line_numbers, type=pa.int64()),
        pa.array([metadata.ingested_at] * n, type=_TIMESTAMP_TYPE),
        pa.array([metadata.batch_id] * n, type=pa.string()),
    ]
    return pa.RecordBatch.from_arrays(source_arrays + metadata_arrays, schema=schema)


def parquet_row_count(path: Path) -> int:
    """Row count from the Parquet footer (independent of the writer's own count)."""
    return pq.read_metadata(path).num_rows
