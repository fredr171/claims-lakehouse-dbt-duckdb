"""CSV -> Parquet: all-string source columns, metadata columns, row numbers, batching."""

from __future__ import annotations

import io
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from claims_etl.ingestion.models import METADATA_COLUMNS
from claims_etl.ingestion.parquet_writer import (
    SourceMetadata,
    build_schema,
    parquet_row_count,
    write_parquet,
)
from tests.conftest import COLUMNS, FILE_NAME, VALID_ROWS, csv_bytes

META = SourceMetadata(
    source_uri=f"s3://test-landing/cms/cclf/claims_header/{FILE_NAME}",
    source_file=FILE_NAME,
    source_checksum="ab" * 32,
    ingested_at=datetime(2026, 11, 2, 12, 0, tzinfo=UTC),
    batch_id="00000000-0000-0000-0000-000000000001",
)


def _write(tmp_path: Path, data: bytes, batch_rows: int = 2) -> tuple[int, pa.Table]:
    out = tmp_path / "part-0.parquet"
    written = write_parquet(io.BytesIO(data), out, COLUMNS, META, batch_rows=batch_rows)
    return written, pq.read_table(out)


def test_schema_has_source_columns_as_string_then_metadata() -> None:
    schema = build_schema(COLUMNS)
    assert schema.names == [*COLUMNS, *METADATA_COLUMNS]
    assert all(schema.field(c).type == pa.string() for c in COLUMNS)
    assert schema.field("_source_row_number").type == pa.int64()
    assert schema.field("_ingested_at").type == pa.timestamp("us", tz="UTC")


def test_values_are_preserved_exactly(tmp_path: Path) -> None:
    written, table = _write(tmp_path, csv_bytes(VALID_ROWS))
    assert written == 3
    assert table.num_rows == 3
    # leading zeros kept, empty string kept as "" (not NULL); dbt decides later
    assert table.column("PRVDR_FAC_FIPS_ST_CD").to_pylist() == ["39", "05", ""]
    assert table.column("CUR_CLM_UNIQ_ID").to_pylist()[0] == "1000085822452"


def test_metadata_columns_are_populated(tmp_path: Path) -> None:
    _, table = _write(tmp_path, csv_bytes(VALID_ROWS))
    row = table.slice(0, 1).to_pylist()[0]
    assert row["_source_uri"] == META.source_uri
    assert row["_source_file"] == FILE_NAME
    assert row["_source_checksum"] == META.source_checksum
    assert row["_ingested_at"] == META.ingested_at
    assert row["_batch_id"] == META.batch_id


def test_row_numbers_are_csv_line_numbers(tmp_path: Path) -> None:
    _, table = _write(tmp_path, csv_bytes(VALID_ROWS))
    assert table.column("_source_row_number").to_pylist() == [2, 3, 4]


def test_multiple_batches_keep_all_rows_in_order(tmp_path: Path) -> None:
    rows = [[f"{1000085822000 + i}", f"{i}AA0AA0AA00", f"{i % 60:02d}"] for i in range(7)]
    written, table = _write(tmp_path, csv_bytes(rows), batch_rows=3)
    assert written == table.num_rows == 7
    assert table.column("CUR_CLM_UNIQ_ID").to_pylist() == [r[0] for r in rows]
    assert table.column("_source_row_number").to_pylist() == list(range(2, 9))


def test_parquet_row_count_reads_footer(tmp_path: Path) -> None:
    _write(tmp_path, csv_bytes(VALID_ROWS))
    assert parquet_row_count(tmp_path / "part-0.parquet") == 3


def test_header_mismatch_raises(tmp_path: Path) -> None:
    data = csv_bytes(VALID_ROWS, header=["A", "B", "C"])
    with pytest.raises(ValueError, match="header"):
        write_parquet(io.BytesIO(data), tmp_path / "x.parquet", COLUMNS, META)


def test_naive_timestamp_is_rejected(tmp_path: Path) -> None:
    naive = SourceMetadata(**{**META.__dict__, "ingested_at": datetime(2026, 11, 2)})
    with pytest.raises(ValueError, match="timezone"):
        write_parquet(io.BytesIO(csv_bytes(VALID_ROWS)), tmp_path / "x.parquet", COLUMNS, naive)
