"""Shared helpers for the offline dbt end-to-end tests (no S3, no AWS credentials).

Generates both feeds with the root generators, runs every CSV through the real
ingestion code into a local raw folder laid out like S3, and runs dbt against it.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from claims_etl.ingestion.config import load_config
from claims_etl.ingestion.parquet_writer import SourceMetadata, write_parquet
from claims_etl.ingestion.utils import sha256_file
from claims_etl.ingestion.validation import validate_file

LAKEHOUSE = Path(__file__).resolve().parents[1]
DBT_DIR = LAKEHOUSE / "dbt"
# The generators come from the pinned synthetic-payer-files package (dev extra).
GENERATOR = "synthetic_payer_files.generate_cclf"
MA_PAYER_GENERATOR = "synthetic_payer_files.generate_ma_payer"
DBT = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")
INTERMEDIATE = [
    "int_cclf__beneficiary_versions",
    "int_cclf__claim_families",
    "int_ma_payer__member_versions",
    "int_ma_payer__claim_families",
]
MARTS = ["dim_payer", "dim_member", "fct_claims", "fct_member_months", "agg_payer_monthly"]
FIXED_INGESTED_AT = datetime(2026, 1, 1, tzinfo=UTC)

HAS_DBT = importlib.util.find_spec("synthetic_payer_files") is not None and DBT.is_file()


# --- data preparation -----------------------------------------------------------------


def _generate(out: Path) -> tuple[Path, Path]:
    """Run both generators (seed 42, 36 months) into out/; return (CSV root, CMS answer key).

    CSVs land in out/output/{cms,ma_payer}/YYYY-MM/; the MA payer answer key in
    out/answer_key/ma_payer/ and the shared crossover list in out/answer_key/.
    """
    csv_root, key_dir = out / "output", out / "answer_key"
    months = ["--seed", "42", "--start", "2023-01", "--end", "2025-12"]
    subprocess.run(
        [sys.executable, "-m", GENERATOR, *months,
         "--output-dir", str(csv_root / "cms"), "--answer-key-dir", str(key_dir)],
        check=True, capture_output=True,
    )  # fmt: skip
    subprocess.run(
        [sys.executable, "-m", MA_PAYER_GENERATOR, *months,
         "--output-dir", str(csv_root / "ma_payer"), "--answer-key-dir", str(key_dir / "ma_payer"),
         "--crossover-dir", str(key_dir)],
        check=True, capture_output=True,
    )  # fmt: skip
    return csv_root, key_dir


def _ingest_locally(csv_root: Path, raw_store: Path) -> list[str]:
    """Validate + convert every CSV (both feeds) as the Lambda does; return delivery months."""
    config = load_config()
    months = set()
    for path in sorted(csv_root.glob("*/*/*.csv")):
        [file_type] = [n for n, ft in config.file_types.items() if ft.match_file_name(path.name)]
        key = f"{config.file_types[file_type].landing_prefix}{file_type}/{path.name}"
        validation = validate_file(path, key, config)
        assert validation.passed, (path.name, validation.failure)
        ft = validation.file_type_config
        checksum = sha256_file(path)
        out = raw_store / ft.raw_key(validation.name_groups, checksum)  # same layout as S3
        out.parent.mkdir(parents=True, exist_ok=True)
        metadata = SourceMetadata(
            f"s3://test/{key}", path.name, checksum, FIXED_INGESTED_AT, "test-batch"
        )
        with path.open("rb") as binary:
            write_parquet(binary, out, ft.expected_columns, metadata)
        months.add(f"{validation.name_groups['year']}-{validation.name_groups['month']}")
    return sorted(months)


def _publish(raw_store: Path, raw: Path, months: list[str]) -> None:
    """Make the given delivery months visible in a raw folder (like Lambda uploads)."""
    for partition in raw_store.glob("*/*/*/delivery_month=*"):  # <feed>/<file_type>/...
        if partition.name.split("=")[1] in months:
            target = raw / partition.relative_to(raw_store)
            shutil.copytree(partition, target, dirs_exist_ok=True)


def deliveries(month_dir: Path) -> list[Path]:
    """Every delivery of one file in a raw month folder, oldest ingested first."""

    def ingested_at(path: Path):
        return max(pq.read_table(path, columns=["_ingested_at"])["_ingested_at"].to_pylist())

    return sorted(month_dir.glob("checksum=*/part-0.parquet"), key=ingested_at)


def current_delivery(month_dir: Path) -> Path:
    """The delivery staging reads (the latest ingested one), as stg_<feed>__deliveries does."""
    return deliveries(month_dir)[-1]


def write_resend(table: pa.Table, month_dir: Path, tag: str) -> Path:
    """Write `table` as a corrected re-send the way ingestion lays it out: a NEW delivery
    (its own checksum folder, ingested a day after the current one) next to the original,
    which stays in raw (immutable bronze). Returns the new file."""
    checksum = hashlib.sha256(f"resent {tag}".encode()).hexdigest()
    latest = max(pq.read_table(current_delivery(month_dir), columns=["_ingested_at"])
                 ["_ingested_at"].to_pylist())  # fmt: skip
    columns = {
        "_source_checksum": pa.array([checksum] * len(table), pa.string()),
        "_ingested_at": pa.array(
            [latest + timedelta(days=1)] * len(table), table.schema.field("_ingested_at").type
        ),  # fmt: skip
    }
    for name, values in columns.items():
        table = table.set_column(table.schema.get_field_index(name), name, values)
    out = month_dir / f"checksum={checksum}" / "part-0.parquet"
    out.parent.mkdir(parents=True)
    pq.write_table(table, out)
    return out


# --- dbt + DuckDB helpers --------------------------------------------------------------


@dataclass
class DbtRun:
    """One dbt invocation: exit code, console tail, and node statuses from run_results.json."""

    returncode: int
    output: str
    statuses: dict[str, str]  # unique_id -> success | error | fail | warn | pass | skipped

    def with_status(self, status: str) -> list[str]:
        return sorted(node for node, s in self.statuses.items() if s == status)


def _dbt(
    args: list[str],
    raw: Path,
    db: Path,
    work: Path,
    *,
    check: bool = True,
    extra_vars: dict | None = None,
) -> DbtRun:
    """Run one dbt command against a local raw folder and DuckDB file.

    check=True (the default) fails the test on a non-zero exit code.
    """
    env = {
        k: v for k, v in os.environ.items() if not k.startswith("AWS_") and not k.startswith("DBT_")
    }
    env.update(
        CLAIMS_DUCKDB_PATH=str(db),
        # Marts Parquet next to the database it belongs to (mart views point at these files).
        CLAIMS_MARTS_ROOT=marts_dir(db).as_posix(),
        # Dummy credentials: the profile creates an S3 secret, but nothing reads S3.
        AWS_ACCESS_KEY_ID="testing",
        AWS_SECRET_ACCESS_KEY="testing",
        AWS_REGION="us-east-2",
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
        PYTHONIOENCODING="utf-8",
    )
    marts_dir(db).mkdir(parents=True, exist_ok=True)  # DuckDB does not create folders
    dbt_vars = {
        "cclf_raw_root": (raw / "cms" / "cclf").as_posix(),
        "ma_payer_raw_root": (raw / "ma_payer" / "ma").as_posix(),
        **(extra_vars or {}),
    }
    command = [
        str(DBT), *args,
        "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
        "--target-path", str(work / "target"), "--log-path", str(work / "logs"),
        "--vars", json.dumps(dbt_vars),
    ]  # fmt: skip
    result = subprocess.run(command, env=env, capture_output=True, text=True, encoding="utf-8")
    if check:
        assert result.returncode == 0, result.stdout[-4000:]
    results_file = work / "target" / "run_results.json"
    statuses = {}
    if results_file.is_file():
        run_results = json.loads(results_file.read_text(encoding="utf-8"))
        statuses = {r["unique_id"]: r["status"] for r in run_results["results"]}
    return DbtRun(result.returncode, result.stdout[-4000:], statuses)


def marts_dir(db: Path) -> Path:
    """Where a build into `db` writes its marts Parquet (one file per mart)."""
    return db.parent / f"{db.stem}_marts"


def _query(db: Path, sql: str) -> list[tuple]:
    with duckdb.connect(str(db), read_only=True) as con:
        return con.execute(sql).fetchall()


def _assert_same_intermediate(db_a: Path, db_b: Path) -> None:
    """Both databases hold identical intermediate tables and marts (row for row)."""
    with duckdb.connect() as con:
        con.execute(f"ATTACH '{db_a.as_posix()}' AS a (READ_ONLY)")
        con.execute(f"ATTACH '{db_b.as_posix()}' AS b (READ_ONLY)")
        tables = [f"intermediate.{t}" for t in INTERMEDIATE] + [f"marts.{t}" for t in MARTS]
        for table in tables:
            a, b = f"a.{table}", f"b.{table}"
            counts = con.execute(f"select (select count(*) from {a}), (select count(*) from {b})")
            n_a, n_b = counts.fetchone()
            only_a = con.execute(f"select count(*) from (from {a} except all from {b})").fetchone()
            only_b = con.execute(f"select count(*) from (from {b} except all from {a})").fetchone()
            assert (n_a, only_a[0], only_b[0]) == (n_b, 0, 0), table
