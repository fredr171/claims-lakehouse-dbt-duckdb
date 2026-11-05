"""Shared helpers for the offline dbt end-to-end tests (no S3, no AWS credentials).

Generates both feeds with the root generators, runs every CSV through the real
ingestion code into a local raw folder laid out like S3, and runs dbt against it.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from claims_etl.ingestion.config import load_config
from claims_etl.ingestion.parquet_writer import SourceMetadata, write_parquet
from claims_etl.ingestion.utils import sha256_file
from claims_etl.ingestion.validation import validate_file

LAKEHOUSE = Path(__file__).resolve().parents[1]
DBT_DIR = LAKEHOUSE / "dbt"
GENERATOR = LAKEHOUSE.parent / "generate_cclf.py"
HUMANA_GENERATOR = LAKEHOUSE.parent / "generate_humana.py"
DBT = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")
INTERMEDIATE = [
    "int_cclf__beneficiary_versions",
    "int_cclf__claim_families",
    "int_humana__member_versions",
    "int_humana__claim_families",
]
FIXED_INGESTED_AT = datetime(2026, 1, 1, tzinfo=UTC)

HAS_DBT = GENERATOR.is_file() and HUMANA_GENERATOR.is_file() and DBT.is_file()


# --- data preparation -----------------------------------------------------------------


def _generate(out: Path) -> tuple[Path, Path]:
    """Run both generators (seed 42, 36 months) into out/; return (CSV root, CMS answer key).

    CSVs land in out/output/{cms,humana}/YYYY-MM/; the Humana answer key in
    out/answer_key/humana/ and the shared crossover list in out/answer_key/.
    """
    csv_root, key_dir = out / "output", out / "answer_key"
    months = ["--seed", "42", "--start", "2023-01", "--end", "2025-12"]
    subprocess.run(
        [sys.executable, str(GENERATOR), *months,
         "--output-dir", str(csv_root / "cms"), "--answer-key-dir", str(key_dir)],
        check=True, capture_output=True,
    )  # fmt: skip
    subprocess.run(
        [sys.executable, str(HUMANA_GENERATOR), *months,
         "--output-dir", str(csv_root / "humana"), "--answer-key-dir", str(key_dir / "humana"),
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
        out = raw_store / ft.raw_key(validation.name_groups)  # same key layout as S3
        out.parent.mkdir(parents=True, exist_ok=True)
        metadata = SourceMetadata(
            f"s3://test/{key}", path.name, sha256_file(path), FIXED_INGESTED_AT, "test-batch"
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
        # Dummy credentials: the profile creates an S3 secret, but nothing reads S3.
        AWS_ACCESS_KEY_ID="testing",
        AWS_SECRET_ACCESS_KEY="testing",
        AWS_REGION="us-east-2",
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
        PYTHONIOENCODING="utf-8",
    )
    dbt_vars = {
        "cclf_raw_root": (raw / "cms" / "cclf").as_posix(),
        "humana_raw_root": (raw / "humana" / "ma").as_posix(),
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


def _query(db: Path, sql: str) -> list[tuple]:
    with duckdb.connect(str(db), read_only=True) as con:
        return con.execute(sql).fetchall()


def _assert_same_intermediate(db_a: Path, db_b: Path) -> None:
    """Both databases hold identical intermediate tables (row for row)."""
    with duckdb.connect() as con:
        con.execute(f"ATTACH '{db_a.as_posix()}' AS a (READ_ONLY)")
        con.execute(f"ATTACH '{db_b.as_posix()}' AS b (READ_ONLY)")
        for table in INTERMEDIATE:
            a, b = f"a.intermediate.{table}", f"b.intermediate.{table}"
            counts = con.execute(f"select (select count(*) from {a}), (select count(*) from {b})")
            n_a, n_b = counts.fetchone()
            only_a = con.execute(f"select count(*) from (from {a} except all from {b})").fetchone()
            only_b = con.execute(f"select count(*) from (from {b} except all from {a})").fetchone()
            assert (n_a, only_a[0], only_b[0]) == (n_b, 0, 0), table
