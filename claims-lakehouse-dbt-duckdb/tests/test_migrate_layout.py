"""scripts/migrate_to_immutable_bronze.py against moto S3: old keys -> checksum keys."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

from claims_etl.ingestion.parquet_writer import SourceMetadata, write_parquet
from claims_etl.ingestion.utils import sha256_file, utc_now
from tests.conftest import (
    COLUMNS,
    LANDING_BUCKET,
    LANDING_KEY,
    RAW_BUCKET,
    RAW_PARTITION,
    VALID_ROWS,
    csv_bytes,
    delivery_key,
    raw_key,
    write_csv,
)

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "migrate_to_immutable_bronze.py"
_spec = importlib.util.spec_from_file_location("migrate_to_immutable_bronze", _SCRIPT)
migration = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = migration  # dataclasses look the module up
_spec.loader.exec_module(migration)

OLD_RAW = f"{RAW_PARTITION}/part-0.parquet"
OLD_ARCHIVE = f"archive/{LANDING_KEY}"


def _old_layout_delivery(s3, tmp_path: Path, rows=VALID_ROWS, name: str = "a") -> str:
    """A delivery as ingestion stored it before immutable bronze; returns its checksum."""
    csv_path = write_csv(tmp_path / f"{name}.csv", csv_bytes(rows))
    checksum = sha256_file(csv_path)
    parquet = tmp_path / f"{name}.parquet"
    meta = SourceMetadata(f"s3://{LANDING_BUCKET}/{LANDING_KEY}", "CCLF1_202301.csv", checksum,
                          utc_now(), "batch-old")  # fmt: skip
    with csv_path.open("rb") as binary:
        write_parquet(binary, parquet, COLUMNS, meta)
    s3.upload(parquet, RAW_BUCKET, OLD_RAW)
    s3.upload(csv_path, LANDING_BUCKET, OLD_ARCHIVE)
    s3.put_json(RAW_BUCKET, f"_manifests/{checksum}.json", {
        "source_key": LANDING_KEY, "file_name": "CCLF1_202301.csv", "checksum_sha256": checksum,
        "file_type": "claims_header", "name_fields": {"year": "2023", "month": "01"},
        "outcome": "success", "output_uri": f"s3://{RAW_BUCKET}/{OLD_RAW}",
        "landing_destination_uri": f"s3://{LANDING_BUCKET}/{OLD_ARCHIVE}",
    })  # fmt: skip
    return checksum


def test_dry_run_changes_nothing(s3, config, tmp_path) -> None:
    _old_layout_delivery(s3, tmp_path)
    summary = migration.migrate(s3, config, apply=False)
    assert (summary["moved"], summary["problems"]) == (1, [])
    assert s3.exists(RAW_BUCKET, OLD_RAW) and s3.exists(LANDING_BUCKET, OLD_ARCHIVE)


def test_apply_moves_to_checksum_keys_and_updates_the_manifest(s3, config, tmp_path) -> None:
    checksum = _old_layout_delivery(s3, tmp_path)

    summary = migration.migrate(s3, config, apply=True)

    assert (summary["moved"], summary["problems"]) == (1, [])
    new_raw, new_archive = raw_key(checksum), delivery_key("archive/", LANDING_KEY, checksum)
    assert s3.exists(RAW_BUCKET, new_raw) and not s3.exists(RAW_BUCKET, OLD_RAW)
    assert s3.exists(LANDING_BUCKET, new_archive) and not s3.exists(LANDING_BUCKET, OLD_ARCHIVE)
    manifest = s3.get_json(RAW_BUCKET, f"_manifests/{checksum}.json")
    assert manifest["output_uri"] == f"s3://{RAW_BUCKET}/{new_raw}"
    assert manifest["landing_destination_uri"] == f"s3://{LANDING_BUCKET}/{new_archive}"
    assert manifest["layout_migration"]["previous_output_uri"] == f"s3://{RAW_BUCKET}/{OLD_RAW}"

    again = migration.migrate(s3, config, apply=True)  # idempotent
    assert (again["moved"], again["already_done"], again["problems"]) == (0, 1, [])


def test_object_that_belongs_to_another_delivery_is_left_alone(s3, config, tmp_path) -> None:
    # Before immutable bronze a re-send overwrote the original's keys: the first
    # manifest's raw/archive objects now hold the SECOND delivery's content.
    first = _old_layout_delivery(s3, tmp_path, name="first")
    second = _old_layout_delivery(s3, tmp_path, rows=VALID_ROWS[:1], name="second")

    summary = migration.migrate(s3, config, apply=True)

    assert summary["moved"] == 1  # the second (its content is really there)
    assert len(summary["problems"]) == 2 and all(
        f"({first[:12]})" in p for p in summary["problems"]
    )
    assert s3.exists(RAW_BUCKET, raw_key(second)) and not s3.exists(RAW_BUCKET, raw_key(first))
    assert "layout_migration" not in s3.get_json(RAW_BUCKET, f"_manifests/{first}.json")


def test_cli_exits_nonzero_when_problems_remain(s3, config, tmp_path, monkeypatch) -> None:
    _old_layout_delivery(s3, tmp_path, name="first")
    _old_layout_delivery(s3, tmp_path, rows=VALID_ROWS[:1], name="second")
    monkeypatch.setattr(migration.S3Store, "for_region", classmethod(lambda cls, _r: s3))
    monkeypatch.setattr(migration, "load_config", lambda _d=None: config)
    assert migration.main([]) == 1
    with pytest.raises(SystemExit):
        migration.main(["--bogus"])
