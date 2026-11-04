"""Shared fixtures. Fixture zips are built in tmp_path at test time; no binaries in git.

Tests use a small 3-column beneficiary_summary config so fixtures stay readable.
The real 32-column config is covered separately in test_config.py.
"""

from __future__ import annotations

import zipfile
from collections.abc import Iterator, Sequence
from pathlib import Path

import boto3
import pytest
import yaml
from moto import mock_aws

from claims_etl.ingestion.config import IngestionConfig, load_config
from claims_etl.ingestion.s3_store import S3Store

REGION = "us-east-2"
LANDING_BUCKET = "test-landing"
RAW_BUCKET = "test-raw"

COLUMNS = ["DESYNPUF_ID", "BENE_BIRTH_DT", "SP_STATE_CODE"]
ZIP_NAME = "DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip"
CSV_NAME = "DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"
LANDING_KEY = f"cms/desynpuf/beneficiary_summary/{ZIP_NAME}"
VALID_ROWS = [
    ["00013D2EFD8E45D1", "19230501", "05"],
    ["00016F745862898F", "19430101", "39"],
    ["0001FDD721E223DC", "19360901", ""],
]

TEST_LIMITS = {
    "max_zip_bytes": 1_000_000,
    "max_uncompressed_bytes": 1_000_000,
    "max_compression_ratio": 100,
}


def csv_bytes(
    rows: Sequence[Sequence[str]], header: Sequence[str] = COLUMNS, eol: str = "\r\n"
) -> bytes:
    """Build CSV bytes in the observed DE-SynPUF style: quoted header, unquoted data, CRLF."""
    lines = [",".join(f'"{h}"' for h in header)] + [",".join(r) for r in rows]
    return (eol.join(lines) + eol).encode("utf-8")


def make_zip(
    path: Path, members: dict[str, bytes], compression: int = zipfile.ZIP_DEFLATED
) -> Path:
    """Write a zip with the given member names and contents."""
    with zipfile.ZipFile(path, "w", compression=compression) as zf:
        for name, data in members.items():
            zf.writestr(zipfile.ZipInfo(name), data, compress_type=compression)
    return path


def write_test_config(config_dir: Path) -> Path:
    """Write ingestion.yml + a 3-column beneficiary_summary.yml into config_dir."""
    (config_dir / "file_types").mkdir(parents=True, exist_ok=True)
    settings = {
        "region": REGION,
        "landing_bucket": LANDING_BUCKET,
        "raw_bucket": RAW_BUCKET,
        "landing_prefix": "cms/desynpuf/",
        "archive_prefix": "archive/",
        "quarantine_prefix": "quarantine/",
        "manifest_prefix": "_manifests/",
        "default_limits": TEST_LIMITS,
        "parquet_batch_rows": 2,  # small, so tests exercise multiple batches
    }
    # Reuse the real naming rules so tests exercise them; shrink only the columns.
    real_path = Path(__file__).resolve().parents[1] / "config/file_types/beneficiary_summary.yml"
    file_type = yaml.safe_load(real_path.read_text(encoding="utf-8"))
    file_type.pop("limits", None)
    file_type["expected_columns"] = COLUMNS
    (config_dir / "ingestion.yml").write_text(yaml.safe_dump(settings), encoding="utf-8")
    (config_dir / "file_types" / "beneficiary_summary.yml").write_text(
        yaml.safe_dump(file_type), encoding="utf-8"
    )
    # Notifier settings are used unchanged from the real config.
    real_notifications = Path(__file__).resolve().parents[1] / "config/notifications.yml"
    (config_dir / "notifications.yml").write_text(
        real_notifications.read_text(encoding="utf-8"), encoding="utf-8"
    )
    return config_dir


@pytest.fixture
def config_dir(tmp_path: Path) -> Path:
    return write_test_config(tmp_path / "config")


@pytest.fixture
def config(config_dir: Path) -> IngestionConfig:
    return load_config(config_dir)


@pytest.fixture
def valid_zip(tmp_path: Path) -> Path:
    return make_zip(tmp_path / ZIP_NAME, {CSV_NAME: csv_bytes(VALID_ROWS)})


@pytest.fixture
def s3(monkeypatch: pytest.MonkeyPatch) -> Iterator[S3Store]:
    """In-memory S3 (moto) with empty landing and raw buckets. Never touches AWS."""
    for var in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        for bucket in (LANDING_BUCKET, RAW_BUCKET):
            client.create_bucket(
                Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": REGION}
            )
        yield S3Store(client)
