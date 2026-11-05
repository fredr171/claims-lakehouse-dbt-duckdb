"""Shared fixtures. Fixture CSVs are built in tmp_path at test time; no data files in git.

Tests use a small 3-column claims_header config (CMS CCLF feed) and a 3-column
member_eligibility config (Humana MA feed) so fixtures stay readable. Both keep
the real naming regex, raw key template and key columns; the real headers are
covered separately in test_config.py.
"""

from __future__ import annotations

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

# The two real key columns plus a code that must keep its leading zero.
COLUMNS = ["CUR_CLM_UNIQ_ID", "BENE_MBI_ID", "PRVDR_FAC_FIPS_ST_CD"]
FILE_NAME = "CCLF1_202301.csv"
LANDING_KEY = f"cms/cclf/claims_header/{FILE_NAME}"
RAW_KEY = "cms/cclf/claims_header/delivery_month=2023-01/part-0.parquet"
# Second feed (Humana MA).
HUM_COLUMNS = ["MEMBER_ID", "MBI", "COUNTY_FIPS"]
HUM_FILE_NAME = "HUM_ELIG_202301.csv"
HUM_LANDING_KEY = f"humana/ma/member_eligibility/{HUM_FILE_NAME}"
HUM_RAW_KEY = "humana/ma/member_eligibility/delivery_month=2023-01/part-0.parquet"
HUM_ROWS = [["H100000001", "6AA3QX1NP94", "39099"], ["H100000002", "9GD7PX4RK74", "05001"]]
# Synthetic values in the generator's formats (not real identifiers).
VALID_ROWS = [
    ["1000085822452", "6AA3QX1NP94", "39"],
    ["1000085822479", "9GD7PX4RK74", "05"],
    ["1000085822518", "1KX0GU8TK29", ""],
]

TEST_LIMITS = {"max_file_bytes": 1_000_000}


def csv_bytes(
    rows: Sequence[Sequence[str]], header: Sequence[str] = COLUMNS, eol: str = "\n"
) -> bytes:
    """Build CSV bytes in the delivered CCLF style: unquoted header and data, LF."""
    lines = [",".join(header)] + [",".join(r) for r in rows]
    return (eol.join(lines) + eol).encode("utf-8")


def write_csv(path: Path, data: bytes) -> Path:
    """Write fixture CSV bytes to a file and return its path."""
    path.write_bytes(data)
    return path


def write_test_config(config_dir: Path) -> Path:
    """Write ingestion.yml + a 3-column claims_header.yml into config_dir."""
    (config_dir / "file_types").mkdir(parents=True, exist_ok=True)
    settings = {
        "region": REGION,
        "landing_bucket": LANDING_BUCKET,
        "raw_bucket": RAW_BUCKET,
        "landing_prefixes": ["cms/cclf/", "humana/ma/"],
        "archive_prefix": "archive/",
        "quarantine_prefix": "quarantine/",
        "manifest_prefix": "_manifests/",
        "default_limits": TEST_LIMITS,
        "parquet_batch_rows": 2,  # small, so tests exercise multiple batches
    }
    # Reuse the real naming rules so tests exercise them; shrink only the columns.
    (config_dir / "ingestion.yml").write_text(yaml.safe_dump(settings), encoding="utf-8")
    real_dir = Path(__file__).resolve().parents[1] / "config/file_types"
    for name, columns in (("claims_header", COLUMNS), ("member_eligibility", HUM_COLUMNS)):
        file_type = yaml.safe_load((real_dir / f"{name}.yml").read_text(encoding="utf-8"))
        file_type.pop("limits", None)
        file_type["expected_columns"] = columns
        (config_dir / "file_types" / f"{name}.yml").write_text(
            yaml.safe_dump(file_type), encoding="utf-8"
        )
    # Notifier and transform settings are used unchanged from the real config.
    for name in ("notifications.yml", "transform.yml"):
        real = Path(__file__).resolve().parents[1] / "config" / name
        (config_dir / name).write_text(real.read_text(encoding="utf-8"), encoding="utf-8")
    return config_dir


@pytest.fixture
def config_dir(tmp_path: Path) -> Path:
    return write_test_config(tmp_path / "config")


@pytest.fixture
def config(config_dir: Path) -> IngestionConfig:
    return load_config(config_dir)


@pytest.fixture
def valid_csv(tmp_path: Path) -> Path:
    return write_csv(tmp_path / FILE_NAME, csv_bytes(VALID_ROWS))


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
