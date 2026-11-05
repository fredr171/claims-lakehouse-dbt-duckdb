"""Manifest records: one JSON per delivered-file checksum in the raw bucket.

The manifest is what makes ingestion idempotent: a file whose checksum already
has a ``success`` record is skipped. Only the latest attempt per checksum is
kept; a quarantined file can therefore be fixed and re-delivered.
"""

from __future__ import annotations

from typing import Any

from claims_etl.ingestion.config import IngestionConfig
from claims_etl.ingestion.models import ManifestRecord, Outcome
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.ingestion.utils import s3_uri


def manifest_key(config: IngestionConfig, checksum: str) -> str:
    """Key of the manifest record for a checksum."""
    return f"{config.manifest_prefix}{checksum}.json"


def load_manifest(store: S3Store, config: IngestionConfig, checksum: str) -> dict[str, Any] | None:
    """Existing manifest record for a checksum, or None."""
    return store.get_json(config.raw_bucket, manifest_key(config, checksum))


def is_successful(manifest: dict[str, Any] | None) -> bool:
    """True if the record shows a completed, successful load."""
    return manifest is not None and manifest.get("outcome") == Outcome.SUCCESS.value


def write_manifest(store: S3Store, config: IngestionConfig, record: ManifestRecord) -> str:
    """Write the record and return its S3 URI."""
    if record.checksum is None:
        raise ValueError("cannot write a manifest without a checksum")
    key = manifest_key(config, record.checksum)
    store.put_json(config.raw_bucket, key, record.to_dict())
    return s3_uri(config.raw_bucket, key)
