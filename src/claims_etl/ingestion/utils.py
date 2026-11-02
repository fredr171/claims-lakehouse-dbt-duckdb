"""Small helpers with no project dependencies."""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime
from pathlib import Path

_HASH_CHUNK_BYTES = 1024 * 1024


def utc_now() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def new_batch_id() -> str:
    """Return a new random batch ID (UUID4 string)."""
    return str(uuid.uuid4())


def s3_uri(bucket: str, key: str) -> str:
    """Format an s3:// URI."""
    return f"s3://{bucket}/{key}"


def sha256_file(path: Path) -> str:
    """Return the hex SHA-256 of a file, read in chunks so memory stays flat."""
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(_HASH_CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()
