"""Where the DuckDB file (staging + intermediate) lives between runs.

The runner always builds a WORKING COPY: fetch() puts the current state into the
run's folder, dbt builds against that copy, and publish() replaces the stored state
only when the build succeeded. A failed build therefore never changes the state.

S3StateStore keeps it in S3. The upload is conditional on the ETag downloaded at the
start (If-Match), or on there being no state yet (If-None-Match), so two writers can
never silently overwrite each other; a conflict raises and the run is retried from
the newer state. The previous state is copied to backup_key before each upload.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any, Protocol

from botocore.exceptions import ClientError

from claims_etl.ingestion.log import log_event

logger = logging.getLogger(__name__)


class StateConflictError(RuntimeError):
    """The stored state changed since it was fetched (another writer)."""


class StateStore(Protocol):
    def fetch(self, dest: Path) -> bool:
        """Copy the current state to dest; False if there is no state yet."""

    def publish(self, src: Path) -> None:
        """Replace the stored state with src (only after a successful build)."""

    def describe(self) -> str: ...


class LocalStateStore:
    """A DuckDB file on disk (the CLI's --db option and the offline tests)."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def fetch(self, dest: Path) -> bool:
        if not self.path.exists():
            return False
        shutil.copy2(self.path, dest)
        return True

    def publish(self, src: Path) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, self.path)

    def describe(self) -> str:
        return str(self.path)


class S3StateStore:
    """The state object in S3, with an optimistic-concurrency (ETag) upload."""

    def __init__(self, client: Any, bucket: str, key: str, backup_key: str) -> None:
        self._client, self.bucket, self.key, self.backup_key = client, bucket, key, backup_key
        self._etag: str | None = None
        self._fetched = False

    def fetch(self, dest: Path) -> bool:
        self._fetched = True
        try:
            response = self._client.get_object(Bucket=self.bucket, Key=self.key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                self._etag = None
                log_event(logger, "state_missing", uri=self.describe())
                return False
            raise
        self._etag = response["ETag"]
        with dest.open("wb") as fh:
            for chunk in iter(lambda: response["Body"].read(8 * 1024 * 1024), b""):
                fh.write(chunk)
        log_event(logger, "state_fetched", uri=self.describe(), etag=self._etag,
                  bytes=dest.stat().st_size)  # fmt: skip
        return True

    def publish(self, src: Path) -> None:
        if not self._fetched:
            raise RuntimeError("publish() before fetch(): the ETag to check is unknown")
        condition = {"IfMatch": self._etag} if self._etag else {"IfNoneMatch": "*"}
        if self._etag:
            self._client.copy_object(
                Bucket=self.bucket,
                Key=self.backup_key,
                CopySource={"Bucket": self.bucket, "Key": self.key},
                CopySourceIfMatch=self._etag,
            )
        try:
            with src.open("rb") as body:
                response = self._client.put_object(
                    Bucket=self.bucket, Key=self.key, Body=body, **condition
                )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("PreconditionFailed", "ConditionalRequestConflict", "412", "409"):
                raise StateConflictError(f"{self.describe()} changed during the run") from exc
            raise
        log_event(logger, "state_published", uri=self.describe(), previous_etag=self._etag,
                  etag=response.get("ETag"), bytes=src.stat().st_size)  # fmt: skip
        self._etag = response.get("ETag")

    def describe(self) -> str:
        return f"s3://{self.bucket}/{self.key}"
