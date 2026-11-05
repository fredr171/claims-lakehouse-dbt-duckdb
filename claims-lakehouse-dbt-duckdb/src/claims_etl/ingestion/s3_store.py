"""Thin boto3 wrapper: the only module that talks to S3.

Credentials come from the default AWS credential chain (the AWS CLI profile
locally, the execution role in Lambda); nothing is hardcoded.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError

from claims_etl.ingestion.log import log_event

logger = logging.getLogger(__name__)

_NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound"}


class S3VerificationError(RuntimeError):
    """An S3 write could not be confirmed (missing object or size/ETag mismatch)."""


class S3Store:
    """S3 operations used by ingestion and the quarantine notifier."""

    def __init__(self, client: Any) -> None:
        self._client = client

    @classmethod
    def for_region(cls, region: str) -> S3Store:
        """Create a store backed by a boto3 S3 client in the given region."""
        return cls(boto3.session.Session().client("s3", region_name=region))

    @property
    def client(self) -> Any:
        """The underlying boto3 client (for callers needing calls not wrapped here)."""
        return self._client

    def head(self, bucket: str, key: str) -> dict[str, Any] | None:
        """Object metadata, or None if the object does not exist."""
        try:
            return self._client.head_object(Bucket=bucket, Key=key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in _NOT_FOUND_CODES:
                return None
            raise

    def exists(self, bucket: str, key: str) -> bool:
        """True if the object exists."""
        return self.head(bucket, key) is not None

    def download(self, bucket: str, key: str, dest: Path) -> None:
        """Download an object to a local file."""
        self._client.download_file(bucket, key, str(dest))

    def upload(self, src: Path, bucket: str, key: str) -> None:
        """Upload a local file, then confirm the stored object has the same size."""
        self._client.upload_file(str(src), bucket, key)
        stored = self.head(bucket, key)
        expected = src.stat().st_size
        if stored is None or stored["ContentLength"] != expected:
            raise S3VerificationError(f"upload of s3://{bucket}/{key} not confirmed")
        log_event(logger, "s3_upload", uri=f"s3://{bucket}/{key}", bytes=expected)

    def get_json(self, bucket: str, key: str) -> dict[str, Any] | None:
        """Read a JSON object, or None if it does not exist."""
        try:
            body = self._client.get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in _NOT_FOUND_CODES:
                return None
            raise
        return json.loads(body)

    def put_json(self, bucket: str, key: str, data: dict[str, Any]) -> None:
        """Write a JSON object (pretty-printed, so it is readable in the console)."""
        self._client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, indent=2).encode("utf-8"),
            ContentType="application/json",
        )

    def list_keys(self, bucket: str, prefix: str) -> Iterator[str]:
        """All keys under a prefix, across pages."""
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                yield obj["Key"]

    def move(self, bucket: str, src_key: str, dst_key: str) -> None:
        """Move an object within a bucket: copy, verify the copy, then delete the original.

        S3 has no rename. The ETag is compared only for single-part sources,
        because a copy of a multipart upload gets a different ETag. Uses
        CopyObject, so objects must be under 5 GB.
        """
        source = self.head(bucket, src_key)
        if source is None:
            raise FileNotFoundError(f"s3://{bucket}/{src_key} does not exist")

        self._client.copy_object(
            Bucket=bucket, Key=dst_key, CopySource={"Bucket": bucket, "Key": src_key}
        )
        copy = self.head(bucket, dst_key)
        if copy is None or copy["ContentLength"] != source["ContentLength"]:
            raise S3VerificationError(f"copy to s3://{bucket}/{dst_key} not confirmed")
        source_is_multipart = "-" in source["ETag"]
        if not source_is_multipart and copy["ETag"] != source["ETag"]:
            raise S3VerificationError(f"ETag mismatch copying to s3://{bucket}/{dst_key}")

        self._client.delete_object(Bucket=bucket, Key=src_key)
        log_event(logger, "s3_move", bucket=bucket, src=src_key, dst=dst_key)
