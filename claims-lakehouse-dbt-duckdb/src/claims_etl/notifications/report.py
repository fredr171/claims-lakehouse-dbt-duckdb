"""Typed view of ingestion's .error.json quarantine report."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class InvalidReportError(ValueError):
    """The .error.json is missing fields or is not a quarantine report."""


@dataclass(frozen=True)
class CheckSummary:
    """One check's name and pass/fail (details are deliberately not carried here)."""

    name: str
    passed: bool


@dataclass(frozen=True)
class QuarantineReport:
    """The fields of an .error.json the notifier uses."""

    error_key: str
    source_uri: str
    file_name: str
    checksum: str
    batch_id: str
    file_type: str | None
    failed_check: str | None
    failure_details: str
    quarantine_uri: str | None
    quarantined_at: str | None
    checks: list[CheckSummary] = field(default_factory=list)

    @classmethod
    def from_dict(cls, error_key: str, data: dict[str, Any]) -> QuarantineReport:
        """Validate and build from the JSON written by ingestion's _quarantine."""
        if data.get("outcome") != "quarantined":
            raise InvalidReportError(f"{error_key}: outcome is {data.get('outcome')!r}")
        checksum = data.get("checksum_sha256") or ""
        # Validated so a crafted report cannot inject text into the checksum line.
        if not _SHA256.fullmatch(checksum):
            raise InvalidReportError(f"{error_key}: checksum_sha256 is not a SHA-256 hex digest")
        for required in ("source_uri", "file_name", "batch_id"):
            if not data.get(required):
                raise InvalidReportError(f"{error_key}: missing {required}")
        return cls(
            error_key=error_key,
            source_uri=data["source_uri"],
            file_name=data["file_name"],
            checksum=checksum,
            batch_id=data["batch_id"],
            file_type=data.get("file_type"),
            failed_check=data.get("failed_check"),
            failure_details=data.get("failure_details") or "",
            quarantine_uri=data.get("landing_destination_uri"),
            quarantined_at=data.get("finished_at"),
            checks=[
                CheckSummary(name=c["name"], passed=bool(c["passed"]))
                for c in data.get("checks", [])
                if isinstance(c, dict) and "name" in c
            ],
        )
