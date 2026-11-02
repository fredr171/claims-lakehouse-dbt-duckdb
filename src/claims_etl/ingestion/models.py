"""Plain data types shared across the ingestion modules."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

# Metadata columns appended to every raw table, in output order (see CLAUDE.md).
METADATA_COLUMNS: tuple[str, ...] = (
    "_source_uri",
    "_source_file",
    "_source_checksum",
    "_source_row_number",
    "_ingested_at",
    "_batch_id",
)


class Outcome(StrEnum):
    """Final state of one zip after process_zip."""

    SUCCESS = "success"
    SKIPPED_DUPLICATE = "skipped_duplicate"
    QUARANTINED = "quarantined"  # the data failed a check; zip moved to quarantine/
    ERROR = "error"  # infrastructure failure; zip left in place for a retry


@dataclass(frozen=True)
class CheckResult:
    """Result of a single validation check."""

    name: str
    passed: bool
    details: str = ""

    @classmethod
    def ok(cls, name: str, details: str = "") -> CheckResult:
        """Build a passing result."""
        return cls(name=name, passed=True, details=details)

    @classmethod
    def fail(cls, name: str, details: str) -> CheckResult:
        """Build a failing result; details should say what was wrong and where."""
        return cls(name=name, passed=False, details=details)

    def to_dict(self) -> dict[str, Any]:
        """Serialize for the manifest."""
        return {"name": self.name, "passed": self.passed, "details": self.details}


@dataclass
class CsvProfile:
    """Facts gathered from a single streaming pass over a CSV member.

    The CSV checks are small functions that read this profile, so the file is
    parsed once no matter how many checks run.
    """

    header: list[str] | None = None
    row_count: int = 0
    ragged_row_count: int = 0
    ragged_row_examples: list[str] = field(default_factory=list)
    empty_key_count: int = 0
    empty_key_examples: list[str] = field(default_factory=list)
    decode_error: str | None = None
    parse_error: str | None = None


@dataclass
class ManifestRecord:
    """Audit record for one processed zip, stored at _manifests/<sha256>.json."""

    source_uri: str
    source_key: str
    zip_name: str
    batch_id: str
    started_at: datetime
    outcome: Outcome = Outcome.ERROR
    checksum: str | None = None
    file_type: str | None = None
    year: str | None = None
    sample: str | None = None
    csv_row_count: int | None = None
    parquet_row_count: int | None = None
    output_uri: str | None = None
    landing_destination_uri: str | None = None
    failed_check: str | None = None
    failure_details: str | None = None
    checks: list[CheckResult] = field(default_factory=list)
    finished_at: datetime | None = None

    @property
    def duration_seconds(self) -> float | None:
        """Wall-clock seconds from start to finish, once finished."""
        if self.finished_at is None:
            return None
        return round((self.finished_at - self.started_at).total_seconds(), 3)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to JSON-safe types (ISO-8601 timestamps, enum values)."""
        return {
            "source_uri": self.source_uri,
            "source_key": self.source_key,
            "zip_name": self.zip_name,
            "checksum_sha256": self.checksum,
            "file_type": self.file_type,
            "year": self.year,
            "sample": self.sample,
            "batch_id": self.batch_id,
            "outcome": self.outcome.value,
            "csv_row_count": self.csv_row_count,
            "parquet_row_count": self.parquet_row_count,
            "output_uri": self.output_uri,
            "landing_destination_uri": self.landing_destination_uri,
            "failed_check": self.failed_check,
            "failure_details": self.failure_details,
            "checks": [c.to_dict() for c in self.checks],
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "duration_seconds": self.duration_seconds,
        }
