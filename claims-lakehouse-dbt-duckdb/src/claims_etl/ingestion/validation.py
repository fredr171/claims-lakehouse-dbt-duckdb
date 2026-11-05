"""Run the validation checks in order, stopping at the first failure.

Order matters: cheap checks on the key and file metadata come first, then a
single streaming pass over the CSV feeds every CSV-level check.
"""

from __future__ import annotations

import logging
import posixpath
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path

from claims_etl.ingestion import checks
from claims_etl.ingestion.config import FileTypeConfig, IngestionConfig
from claims_etl.ingestion.csv_inspect import profile_csv
from claims_etl.ingestion.log import log_event
from claims_etl.ingestion.models import CheckResult

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Every check that ran, plus facts later steps need from a passing file."""

    checks: list[CheckResult] = field(default_factory=list)
    file_type_config: FileTypeConfig | None = None
    name_groups: dict[str, str] = field(default_factory=dict)
    csv_row_count: int | None = None

    @property
    def passed(self) -> bool:
        """True if at least one check ran and none failed."""
        return bool(self.checks) and all(c.passed for c in self.checks)

    @property
    def failure(self) -> CheckResult | None:
        """The first failed check, if any."""
        return next((c for c in self.checks if not c.passed), None)


def validate_file(local_csv: Path, s3_key: str, config: IngestionConfig) -> ValidationResult:
    """Run all file and CSV checks against a downloaded CSV."""
    result = ValidationResult()

    def record(check: CheckResult) -> bool:
        result.checks.append(check)
        log_event(
            logger,
            "check",
            level=logging.INFO if check.passed else logging.WARNING,
            key=s3_key,
            check=check.name,
            passed=check.passed,
            details=check.details,
        )
        return check.passed

    def run(steps: Iterable[Callable[[], CheckResult]]) -> bool:
        # all() short-circuits, so later steps never run after a failure.
        return all(record(step()) for step in steps)

    # File level
    file_type = config.file_type_for_key(s3_key)
    known = config.file_types_for_key(s3_key)
    if not record(checks.check_known_file_type(file_type, known)):
        return result
    ft = config.file_types[file_type]  # type: ignore[index]  # known after the check above
    result.file_type_config = ft
    file_name = posixpath.basename(s3_key)

    if not run(
        [
            lambda: checks.check_file_name(file_name, ft.file_name_regex),
            lambda: checks.check_file_size(local_csv.stat().st_size, ft.limits.max_file_bytes),
        ]
    ):
        return result
    result.name_groups = ft.match_file_name(file_name) or {}

    # CSV level: one streaming pass, then each check reads the profile.
    with local_csv.open("rb") as binary:
        profile = profile_csv(binary, ft.key_columns)

    if not run(
        [
            lambda: checks.check_csv_utf8(profile),
            lambda: checks.check_csv_parseable(profile),
            lambda: checks.check_csv_header(profile.header, ft.expected_columns),
            lambda: checks.check_csv_field_counts(profile),
            lambda: checks.check_csv_row_count(profile),
            lambda: checks.check_csv_key_columns(profile, ft.key_columns),
        ]
    ):
        return result

    result.csv_row_count = profile.row_count
    return result
