"""Run the validation checks in order, stopping at the first failure.

Order matters: cheap checks on metadata come first, and archive-safety checks
(paths, sizes, ratio) run before any member data is decompressed.
"""

from __future__ import annotations

import logging
import posixpath
import zipfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path

from claims_etl.ingestion import checks
from claims_etl.ingestion.config import FileTypeConfig, IngestionConfig
from claims_etl.ingestion.log import log_event
from claims_etl.ingestion.models import CheckResult
from claims_etl.ingestion.zip_inspect import profile_csv

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Every check that ran, plus facts later steps need from a passing zip."""

    checks: list[CheckResult] = field(default_factory=list)
    file_type_config: FileTypeConfig | None = None
    name_groups: dict[str, str] = field(default_factory=dict)
    csv_member: str | None = None
    csv_row_count: int | None = None

    @property
    def passed(self) -> bool:
        """True if at least one check ran and none failed."""
        return bool(self.checks) and all(c.passed for c in self.checks)

    @property
    def failure(self) -> CheckResult | None:
        """The first failed check, if any."""
        return next((c for c in self.checks if not c.passed), None)


def validate_zip(local_zip: Path, s3_key: str, config: IngestionConfig) -> ValidationResult:
    """Run all file, archive, and CSV checks against a downloaded zip."""
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
    if not record(checks.check_known_file_type(file_type, config.file_types)):
        return result
    ft = config.file_types[file_type]  # type: ignore[index]  # known after the check above
    result.file_type_config = ft
    zip_name = posixpath.basename(s3_key)

    if not run(
        [
            lambda: checks.check_file_name(zip_name, ft.zip_name_regex),
            lambda: checks.check_file_size(local_zip.stat().st_size, ft.limits.max_zip_bytes),
            lambda: checks.check_zip_opens(local_zip),
        ]
    ):
        return result
    result.name_groups = ft.match_zip_name(zip_name) or {}
    expected_member = ft.csv_member_name(result.name_groups)

    with zipfile.ZipFile(local_zip) as zf:
        infos = zf.infolist()
        # Archive level: metadata-only checks first, then the full CRC read.
        if not run(
            [
                lambda: checks.check_member_paths(infos),
                lambda: checks.check_expected_member(infos, expected_member),
                lambda: checks.check_uncompressed_size(infos, ft.limits.max_uncompressed_bytes),
                lambda: checks.check_compression_ratio(infos, ft.limits.max_compression_ratio),
                lambda: checks.check_zip_crc(zf),
            ]
        ):
            return result

        # CSV level: one streaming pass, then each check reads the profile.
        with zf.open(expected_member) as member:
            profile = profile_csv(member, ft.key_columns)

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

    result.csv_member = expected_member
    result.csv_row_count = profile.row_count
    return result
