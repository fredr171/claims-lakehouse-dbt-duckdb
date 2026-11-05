"""process_file: the landing -> raw flow for one delivered CSV (see CLAUDE.md).

1. Triggered with a landing key (Lambda S3 event or the CLI).
2. Download, SHA-256.            -> already loaded? skipped_duplicate, archive.
3. Validate.                     -> failure? quarantine + .error.json.
4. Write Parquet locally.
5. Reconcile row counts.         -> mismatch? quarantine.
6. Upload to raw (deterministic part-0.parquet, so re-runs overwrite).
7. Write the manifest, archive the file.

Data problems end in ``quarantined``. Infrastructure problems (S3, network,
disk) end in ``error`` and leave the file where it is, so a re-run retries it.
"""

from __future__ import annotations

import logging
import posixpath
import tempfile
from pathlib import Path

from claims_etl.ingestion import manifest
from claims_etl.ingestion.checks import check_row_count_reconciliation
from claims_etl.ingestion.config import IngestionConfig, load_config
from claims_etl.ingestion.log import log_event
from claims_etl.ingestion.models import CheckResult, ManifestRecord, Outcome
from claims_etl.ingestion.parquet_writer import SourceMetadata, parquet_row_count, write_parquet
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.ingestion.utils import new_batch_id, s3_uri, sha256_file, utc_now
from claims_etl.ingestion.validation import validate_file

logger = logging.getLogger(__name__)


def process_file(
    s3_key: str,
    *,
    config: IngestionConfig | None = None,
    store: S3Store | None = None,
    batch_id: str | None = None,
    work_dir: Path | None = None,
) -> ManifestRecord:
    """Ingest one landing CSV into the raw bucket. Never raises for bad data.

    Args:
        s3_key: Key in the landing bucket, under the landing prefix, ending in .csv.
        config: Loaded config; defaults to config/ in the repo.
        store: S3 access; defaults to a boto3 client in the configured region.
        batch_id: Run ID shared by every file in one run; generated if omitted.
        work_dir: Parent dir for the temporary working folder.

    Returns:
        The manifest record describing what happened.
    """
    config = config or load_config()
    if not config.is_landing_file(s3_key):
        raise ValueError(f"{s3_key!r} is not a .csv under {list(config.landing_prefixes)}")
    store = store or S3Store.for_region(config.region)

    record = ManifestRecord(
        source_uri=s3_uri(config.landing_bucket, s3_key),
        source_key=s3_key,
        file_name=posixpath.basename(s3_key),
        batch_id=batch_id or new_batch_id(),
        started_at=utc_now(),
    )
    log_event(logger, "file_started", key=s3_key, batch_id=record.batch_id)

    try:
        with tempfile.TemporaryDirectory(prefix="claims_etl_", dir=work_dir) as tmp:
            _run(s3_key, record, config, store, Path(tmp))
    except Exception as exc:  # noqa: BLE001 - one failing file must not crash a scan
        record.outcome = Outcome.ERROR
        record.failure_details = f"{type(exc).__name__}: {exc}"
        logger.exception("file_error", extra={"fields": {"key": s3_key}})

    record.finished_at = utc_now()
    log_event(
        logger,
        "file_finished",
        key=s3_key,
        outcome=record.outcome.value,
        csv_rows=record.csv_row_count,
        parquet_rows=record.parquet_row_count,
        output_uri=record.output_uri,
        failed_check=record.failed_check,
        duration_seconds=record.duration_seconds,
    )
    return record


def _run(
    s3_key: str, record: ManifestRecord, config: IngestionConfig, store: S3Store, tmp: Path
) -> None:
    """Carry out the flow, updating ``record`` in place."""
    local_csv = tmp / "source.csv"  # fixed local name: never trust the key as a path
    store.download(config.landing_bucket, s3_key, local_csv)
    record.checksum = sha256_file(local_csv)
    log_event(
        logger,
        "file_downloaded",
        key=s3_key,
        sha256=record.checksum,
        bytes=local_csv.stat().st_size,
    )

    # Step 2: idempotency (download + checksum above)
    existing = manifest.load_manifest(store, config, record.checksum)
    if manifest.is_successful(existing):
        assert existing is not None
        record.outcome = Outcome.SKIPPED_DUPLICATE
        record.output_uri = existing.get("output_uri")
        # The existing success record is left untouched; overwriting it would
        # make the next copy of this file look new.
        _archive(record, config, store)
        return

    # Step 3: validation
    validation = validate_file(local_csv, s3_key, config)
    record.checks = list(validation.checks)
    record.file_type = (
        validation.file_type_config.file_type if validation.file_type_config else None
    )
    record.name_fields = dict(validation.name_groups)
    if not validation.passed:
        _quarantine(record, validation.failure, config, store)  # type: ignore[arg-type]
        return
    ft = validation.file_type_config
    assert ft is not None
    record.csv_row_count = validation.csv_row_count

    # Step 4: write Parquet locally
    local_parquet = tmp / "part-0.parquet"
    metadata = SourceMetadata(
        source_uri=record.source_uri,
        source_file=record.file_name,
        source_checksum=record.checksum,
        ingested_at=utc_now(),
        batch_id=record.batch_id,
    )
    with local_csv.open("rb") as binary:
        written = write_parquet(
            binary, local_parquet, ft.expected_columns, metadata, config.parquet_batch_rows
        )
    log_event(
        logger, "parquet_written", key=s3_key, rows=written, bytes=local_parquet.stat().st_size
    )

    # Step 5: reconcile before anything is published
    record.parquet_row_count = parquet_row_count(local_parquet)
    reconciliation = check_row_count_reconciliation(record.csv_row_count, record.parquet_row_count)
    record.checks.append(reconciliation)
    log_event(
        logger,
        "check",
        key=s3_key,
        check=reconciliation.name,
        passed=reconciliation.passed,
        details=reconciliation.details,
    )
    if not reconciliation.passed:
        _quarantine(record, reconciliation, config, store)
        return

    # Steps 6-7: publish, record, archive
    raw_key = ft.raw_key(validation.name_groups)
    store.upload(local_parquet, config.raw_bucket, raw_key)
    record.output_uri = s3_uri(config.raw_bucket, raw_key)
    record.outcome = Outcome.SUCCESS
    record.landing_destination_uri = s3_uri(config.landing_bucket, config.archive_prefix + s3_key)
    record.finished_at = utc_now()
    manifest.write_manifest(store, config, record)
    # If this move fails the run reports `error`; the next run sees the success
    # record, reports skipped_duplicate, and finishes the archive.
    _archive(record, config, store)


def _archive(record: ManifestRecord, config: IngestionConfig, store: S3Store) -> None:
    """Move the file to archive/<original key>."""
    archive_key = config.archive_prefix + record.source_key
    store.move(config.landing_bucket, record.source_key, archive_key)
    record.landing_destination_uri = s3_uri(config.landing_bucket, archive_key)


def _quarantine(
    record: ManifestRecord, failure: CheckResult, config: IngestionConfig, store: S3Store
) -> None:
    """Move the file to quarantine/, write <file>.error.json beside it, record the manifest."""
    quarantine_key = config.quarantine_prefix + record.source_key
    record.outcome = Outcome.QUARANTINED
    record.failed_check = failure.name
    record.failure_details = failure.details
    record.landing_destination_uri = s3_uri(config.landing_bucket, quarantine_key)

    store.move(config.landing_bucket, record.source_key, quarantine_key)
    record.finished_at = utc_now()
    store.put_json(config.landing_bucket, f"{quarantine_key}.error.json", record.to_dict())
    manifest.write_manifest(store, config, record)
