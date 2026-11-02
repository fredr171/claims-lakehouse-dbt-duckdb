# Claims ETL Pipeline

Portfolio project: an end-to-end ETL pipeline for synthetic healthcare claims data,
built to demonstrate data engineering skills in job interviews. Code quality,
clarity, tests, and documentation matter as much as working output.

## Data source

- CMS DE-SynPUF (2008–2010 Medicare Data Entrepreneurs' Synthetic Public Use File), Sample 1
- Fully synthetic. NEVER use real claims, PHI, or PII anywhere in this project.
- File types: beneficiary_summary (one file per year), inpatient_claims,
  outpatient_claims, carrier_claims (large, deferred), prescription_drug_events (deferred)
- No codebook is used. Expected columns in config/file_types/*.yml come from
  the actual header of each delivered file (inspected before writing config).
- Observed CSV format (2008 beneficiary, Sample 1): one CSV per zip, same base
  name as the zip; comma-delimited; pure ASCII, no BOM; CRLF line endings;
  header fields double-quoted, data fields unquoted; empty string = missing.
  Codes carry leading zeros (SP_STATE_CODE "05", BENE_COUNTY_CD "010").

## Architecture

| Layer | Location | Medallion | Format |
|---|---|---|---|
| landing | s3://fredr-claims-landing-2026 | — | .zip as delivered |
| raw | s3://fredr-claims-raw-2026 | bronze | Parquet |
| staging + intermediate | DuckDB (dbt models) | silver | DuckDB tables/views |
| marts | s3://fredr-claims-marts-2026 | gold | Parquet (dbt-duckdb external materialization) |

- AWS region: us-east-2 (Ohio)
- Flow: zip lands in landing -> Python ingestion extracts CSV, adds metadata,
  writes Parquet to raw -> dbt + DuckDB reads raw from S3, builds staging,
  intermediate, and marts -> marts published back to S3
- Ingestion runs locally first; later wrapped in AWS Lambda triggered by S3 uploads

## S3 key layout

All bucket names, prefixes, and file names are lowercase snake_case.
Partitions use Hive style (key=value).

```
landing: cms/desynpuf/<file_type>/<original_zip_name>.zip
raw:     cms/desynpuf/<file_type>/year=<yyyy>/sample=<n>/part-0.parquet   (beneficiary_summary)
         cms/desynpuf/<file_type>/sample=<n>/part-0.parquet               (claims files span 2008–2010)
         _manifests/<sha256>.json   (one record per processed zip)
landing archive/quarantine mirror the original key:
         archive/cms/desynpuf/<file_type>/<original_zip_name>.zip
         quarantine/cms/desynpuf/<file_type>/<original_zip_name>.zip
         quarantine/cms/desynpuf/<file_type>/<original_zip_name>.zip.error.json
```

## Ingestion rules (landing -> raw)

- Read ALL source columns as VARCHAR. Never infer types in raw: claims codes
  (ICD-9, state/county codes, provider IDs) lose leading zeros or fail to parse.
  Type casting happens in dbt staging models.
- Do not rename, clean, or drop source columns in raw.
- Append these metadata columns:
  - `_source_uri` - S3 URI of the zip in landing
  - `_source_file` - CSV file name inside the zip
  - `_source_checksum` - SHA-256 of the zip
  - `_source_row_number` - line number in the source CSV
  - `_ingested_at` - UTC timestamp
  - `_batch_id` - UUID per run
- Idempotency: write a manifest record per zip (source URI, checksum, row count,
  batch ID, status). Skip zips whose checksum was already loaded successfully.
- Validation: zip opens, contains expected CSV, headers match config, row
  count > 0. Failures go to landing quarantine/ prefix; do not crash the run.
- Safe extraction: ignore unexpected file types, guard against path traversal
  and oversized archives.
- Reconcile CSV row count against Parquet row count before marking success.

### Ingestion flow (process_zip)

1. Triggered by an S3 key under `cms/desynpuf/` ending in `.zip` in
   fredr-claims-landing-2026 (us-east-2).
2. Compute SHA-256 of the zip. If a successful manifest record with that
   checksum exists -> outcome `skipped_duplicate`, move zip to `archive/`, stop.
3. Run validation checks (below). Any failure -> move zip to `quarantine/`,
   write `<zip_name>.error.json` next to it, outcome `quarantined`, stop.
4. Extract CSV, read ALL columns as VARCHAR, append metadata columns, write
   Parquet LOCALLY first.
5. Reconcile: Parquet row count must equal CSV data row count. Mismatch ->
   quarantine. Only upload to raw after reconciliation passes.
6. Upload Parquet to fredr-claims-raw-2026 using the key layout above, with a
   deterministic file name (`part-0.parquet`) so re-runs overwrite, not duplicate.
7. Write manifest record, outcome `success`, move zip to `archive/`.

S3 has no move: copy, verify the copy exists, then delete the original.

### Validation checks (each its own small function)

File level:
- File name matches the regex for its file type (from config); captures year
  and sample as NAMED groups (templates use them). Beneficiary:
  `^DE1_0_(?P<year>\d{4})_Beneficiary_Summary_File_Sample_(?P<sample>\d+)\.zip$`
- Size > 0 and below a configurable maximum

Archive:
- Valid zip, passes CRC check (`testzip`)
- Contains exactly the expected CSV member
- No absolute paths, `../` traversal, or nested archives
- Uncompressed size and compression ratio within configurable limits

CSV:
- Decodes as UTF-8/ASCII
- Header matches expected columns exactly (names and order, from config)
- Every row has the same field count as the header
- Data row count > 0
- Key column(s) never empty (`DESYNPUF_ID` for beneficiary_summary)

Row-level data quality (code formats, dates, amounts) is NOT checked here;
that belongs in dbt staging tests.

### Manifest

One JSON record per processed zip at `s3://fredr-claims-raw-2026/_manifests/<sha256>.json`:
source URI, checksum, file type, year, sample, row count, batch ID, outcome,
failed check name and details (if any), output key, start/end timestamps.

### Config

Per-file-type config in `config/file_types/<file_type>.yml`: naming regex,
expected columns, key columns, raw output path template. Adding a new file
type should only require a new config file.

### Entry points

- Core: `process_zip(s3_key)`
- CLI: `--key <s3_key>` (one file) or `--scan` (all unprocessed zips under `cms/desynpuf/`)
- Lambda handler stub that parses an S3 event and calls `process_zip` (not deployed yet)

### Tests

pytest unit tests for every check using small local fixture zips that are
deliberately broken (bad name, corrupt zip, wrong header, ragged row, empty
file, missing key, path traversal entry). Unit tests must never touch real S3.

### Rules

- Ask before any S3 write, copy, or delete outside the planned flow.
- Keep data/ and any downloaded files out of git.

## Tech stack

- Python 3, standard library zipfile, DuckDB (and/or pyarrow), boto3
- dbt-duckdb for transformations
- pytest for tests
- Later: Terraform or AWS CDK for infrastructure, GitHub Actions for scheduling

## Security

- NEVER commit credentials, access keys, .env files, or the IAM user CSV.
- AWS credentials come from the AWS CLI profile (~/.aws), never hardcoded.
- All buckets: Block Public Access on, SSE-S3 encryption.
- Keep .gitignore up to date (credentials, .env, *.duckdb, local data/ folder, __pycache__).

## Conventions

- Small, readable functions with type hints and docstrings.
- Log what each step does (file processed, row counts, duration).
- Every new transformation gets at least one test.
- Update README.md when the architecture changes.

## Current status

- [x] AWS account, root MFA, budget alert, IAM admin user with MFA
- [x] Buckets: landing, raw, marts (us-east-2, Block Public Access, SSE-S3 verified)
- [x] AWS CLI v2 installed and configured (IAM user fredr-admin, default profile)
- [x] Ingestion script: beneficiary_summary 2008 (116,352 rows to raw, reconciled 2026-11-02)
- [ ] Ingest beneficiary_summary 2009, 2010; then inpatient/outpatient claims (new configs)
- [ ] Deploy Lambda handler (S3 trigger)
- [ ] dbt project setup
