# Claims ETL Pipeline

Portfolio project: an end-to-end ETL pipeline for synthetic healthcare claims data,
built to demonstrate data engineering skills in job interviews. Code quality,
clarity, tests, and documentation matter as much as working output.

## Data source

- CMS DE-SynPUF (2008–2010 Medicare Data Entrepreneurs' Synthetic Public Use File), Sample 1
- Fully synthetic. NEVER use real claims, PHI, or PII anywhere in this project.
- File types: beneficiary_summary (one file per year), inpatient_claims,
  outpatient_claims, carrier_claims (large, deferred), prescription_drug_events (deferred)
- Column definitions: see the DE 1.0 Codebook (docs/ folder)

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
         _manifests/   (one record per processed zip)
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
- Validation: zip opens, contains expected CSV, headers match codebook, row
  count > 0. Failures go to landing quarantine/ prefix; do not crash the run.
- Safe extraction: ignore unexpected file types, guard against path traversal
  and oversized archives.
- Reconcile CSV row count against Parquet row count before marking success.

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
- [ ] Ingestion script: beneficiary_summary 2008
- [ ] dbt project setup
