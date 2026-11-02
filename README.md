# Claims Lakehouse: dbt + DuckDB on S3

An end-to-end ETL pipeline for **synthetic** Medicare claims (CMS DE-SynPUF 2008–2010,
Sample 1). No real claims, PHI, or PII are used anywhere.

```
landing (S3, .zip)  ──►  raw / bronze (S3, Parquet)  ──►  staging + intermediate / silver (DuckDB via dbt)  ──►  marts / gold (S3, Parquet)
   └── ingestion (this repo, Python) ──┘                     └──────────────── dbt-duckdb (next) ────────────────┘
```

| Layer | Location | Format |
|---|---|---|
| landing | `s3://fredr-claims-landing-2026` | `.zip` as delivered |
| raw (bronze) | `s3://fredr-claims-raw-2026` | Parquet, all source columns as string |
| staging/intermediate (silver) | DuckDB (dbt models) | tables / views |
| marts (gold) | `s3://fredr-claims-marts-2026` | Parquet |

All buckets are in `us-east-2` with Block Public Access and SSE-S3 encryption.

## Ingestion: landing → raw

`process_zip(s3_key)` in [`src/claims_etl/ingestion/pipeline.py`](src/claims_etl/ingestion/pipeline.py):

1. **Download + SHA-256.** If `_manifests/<sha256>.json` already says `success`, the outcome is
   `skipped_duplicate` and the zip is archived.
2. **Validate**: file, archive, and CSV checks run in order and stop at the first failure (below).
   A failure moves the zip to `quarantine/`, writes `<zip>.error.json` next to it, and records
   `quarantined`.
3. **Write Parquet locally**: every source column as `string` (codes keep their leading zeros:
   `SP_STATE_CODE = "05"`), plus `_source_uri`, `_source_file`, `_source_checksum`,
   `_source_row_number`, `_ingested_at` (UTC), `_batch_id`.
4. **Reconcile**: the Parquet footer row count must equal the CSV data row count before anything
   is published.
5. **Publish**: upload to a deterministic key (re-runs overwrite rather than duplicate), write
   the manifest, and move the zip to `archive/`.

Data problems end in `quarantined`. Infrastructure problems (S3, network) end in `error`, and the
zip stays in landing so the next run retries it.

### Validation checks

| Level | Check | Fails when |
|---|---|---|
| file | `known_file_type` | key is not `cms/desynpuf/<configured type>/<zip>` |
| file | `file_name` | name does not match the type's regex (captures `year`, `sample`) |
| file | `file_size` | 0 bytes or above the configured maximum |
| archive | `zip_opens` | not a readable zip |
| archive | `member_paths` | absolute path, `..` traversal, or nested archive |
| archive | `expected_member` | anything other than exactly the expected CSV |
| archive | `uncompressed_size` / `compression_ratio` | above limits (zip-bomb guard) |
| archive | `zip_crc` | CRC-32 mismatch |
| CSV | `csv_utf8` / `csv_parseable` | not UTF-8, or malformed quoting |
| CSV | `csv_header` | names or order differ from config |
| CSV | `csv_field_counts` | any row's field count differs from the header's |
| CSV | `csv_row_count` | no data rows |
| CSV | `csv_key_columns` | empty key (e.g. `DESYNPUF_ID`) |
| post-write | `row_count_reconciliation` | Parquet rows ≠ CSV rows |

Row-level data quality (code formats, dates, amounts) belongs in dbt staging tests, not here.

### S3 layout

```
landing   cms/desynpuf/<file_type>/<zip>                       ← drop new files here
          archive/cms/desynpuf/<file_type>/<zip>               ← processed
          quarantine/cms/desynpuf/<file_type>/<zip>[.error.json]
raw       cms/desynpuf/beneficiary_summary/year=<yyyy>/sample=<n>/part-0.parquet
          _manifests/<sha256>.json
```

### Adding a file type

Add `config/file_types/<file_type>.yml` with the naming regex (named groups), the CSV member
and raw key templates, the expected columns (copied from the real header), and the key columns.
The config is validated on load (for example, a template may only use groups the regex captures).

## Running it

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"     # Windows; use .venv/bin on macOS/Linux

# credentials come from the AWS CLI default profile (aws configure)
.venv/Scripts/python -m claims_etl.ingestion.cli --key cms/desynpuf/beneficiary_summary/DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip
.venv/Scripts/python -m claims_etl.ingestion.cli --scan
```

JSON-lines logs go to stderr, and a JSON summary of manifest records goes to stdout. Exit codes:
`0` all success/skipped, `2` something was quarantined, `1` an error occurred.

`lambda_handler.handler` accepts S3 `ObjectCreated` events and calls the same `process_zip`
(not deployed yet).

## Tests

```bash
.venv/Scripts/python -m pytest      # 99 tests, no AWS access (S3 is mocked with moto)
.venv/Scripts/python -m ruff check src tests
```

Fixture zips are built at test time, including deliberately broken ones: bad name, corrupt or
truncated zip, CRC mismatch, path traversal, nested archive, zip bomb, wrong/reordered header,
ragged row, header only, empty key, non-UTF-8 bytes.

## First load (2008 beneficiary summary, Sample 1)

| | |
|---|---|
| Source | `DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip`, 3.1 MB, SHA-256 `2b0c9cbf…3d70b18b` |
| CSV | 116,352 data rows × 32 columns, ASCII, CRLF |
| Raw | `year=2008/sample=1/part-0.parquet`, 4.2 MB, 116,352 rows × 38 columns |
| Reconciliation | CSV 116,352 = Parquet 116,352; `_source_row_number` 2–116,353 |

## Project layout

```
config/                     ingestion.yml + file_types/*.yml
src/claims_etl/ingestion/
  pipeline.py               process_zip orchestration
  validation.py             runs checks in order
  checks.py                 one function per check
  zip_inspect.py            safe member handling, one-pass CSV profile
  parquet_writer.py         CSV → Parquet (string columns + metadata)
  s3_store.py               the only boto3 code (download, upload, move = copy→verify→delete)
  manifest.py               idempotency records
  cli.py / lambda_handler.py
tests/
```
