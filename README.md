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

## Quarantine notifications (GitHub issues)

A decoupled notifier turns each quarantine report (`quarantine/.../<zip>.zip.error.json`) into
a GitHub issue. Ingestion doesn't change, and a GitHub outage can't affect loading or quarantine.

- **One issue per checksum.** Before opening an issue it looks for an *open* `quarantine` issue
  whose body has the line ``Checksum: `<sha256>` ``. The lookup uses the Issues list endpoint, not
  search, which lags. If it finds one, it adds a "quarantined again" comment instead.
- **Ticket record:** `s3://fredr-claims-raw-2026/_tickets/<sha256>.json` stores the issue URL and
  number and every batch already notified. It's kept apart from the manifest, which only
  ingestion writes. Re-running for the same event makes no API calls.
- **Public-safe content.** Issues carry check names, counts, line numbers, column names, and file
  metadata. Row values are redacted, for example when a headerless file's first data row would be
  read as column names. Details are truncated and rendered as code so they can't @mention anyone.
- **Resilient.** It retries 5xx/429/rate-limited responses with backoff, honoring `Retry-After` and
  `x-ratelimit-reset`. It never logs the token. Failures exit non-zero, and `--scan` retries later.
- **Configurable:** labels, title template, and per-check next steps live in
  `config/notifications.yml`. Moving to Jira would only replace `github_client.py`.

```bash
cp .env.example .env    # then set GITHUB_TOKEN (fine-grained, this repo only, Issues: read/write)
.venv/Scripts/python -m claims_etl.notifications.cli --scan
.venv/Scripts/python -m claims_etl.notifications.cli --error-key quarantine/cms/desynpuf/<type>/<zip>.zip.error.json
```

In AWS, the `claims-etl-notify` Lambda ([`infra/terraform/notifier.tf`](infra/terraform/notifier.tf))
runs this automatically for every new `quarantine/*.error.json`. It reads `GITHUB_TOKEN` from SSM
Parameter Store (`/claims-etl/GITHUB_TOKEN`, a SecureString created outside Terraform so the token
never lands in Terraform state) and `GITHUB_REPO` from a Terraform-managed parameter. A failure
raises, so Lambda retries it twice. S3 triggers don't fire for files that already exist, so after
an outage run `--scan` once.

One-time token setup (PowerShell, from the project folder; reads the token from `.env`):

```powershell
$t = (Select-String -Path .env -Pattern '^GITHUB_TOKEN=(.+)$').Matches[0].Groups[1].Value
aws ssm put-parameter --region us-east-2 --name /claims-etl/GITHUB_TOKEN --type SecureString --value $t
Remove-Variable t
```

## Automatic ingestion (AWS Lambda)

Uploading a `.zip` under `s3://fredr-claims-landing-2026/cms/desynpuf/` triggers the
`claims-etl-ingest` Lambda, which runs the same `process_zip`. It's defined in
[`infra/terraform/`](infra/terraform/):

| Resource | Details |
|---|---|
| Lambda `claims-etl-ingest` | Python 3.13, 1024 MB, 300 s timeout, 2 GB `/tmp` |
| pyarrow | AWS SDK for pandas public layer (`AWSSDKPandas-Python313:17`, pyarrow 24) |
| Package | `build/lambda.zip`: our code, `config/`, PyYAML (~0.8 MB, deterministic) |
| IAM role | Least privilege: read/delete `cms/desynpuf/*`, write `archive/*` and `quarantine/*` in landing, write `cms/desynpuf/*` and `_manifests/*` in raw, own log group only |
| Trigger | S3 `ObjectCreated`, prefix `cms/desynpuf/`, suffix `.zip` (archive/quarantine moves can't re-trigger) |
| Retries | An `error` outcome raises, so Lambda retries async invocations twice. `quarantined` is final and not retried |
| Logs | `/aws/lambda/claims-etl-ingest`, JSON lines, 30-day retention |

Deploy or update:

```bash
.venv/Scripts/python scripts/build_lambda.py        # rebuild the package
cd infra/terraform
terraform init                                       # first time only
terraform plan -out=tfplan && terraform apply tfplan
```

Terraform state is local (gitignored). Move it to an S3 backend before CI or anyone else runs it.

## Tests

```bash
.venv/Scripts/python -m pytest      # 145 tests; no AWS or GitHub access (moto + responses)
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
src/claims_etl/notifications/
  notifier.py               notify_quarantine: create issue or comment, record result
  content.py                title/body/comment + sanitizing (no tracker dependency)
  github_client.py          GitHub REST: list/create/comment/labels, retries
  ticket_record.py          _tickets/<sha256>.json
  settings.py               get_setting(): env now, SSM later
scripts/build_lambda.py     Lambda deployment package
infra/terraform/            Lambda, IAM role, log group, S3 trigger
tests/
```
