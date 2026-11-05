# Claims ETL Pipeline

Portfolio project: an end-to-end ETL pipeline for synthetic healthcare claims data,
built to demonstrate data engineering skills in job interviews. Code quality,
clarity, tests, and documentation matter as much as working output.

## Data source

Two feeds, each with its own landing prefix (`landing_prefixes` in config/ingestion.yml):

- `cms/cclf/`: synthetic CMS CCLF (Claim and Claim Line Feed) monthly deliveries, produced
  by the generator at the repo root (`../generate_cclf.py --seed 42`; files currently in
  `../output/cms/YYYY-MM/`). 2023-01 to 2025-12: 36 months x 4 files = 144 files.
  Replaced CMS DE-SynPUF zips on 2026-11-04.
- `humana/ma/` (added 2026-11-04): synthetic Humana Medicare Advantage files from
  `../generate_humana.py --seed 42` (`../output/humana/YYYY-MM/`), 36 months x 2 files =
  72 files. Field names are invented (Humana's real layouts are proprietary); never
  model them on real Humana files. HUM_ELIG names/addresses are treated like CCLF8 (PII).
- Fully synthetic. NEVER use real claims, PHI, or PII anywhere in this project.
  CCLF8 contains synthetic names and addresses: treat them like PII anyway (never
  in issues, logs, or docs).
- File types (folder name = `file_type`, unique across feeds):
  - `claims_header` = CCLF1 Part A claim header (one row per claim version)
  - `claims_diagnosis` = CCLF4 Part A diagnosis codes (1-6 rows per claim)
  - `part_b_physician_lines` = CCLF5 Part B physician claim lines
  - `beneficiary_demographics` = CCLF8 (beneficiaries new or changed that month)
  - `member_eligibility` = HUM_ELIG (members new, changed or terminated that month)
  - `medical_claim_lines` = HUM_MEDCLM (claim lines; reversal = same CLAIM_ID, freq 8;
    replacement = new CLAIM_ID, freq 7, ORIG_CLAIM_ID = the original)
- No codebook is used. Expected columns in config/file_types/*.yml come from
  the actual header of each delivered file (inspected before writing config).
- Observed CSV format (all 216 files, both feeds): delivered as plain .csv (no zip), named
  `CCLF<n>_YYYYMM.csv` / `HUM_<ELIG|MEDCLM>_YYYYMM.csv`; comma-delimited; ASCII, no BOM; LF line endings; header
  and data unquoted; empty string = missing. Headers identical across all months.
  Codes carry leading zeros (PRVDR_FAC_FIPS_ST_CD "05").

## Architecture

| Layer | Location | Medallion | Format |
|---|---|---|---|
| landing | s3://fredr-claims-landing-2026 | — | .csv as delivered |
| raw | s3://fredr-claims-raw-2026 | bronze | Parquet |
| staging + intermediate | DuckDB (dbt models) | silver | DuckDB tables/views |
| marts | s3://fredr-claims-marts-2026 | gold | Parquet (dbt-duckdb external materialization) |

- AWS region: us-east-2 (Ohio)
- Flow (same for every feed): CSV lands in landing -> Python ingestion validates it, adds metadata,
  writes Parquet to raw -> dbt + DuckDB reads raw from S3, builds staging,
  intermediate, and marts -> marts published back to S3
- Side flow (decoupled): ingestion quarantines a file and writes `<file>.error.json`
  -> quarantine notifier reads it and opens/updates a GitHub issue. Ticketing
  failures never affect ingestion or quarantine.
- Ingestion runs in AWS Lambda `claims-etl-ingest`, triggered by S3 uploads under
  each landing prefix (cms/cclf/*.csv, humana/ma/*.csv); the CLI runs the same
  process_file locally.
- Infra changes go through Terraform (infra/terraform), never the console. Rebuild
  the packages with `python scripts/build_lambda.py` (and `--transform`) before
  `terraform apply`.
- Lambda Python version (3.13), the pandas layer ARN, and LAMBDA_PYTHON_VERSION in
  scripts/build_lambda.py must stay in sync.
- `landing_prefixes` in config/ingestion.yml and the Terraform `landing_prefixes`
  variable must match (the triggers, IAM, and raw keys all use them). Each file type
  config names its feed's `landing_prefix`; prefixes never overlap.

## S3 key layout

All bucket names, prefixes, and folder names are lowercase snake_case; delivered
file names are kept exactly as delivered. Partitions use Hive style (key=value).

```
<feed> = cms/cclf | humana/ma
landing: <feed>/<file_type>/<file_name>.csv            e.g. cms/cclf/claims_header/CCLF1_202301.csv
                                                            humana/ma/member_eligibility/HUM_ELIG_202301.csv
raw:     <feed>/<file_type>/delivery_month=<yyyy-mm>/part-0.parquet
         _manifests/<sha256>.json   (one record per processed file; written only by ingestion)
         _tickets/<sha256>.json     (quarantine ticket record; written only by the notifier)
         _transform/state/claims_lakehouse.duckdb (+ .previous.duckdb)  (transform runner only)
         _transform/runs/<run_id>/report.json, run_results.json         (transform runner only)
landing archive/quarantine mirror the original key:
         archive/<feed>/<file_type>/<file_name>.csv
         quarantine/<feed>/<file_type>/<file_name>.csv
         quarantine/<feed>/<file_type>/<file_name>.csv.error.json
```

Raw partitions encode DELIVERY identity (what the file name says), never data
content such as claim dates. Each delivery month gets its own partition; a
corrected re-send of the same file overwrites its own partition. Never split or
edit a delivered file to change partitioning; late-arriving claims, cancellations
and adjustments of earlier months, and cross-delivery dedup belong in dbt.

## Ingestion rules (landing -> raw)

- Read ALL source columns as VARCHAR. Never infer types in raw: claims codes
  (ICD-10, FIPS state/county codes, NPIs, MBIs) lose leading zeros or fail to parse.
  Type casting happens in dbt staging models.
- Do not rename, clean, or drop source columns in raw.
- Append these metadata columns:
  - `_source_uri` - S3 URI of the file in landing
  - `_source_file` - delivered CSV file name
  - `_source_checksum` - SHA-256 of the delivered file
  - `_source_row_number` - line number in the source CSV
  - `_ingested_at` - UTC timestamp
  - `_batch_id` - UUID per run
- Idempotency: write a manifest record per file (source URI, checksum, row count,
  batch ID, status). Skip files whose checksum was already loaded successfully.
- Validation: name and size are valid, headers match config, row count > 0.
  Failures go to landing quarantine/ prefix; do not crash the run.
- Only `.csv` keys directly under `<feed>/<file_type>/` are processed, and the file
  type must belong to that feed; anything else is ignored or quarantined. The key is never used as a local path (fixed local file name).
- Reconcile CSV row count against Parquet row count before marking success.

### Ingestion flow (process_file)

1. Triggered by an S3 key under a feed prefix (`cms/cclf/`, `humana/ma/`) ending in
   `.csv` in fredr-claims-landing-2026 (us-east-2).
2. Compute SHA-256 of the file. If a successful manifest record with that
   checksum exists -> outcome `skipped_duplicate`, move file to `archive/`, stop.
3. Run validation checks (below). Any failure -> move file to `quarantine/`,
   write `<file_name>.error.json` next to it, outcome `quarantined`, stop.
4. Read ALL columns as VARCHAR, append metadata columns, write Parquet LOCALLY first.
5. Reconcile: Parquet row count must equal CSV data row count. Mismatch ->
   quarantine. Only upload to raw after reconciliation passes.
6. Upload Parquet to fredr-claims-raw-2026 using the key layout above, with a
   deterministic file name (`part-0.parquet`) so re-runs overwrite, not duplicate.
7. Write manifest record, outcome `success`, move file to `archive/`.

S3 has no move: copy, verify the copy exists, then delete the original.

### Validation checks (each its own small function)

File level:
- Key is `<feed>/<file_type>/<file>` and `<file_type>` has a config for that feed
- File name matches the regex for its file type (from config); captures year
  and month as NAMED groups (templates use them). Claims header:
  `^CCLF1_(?P<year>\d{4})(?P<month>0[1-9]|1[0-2])\.csv$`
- Size > 0 and below a configurable maximum (`max_file_bytes`)

CSV:
- Decodes as UTF-8/ASCII; parses under strict quoting
- Header matches expected columns exactly (names and order, from config)
- Every row has the same field count as the header
- Data row count > 0
- Key column(s) never empty or whitespace: `CUR_CLM_UNIQ_ID` + `BENE_MBI_ID`
  (CCLF1); + `CLM_VAL_SQNC_NUM` (CCLF4); + `CLM_LINE_NUM` (CCLF5); `BENE_MBI_ID`
  (CCLF8); `MEMBER_ID` (HUM_ELIG); `CLAIM_ID` + `CLAIM_LINE_NUM` +
  `CLAIM_FREQ_CD` + `MEMBER_ID` (HUM_MEDCLM). MBI is not a Humana key (dbt test).
  Dates are not keys.

Row-level data quality (code formats, dates, amounts, duplicate keys, referential
integrity across files) is NOT checked here; that belongs in dbt staging tests.

### Manifest

One JSON record per processed file at `s3://fredr-claims-raw-2026/_manifests/<sha256>.json`:
source URI, file name, checksum, file type, name fields (year, month), row count,
batch ID, outcome, failed check name and details (if any), output key, start/end
timestamps.

### Config

Per-file-type config in `config/file_types/<file_type>.yml`: feed landing_prefix,
naming regex, expected columns, key columns, raw output path template. Adding a new
file type should only require a new config file; a new feed also adds its prefix to
`landing_prefixes` (ingestion.yml + Terraform), then rebuild + apply.

### Entry points

- Core: `process_file(s3_key)`
- CLI: `--key <s3_key>` (one file) or `--scan` (all unprocessed .csv files under every feed prefix)
- Lambda handler that parses an S3 event and calls `process_file` (deployed as `claims-etl-ingest`)

### Tests

pytest unit tests for every check using small local fixture CSVs that are
deliberately broken (bad name, empty file, wrong header, BOM, ragged row, header
only, missing key, bad encoding/quoting). Unit tests must never touch real S3.

### Rules

- Ask before any S3 write, copy, or delete outside the planned flow.
- Keep data/ and any downloaded files out of git.

## Quarantine notifications (quarantine -> GitHub issue)

Decoupled from ingestion: ingestion is unchanged and only writes
`quarantine/.../<file_name>.error.json` in fredr-claims-landing-2026. A separate
notifier turns that file into a GitHub issue. If ticketing fails, ingestion and
quarantine are unaffected; the notifier logs the error, exits non-zero, and is
retried later (`--scan`).

### Flow: notify_quarantine(error_json_key)

1. Read the `.error.json` from S3.
2. Look for an existing OPEN issue for the same checksum using the Issues list
   endpoint (`GET /repos/{owner}/{repo}/issues?labels=quarantine&state=open`,
   paginated), NOT the search API (search has an indexing delay that would
   cause duplicate tickets). Match on the body line ``Checksum: `<sha256>` ``.
3. Found -> add a comment (quarantined again: timestamp, batch ID, failed check).
   Not found -> create a new issue.
4. Record the result in `s3://fredr-claims-raw-2026/_tickets/<sha256>.json`
   (ticket_url, ticket_number, notified batch IDs). NOT in the manifest: ingestion
   writes the manifest after the `.error.json`, so a notifier write there could be
   overwritten (race), and the manifest must keep a single writer.
   A batch ID already in the record -> `already_notified`, no API calls.

### Issue content

- Title: `[Quarantine] <file_type> <file_name>: <failed_check>`
  (the file name carries the delivery month, e.g. CCLF1_202301.csv)
- Body (markdown): source URI, quarantine location, ``Checksum: `<sha256>` ``,
  failed check name + details, batch ID, UTC timestamp, suggested next steps
  for that check, link/path to the `.error.json`.
- Labels: `quarantine`, `data-quality`, `<file_type>`; missing labels are created.
- The repo is PUBLIC: never put row-level data values in issues. Only check
  names, counts, line numbers, column names, and file metadata. Truncate long details.
  Details are sanitized, not trusted: csv_utf8 byte values are stripped, quoted
  values in CSV check details survive only if shaped like a column name
  (`^[A-Z][A-Z0-9]*(_[A-Z0-9]+)+$`; plain words are redacted because CCLF8 names
  and cities look like identifiers), and values are wrapped in code spans (no
  @mentions or markdown injection).

### GitHub API and settings

- REST API via `requests`; no SDK. Retry with backoff on 5xx and rate limits,
  honoring `Retry-After` / `x-ratelimit-*` headers. Never log the token.
- `GITHUB_TOKEN`, `GITHUB_REPO` (owner/name) via `get_setting()`: environment
  (from a gitignored `.env` locally via python-dotenv), or SSM Parameter Store
  (`/claims-etl/<NAME>`) when CLAIMS_ETL_SETTINGS_SOURCE=ssm, as in Lambda.
  Placeholders live in committed `.env.example`.
- Ticketing config (labels, title template, per-check next steps) in
  `config/notifications.yml`; switching to Jira later changes only the client.

### Entry points

- CLI: `--error-key <s3_key>` (one file) or `--scan` (every `.error.json` under
  `quarantine/` not yet ticketed; this is the retry mechanism).
- Lambda `claims-etl-notify` (deployed via infra/terraform/notifier.tf), triggered
  by S3 ObjectCreated (prefix `quarantine/`, suffix `.error.json`). Reads settings
  from SSM because CLAIMS_ETL_SETTINGS_SOURCE=ssm; must never import pyarrow (no
  pandas layer), so shared S3-event parsing lives in `claims_etl/s3_events.py`.
- S3 triggers are not retroactive: after a notifier outage or a new deploy, run
  `--scan` once to ticket any .error.json files that were already waiting.

### Tests

Mock GitHub (responses) and S3 (moto); never hit real GitHub or S3. Cover: new
issue (title/body/labels), duplicate checksum -> comment only, missing labels
created, 5xx -> retries then clean failure, truncation + no row-level values,
ticket result recorded.

## Transformations (dbt + DuckDB, `dbt/`)

- dbt Core 1.12 + dbt-duckdb 1.11 (the `dbt` extra: `pip install -e ".[dev,dbt]"`;
  never in the Lambda package). Stay on dbt Core v1: dbt v2 has no DuckDB adapter here.
- Run every dbt command from `dbt/`: `profiles.yml` (committed, no secrets) and
  `claims_lakehouse.duckdb` (gitignored) live there. `../.venv/Scripts/dbt build`.
- Sources read raw Parquet directly from S3 via `httpfs` + the AWS credential chain
  (`_cclf__sources.yml` external_location); `delivery_month` comes from the Hive
  partition as text (`hive_types_autocast = false`) and is cast to a date in staging.
- Staging per feed (`models/staging/<cclf|humana>/stg_<feed>__<file_type>.sql`, views,
  schema `staging`; one dbt source per feed, `cclf` and `humana`). Shared concepts use the
  same column name in both feeds (beneficiary_mbi, claim_start_date, facility_ccn, ...).
  rename to readable snake_case (description keeps the CMS name), `nullif(x, '')`,
  cast. Dates -> DATE, amounts -> DECIMAL(18,2), counts -> INTEGER; codes and IDs stay
  VARCHAR (leading zeros, 'NA'). No filtering, no dedup, no business logic: every
  delivery row is kept. Current-state beneficiaries, claim families, and netting of
  cancellations/adjustments belong in intermediate.
- Intermediate (`models/intermediate/<cclf|humana>/`, incremental tables, schema
  `intermediate`): `int_cclf__beneficiary_versions` / `int_humana__member_versions` (SCD2 by
  attribute hash) and `int_cclf__claim_families` / `int_humana__claim_families`. Humana
  family key = coalesce(original_claim_id, claim_id); status from ACTIVE (unreversed)
  versions, never from version order; net = sum of paid over all versions, tested
  against the status rule.
  Incremental by AFFECTED KEYS (macros/incremental_by_affected_keys.sql), never append-only
  and never a delivery-month watermark: each row keeps `_source_checksums`; a run recomputes
  the full history of keys touched by unprocessed or vanished raw files (delete+insert on
  the key) and a post-hook drops keys whose source rows are gone. Any change here must keep
  `tests/test_dbt_intermediate.py` passing (answer key + incremental == full refresh,
  including old-month re-sends). After changing a model's LOGIC, run it with
  `--full-refresh`: an incremental run only recomputes affected keys.
- `cclf_raw_root` / `humana_raw_root` vars (default the S3 raw roots) and `CLAIMS_DUCKDB_PATH` env var let tests
  run dbt fully offline against a local raw folder.
- CCLF8 and HUM_ELIG names/addresses are in staging only; never select them into marts.
- `RECORD_CHANGE_TYPE` (CCLF8 and HUM_ELIG) is a synthetic generator helper, not a real
  field: kept in staging only to verify change detection that downstream models derive
  themselves.
- Tests: `data_tests:` with the `arguments:` key (dbt 1.10+ style). Generic
  `unique_combination_of_columns` is local (tests/generic), so there is no package
  download. Every new model gets at least key tests; a new test must be shown to fail
  on deliberately broken data before it is trusted.
- Test severity (dbt/macros/test_scoping.sql): STOP = severity error (default) for
  anything that would make intermediate wrong (keys/grain, identity, family linkage,
  netting amounts, service dates, codes the logic branches on); `dbt build` skips the
  downstream intermediate models. WARN = `config: *warn` (YAML) or `severity='warn'`
  (singular) when intermediate stays correct. STOPs are NEVER scoped to the batch's
  delivery months (a skipped file stays unprocessed; a scoped stop would let the next
  run pull it in); WARNs are scoped via the `delivery_months` var (`__in_batch__` /
  `in_batch()`). Classify every new test; `tests/test_dbt_gating.py` must keep passing.
- Correct downstream results are defined by `../answer_key/` (net paid by service and
  load month, final members, claim actions, member changes).

## Event-driven transforms (raw -> staging + intermediate)

- Ingestion publishes `FileIngested` (claims_etl/events.py) to the default EventBridge bus
  for every file in raw (success AND skipped_duplicate, `redelivered: true`), after the
  manifest and before the archive move; a failed publish = `error`, file stays in landing.
  `publish_events: true` in ingestion.yml.
- EventBridge rule -> SQS FIFO `claims-etl-transform.fifo` (ONE message group: one build at
  a time) -> Lambda `claims-etl-transform` (infra/terraform/transform.tf), batch up to 10;
  DLQ `claims-etl-transform-dlq.fifo` after 3 receives. The queue MUST keep
  content_based_deduplication = true: EventBridge sends no deduplication ID, and without it
  every delivery fails (2026-11-05: 2023-04 events silently dropped). Undeliverable events
  go to the standard queue `claims-etl-transform-undelivered` (rule target DLQ).
- The runner (claims_etl/transform/runner.py) builds the WHOLE FEED (`source:<cclf|humana>+`),
  never one file type: a stop in CCLF5 must still block claim families when CCLF1 arrives.
  `delivery_months` = the events' months (WARN scoping only). dbt runs as a SUBPROCESS
  (process exit closes and checkpoints DuckDB; no dbt state in a warm Lambda) via
  `python -m claims_etl.transform.dbt_cli`, never `dbt.cli.main` directly: Lambda has no
  /dev/shm, so dbt's multiprocessing locks crash; dbt_cli swaps in threading locks on
  the BASE context (ThreadPool uses the platform default = fork on Linux, not spawn).
  tests/test_dbt_cli.py simulates Lambda (no semaphores + a non-spawn default context)
  and runs a real `dbt build`; a `dbt parse`-only or Windows-default test misses it.
- Outcomes: passed / warned / stopped -> publish the state, write the report, delete the
  messages (final answers). error (model/test could not run, dbt crash, state conflict) ->
  keep the old state, write the report, raise (retry, then DLQ). Before raising, the handler
  sets the batch's visibility to `retry_backoff_seconds` (transform.yml; 2/5/10 min by
  receive count) so a failure is retried in minutes, not after the queue's 1-hour timeout.
- State: `s3://fredr-claims-raw-2026/_transform/state/claims_lakehouse.duckdb`, single
  writer, upload conditional on the downloaded ETag (If-None-Match on the first run);
  previous version copied to `claims_lakehouse.previous.duckdb`. Run artifacts:
  `_transform/runs/<run_id>/report.json` + `run_results.json`. Reports carry test names and
  failure COUNTS only, never row values (the step-3 notifier posts them to the public repo).
- CLI `claims-transform` (--feed/--all, --months, --db for a local state file, --raw-root for
  local raw). Exit 0 passed/warned, 2 stopped, 1 error.
- Package: `python scripts/build_lambda.py --transform` -> build/transform_lambda.zip
  (dbt-core/dbt-duckdb/duckdb pinned to the locally installed, tested versions; dbt project
  bundled as `dbt_project/` because dbt-core's Python package is `dbt/`; DuckDB httpfs + aws
  extensions bundled, autoinstall off; empty placeholder for dbt-core-experimental-parser;
  Babel locales trimmed to English). Must stay under 250 MB unzipped (the build fails
  otherwise); deployed via the Terraform-managed `fredr-claims-artifacts-2026` bucket.
  Rebuild it before `terraform apply` whenever dbt/, config/ or src/ change.
- profiles.yml target `lambda` = the Lambda's settings (CLAIMS_DUCKDB_EXTENSION_DIR); `dev`
  stays the local default.

## Tech stack

- Python 3 (3.13 in Lambda), standard library csv, pyarrow, boto3, requests
- AWS Lambda + S3 event triggers; Terraform for infrastructure (infra/terraform)
- pytest with moto (S3/SSM) and responses (GitHub); ruff for lint/format
- dbt Core 1.12 + dbt-duckdb 1.11 for transformations; later GitHub Actions for CI/scheduling

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
- [x] DE-SynPUF phase (2026-11-02 to 2026-11-04, superseded): beneficiary 2008-2010
      Sample 1 loaded by Lambda, quarantine notifier live-tested (issues #1, #2)
- [x] Lambda `claims-etl-ingest` deployed via Terraform (infra/terraform, local state);
      pyarrow from AWSSDKPandas-Python313 layer
- [x] Notifier Lambda `claims-etl-notify` deployed (infra/terraform/notifier.tf), S3
      trigger quarantine/*.error.json; token in SSM /claims-etl/GITHUB_TOKEN (created
      outside Terraform, never in state). GitHub token expires 2027-02-02
- [x] 2026-11-04: landing and raw buckets emptied (versioning off, nothing retained).
      Marts bucket untouched. Old DE-SynPUF GitHub quarantine issues still open.
- [x] 2026-11-04: ingestion switched from DE-SynPUF zips to CCLF CSVs (code, config,
      tests, Terraform files). CSV only: archive checks removed. 4 file type configs
      from real headers; all 144 generated files validate and reconcile locally.
- [x] 2026-11-04: CSV switch deployed (terraform apply: 4 in-place changes). Live
      trigger cms/cclf/*.csv -> claims-etl-ingest; IAM scoped to cms/cclf/*; both
      Lambdas on the new package; follow-up plan shows no drift
- [x] 2026-11-04: CCLF delivery months 2023-01 to 2023-03 landed by the user and loaded
      by Lambda (12 files: 87 CCLF1, 206 CCLF4, 82 CCLF5, 161 CCLF8 rows)
- [ ] Land the remaining months (2023-04 to 2025-12)
- [x] 2026-11-04: Humana MA feed (`humana/ma/`) added to ingestion: 2 file type configs from
      the real headers, multi-feed config (landing_prefixes), Terraform triggers + IAM per
      feed; all 216 generated files validate and reconcile locally
- [x] 2026-11-04: Humana feed deployed (terraform apply: 4 in-place changes; triggers
      cms/cclf/*.csv + humana/ma/*.csv -> claims-etl-ingest; no drift after)
- [x] 2026-11-04: old CMS data deleted (landing archive/cms/cclf/, raw cms/cclf/, all 12
      manifests): the root generator changed, so CMS is re-landed from ../output/cms/
- [ ] Land CMS + Humana month by month from 2023-01 (user uploads)
- [x] 2026-11-04: CMS + Humana 2023-01..03 landed; CMS intermediate matches the answer key
      each month, incremental == full rebuild
- [x] 2026-11-04: Humana staging (stg_humana__member_eligibility, stg_humana__medical_claim_lines),
      43 tests, each shown to fail on broken data; counts and net paid by processed month
      match the CSVs and ../answer_key/humana/
- [x] 2026-11-04: Humana intermediate (int_humana__member_versions, int_humana__claim_families):
      offline test matches ../answer_key/humana/ on all 36 months; incremental == full
      refresh incl. Humana re-sends; new tests shown to fail on broken data
- [x] 2026-11-04: generator refactored onto a shared member registry (FFS <-> MA
      switchers; CCLF8 FFS_END / FFS_RETURN updates set BENE_RNG_END_DT / BGN_DT).
      Seed 42 output changed; the stale S3 files were deleted and re-landed the same day
      (verified 2026-11-05: all 18 raw manifests for 2023-01..03 match a fresh seed-42
      regeneration). A Humana MA generator was added at the repo root
      (`../output/humana/`); it is not ingested yet (no file type configs)
- [x] 2026-11-04: dbt project + staging: 4 stg_cclf__* views over raw S3, 48 tests
      passing (row counts match raw); tests verified to fail on broken data
- [x] 2026-11-04: intermediate incremental models (beneficiary SCD2, claim families),
      affected-key strategy; offline end-to-end test matches ../answer_key/ for all 36
      months and incremental (batches + old-month re-sends) == full refresh
- [x] 2026-11-05: test severities (STOP vs WARN) + gating tests (PR #5)
- [x] 2026-11-05: event-driven transform runner (FileIngested -> EventBridge -> SQS FIFO ->
      claims-etl-transform) deployed (terraform apply: 17 added, 2 changed); first build via
      `claims-transform --all` passed (143 tests, 10 models); S3 state matches an offline
      build of 2023-01..03 row for row
- [x] 2026-11-05: first live Lambda run (2023-04 landed by the user). Found and fixed:
      FIFO queue needed content-based dedup (EventBridge deliveries failed, PR #7) and
      dbt's multiprocessing locks crash on Lambda (no /dev/shm; dbt_cli). The retry
      passed (143 tests, 10 models, 19 s); S3 state matches the local build row for row,
      which matches both answer keys through 2023-04
- [ ] Transform retry backoff (2/5/10 min instead of the 1-hour visibility timeout)
- [ ] Transform reports -> GitHub issues (notifier extension) + CloudWatch alarms (DLQs,
      EventBridge FailedInvocations, ingest errors) -> SNS -> notifier -> GitHub issues;
      no email (decided 2026-11-05)
- [ ] Marts to s3://fredr-claims-marts-2026 (external materialization)
- [ ] Move Terraform state to an S3 backend before CI
