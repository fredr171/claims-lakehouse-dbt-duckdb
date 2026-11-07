# Pipeline details

The reference behind the [README](../README.md): every layer, check, model, resource and
command. Relative links point at the repo root.

An end-to-end ETL pipeline for **synthetic** Medicare claims from two feeds, both produced by
the seeded generators in a separate repo,
[synthetic-payer-files](https://github.com/fredr171/synthetic-payer-files):

- **CMS CCLF** (Claim and Claim Line Feed, Medicare fee-for-service), from `generate-cclf`.
- **Humana Medicare Advantage** eligibility and medical claims, from `generate-humana`.
  Its field names are invented: Humana's real layouts are proprietary.

The files landed in S3 are that repo's committed seed-42 output (`output/cms/`,
`output/humana/`). This repo's dev extra pins a release tag (`v1.0.0`), so the end-to-end
dbt tests generate exactly the data their hardcoded counts and answer keys describe.

No real claims, PHI, or PII are used anywhere.

```
landing (S3, .csv)  ──►  raw / bronze (S3, Parquet)  ──►  staging + intermediate / silver (DuckDB via dbt)  ──►  marts / gold (S3, Parquet)
   └── ingestion (Python, Lambda) ──┘                        └──────── dbt-duckdb (staging + intermediate + marts) ────────┘
                                  └── FileIngested event ──► transform Lambda (dbt build per feed)
```

| Layer | Location | Format |
|---|---|---|
| landing | `s3://fredr-claims-landing-2026` | `.csv` as delivered |
| raw (bronze) | `s3://fredr-claims-raw-2026` | Parquet, all source columns as string |
| staging/intermediate (silver) | DuckDB (dbt models) | tables / views |
| marts (gold) | `s3://fredr-claims-marts-2026` | Parquet |

All buckets are in `us-east-2` with Block Public Access and SSE-S3 encryption.

## Source files

One CSV per file type per month, 2023-01 to 2025-12: 36 months × 4 CCLF files (144) plus
36 months × 2 Humana files (72). Both feeds share the same format: comma-delimited, unquoted,
UTF-8 (ASCII in practice), LF line endings, empty string = missing.

Each feed has its own landing prefix, and every file type belongs to exactly one feed.

**CMS CCLF** (landing prefix `cms/cclf/`)

| File | `file_type` | Content | Columns | Key columns (never empty) |
|---|---|---|---|---|
| `CCLF1_YYYYMM.csv` | `claims_header` | Part A claim header, one row per claim version | 30 | `CUR_CLM_UNIQ_ID`, `BENE_MBI_ID` |
| `CCLF4_YYYYMM.csv` | `claims_diagnosis` | Part A diagnosis codes, 1–6 per claim | 12 | `CUR_CLM_UNIQ_ID`, `BENE_MBI_ID`, `CLM_VAL_SQNC_NUM` |
| `CCLF5_YYYYMM.csv` | `part_b_physician_lines` | Part B physician claim lines | 33 | `CUR_CLM_UNIQ_ID`, `CLM_LINE_NUM`, `BENE_MBI_ID` |
| `CCLF8_YYYYMM.csv` | `beneficiary_demographics` | Beneficiaries that are new or changed that month | 26 | `BENE_MBI_ID` |

**Humana Medicare Advantage** (landing prefix `humana/ma/`)

| File | `file_type` | Content | Columns | Key columns (never empty) |
|---|---|---|---|---|
| `HUM_ELIG_YYYYMM.csv` | `member_eligibility` | Members that are new, changed or terminated that month (full rows) | 28 | `MEMBER_ID` |
| `HUM_MEDCLM_YYYYMM.csv` | `medical_claim_lines` | Claim lines, institutional and professional in one file; a reversal reuses `CLAIM_ID` with `CLAIM_FREQ_CD 8` | 56 | `CLAIM_ID`, `CLAIM_LINE_NUM`, `CLAIM_FREQ_CD`, `MEMBER_ID` |

`MBI` (the Medicare ID that links a member who switches between fee-for-service and Medicare
Advantage) is not an ingestion key. Missing MBIs are a dbt staging test, not a reason to
quarantine a whole file.

Expected columns in `config/file_types/*.yml` were copied from the delivered headers, which are
identical in all 36 months of each feed.

## Ingestion: landing → raw

`process_file(s3_key)` in [`src/claims_etl/ingestion/pipeline.py`](../src/claims_etl/ingestion/pipeline.py):

1. **Download + SHA-256.** If `_manifests/<sha256>.json` already says `success`, the outcome is
   `skipped_duplicate` and the file is archived.
2. **Validate**: file and CSV checks run in order and stop at the first failure (below).
   A failure moves the file to `quarantine/`, writes `<file>.error.json` next to it, and records
   `quarantined`.
3. **Write Parquet locally**: every source column as `string` (codes keep their leading zeros:
   `PRVDR_FAC_FIPS_ST_CD = "05"`), plus `_source_uri`, `_source_file`, `_source_checksum`,
   `_source_row_number`, `_ingested_at` (UTC), `_batch_id`.
4. **Reconcile**: the Parquet footer row count must equal the CSV data row count before anything
   is published.
5. **Publish**: upload to the delivery's own key (delivery month + checksum: a re-run rewrites
   only its own key, a re-send never touches the original), write the manifest, and move the
   file to `archive/.../checksum=<sha256>/`.

Data problems end in `quarantined`. Infrastructure problems (S3, network) end in `error`, and the
file stays in landing so the next run retries it.

### Validation checks

| Level | Check | Fails when |
|---|---|---|
| file | `known_file_type` | key is not `<feed prefix><file type of that feed>/<file>` (e.g. a CCLF folder under `humana/ma/`) |
| file | `file_name` | name does not match the type's regex (`CCLF1_YYYYMM.csv`, `HUM_ELIG_YYYYMM.csv`, month 01–12) |
| file | `file_size` | 0 bytes or above the configured maximum |
| CSV | `csv_utf8` / `csv_parseable` | not UTF-8, or malformed quoting |
| CSV | `csv_header` | names or order differ from config (a UTF-8 BOM also fails here) |
| CSV | `csv_field_counts` | any row's field count differs from the header's |
| CSV | `csv_row_count` | no data rows |
| CSV | `csv_key_columns` | an empty or whitespace-only key value |
| post-write | `row_count_reconciliation` | Parquet rows ≠ CSV rows |

Row-level data quality (code formats, dates, amounts, duplicate keys, referential integrity)
belongs in dbt staging tests, not here.

### S3 layout

`<feed>` is `cms/cclf` or `humana/ma`:

```
landing   <feed>/<file_type>/<file>.csv                     ← drop new files here
          archive/<feed>/<file_type>/checksum=<sha256>/<file>.csv     ← processed
          quarantine/<feed>/<file_type>/checksum=<sha256>/<file>.csv[.error.json]
raw       <feed>/<file_type>/delivery_month=<yyyy-mm>/checksum=<sha256>/part-0.parquet
          _manifests/<sha256>.json                          ← one per processed file, all feeds
          _tickets/<sha256>.json
```

For example, `humana/ma/member_eligibility/HUM_ELIG_202301.csv` becomes
`humana/ma/member_eligibility/delivery_month=2023-01/checksum=<sha256>/part-0.parquet` in raw.

**Immutable bronze.** Nothing a payer delivers is ever overwritten or deleted. Raw partitions
encode **delivery identity**: the month from the file name (never data content:
`CCLF1_202301.csv` → `claims_header/delivery_month=2023-01`) plus the file's SHA-256. A
corrected re-send of a file lands *next to* the original, and dbt decides which delivery is
current (see `stg_<feed>__deliveries` below). So you can always answer "what did the payer
send, and when", re-run a stopped build on exactly the data it saw, or roll back a bad
correction. Versioning and a lifecycle rule on the landing and raw buckets
([`storage.tf`](../infra/terraform/storage.tf)) are the safety net for human mistakes: a
deleted or overwritten object stays restorable for 90 days. Claims
arrive late (service dates up to 12 months before the load month), and cancellations and
adjustments in later months refer back to earlier originals, so claim-date logic and
de-duplication across deliveries happen in dbt, never by splitting files at ingestion.

### Adding a file type or a feed

**A file type in an existing feed** only needs `config/file_types/<file_type>.yml`, with:

- the feed's `landing_prefix`;
- the naming regex (named groups);
- the raw key template;
- the expected columns, copied from the real header;
- the key columns.

**A new feed** (another payer) also needs its prefix added to `landing_prefixes` in
`config/ingestion.yml` and to the Terraform variable `landing_prefixes`. That one list drives
the S3 triggers, the IAM scoping and the CLI `--scan`, so the two must match. Then rebuild the
package and `terraform apply`.

The config is validated on load. For example:

- feed prefixes may not overlap each other or `archive/` / `quarantine/`;
- a template may only use groups the regex captures;
- a raw key must stay inside its own `<feed>/<file_type>/` folder, because IAM grants raw writes
  per feed.

## Transformations: raw → staging (dbt + DuckDB)

The dbt project in [`dbt/`](../dbt/) reads raw Parquet straight from S3 (DuckDB `httpfs`, AWS
credential chain; no copy step) and builds staging views in a local DuckDB file. Each feed has
its own dbt source and staging folder (`models/staging/cclf/`, `models/staging/humana/`).

| Model | Grain | Unique key |
|---|---|---|
| `staging.stg_cclf__deliveries` / `stg_humana__deliveries` (tables) | delivered file | `file_type`, `checksum` |
| `staging.stg_cclf__claims_header` | claim version | `claim_id` |
| `staging.stg_cclf__claims_diagnosis` | diagnosis per claim version | `claim_id`, `diagnosis_sequence_number` |
| `staging.stg_cclf__part_b_physician_lines` | line per claim version | `claim_id`, `line_number` |
| `staging.stg_cclf__beneficiary_demographics` | beneficiary per delivery | `beneficiary_mbi`, `delivery_month` |
| `staging.stg_humana__member_eligibility` | member per delivery | `member_id`, `delivery_month` |
| `staging.stg_humana__medical_claim_lines` | line per claim version | `claim_id`, `line_number`, `claim_frequency_code` |

**Which delivery counts.** Raw keeps every delivery. `stg_<feed>__deliveries` lists them all,
one row per delivered file, with `is_current` (the latest ingested delivery of each file name),
`current_checksum` (on a superseded row, the delivery that replaced it) and
`deliveries_of_file`. Every staging model reads only the rows of current deliveries
([`macros/deliveries.sql`](../dbt/macros/deliveries.sql)). A STOP test
(`delivery_index_is_consistent`) fails the feed if a file sits under another file's checksum
folder or two deliveries of one file tie for "current". Because raw never loses a delivery,
another STOP (`raw_keeps_every_built_delivery`) fails the feed if a checksum that
intermediate was built from is missing from raw entirely (a mistaken delete, a wrong glob):
intermediate keeps its state instead of having those keys pruned.

Apart from that, staging does only three things, the same way for both feeds:

- renames delivered columns to readable names (`CUR_CLM_UNIQ_ID` → `claim_id`,
  `SERVICE_FROM_DT` → `claim_start_date`; each column's description keeps the delivered name);
- turns empty strings into `NULL`;
- casts types.

Shared concepts get the same name in both feeds (`beneficiary_mbi`, `claim_start_date`,
`facility_ccn`, `drg_code`, …), which prepares the later combined layer. Dates become `DATE`, amounts `DECIMAL(18,2)`, counts `INTEGER`. Codes and
identifiers stay text, because many carry leading zeros (`fips_county_code = '013'`) or
non-numeric values (`dual_status_code = 'NA'`). Every delivery is kept. Picking current
beneficiary rows and netting cancellations/adjustments is the next layer's job.

Tests (`dbt build` runs them): keys unique and not null, code domains (`accepted_values`),
and delivery rules:

- **CMS:**
  - every CCLF4 diagnosis belongs to a CCLF1 claim;
  - claim load dates fall inside their delivery month;
  - every claim's beneficiary arrived in a CCLF8 delivery no later than the claim.
- **Humana:**
  - processed dates and the eligibility as-of date fall inside the delivery month, and a claim
    is never processed before it was received;
  - frequency code, status and `original_claim_id` agree (original, replacement, reversal);
  - `member_id` and MBI map one to one;
  - every claim's member arrived in an eligibility delivery no later than the claim.

  Because reversals carry negated amounts, `sum(paid_amount)` per delivery month already
  matches the answer key's `net_paid_by_processed_month.csv`.
`dbt source freshness` warns when a monthly delivery is 35 days overdue.

### Stop vs warn

Each test is a **stop** or a **warn** (policy in `dbt/macros/test_scoping.sql`):

- **Stop** (dbt severity `error`, the default): the data would make the intermediate
  layer wrong. That covers keys and grain, identity columns, claim-family linkage,
  amounts used in netting, service dates, and codes the intermediate logic branches on.
  `dbt build` then **skips** every model downstream of the failing staging model, so
  the intermediate tables keep their last good state. The skipped file stays
  "unprocessed", so a corrected re-send catches the intermediate layer up with no
  backfill. Stops always check every delivery month. If they checked only the new
  months, the next delivery's run would pass and pull the skipped file's rows in.
- **Warn** (`config: *warn` in the YAML, `severity='warn'` in singular tests): the
  intermediate layer stays correct, but the data looks off. Examples are descriptive
  codes, claims for a member not delivered yet, dates outside the delivery month, and
  odd amount lines. Nothing is skipped. When dbt is given the months being processed
  (`--vars '{delivery_months: ["2025-03"]}'`), warns check only those months, so an
  accepted warning does not come back on every later delivery. Local builds without
  the var check everything.
- Humana amount rules (paid ≤ allowed ≤ billed, denied pays nothing, cost share =
  allowed − paid, reversals never positive) warn per line. They **stop** when more than
  1% of a month's lines (and at least 5) break them, because that means a broken
  extract rather than a few odd lines.

`tests/test_dbt_gating.py` proves the gating on corrupted copies of real deliveries:
- a stop skips the intermediate model;
- a stop keeps blocking later months;
- a re-send recovers the intermediate tables exactly;
- warns build and are scoped to the delivery months being processed;
- many bad amount lines escalate to a stop.

```bash
.venv/Scripts/python -m pip install -e ".[dev,dbt]"
cd dbt
../.venv/Scripts/dbt build               # models + tests (profiles.yml is in dbt/)
../.venv/Scripts/dbt source freshness
```

## Intermediate: state across deliveries (incremental)

Both feeds deliver deltas. CCLF8 and HUM_ELIG carry only new, changed or terminated members.
Claim files carry only the versions processed that month, and a cancellation, adjustment,
reversal or replacement of an earlier claim can arrive months later. So intermediate is
incremental, but **not append-only**: a new delivery can change rows built from older ones.

| Model (schema `intermediate`) | Grain | Key |
|---|---|---|
| `int_cclf__beneficiary_versions` | beneficiary version (SCD type 2) | `beneficiary_version_id` |
| `int_cclf__claim_families` | claim family = original + later cancellation/adjustment | `claim_family_key` (`part:claim_control_number`) |
| `int_humana__member_versions` | member version (SCD type 2) | `member_version_id` |
| `int_humana__claim_families` | claim family = original + later reversal/replacement | `claim_family_key` (the original's `claim_id`) |

- **Beneficiary versions.** A new version starts when the hash of the tracked attributes
  differs from the previous delivered row. This works for change-only deliveries and for
  full monthly snapshots. Fields:
  - `valid_from` / `valid_to` are delivery months (`valid_to` exclusive, NULL while current).
  - `change_type` (NEW/UPDATE) and `changed_columns` are derived. They are never read from
    the generator's `RECORD_CHANGE_TYPE` helper; a test checks the two agree.
  - Age is not tracked, because it changes every month without a real change.
- **Claim families.** Versions are grouped by (part, control number), and the latest
  version decides the net:
  - cancellation → 0;
  - adjustment → the adjustment's amount;
  - otherwise → the original's amount.

  Part B rolls CCLF5 lines up to claims first. A cancellation whose original was never
  loaded still forms a family (`has_original = false`, a warning) instead of being dropped.
- **Humana member versions.** The same hash-based SCD2. `change_type` adds `TERM` for the
  version in which `enrollment_end_date` first appears. `file_as_of_date` moves every month,
  so it isn't tracked; `last_file_as_of_date` keeps the newest one.
- **Humana claim families.** Lines roll up to claim versions, which group on
  `coalesce(original_claim_id, claim_id)`, the original's ID. The status comes from which
  versions are still active (not cancelled by a reversal, which repeats the claim ID it
  reverses), so it never depends on version order:
  - an active replacement → `REPLACED`;
  - an active original → `DENIED` (status D) or `ORIGINAL`;
  - nothing active → `VOIDED`.

  Humana reversals carry negated amounts, so `net_paid_amount` is simply the sum over every
  version. A test checks that sum against the status rule (voided 0, replaced = the
  replacement, denied 0, else the original). That test caught a real bug while the model
  was being built: a same-day reversal sorted after its replacement.

**Incremental strategy: affected keys**
([`macros/incremental_by_affected_keys.sql`](../dbt/macros/incremental_by_affected_keys.sql)).
Every row records the SHA-256s of the raw files it came from (`_source_checksums`). Each run:

1. Finds the keys that have rows from a raw file the model has not processed yet: a new
   month, or a corrected re-send of an old one.
2. Adds the keys built from a file that staging no longer shows: a delivery a re-send
   superseded (it stays in raw, but is no longer current).
3. Recomputes the full history of only those keys and replaces them (`delete+insert`).
4. A post-hook deletes keys whose source rows vanished entirely.

Delivery order doesn't matter, and no watermark can drift. Recomputing a key's whole
history is cheap, since a beneficiary or claim family has only a few rows.
`dbt build --full-refresh` rebuilds everything.

**Proof** (`tests/test_dbt_intermediate.py`, offline, about 2 minutes). The test runs both
generators (the pinned `synthetic-payer-files` package) for all 36 months, ingests every file with the real validation and Parquet code
into a local raw folder, and runs dbt against it. Checks:

- A full refresh matches both answer keys exactly:
  - **CMS:** every current member with every attribute, every member change, all claim
    actions, net paid by service month and by load month, and the totals.
  - **Humana:** every current member, every member change (including disenrollments and
    deaths), every void and replacement, net paid by service month, capitated encounters
    by month, and the totals.
- Loading the months in uneven batches (3, then 1 at a time, then 24 at once) gives the
  same tables as a full refresh, row for row.
- Re-sending old months then also equals a full refresh:
  - CCLF8 drops a beneficiary and a history row;
  - CCLF1 drops a claim and reprices one;
  - HUM_MEDCLM drops a replacement, so that family falls back to `VOIDED`;
  - HUM_ELIG drops a member update.

The test was shown to fail when the vanished-file cleanup is removed. Skip it with
`pytest -m "not dbt"`.

## Marts (gold): payer-agnostic

The marts know nothing about any payer. They are built from two things only:

- **The payer registry** (`payers` in [`dbt/dbt_project.yml`](../dbt/dbt_project.yml)):
  `payer_id`, name, line of business, `dbt_source` (the feed's dbt source),
  `onboarded_month`, `retired_month`. Validated on parse
  ([`macros/payers.sql`](../dbt/macros/payers.sql)): unique snake_case ids and sources,
  `YYYY-MM` months.
- **The conformed contract**: every registered payer provides two views,
  `int_<src>__conformed_coverage` (one row per coverage span) and
  `int_<src>__conformed_claims` (one row per claim family). Their columns and types are
  written once in [`_conformed__models.yml`](../dbt/models/intermediate/_conformed__models.yml)
  (YAML anchors) and enforced by dbt (`contract: enforced`): a view with a missing, extra or
  mistyped column does not build. Payer-specific mapping happens in these views only:

  | Concept | CMS FFS (CCLF) | Humana MA |
  |---|---|---|
  | member id | MBI | `MEMBER_ID` |
  | coverage span | `BENE_RNG_BGN_DT`/`END_DT`, latest delivered row per start date | enrollment start/end, latest row per start date |
  | sex | `1`/`2` → `M`/`F` | `M`/`F` |
  | dual | `BENE_DUAL_STUS_CD` ≠ `NA` | `DUAL_FLAG = Y` |
  | claim category | Part A / B → institutional / professional | form `I` / `P` |
  | claim status | `ORIGINAL`/`ADJUSTED`/`CANCELLED` → `PAID`/`ADJUSTED`/`VOIDED` | `ORIGINAL`/`REPLACED`/`VOIDED`/`DENIED` → `PAID`/`ADJUSTED`/`VOIDED`/`DENIED` |

  Coverage ends at death when that comes first.

| Mart (schema `marts`) | Grain | Notes |
|---|---|---|
| `dim_payer` | registered payer | status `active` / `retired` / `awaiting_data`, delivery window (first/last current delivery) |
| `dim_member` | person, keyed by MBI | attributes from the latest span (birth year, sex, state, dual, deceased); current payer = active payer of the latest open span; `payer_count`, `is_multi_payer`, span count |
| `fct_claims` | claim family | `claim_key` = `<payer_id>:<claim id>`; normalized status and category; net paid after reversals and adjustments |
| `fct_member_months` | payer × member × month | covered on any day of the month, within the payer's delivery window |
| `agg_payer_monthly` | payer × month | member months, non-voided claims and net paid by service month, PMPM |

All five are rebuilt in full on every run (dbt-duckdb `external` materialization: one Parquet
file per mart under the profile's `external_root`, plus a DuckDB view over it). Gold is small
and derived, so a full rebuild is always consistent with intermediate. No names, addresses or
full birth dates reach the marts.

**Payer lifecycle** (registry only; the marts' SQL never changes):

- **Onboard:** ingestion config + staging + intermediate for the feed, its two conformed
  views (two entries in `_conformed__models.yml`), and a registry entry. A registered payer
  with nothing built yet shows as `awaiting_data` and is left out of the unions instead of
  failing the other payers' marts.
- **Retire:** set `retired_month`. History stays in every mart; its members no longer count
  as currently covered; member months end at its last delivery. A file delivered after the
  retirement month raises a WARN (`assert_no_deliveries_after_payer_retirement`).
- **Offboard:** remove the entry. Its conformed views are disabled (`enabled` follows the
  registry) and it drops out of every mart; its staging and intermediate history stays.

**Tests:** keys and grain on every mart, relationships to `dim_payer` and `dim_member`, a STOP
reconciliation (`assert_marts_reconcile`: the summary adds up to the facts per payer) and a
WARN for claims whose member has no coverage anywhere. `test_dbt_intermediate.py` checks the
marts against both answer keys (net paid by service month, the 17 payer switchers, member
months in the last month); `test_dbt_gating.py` checks payer isolation and the lifecycle.

## Running it

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev]"     # Windows; use .venv/bin on macOS/Linux

# credentials come from the AWS CLI default profile (aws configure)
.venv/Scripts/python -m claims_etl.ingestion.cli --key cms/cclf/claims_header/CCLF1_202301.csv
.venv/Scripts/python -m claims_etl.ingestion.cli --scan
```

JSON-lines logs go to stderr, and a JSON summary of manifest records goes to stdout. Exit codes:
`0` all success/skipped, `2` something was quarantined, `1` an error occurred.

## Quarantine notifications (GitHub issues)

A decoupled notifier turns each quarantine report (`quarantine/.../<file>.csv.error.json`) into
a GitHub issue. Ingestion doesn't change, and a GitHub outage can't affect loading or quarantine.

- **One issue per checksum.** Before opening an issue it looks for an *open* `quarantine` issue
  whose body has the line ``Checksum: `<sha256>` ``. The lookup uses the Issues list endpoint, not
  search, which lags. If it finds one, it adds a "quarantined again" comment instead.
- **Ticket record:** `s3://fredr-claims-raw-2026/_tickets/<sha256>.json` stores the issue URL and
  number and every batch already notified. It's kept apart from the manifest, which only
  ingestion writes. Re-running for the same event makes no API calls.
- **Public-safe content.** Issues carry check names, counts, line numbers, column names, and file
  metadata. Quoted values in CSV check details survive only if they are shaped like column names
  (`BENE_MBI_ID`), so a headerless CCLF8 file can't publish beneficiary names or cities. Details
  are truncated and rendered as code so they can't @mention anyone.
- **Resilient.** It retries 5xx/429/rate-limited responses with backoff, honoring `Retry-After` and
  `x-ratelimit-reset`. It never logs the token. Failures exit non-zero, and `--scan` retries later.
- **Configurable:** labels, title template, and per-check next steps live in
  `config/notifications.yml`. Moving to Jira would only replace `github_client.py`.

Issue title: `[Quarantine] <file_type> <file name>: <failed check>`, for example
`[Quarantine] claims_header CCLF1_202301.csv: csv_header`.

```bash
cp .env.example .env    # then set GITHUB_TOKEN (fine-grained, this repo only, Issues: read/write)
.venv/Scripts/python -m claims_etl.notifications.cli --scan
.venv/Scripts/python -m claims_etl.notifications.cli --error-key quarantine/cms/cclf/<type>/<file>.csv.error.json
```

In AWS, the `claims-etl-notify` Lambda ([`infra/terraform/notifier.tf`](../infra/terraform/notifier.tf))
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

Uploading a `.csv` under `s3://fredr-claims-landing-2026/cms/cclf/` or `humana/ma/` triggers the
`claims-etl-ingest` Lambda, which runs the same `process_file`. It's defined in
[`infra/terraform/`](../infra/terraform/):

| Resource | Details |
|---|---|
| Lambda `claims-etl-ingest` | Python 3.13, 1024 MB, 300 s timeout, 2 GB `/tmp` |
| pyarrow | AWS SDK for pandas public layer (`AWSSDKPandas-Python313:17`, pyarrow 24) |
| Package | `build/lambda.zip`: our code, `config/`, PyYAML, requests (~1.6 MB, deterministic; shared with the notifier) |
| IAM role | Least privilege: read/delete each feed prefix (`cms/cclf/*`, `humana/ma/*`), write `archive/*` and `quarantine/*` in landing, write the feed prefixes and `_manifests/*` in raw, own log group only |
| Trigger | S3 `ObjectCreated`, one per feed prefix (`cms/cclf/`, `humana/ma/`), suffix `.csv` (archive/quarantine moves can't re-trigger) |
| Retries | An `error` outcome raises, so Lambda retries async invocations twice. `quarantined` is final and not retried |
| Logs | `/aws/lambda/claims-etl-ingest`, JSON lines, 30-day retention |
| Notifier | `claims-etl-notify` (`notifier.tf`): 256 MB, 300 s timeout, same package without the pandas layer; trigger `quarantine/*.error.json` |

Deploy or update (both packages; the transform Lambda is in the next section):

```bash
.venv/Scripts/python scripts/build_lambda.py              # ingest + notify package
.venv/Scripts/python scripts/build_lambda.py --transform  # transform package
cd infra/terraform
terraform init                                       # first time only
terraform plan -out=tfplan && terraform apply tfplan
```

Terraform state is local (gitignored). Move it to an S3 backend before CI or anyone else runs it.

## Event-driven transforms (staging + intermediate + marts)

Every file that ingestion puts in raw triggers a dbt build of its feed, with no schedule:

```
claims-etl-ingest ──FileIngested──► EventBridge rule ──► SQS FIFO (one message group) ──► claims-etl-transform
                          └── undeliverable ──► claims-etl-transform-undelivered
                                                              └── after 3 failed receives ──► dead-letter queue
```

1. **Event.** After the manifest is written and before the landing file is archived,
   ingestion publishes `FileIngested` (feed, file type, delivery month, checksum, raw URI) to
   the default EventBridge bus ([`events.py`](../src/claims_etl/events.py)). If publishing fails,
   the run ends in `error` with the file still in landing. The retry finds it already loaded
   (`skipped_duplicate`), publishes again with `redelivered: true`, and archives it.
2. **Queue.** A FIFO queue with a single message group runs builds one at a time, in order.
   Up to 10 waiting events share one build.
3. **Build** ([`transform/runner.py`](../src/claims_etl/transform/runner.py)). The runner selects
   the **whole feed** (`source:cclf+` / `source:humana+`), not just the file type that arrived.
   A bad CCLF5 file must keep blocking claim families even when a clean CCLF1 comes next. With
   `source:cclf.claims_header+` the CCLF5 stop tests would not run, and the bad lines would
   flow in (`test_dbt_gating.py` checks this). `delivery_months` is set to the events' months,
   so only the WARN tests are scoped (see [Stop vs warn](#stop-vs-warn)). dbt runs in a
   subprocess against a **working copy** of the DuckDB state file, **one pass per feed**
   (in order). The marts are downstream of every feed, so each pass rebuilds them: a stop in
   Humana skips the marts in the Humana pass, but the CMS pass already rebuilt them from
   CMS's new data and Humana's last good state. Results are merged by node, and a later
   `skipped` never hides an earlier result that ran. The marts follow the state: the S3 state
   publishes them to `marts_root` in `config/transform.yml` (the marts bucket), a local state
   file (`--db`) gets a `<state>_marts/` folder next to it.
4. **Outcome.** The runner reads `run_results.json` and records one of four outcomes:

   | Outcome | Meaning | State | Queue |
   |---|---|---|---|
   | `passed` | all tests pass | published | message deleted |
   | `warned` | WARN tests found issues; intermediate built | published | deleted |
   | `stopped` | a STOP test failed; dbt skipped the intermediate models below it | published (the stopped feed stays pending: its checksum is not in intermediate yet) | deleted |
   | `error` | a model or test could not run (SQL error, S3 trouble) or dbt crashed | **kept** | raised → retried after a backoff → DLQ |

   **Retry backoff.** SQS retries a failed message only when its visibility timeout runs
   out: 3600 s here (6 × the function timeout, AWS's guidance for Lambda consumers). Waiting
   that long after a 6-second crash would stall the single FIFO group for an hour per attempt.
   So before raising, the handler shortens the batch's visibility to `retry_backoff_seconds`
   in `config/transform.yml`, by receive count: 2 min, 5 min, then 10 min. A passing problem
   (a state conflict, S3 trouble) is retried within minutes, and a persistent one reaches the
   DLQ in about 17 minutes instead of 3 hours. The hour stays the backstop for a function
   that dies before it can shorten anything (timeout, out of memory).

   A run report (`report.json`: outcome, test names, failure **counts**, skipped models, the
   triggering files; never row values) and `run_results.json` go to
   `s3://fredr-claims-raw-2026/_transform/runs/<run_id>/`.
5. **State.** The DuckDB file lives at `_transform/state/claims_lakehouse.duckdb` in the raw
   bucket. The upload is conditional on the ETag downloaded at the start (`If-Match`, or
   `If-None-Match` for the first run), so a second writer can never silently overwrite it.
   The previous version is copied to `claims_lakehouse.previous.duckdb` first.

Run the same build by hand ([`transform/cli.py`](../src/claims_etl/transform/cli.py)):

```bash
claims-transform --feed cclf --months 2023-03     # S3 state + S3 raw, exactly like the Lambda
claims-transform --all                            # both feeds; WARN tests over all history
claims-transform --all --db dbt/claims_lakehouse.duckdb --raw-root <folder>   # fully local
```

Exit codes: 0 = passed or warned, 2 = stopped, 1 = error.

| Resource (`transform.tf`) | Details |
|---|---|
| Lambda `claims-etl-transform` | Python 3.13, 2048 MB, 600 s timeout, 2 GB `/tmp`; dbt target `lambda`; dbt starts via `claims_etl.transform.dbt_cli` (threading locks: Lambda has no `/dev/shm`) |
| Package | `build/transform_lambda.zip` (`build_lambda.py --transform`): dbt-core, dbt-duckdb, duckdb at the locally tested versions, the dbt project, and the DuckDB `httpfs` + `aws` extensions (nothing is downloaded at run time). 219 MiB unzipped (limit 250): the unused 275 MB dbt v2 parser binary is replaced by an empty placeholder wheel, and Babel's locale data is cut to English. 72 MB zipped, so it deploys through `s3://fredr-claims-artifacts-2026` |
| Queue | `claims-etl-transform.fifo`, content-based deduplication (required: EventBridge sends no deduplication ID), visibility timeout 3600 s (6 × the function timeout; a failed build sets its own shorter backoff), DLQ `claims-etl-transform-dlq.fifo` (14 days) |
| Rule | `source = claims-etl.ingestion`, `detail-type = FileIngested` → queue, message group `transform`; retries for 24 h, then the standard queue `claims-etl-transform-undelivered` (14 days) |
| IAM | Read raw feed prefixes; read/write `_transform/*`; list/read/write the marts bucket; consume its queue; own log group. Ingestion gains `events:PutEvents` on the default bus, limited to its own source |

## Pipeline alerts (GitHub issues, no email)

Every failure ends up as a GitHub issue in this repo, through the same notifier Lambda,
token and sanitizing as quarantine issues
([`notifications/alerts.py`](../src/claims_etl/notifications/alerts.py)). GitHub notifies
the repo owner of new issues and comments, so a failure reaches you within a minute or two.

```
transform run report.json ──S3 trigger──► claims-etl-notify ──► issue / comment / close
CloudWatch alarm (ALARM/OK) ──► SNS claims-etl-alerts ──► claims-etl-notify ──► issue / comment / close
```

- **Transform runs.** A `stopped` or `error` run opens an issue labeled `pipeline-alert`,
  `transform` and `stopped` or `error`. The issue names the feeds the *failing* tests or
  nodes read (traced through dbt's manifest to their sources), not every feed the run built:
  a run that builds CMS and Humana but stops on a Humana test opens a `humana` issue. The
  same problem in a later run adds a comment instead. A later `passed` or `warned` run that
  built those feeds comments and closes it, so an open transform issue always means
  "still broken".
- **Alarms** cover failures that leave no report:

  | Alarm | Fires when |
  |---|---|
  | `claims-etl-transform-dlq-not-empty` | a transform batch ran out of retries |
  | `claims-etl-events-delivery-failed` | EventBridge failed to deliver a `FileIngested` event (the 2023-04 bug) |
  | `claims-etl-events-undelivered` | EventBridge gave up after 24 h of retries |
  | `claims-etl-ingest-errors` | ingestion raised (bad data is quarantined instead) |
  | `claims-etl-transform-killed` | the transform Lambda timed out or ran out of memory (log metric filter) |

  ALARM opens the alarm's issue (or comments on it); OK comments and closes it.
- **Public-repo safe.** Issues carry run IDs, outcomes, feeds, months, file names, test
  names and failure counts. Everything is whitelisted: names must look like identifiers,
  the build exception keeps only its class name, and dbt error messages are never posted
  (a SQL error can quote a data value). The issue links to the private report instead.
- **Idempotent.** Each handled event leaves a record under `_tickets/alerts/`, so a
  re-delivered S3 or SNS event makes no API calls.
- **Not covered:** the notifier itself (its alarm would be delivered by the failing
  function) and the GitHub token, which expires 2027-02-02. Both are in CLAUDE.md.

## Tests

```bash
.venv/Scripts/python -m pytest      # 250 tests; no AWS or GitHub access (moto + responses)
.venv/Scripts/python -m pytest -m "not dbt"   # skip the ~30 min dbt end-to-end tests
.venv/Scripts/python -m ruff check src tests
```

Fixture CSVs are built at test time, including deliberately broken ones: bad name (month 13,
wrong CCLF number, re-download suffix), empty file, wrong/reordered/renamed header, UTF-8 BOM,
ragged row, blank line, header only, empty or whitespace key, non-UTF-8 bytes, malformed quoting,
and a headerless file whose first row must not reach a public issue.

## Loaded so far

Both feeds are landed month by month from 2023-01 (through 2023-08 so far), including
deliberate curve balls: 2023-07 landed before 2023-06, and a HUM_ELIG_202308 with an empty MBI
that was later re-sent. All 216 generated files (seed 42) pass validation and reconcile
locally against the real config:

| Feed | File type | Files | Rows |
|---|---|---|---|
| CMS CCLF | `claims_header` / `claims_diagnosis` / `part_b_physician_lines` / `beneficiary_demographics` | 4 × 36 | 749 / 1,716 / 634 / 931 |
| Humana MA | `member_eligibility` / `medical_claim_lines` | 2 × 36 | 861 / 602 |

## Project layout

```
config/                     ingestion.yml, notifications.yml, transform.yml, file_types/*.yml
src/claims_etl/ingestion/
  pipeline.py               process_file orchestration
  validation.py             runs checks in order
  checks.py                 one function per check
  csv_inspect.py            strict CSV reader, one-pass CSV profile
  parquet_writer.py         CSV → Parquet (string columns + metadata)
  s3_store.py               the only boto3 code (download, upload, move = copy→verify→delete)
  manifest.py               idempotency records
  log.py / utils.py          JSON-lines logging, checksum and time helpers (shared)
  cli.py / lambda_handler.py
src/claims_etl/s3_events.py S3 event parsing shared by both handlers (no pyarrow)
src/claims_etl/notifications/alerts.py, alert_config.py
                            transform reports + CloudWatch alarms -> GitHub issues
src/claims_etl/events.py    FileIngested event (ingestion -> EventBridge -> transform)
src/claims_etl/transform/
  runner.py                 run_transform: select feeds, dbt build (subprocess), classify, publish
  state.py                  DuckDB state file: S3 with ETag-conditional upload, or local
  sinks.py                  run report + run_results.json (S3 or a local folder)
  config.py                 transform.yml (state keys, feed -> dbt source)
  cli.py / lambda_handler.py
src/claims_etl/notifications/
  notifier.py               notify_quarantine: create issue or comment, record result
  content.py                title/body/comment + sanitizing (no tracker dependency)
  github_client.py          GitHub REST: list/create/comment/labels, retries
  ticket_record.py          _tickets/<sha256>.json
  report.py                 typed, validated view of an .error.json
  config.py                 notifications.yml (labels, title template, next steps)
  settings.py               get_setting(): env locally, SSM in Lambda
  cli.py / lambda_handler.py
scripts/migrate_to_immutable_bronze.py  one-off: move pre-2026-11-05 objects to checksum keys
scripts/build_lambda.py     Lambda packages: lambda.zip (ingest + notify), --transform
infra/terraform/            main.tf (ingestion), notifier.tf, transform.tf, triggers, IAM, logs
tests/                      pytest for ingestion + notifier; test_dbt_intermediate.py (dbt end to end)
dbt/
  dbt_project.yml, profiles.yml   DuckDB target, S3 via credential chain (no secrets)
  models/staging/cclf/            _cclf__sources.yml, _cclf__models.yml, stg_cclf__*.sql
  models/staging/humana/          _humana__sources.yml, _humana__models.yml, stg_humana__*.sql
  models/intermediate/cclf/       int_cclf__beneficiary_versions, int_cclf__claim_families
  models/intermediate/humana/     int_humana__member_versions, int_humana__claim_families
  models/intermediate/*/          int_<src>__conformed_coverage / _claims (+ _conformed__models.yml)
  models/marts/                   dim_payer, dim_member, fct_claims, fct_member_months, agg_payer_monthly
  macros/                         generate_schema_name, incremental_by_affected_keys, payers
  tests/                          singular tests + generic unique_combination_of_columns
```
