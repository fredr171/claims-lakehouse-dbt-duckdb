# Claims Lakehouse: S3 + Lambda + dbt-duckdb

An event-driven ETL pipeline on AWS for monthly healthcare claims deliveries from two payers:
CMS CCLF (Medicare fee-for-service) and Humana Medicare Advantage. A file dropped in S3 is
validated, stored immutably as Parquet, and modeled with dbt into current members and netted
claims. Every failure becomes a GitHub issue that closes itself when the data recovers.

All data is **synthetic**, from my seeded generator repo
[synthetic-payer-files](https://github.com/fredr171/synthetic-payer-files), which also writes
**answer keys**: the exact results a correct pipeline must produce. The tests check the
pipeline against them.

**Stack:** Python 3.13, AWS (S3, Lambda, EventBridge, SQS FIFO, SNS, CloudWatch, SSM),
Terraform, dbt Core + DuckDB, pyarrow, pytest (moto, responses), ruff.

## Architecture

```
 payer CSV ─► landing (S3) ─► ingest Lambda ─► raw / bronze (S3 Parquet, immutable)
                  │               │                        │
          quarantine + issue   FileIngested ─► EventBridge ─► SQS FIFO ─► transform Lambda
                                                                           (dbt build)
                                                                               │
                              staging + intermediate / silver (DuckDB) ◄───────┘
                              marts / gold (S3 Parquet)  ← next

 failures (quarantine, stopped/errored builds, CloudWatch alarms) ─► notifier Lambda ─► GitHub issues
```

| Layer | Where | What it holds |
|---|---|---|
| Landing | S3 | CSVs exactly as delivered |
| Raw (bronze) | S3, Parquet | Every delivery, all columns as text, plus lineage columns |
| Staging (silver) | DuckDB views | Typed, renamed, current deliveries only |
| Intermediate (silver) | DuckDB incremental tables | Member history (SCD2), claim families with net paid |
| Marts (gold) | S3, Parquet | *Planned* |

## Key decisions, layer by layer

### 1. Ingestion (landing → raw): validate files, not rows
- **File-level checks only:** name, size, encoding, strict CSV parsing, exact header, field
  counts, non-empty keys. A file that fails is quarantined (with an error report) and the run
  continues. Row-level quality belongs in dbt, where a bad value doesn't reject a whole delivery.
- **Everything is read as text.** Claims codes carry leading zeros (FIPS `05`, ICD-10, NPIs);
  type inference would silently corrupt them. Typing happens in staging.
- **Idempotent and reconciled:** a SHA-256 manifest per file skips duplicates; Parquet row
  counts must match the CSV before anything is published.
- **Config-driven:** a new file type is one YAML file; a new payer is one prefix plus the YAML.

### 2. Raw (bronze): immutable, keyed by delivery
- Each delivery gets its own key: `<feed>/<file_type>/delivery_month=<m>/checksum=<sha256>/`.
  A corrected re-send lands **next to** the bad file instead of overwriting it, so you can
  always answer "what did the payer send, and when", and roll back a bad correction.
- Partitions follow the delivery (from the file name), never claim dates: late claims and
  adjustments of earlier months are dbt's job, not a reason to split files.
- Versioning plus a 90-day lifecycle rule are the safety net for human mistakes.

### 3. Staging (silver): one clean view per file, no business logic
- Rename to readable names shared across payers (`beneficiary_mbi`, `claim_start_date`),
  empty string → NULL, cast types. Codes and IDs stay text.
- A deliveries index picks the **current** delivery of each file (the latest one ingested);
  staging reads only those.
- **Stop vs warn tests.** A *stop* test (keys, identity, claim linkage, netting amounts)
  means the data would make intermediate wrong, so dbt skips everything downstream and
  intermediate keeps its last good state. A *warn* test flags odd data that is still safe.
  An amount rule escalates from warn to stop when more than 1% of a month's lines break it
  (a broken extract, not a few odd lines).

### 4. Intermediate (silver): correct state from out-of-order deltas
- **Member history (SCD type 2)** from change-only files: a new version when a hash of the
  tracked attributes changes; change type (new, update, termination) is derived, not trusted
  from the file.
- **Claim families:** an original plus its later reversals or replacements, which can arrive
  months later. Status comes from which versions are still active, never from file order.
- **Incremental by affected keys**, not append-only or a date watermark: every row records
  which raw files built it; each run recomputes only the keys touched by new or superseded
  files. Delivery order doesn't matter, and an incremental run equals a full rebuild.

### 5. Orchestration and alerting: event-driven, one build at a time
- Each ingested file publishes an event; a FIFO queue serializes builds of the **whole feed**
  (so a bad file of one type keeps blocking even when a clean file of another type arrives).
- Build outcomes: passed, warned, stopped (state published, bad feed held back) or error (old
  state kept, retried with a 2/5/10-minute backoff, then a dead-letter queue).
- DuckDB state lives in S3 with an ETag-conditional upload, so two writers can't overwrite
  each other.
- **GitHub issues instead of email:** quarantines, stopped builds and five CloudWatch
  alarms each open one issue, add comments while the problem persists, and close it on
  recovery. The repo is public, so issues carry only identifiers and counts, never data values.

## Testing

243 pytest tests run with no AWS or GitHub access (moto and responses mock them):

- **Every ingestion check** against deliberately broken fixture files (BOM, ragged rows, bad
  encoding, missing keys, ...).
- **End to end against the answer keys:** generate 36 months for both payers, ingest them with
  the real code, run dbt, and match current members, member changes, claim actions and net
  paid exactly. Loading the months in uneven batches, or re-sending old months, must give the
  same tables as a full rebuild.
- **Gating:** corrupted deliveries prove that stops block intermediate (and keep blocking later
  months), a re-send recovers it, and warns don't block.
- Every new dbt test was first shown to **fail on broken data** before it was trusted.

## Found in live runs

Things the live pipeline exposed, each fixed and covered by a test:

- EventBridge → SQS FIFO silently dropped events until content-based deduplication was on.
- dbt crashed in Lambda (no `/dev/shm` for multiprocessing locks); dbt now starts with threading
  locks, and a test simulates Lambda's environment.
- Landing 2023-07 before 2023-06 stopped the Humana build on a member change test until the
  missing month arrived, then caught up cleanly.
- A bad eligibility file and its re-send drove the switch to immutable bronze, and showed
  that an alert must be keyed by the feed the *failing test* reads, not every feed the run built.

## Run it

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -e ".[dev,dbt]"   # also installs the pinned generator
.venv/Scripts/python -m pytest                        # all tests, offline (~10 min)
.venv/Scripts/python -m pytest -m "not dbt"           # fast tests (~1 min)
cd dbt && ../.venv/Scripts/dbt build                  # build against S3 (AWS CLI profile)
```

Infrastructure is Terraform in [`infra/terraform/`](infra/terraform/). For every check,
model, AWS resource and CLI, see [docs/pipeline-details.md](docs/pipeline-details.md).

## Next

Marts published to S3 (dbt external materialization), Terraform state in an S3 backend, and
CI with GitHub Actions.
