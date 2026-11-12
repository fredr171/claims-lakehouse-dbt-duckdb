"""STOP vs WARN gating in `dbt build` (policy in dbt/macros/test_scoping.sql), offline.

A clean baseline (delivery months 2023-01 and 2023-02, both feeds) is built once; each
scenario copies it, corrupts one raw partition the way a bad delivery would, and runs
dbt the way the transform runner does: `build --select source:<feed>+` (the whole feed)
with the delivery months being processed. Checks:

1. Clean data raises no failures and no warnings.
2. A STOP (duplicate claim_id in CCLF1) fails the build and SKIPS the intermediate
   model, which never sees the bad file; it keeps blocking a later month's run (stops
   check all months); after a corrected re-send the build passes and equals a full
   refresh of the same raw data.
3. A WARN (bad IN_NETWORK_FLAG, one line paid above allowed) passes the build, builds
   the intermediate model, and is scoped: a later month's run does not re-raise it,
   an unscoped run does.
4. Many broken amount lines in one month turn the amount warning into a STOP.
6. Immutable bronze: an inconsistent deliveries index (a file under another file's
   checksum folder, or two deliveries of one file tied for "current") is a STOP that
   skips the whole feed's staging and intermediate models; so is raw losing a delivery
   that intermediate was built from (intermediate keeps its state).
5. The transform runner itself (claims_etl.transform, real dbt subprocess): a STOP in one
   file type (CCLF5) keeps the claim families blocked when a different, clean file type
   that feeds them (CCLF1) arrives next; the state is still published; the fix unblocks it.

Slow (~1 min, several dbt runs). Skip with: pytest -m "not dbt".
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from tests.dbt_harness import (
    HAS_DBT,
    DbtRun,
    _assert_same_intermediate,
    _dbt,
    _generate,
    _ingest_locally,
    _publish,
    _query,
    current_delivery,
    marts_dir,
    write_resend,
)

pytestmark = pytest.mark.dbt

if not HAS_DBT:
    pytest.skip("needs the dev + dbt extras (synthetic-payer-files, dbt)", allow_module_level=True)

BASELINE_MONTHS = ["2023-01", "2023-02"]
CCLF_CLAIMS = "source:cclf+"  # the runner's selectors (claims_etl.transform.runner)
MA_PAYER_CLAIMS = "source:ma_payer+"


# --- helpers ---------------------------------------------------------------------------


def _month_dir(raw: Path, feed: str, file_type: str, month: str) -> Path:
    return raw / feed / file_type / f"delivery_month={month}"


def _partition(raw: Path, feed: str, file_type: str, month: str) -> Path:
    """The current delivery of one file (its checksum=<sha>/part-0.parquet)."""
    return current_delivery(_month_dir(raw, feed, file_type, month))


def _edit(path: Path, column: str, rows: list[int], value) -> None:
    """Set `column` on the given row positions (value: str, or callable(row dict) -> str)."""
    table = pq.read_table(path)
    values = table[column].to_pylist()
    for i in rows:
        values[i] = value(table.slice(i, 1).to_pylist()[0]) if callable(value) else value
    position = table.schema.get_field_index(column)
    pq.write_table(table.set_column(position, column, pa.array(values, pa.string())), path)


def _checksum(path: Path) -> str:
    [checksum] = pc.unique(pq.read_table(path)["_source_checksum"]).to_pylist()
    return checksum


def _families_from(db: Path, model: str, checksum: str) -> int:
    [(n,)] = _query(
        db,
        f"select count(*) from intermediate.{model} "
        f"where list_contains(_source_checksums, '{checksum}')",
    )
    return n


def _status(run: DbtRun, fragment: str) -> str:
    """Status of the one node whose unique_id contains `fragment`."""
    [status] = [s for node, s in run.statuses.items() if fragment in node]
    return status


def _plain_paid_lines(raw_store: Path, path: Path, count: int) -> list[int]:
    """Row positions of paid, non-capitated originals whose claim is never reversed or
    replaced anywhere, so raising their paid amount leaves claim-family netting intact."""
    actioned = set()
    for part in raw_store.glob("ma_payer/ma/medical_claim_lines/*/*/part-0.parquet"):
        t = pq.read_table(part, columns=["CLAIM_ID", "ORIG_CLAIM_ID", "CLAIM_FREQ_CD"])
        actioned |= {c for c, f in zip(t["CLAIM_ID"].to_pylist(), t["CLAIM_FREQ_CD"].to_pylist(),
                                       strict=True) if f != "1"}  # fmt: skip
        actioned |= {c for c in t["ORIG_CLAIM_ID"].to_pylist() if c}
    rows = pq.read_table(path).to_pylist()
    picks = [
        i for i, r in enumerate(rows)
        if r["CLAIM_FREQ_CD"] == "1" and r["CLAIM_STATUS"] == "P"
        and r["CAPITATED_FLAG"] == "N" and r["CLAIM_ID"] not in actioned
    ]  # fmt: skip
    assert len(picks) >= count
    return picks[:count]


def _paid_above_allowed(row: dict) -> str:
    return f"{float(row['ALLOWED_AMT']) + 1:.2f}"


# --- fixtures --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def baseline(tmp_path_factory: pytest.TempPathFactory) -> dict:
    root = tmp_path_factory.mktemp("gating")
    csv_root, _ = _generate(root)
    raw_store = root / "raw_store"
    _ingest_locally(csv_root, raw_store)
    raw, db = root / "raw", root / "baseline.duckdb"
    _publish(raw_store, raw, BASELINE_MONTHS)
    run = _dbt(["build"], raw, db, root / "work")
    return {"raw_store": raw_store, "raw": raw, "db": db, "run": run}


@pytest.fixture
def lake(baseline: dict, tmp_path: Path) -> dict:
    """A private copy of the baseline raw folder and DuckDB file."""
    raw, db = tmp_path / "raw", tmp_path / "lake.duckdb"
    shutil.copytree(baseline["raw"], raw)
    shutil.copy(baseline["db"], db)
    return {"raw_store": baseline["raw_store"], "raw": raw, "db": db, "work": tmp_path / "work"}


def _run(lake: dict, select: list[str], months: list[str] | None, check: bool = False) -> DbtRun:
    args = ["build", "--select", *select] if select else ["build"]
    extra = {"delivery_months": months} if months else None
    return _dbt(args, lake["raw"], lake["db"], lake["work"], check=check, extra_vars=extra)


# --- 1. clean data ----------------------------------------------------------------------


def test_clean_data_has_no_failures_or_warnings(baseline: dict) -> None:
    run = baseline["run"]
    assert run.with_status("warn") == run.with_status("fail") == run.with_status("error") == []
    assert run.with_status("skipped") == []


# --- 2. a STOP --------------------------------------------------------------------------


def test_stop_skips_intermediate_until_the_file_is_fixed(lake: dict, tmp_path: Path) -> None:
    raw, db, store = lake["raw"], lake["db"], lake["raw_store"]
    _publish(store, raw, ["2023-03"])
    bad = _partition(raw, "cms/cclf", "claims_header", "2023-03")
    first_id = pq.read_table(bad)["CUR_CLM_UNIQ_ID"][0].as_py()
    _edit(bad, "CUR_CLM_UNIQ_ID", [1], first_id)  # duplicate claim_id: breaks the grain

    run = _run(lake, [CCLF_CLAIMS], ["2023-03"])
    assert run.returncode != 0
    assert _status(run, "unique_stg_cclf__claims_header_claim_id") == "fail"
    assert _status(run, "model.claims_lakehouse.int_cclf__claim_families") == "skipped"
    assert _families_from(db, "int_cclf__claim_families", _checksum(bad)) == 0

    # A later month cannot slip the bad file in: stops check every month.
    _publish(store, raw, ["2023-04"])
    run = _run(lake, [CCLF_CLAIMS], ["2023-04"])
    assert run.returncode != 0
    assert _status(run, "model.claims_lakehouse.int_cclf__claim_families") == "skipped"

    # Corrected re-send of 2023-03 (a new delivery next to the bad one): passes, and the
    # incremental state catches up exactly. The bad delivery stays in raw, superseded.
    clean = pq.read_table(_partition(store, "cms/cclf", "claims_header", "2023-03"))
    fixed = write_resend(clean, bad.parent.parent, "claims_header 2023-03")
    run = _run(lake, [CCLF_CLAIMS], ["2023-03"], check=True)
    assert _status(run, "model.claims_lakehouse.int_cclf__claim_families") == "success"
    assert _families_from(db, "int_cclf__claim_families", _checksum(fixed)) > 0
    assert _families_from(db, "int_cclf__claim_families", _checksum(bad)) == 0
    assert bad.is_file()
    assert _query(db, "select is_current from staging.stg_cclf__deliveries where checksum = "
                      f"'{_checksum(bad)}'") == [(False,)]  # fmt: skip
    _run(lake, [], None, check=True)  # the other file types' new months
    rebuilt = tmp_path / "rebuilt.duckdb"
    _dbt(["build", "--full-refresh"], raw, rebuilt, tmp_path / "work_rebuilt")
    _assert_same_intermediate(rebuilt, db)


# --- 3. a WARN --------------------------------------------------------------------------


def test_warn_builds_intermediate_and_is_scoped_to_the_batch(lake: dict) -> None:
    raw, db, store = lake["raw"], lake["db"], lake["raw_store"]
    _publish(store, raw, ["2023-03"])
    odd = _partition(raw, "ma_payer/ma", "medical_claim_lines", "2023-03")
    _edit(odd, "IN_NETWORK_FLAG", [0], "X")
    _edit(odd, "PAID_AMT", _plain_paid_lines(store, odd, 1), _paid_above_allowed)

    run = _run(lake, [MA_PAYER_CLAIMS], ["2023-03"])
    assert run.returncode == 0, run.output
    assert (
        _status(run, "accepted_values_stg_ma_payer__medical_claim_lines_in_network_flag") == "warn"
    )
    assert _status(run, "assert_ma_payer_amounts_consistent") == "warn"
    assert _status(run, "assert_ma_payer_amounts_mostly_consistent") == "pass"
    assert run.with_status("fail") == run.with_status("error") == []
    assert _status(run, "model.claims_lakehouse.int_ma_payer__claim_families") == "success"
    assert _families_from(db, "int_ma_payer__claim_families", _checksum(odd)) > 0

    # Next month: the accepted warnings are not raised again ...
    _publish(store, raw, ["2023-04"])
    run = _run(lake, [MA_PAYER_CLAIMS], ["2023-04"], check=True)
    assert run.with_status("warn") == []
    # ... but an unscoped (local, full-history) build still reports them.
    run = _run(lake, [MA_PAYER_CLAIMS], None, check=True)
    assert _status(run, "assert_ma_payer_amounts_consistent") == "warn"


# --- 4. many WARN rows become a STOP ------------------------------------------------------


def test_many_broken_amount_lines_stop_the_build(lake: dict) -> None:
    raw, db, store = lake["raw"], lake["db"], lake["raw_store"]
    _publish(store, raw, ["2023-03"])
    broken = _partition(raw, "ma_payer/ma", "medical_claim_lines", "2023-03")
    _edit(broken, "PAID_AMT", _plain_paid_lines(store, broken, 5), _paid_above_allowed)

    run = _run(lake, [MA_PAYER_CLAIMS], ["2023-03"])
    assert run.returncode != 0
    assert _status(run, "assert_ma_payer_amounts_mostly_consistent") == "fail"
    assert _status(run, "model.claims_lakehouse.int_ma_payer__claim_families") == "skipped"
    assert _families_from(db, "int_ma_payer__claim_families", _checksum(broken)) == 0


# --- 6. the deliveries index (immutable bronze) -------------------------------------------


def test_inconsistent_delivery_index_stops_the_feed(lake: dict) -> None:
    raw, store = lake["raw"], lake["raw_store"]
    _publish(store, raw, ["2023-03"])
    delivery = _partition(raw, "cms/cclf", "claims_header", "2023-03")
    test = "delivery_index_is_consistent_stg_cclf__deliveries"
    families = "model.claims_lakehouse.int_cclf__claim_families"

    # A file moved by hand under another checksum folder: its rows no longer match it.
    misplaced = delivery.parent.parent / f"checksum={'0' * 64}" / "part-0.parquet"
    misplaced.parent.mkdir()
    shutil.move(delivery, misplaced)
    run = _run(lake, [CCLF_CLAIMS], ["2023-03"])
    assert run.returncode != 0
    assert _status(run, test) == "fail"
    assert _status(run, "model.claims_lakehouse.stg_cclf__claims_header") == "skipped"
    assert _status(run, families) == "skipped"
    shutil.move(misplaced, delivery)
    misplaced.parent.rmdir()
    assert _run(lake, [CCLF_CLAIMS], ["2023-03"]).returncode == 0

    # A re-send ingested at exactly the same time as the original: "current" is ambiguous.
    tie = write_resend(pq.read_table(delivery), delivery.parent.parent, "tie")
    table = pq.read_table(tie)
    ingested = pq.read_table(delivery)["_ingested_at"]
    pq.write_table(table.set_column(table.schema.get_field_index("_ingested_at"), "_ingested_at",
                                    ingested), tie)  # fmt: skip
    run = _run(lake, [CCLF_CLAIMS], ["2023-03"])
    assert run.returncode != 0
    assert _status(run, test) == "fail"
    assert _status(run, families) == "skipped"


def test_raw_losing_a_built_delivery_stops_the_feed(lake: dict) -> None:
    """Immutable bronze: raw never loses a delivery, so a missing one is a STOP that keeps
    intermediate as it was (the post-hook would otherwise delete every key built from it)."""
    raw, db = lake["raw"], lake["db"]
    lost = _partition(raw, "cms/cclf", "claims_header", "2023-02")
    checksum = _checksum(lost)
    before = _families_from(db, "int_cclf__claim_families", checksum)
    assert before > 0
    lost.unlink()  # a mistaken delete (or a layout change deployed before migrating)

    run = _run(lake, [CCLF_CLAIMS], None)
    assert run.returncode != 0
    assert _status(run, "raw_keeps_every_built_delivery_stg_cclf__deliveries") == "fail"
    assert _status(run, "model.claims_lakehouse.int_cclf__claim_families") == "skipped"
    assert _families_from(db, "int_cclf__claim_families", checksum) == before


# --- 5. the transform runner ---------------------------------------------------------------


def test_runner_keeps_feed_blocked_across_file_types(
    lake: dict, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from claims_etl.events import FileIngested
    from claims_etl.transform.config import load_transform_config
    from claims_etl.transform.runner import Outcome, run_transform
    from claims_etl.transform.sinks import LocalRunSink
    from claims_etl.transform.state import LocalStateStore

    for var in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")  # the profile's S3 secret; unused
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    raw, store = lake["raw"], lake["raw_store"]
    state = LocalStateStore(lake["db"])
    raw_vars = {"cclf_raw_root": (raw / "cms" / "cclf").as_posix(),
                "ma_payer_raw_root": (raw / "ma_payer" / "ma").as_posix()}  # fmt: skip

    def deliver(file_type: str, resend: bool = False) -> tuple[Path, FileIngested]:
        src = _partition(store, "cms/cclf", file_type, "2023-03")
        if resend:  # a corrected re-send: a new delivery next to the current one
            dst = write_resend(pq.read_table(src), _month_dir(raw, "cms/cclf", file_type,
                                                              "2023-03"), file_type)  # fmt: skip
        else:
            dst = raw / src.relative_to(store)
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(src, dst)
        event = FileIngested("cms/cclf/", file_type, "2023-03", _checksum(dst), dst.as_uri(),
                             dst.name, "test-batch")  # fmt: skip
        return dst, event

    def run(event: FileIngested, name: str):
        return run_transform([event], config=load_transform_config(), state=state,
                             sink=LocalRunSink(tmp_path / "runs", name), extra_vars=raw_vars,
                             run_id=name, work_dir=tmp_path)  # fmt: skip

    # CCLF5 for 2023-03 arrives with a duplicated (claim_id, line_number) key: STOP.
    lines, lines_event = deliver("part_b_physician_lines")
    table = pq.read_table(lines)
    for column in ("CUR_CLM_UNIQ_ID", "CLM_LINE_NUM", "BENE_MBI_ID"):
        _edit(lines, column, [1], table[column][0].as_py())
    report = run(lines_event, "r1")
    assert report.outcome is Outcome.STOPPED and report.published
    assert [s["name"] for s in report.stops] == [
        "unique_combination_of_columns_stg_cclf__part_b_physician_lines_claim_id__line_number"
    ]
    assert "model.claims_lakehouse.int_cclf__claim_families" in report.skipped
    assert report.stops[0]["feeds"] == ["cclf"]  # from dbt's real manifest (issue keys)
    assert _families_from(lake["db"], "int_cclf__claim_families", _checksum(lines)) == 0

    # Then a clean CCLF1, which also feeds the claim families. Selecting only
    # source:cclf.claims_header+ would skip the CCLF5 stop and build families with the
    # bad lines; the runner builds the whole feed, so the stop runs and still blocks.
    header, header_event = deliver("claims_header")
    report = run(header_event, "r2")
    assert report.outcome is Outcome.STOPPED
    assert _families_from(lake["db"], "int_cclf__claim_families", _checksum(header)) == 0
    saved = json.loads((tmp_path / "runs" / "r2" / "report.json").read_text(encoding="utf-8"))
    assert saved["triggers"][0]["file_type"] == "claims_header"

    # Corrected re-send of the CCLF5 file: passes and builds both files into families.
    fixed, fixed_event = deliver("part_b_physician_lines", resend=True)
    report = run(fixed_event, "r3")
    assert report.outcome is Outcome.PASSED, report.to_dict()
    assert _families_from(lake["db"], "int_cclf__claim_families", _checksum(header)) > 0
    assert _families_from(lake["db"], "int_cclf__claim_families", _checksum(fixed)) > 0


# --- 6. marts: payer isolation and the payer registry ---------------------------------------


def test_stop_in_one_payer_still_refreshes_the_other_payers_marts(
    lake: dict, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One batch: a clean CMS claims file and an MA payer eligibility file with an empty MBI
    (a STOP). The runner builds one pass per feed, so CMS's new month reaches the marts
    while MA payer is held back; the marts are not reported as skipped."""
    from claims_etl.events import FileIngested
    from claims_etl.transform.config import load_transform_config
    from claims_etl.transform.runner import Outcome, run_transform
    from claims_etl.transform.sinks import LocalRunSink
    from claims_etl.transform.state import LocalStateStore

    for var in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "CLAIMS_MARTS_ROOT"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    raw, store = lake["raw"], lake["raw_store"]

    def deliver(feed: str, file_type: str) -> tuple[Path, FileIngested]:
        src = _partition(store, feed, file_type, "2023-03")
        dst = raw / src.relative_to(store)
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, dst)
        event = FileIngested(f"{feed}/", file_type, "2023-03", _checksum(dst), dst.as_uri(),
                             dst.name, "test-batch")  # fmt: skip
        return dst, event

    header, header_event = deliver("cms/cclf", "claims_header")
    elig, elig_event = deliver("ma_payer/ma", "member_eligibility")
    _edit(elig, "MBI", [0], "")  # STOP: not_null beneficiary_mbi

    report = run_transform(
        [elig_event, header_event], config=load_transform_config(),
        state=LocalStateStore(lake["db"]), sink=LocalRunSink(tmp_path / "runs", "r"),
        extra_vars={"cclf_raw_root": (raw / "cms" / "cclf").as_posix(),
                    "ma_payer_raw_root": (raw / "ma_payer" / "ma").as_posix()},
        run_id="r", work_dir=tmp_path,
    )  # fmt: skip

    assert report.outcome is Outcome.STOPPED
    assert [s["feeds"] for s in report.stops] == [["ma_payer"]]
    assert "model.claims_lakehouse.int_ma_payer__member_versions" in report.skipped
    assert not [n for n in report.skipped if ".fct_" in n or ".dim_" in n or ".agg_" in n]
    assert report.marts_root == str(marts_dir(lake["db"]))  # next to the state, not the temp copy
    cms_march = _query(lake["db"], "select count(*) from marts.fct_claims where payer_id = "
                       "'cms_ffs' and latest_delivery_month = '2023-03-01'")  # fmt: skip
    assert cms_march[0][0] > 0  # CMS's new month is in gold despite the MA payer stop
    assert _families_from(lake["db"], "int_ma_payer__member_versions", _checksum(elig)) == 0


PAYERS = {
    "cms_ffs": {"payer_id": "cms_ffs", "payer_name": "CMS Medicare Fee-for-Service",
                "line_of_business": "Medicare FFS", "dbt_source": "cclf",
                "onboarded_month": "2023-01", "retired_month": None},
    "ma_payer": {"payer_id": "ma_payer", "payer_name": "a fictional Medicare Advantage payer",
                  "line_of_business": "Medicare Advantage", "dbt_source": "ma_payer",
                  "onboarded_month": "2023-01", "retired_month": None},
}  # fmt: skip


def test_payer_registry_retires_offboards_and_onboards_without_sql_changes(lake: dict) -> None:
    def build(payers: list[dict]) -> DbtRun:
        return _dbt(["build"], lake["raw"], lake["db"], lake["work"], check=False,
                    extra_vars={"payers": payers})  # fmt: skip

    def payer_rows(sql: str) -> dict:
        return dict(_query(lake["db"], sql))

    claims_sql = "select payer_id, count(*) from marts.fct_claims group by 1"
    before = payer_rows(claims_sql)
    assert set(before) == {"cms_ffs", "ma_payer"}

    # Retire MA payer as of 2023-01: its history stays, its members are no longer current,
    # and its 2023-02 files (after retirement) raise a WARN, not a stop.
    run = build([PAYERS["cms_ffs"], {**PAYERS["ma_payer"], "retired_month": "2023-01"}])
    assert run.returncode == 0, run.stdout  # warnings do not fail a build
    assert _status(run, "assert_no_deliveries_after_payer_retirement") == "warn"
    assert payer_rows("select payer_id, payer_status from marts.dim_payer") == {
        "cms_ffs": "active", "ma_payer": "retired"}  # fmt: skip
    assert payer_rows(claims_sql) == before
    assert _query(lake["db"], "select count(*) from marts.dim_member"
                              " where current_payer_id = 'ma_payer'") == [(0,)]  # fmt: skip

    # Offboard MA payer (removed from the registry): gone from every mart, history kept below.
    run = build([PAYERS["cms_ffs"]])
    assert run.returncode == 0, run.stdout
    assert payer_rows(claims_sql) == {"cms_ffs": before["cms_ffs"]}
    assert not [n for n in run.statuses if "int_ma_payer__conformed" in n]  # views disabled
    assert _query(lake["db"], "select count(*) > 0 from intermediate.int_ma_payer__claim_families")[
        0
    ] == (True,)

    # Onboard it again: back in every mart, same numbers.
    run = build([PAYERS["cms_ffs"], PAYERS["ma_payer"]])
    assert run.returncode == 0, run.stdout
    assert payer_rows(claims_sql) == before
