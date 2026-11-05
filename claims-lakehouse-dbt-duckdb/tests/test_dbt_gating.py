"""STOP vs WARN gating in `dbt build` (policy in dbt/macros/test_scoping.sql), offline.

A clean baseline (delivery months 2023-01 and 2023-02, both feeds) is built once; each
scenario copies it, corrupts one raw partition the way a bad delivery would, and runs
dbt the way the transform runner will: `build --select source:<feed>.<file_type>+`
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

Slow (~1 min, several dbt runs). Skip with: pytest -m "not dbt".
"""

from __future__ import annotations

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
)

pytestmark = pytest.mark.dbt

if not HAS_DBT:
    pytest.skip("needs the root CCLF/Humana generators and the dbt extra", allow_module_level=True)

BASELINE_MONTHS = ["2023-01", "2023-02"]
CCLF_CLAIMS = "source:cclf.claims_header+"
HUMANA_CLAIMS = "source:humana.medical_claim_lines+"


# --- helpers ---------------------------------------------------------------------------


def _partition(raw: Path, feed: str, file_type: str, month: str) -> Path:
    return raw / feed / file_type / f"delivery_month={month}" / "part-0.parquet"


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
    for part in raw_store.glob("humana/ma/medical_claim_lines/*/part-0.parquet"):
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

    # Corrected re-send of 2023-03: passes, and the incremental state catches up exactly.
    shutil.copy(_partition(store, "cms/cclf", "claims_header", "2023-03"), bad)
    run = _run(lake, [CCLF_CLAIMS], ["2023-03"], check=True)
    assert _status(run, "model.claims_lakehouse.int_cclf__claim_families") == "success"
    assert _families_from(db, "int_cclf__claim_families", _checksum(bad)) > 0
    _run(lake, [], None, check=True)  # the other file types' new months
    rebuilt = tmp_path / "rebuilt.duckdb"
    _dbt(["build", "--full-refresh"], raw, rebuilt, tmp_path / "work_rebuilt")
    _assert_same_intermediate(rebuilt, db)


# --- 3. a WARN --------------------------------------------------------------------------


def test_warn_builds_intermediate_and_is_scoped_to_the_batch(lake: dict) -> None:
    raw, db, store = lake["raw"], lake["db"], lake["raw_store"]
    _publish(store, raw, ["2023-03"])
    odd = _partition(raw, "humana/ma", "medical_claim_lines", "2023-03")
    _edit(odd, "IN_NETWORK_FLAG", [0], "X")
    _edit(odd, "PAID_AMT", _plain_paid_lines(store, odd, 1), _paid_above_allowed)

    run = _run(lake, [HUMANA_CLAIMS], ["2023-03"])
    assert run.returncode == 0, run.output
    assert _status(run, "accepted_values_stg_humana__medical_claim_lines_in_network_flag") == "warn"
    assert _status(run, "assert_humana_amounts_consistent") == "warn"
    assert _status(run, "assert_humana_amounts_mostly_consistent") == "pass"
    assert run.with_status("fail") == run.with_status("error") == []
    assert _status(run, "model.claims_lakehouse.int_humana__claim_families") == "success"
    assert _families_from(db, "int_humana__claim_families", _checksum(odd)) > 0

    # Next month: the accepted warnings are not raised again ...
    _publish(store, raw, ["2023-04"])
    run = _run(lake, [HUMANA_CLAIMS], ["2023-04"], check=True)
    assert run.with_status("warn") == []
    # ... but an unscoped (local, full-history) build still reports them.
    run = _run(lake, [HUMANA_CLAIMS], None, check=True)
    assert _status(run, "assert_humana_amounts_consistent") == "warn"


# --- 4. many WARN rows become a STOP ------------------------------------------------------


def test_many_broken_amount_lines_stop_the_build(lake: dict) -> None:
    raw, db, store = lake["raw"], lake["db"], lake["raw_store"]
    _publish(store, raw, ["2023-03"])
    broken = _partition(raw, "humana/ma", "medical_claim_lines", "2023-03")
    _edit(broken, "PAID_AMT", _plain_paid_lines(store, broken, 5), _paid_above_allowed)

    run = _run(lake, [HUMANA_CLAIMS], ["2023-03"])
    assert run.returncode != 0
    assert _status(run, "assert_humana_amounts_mostly_consistent") == "fail"
    assert _status(run, "model.claims_lakehouse.int_humana__claim_families") == "skipped"
    assert _families_from(db, "int_humana__claim_families", _checksum(broken)) == 0
