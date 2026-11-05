"""dbt end to end on generated CCLF data, fully offline (no S3, no AWS credentials).

Both root generators (CMS CCLF and Humana MA, same seed) write all 36 months plus their
answer keys into a temp folder; each CSV goes through the real ingestion code
(validate_file + write_parquet) into a local raw folder laid out per feed exactly like
S3 (<feed>/<file_type>/delivery_month=...); dbt then builds every model against it.
Checks:

1. Full refresh matches both generators' answer keys: CMS (members, changes, claim
   actions, net paid by service month and by load month) and Humana (members, changes,
   voids/replacements, net paid by service month, capitated encounters, totals).
2. Incremental: the same 36 months added in uneven batches equals the full refresh.
3. Re-sends: replacing two old months (a dropped beneficiary, a dropped claim, a
   changed amount) incrementally equals a full refresh of the same raw data.

Slow (~2 min, several dbt runs). Skip with: pytest -m "not dbt".
"""

from __future__ import annotations

import csv
import hashlib
import json
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from tests.dbt_harness import (
    HAS_DBT,
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


# --- fixtures --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def generated(tmp_path_factory: pytest.TempPathFactory) -> dict:
    root = tmp_path_factory.mktemp("cclf")
    csv_root, key_dir = _generate(root)
    raw_store = root / "raw_store"
    months = _ingest_locally(csv_root, raw_store)
    assert len(months) == 36
    return {"root": root, "key_dir": key_dir, "raw_store": raw_store, "months": months}


@pytest.fixture(scope="module")
def full_build(generated: dict) -> Path:
    """All 36 months, built once with --full-refresh, models + tests."""
    root = generated["root"]
    raw = root / "raw_full"
    _publish(generated["raw_store"], raw, generated["months"])
    db = root / "full.duckdb"
    _dbt(["build", "--full-refresh"], raw, db, root / "work_full")
    return db


def _read_key(generated: dict, name: str) -> list[dict]:
    with (generated["key_dir"] / name).open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# --- 1. full refresh vs the answer key ---------------------------------------------------

# final_members.csv column -> int_cclf__beneficiary_versions column (age is not tracked).
MEMBER_COLUMNS = {
    "BENE_MBI_ID": "beneficiary_mbi",
    "BENE_HIC_NUM": "beneficiary_hicn",
    "BENE_FIPS_STATE_CD": "fips_state_code",
    "BENE_FIPS_CNTY_CD": "fips_county_code",
    "BENE_ZIP_CD": "zip9_code",
    "BENE_DOB": "birth_date",
    "BENE_SEX_CD": "sex_code",
    "BENE_RACE_CD": "race_code",
    "BENE_MDCR_STUS_CD": "medicare_status_code",
    "BENE_DUAL_STUS_CD": "dual_status_code",
    "BENE_DEATH_DT": "death_date",
    "BENE_RNG_BGN_DT": "coverage_start_date",
    "BENE_RNG_END_DT": "coverage_end_date",
    "BENE_1ST_NAME": "first_name",
    "BENE_MIDL_NAME": "middle_name",
    "BENE_LAST_NAME": "last_name",
    "BENE_ORGNL_ENTLMT_RSN_CD": "original_entitlement_reason_code",
    "BENE_ENTLMT_BUYIN_IND": "entitlement_buyin_indicator",
    "BENE_PART_A_ENRLMT_BGN_DT": "part_a_enrollment_start_date",
    "BENE_PART_B_ENRLMT_BGN_DT": "part_b_enrollment_start_date",
    "BENE_LINE_1_ADR": "address_line_1",
    "GEO_ZIP_PLC_NAME": "city",
    "GEO_USPS_STATE_CD": "usps_state_code",
    "GEO_ZIP5_CD": "zip5_code",
}


def test_full_refresh_current_members_match_answer_key(generated, full_build) -> None:
    expected = {row["BENE_MBI_ID"]: row for row in _read_key(generated, "final_members.csv")}
    select = ", ".join(f"coalesce(cast({c} as varchar), '')" for c in MEMBER_COLUMNS.values())
    rows = _query(
        full_build,
        f"""select {select}, strftime(first_month, '%Y-%m'), strftime(last_delivered_month, '%Y-%m')
            from intermediate.int_cclf__beneficiary_versions
            join (select beneficiary_mbi, min(valid_from) as first_month
                  from intermediate.int_cclf__beneficiary_versions group by 1)
              using (beneficiary_mbi)
            where is_current""",
    )
    assert len(rows) == len(expected) == 426
    for row in rows:
        want = expected[row[0]]
        got = dict(zip(MEMBER_COLUMNS, row[: len(MEMBER_COLUMNS)], strict=True))
        assert got == {k: want[k] for k in MEMBER_COLUMNS}, row[0]
        assert row[-2:] == (want["FIRST_DELIVERY_MONTH"], want["LAST_DELIVERY_MONTH"]), row[0]


# member_changes.csv reason -> a column that must be in changed_columns for that version.
CHANGE_COLUMN = {
    "ADDRESS": "address_line_1",
    "DUAL": "dual_status_code",
    "PARTB": "part_b_enrollment_start_date",
    "DEATH": "death_date",
    "FFS_END": "coverage_end_date",  # switched to Medicare Advantage
    "FFS_RETURN": "coverage_start_date",  # back from Medicare Advantage
}


def test_full_refresh_versions_match_every_member_change(generated, full_build) -> None:
    changes = _read_key(generated, "member_changes.csv")
    versions = {
        (mbi, month): (change_type, set(changed))
        for mbi, month, change_type, changed in _query(
            full_build,
            """select beneficiary_mbi, strftime(valid_from, '%Y-%m'), change_type, changed_columns
               from intermediate.int_cclf__beneficiary_versions""",
        )
    }
    assert len(versions) == len(changes) == 931  # every delivered row is a real change
    for change in changes:
        change_type, changed = versions[(change["BENE_MBI_ID"], change["delivery_month"])]
        if change["change_type"] == "NEW":
            assert change_type == "NEW" and not changed
        else:
            assert change_type == "UPDATE"
            assert CHANGE_COLUMN[change["change_type"]] in changed, change


def test_full_refresh_claim_actions_match_answer_key(generated, full_build) -> None:
    expected = {
        (row["part"], row["CLM_CNTL_NUM"]): row for row in _read_key(generated, "claim_actions.csv")
    }
    rows = _query(
        full_build,
        """select part, claim_control_number, action_type, beneficiary_mbi, original_claim_id,
                  strftime(original_delivery_month, '%Y-%m'), original_amount::varchar,
                  action_claim_id, strftime(action_delivery_month, '%Y-%m'),
                  action_amount::varchar, net_paid_amount::varchar
           from intermediate.int_cclf__claim_families where action_type is not null""",
    )
    assert len(rows) == len(expected) == 104
    fields = ["action_type", "BENE_MBI_ID", "original_clm_id", "original_load_month",
              "original_amount", "action_clm_id", "action_load_month", "action_amount",
              "final_amount"]  # fmt: skip
    for part, control_number, *values in rows:
        want = expected[(part, control_number)]
        assert values[0] == want["action_type"]
        assert values[1:] == [want[f] for f in fields[1:]], (part, control_number)


def test_full_refresh_net_paid_by_service_month_matches_answer_key(generated, full_build) -> None:
    rows = _query(
        full_build,
        """select strftime(service_month, '%Y-%m'),
                  sum(net_paid_amount) filter (where part = 'A'),
                  sum(net_paid_amount) filter (where part = 'B')
           from intermediate.int_cclf__claim_families group by 1""",
    )
    got = {month: (a or Decimal("0.00"), b or Decimal("0.00")) for month, a, b in rows}
    expected = {
        r["service_month"]: (Decimal(r["part_a_net_paid"]), Decimal(r["part_b_net_paid"]))
        for r in _read_key(generated, "net_paid_by_service_month.csv")
    }
    assert got == expected


def test_full_refresh_net_change_by_load_month_matches_answer_key(generated, full_build) -> None:
    # Originals book their amount in their own delivery month; an adjustment books the
    # delta and a cancellation reverses the original in the action's delivery month.
    rows = _query(
        full_build,
        """with bookings as (
               select part, original_delivery_month as month, original_amount as amount
               from intermediate.int_cclf__claim_families where has_original
               union all
               select part, action_delivery_month,
                      case action_type when 'ADJUST' then action_amount - original_amount
                                       else -original_amount end
               from intermediate.int_cclf__claim_families where action_type is not null
           )
           select strftime(month, '%Y-%m'), sum(amount) filter (where part = 'A'),
                  sum(amount) filter (where part = 'B')
           from bookings group by 1""",
    )
    got = {month: (a or Decimal("0.00"), b or Decimal("0.00")) for month, a, b in rows}
    expected = {
        r["load_month"]: (Decimal(r["part_a_net_change"]), Decimal(r["part_b_net_change"]))
        for r in _read_key(generated, "net_paid_by_load_month.csv")
    }
    assert got == expected


def test_full_refresh_totals_match_summary(generated, full_build) -> None:
    summary = json.loads((generated["key_dir"] / "summary.json").read_text(encoding="utf-8"))
    [(a_total, b_total, a_originals, b_originals, cancels, adjusts)] = _query(
        full_build,
        """select sum(net_paid_amount) filter (where part = 'A')::varchar,
                  sum(net_paid_amount) filter (where part = 'B')::varchar,
                  count(*) filter (where part = 'A'), count(*) filter (where part = 'B'),
                  count(*) filter (where action_type = 'CANCEL'),
                  count(*) filter (where action_type = 'ADJUST')
           from intermediate.int_cclf__claim_families""",
    )
    assert (a_total, b_total) == (
        summary["part_a_total_net_paid"],
        summary["part_b_total_net_paid"],
    )
    assert (a_originals, b_originals) == (
        summary["part_a_original_claims"],
        summary["part_b_original_claims"],
    )
    assert (cancels, adjusts) == (summary["cancellations"], summary["adjustments"])


# --- 1b. Humana full refresh vs answer_key/humana ----------------------------------------

# final_members.csv column -> int_humana__member_versions column.
HUMANA_MEMBER_COLUMNS = {
    "MEMBER_ID": "member_id",
    "MBI": "beneficiary_mbi",
    "FIRST_NAME": "first_name",
    "MIDDLE_INITIAL": "middle_initial",
    "LAST_NAME": "last_name",
    "DOB": "birth_date",
    "GENDER": "gender_code",
    "ADDRESS_LINE_1": "address_line_1",
    "CITY": "city",
    "STATE": "usps_state_code",
    "ZIP": "zip5_code",
    "COUNTY_FIPS": "county_fips_code",
    "CONTRACT_ID": "contract_id",
    "PBP_ID": "plan_benefit_package_id",
    "PLAN_NAME": "plan_name",
    "PRODUCT_TYPE": "product_type",
    "ENROLLMENT_START_DT": "enrollment_start_date",
    "ENROLLMENT_END_DT": "enrollment_end_date",
    "PCP_NPI": "pcp_npi",
    "PCP_TIN": "pcp_tin",
    "PCP_ATTRIBUTION_START_DT": "pcp_attribution_start_date",
    "DUAL_FLAG": "dual_flag",
    "LIS_FLAG": "low_income_subsidy_flag",
    "ESRD_FLAG": "esrd_flag",
    "HOSPICE_FLAG": "hospice_flag",
    "DEATH_DT": "death_date",
    "FILE_AS_OF_DT": "last_file_as_of_date",
}


def test_humana_current_members_match_answer_key(generated, full_build) -> None:
    expected = {r["MEMBER_ID"]: r for r in _read_key(generated, "humana/final_members.csv")}
    select = ", ".join(
        f"coalesce(cast({c} as varchar), '')" for c in HUMANA_MEMBER_COLUMNS.values()
    )
    rows = _query(
        full_build,
        f"""select {select}, strftime(first_month, '%Y-%m'), strftime(last_delivered_month, '%Y-%m')
            from intermediate.int_humana__member_versions
            join (select member_id, min(valid_from) as first_month
                  from intermediate.int_humana__member_versions group by 1) using (member_id)
            where is_current""",
    )
    assert len(rows) == len(expected) > 0
    for row in rows:
        want = expected[row[0]]
        got = dict(zip(HUMANA_MEMBER_COLUMNS, row[: len(HUMANA_MEMBER_COLUMNS)], strict=True))
        assert got == {k: want[k] for k in HUMANA_MEMBER_COLUMNS}, row[0]
        assert row[-2:] == (want["FIRST_DELIVERY_MONTH"], want["LAST_DELIVERY_MONTH"]), row[0]


# member_changes.csv reason -> (derived change_type, columns of which one must change).
HUMANA_CHANGE = {
    "NEW": ("NEW", set()),
    "PCP": ("UPDATE", {"pcp_npi"}),
    "ADDRESS": ("UPDATE", {"address_line_1"}),
    "DUAL_LIS": ("UPDATE", {"dual_flag", "low_income_subsidy_flag"}),
    "DISENROLL": ("TERM", {"enrollment_end_date"}),
    "DEATH": ("TERM", {"death_date"}),
}


def test_humana_versions_match_every_member_change(generated, full_build) -> None:
    changes = _read_key(generated, "humana/member_changes.csv")
    versions = {
        (member, month): (change_type, set(changed))
        for member, month, change_type, changed in _query(
            full_build,
            """select member_id, strftime(valid_from, '%Y-%m'), change_type, changed_columns
               from intermediate.int_humana__member_versions""",
        )
    }
    assert len(versions) == len(changes)  # every delivered row is a real change
    reasons = {c["change_type"] for c in changes}
    assert {"DISENROLL", "DEATH"} <= reasons  # terminations are really exercised
    for change in changes:
        change_type, changed = versions[(change["MEMBER_ID"], change["delivery_month"])]
        want_type, columns = HUMANA_CHANGE[change["change_type"]]
        assert change_type == want_type, change
        assert (not changed) if want_type == "NEW" else bool(columns & changed), change


def test_humana_claim_actions_match_answer_key(generated, full_build) -> None:
    expected = {r["original_claim_id"]: r for r in _read_key(generated, "humana/claim_actions.csv")}
    rows = _query(
        full_build,
        """select claim_family_key, action_type, member_id, claim_form_type_code,
                  strftime(original_delivery_month, '%Y-%m'), original_paid_amount::varchar,
                  strftime(action_delivery_month, '%Y-%m'), coalesce(replacement_claim_id, ''),
                  coalesce(replacement_paid_amount::varchar, ''), net_paid_amount::varchar
           from intermediate.int_humana__claim_families where action_type is not null""",
    )
    assert len(rows) == len(expected) > 0
    fields = ["action_type", "MEMBER_ID", "CLAIM_FORM_TYPE", "original_processed_month",
              "original_paid", "action_processed_month", "replacement_claim_id",
              "replacement_paid", "final_paid"]  # fmt: skip
    for key, *values in rows:
        assert values == [expected[key][f] for f in fields], key


def test_humana_net_paid_by_service_month_matches_answer_key(generated, full_build) -> None:
    rows = _query(
        full_build,
        """select strftime(service_month, '%Y-%m'),
                  sum(net_paid_amount) filter (where claim_form_type_code = 'I'),
                  sum(net_paid_amount) filter (where claim_form_type_code = 'P')
           from intermediate.int_humana__claim_families group by 1""",
    )
    got = {month: (i or Decimal("0.00"), p or Decimal("0.00")) for month, i, p in rows}
    expected = {
        r["service_month"]: (
            Decimal(r["institutional_net_paid"]),
            Decimal(r["professional_net_paid"]),
        )
        for r in _read_key(generated, "humana/net_paid_by_service_month.csv")
    }
    assert got == expected


def test_humana_capitated_encounters_match_answer_key(generated, full_build) -> None:
    rows = _query(
        full_build,
        """select strftime(service_month, '%Y-%m'), count(*), sum(original_line_count),
                  sum(capitated_allowed_amount)::varchar
           from intermediate.int_humana__claim_families where is_capitated group by 1""",
    )
    got = {month: (claims, lines, allowed) for month, claims, lines, allowed in rows}
    expected = {
        r["service_month"]: (
            int(r["capitated_claims"]),
            int(r["capitated_lines"]),
            r["capitated_allowed_amt"],
        )
        for r in _read_key(generated, "humana/capitated_encounters_by_month.csv")
    }
    assert got == expected


def test_humana_totals_match_summary(generated, full_build) -> None:
    summary = json.loads(
        (generated["key_dir"] / "humana" / "summary.json").read_text(encoding="utf-8")
    )
    [row] = _query(
        full_build,
        """select count(*) filter (where has_original),
                  count(*) filter (where has_original and claim_form_type_code = 'I'),
                  count(*) filter (where has_original and claim_form_type_code = 'P'),
                  count(*) filter (where action_type = 'REPLACE'),
                  count(*) filter (where action_type = 'VOID'),
                  count(*) filter (where original_status_code = 'D'),
                  count(*) filter (where is_capitated),
                  sum(net_paid_amount) filter (where claim_form_type_code = 'I')::varchar,
                  sum(net_paid_amount) filter (where claim_form_type_code = 'P')::varchar
           from intermediate.int_humana__claim_families""",
    )
    keys = ["original_claims", "institutional_original_claims", "professional_original_claims",
            "replacements", "voids", "denied", "capitated_claims",
            "institutional_total_net_paid", "professional_total_net_paid"]  # fmt: skip
    assert dict(zip(keys, row, strict=True)) == {k: summary[k] for k in keys}
    [(members,)] = _query(
        full_build, "select count(*) from intermediate.int_humana__member_versions where is_current"
    )
    assert members == summary["members"]


# --- 2 + 3. incremental runs equal a full refresh ---------------------------------------


def _resend(raw: Path, file_type: str, month: str, edit, feed: str = "cms/cclf") -> None:
    """Replace one delivery with a corrected re-send (new rows, new checksum)."""
    path = raw / feed / file_type / f"delivery_month={month}" / "part-0.parquet"
    table = edit(pq.read_table(path))
    checksum = hashlib.sha256(f"resent {file_type} {month}".encode()).hexdigest()
    column = table.schema.get_field_index("_source_checksum")
    checksums = pa.array([checksum] * len(table), pa.string())
    table = table.set_column(column, "_source_checksum", checksums)
    pq.write_table(table, path)


def _drop_rows(table, column: str, values: set[str]):
    return table.filter(pc.invert(pc.is_in(table[column], value_set=pa.array(sorted(values)))))


def test_incremental_batches_then_resends_equal_full_refresh(generated, full_build) -> None:
    root, months = generated["root"], generated["months"]
    raw, db, work = root / "raw_incremental", root / "incremental.duckdb", root / "work_incr"

    # Uneven batches: an initial load, single months, then many months at once.
    batches = [months[:3], *[[m] for m in months[3:12]], months[12:]]
    for batch in batches:
        _publish(generated["raw_store"], raw, batch)
        _dbt(["run"], raw, db, work)
    _assert_same_intermediate(full_build, db)

    # Corrected re-sends of old months:
    # - CCLF8 2023-02 without one NEW beneficiary that has no later rows (must vanish)
    #   and without one UPDATE (that beneficiary's history must be re-derived);
    # - CCLF1 2023-04 without one claim version and with one amount changed.
    gone, history_changed = _query(
        full_build,
        """with v as (from intermediate.int_cclf__beneficiary_versions)
           select
             (select min(beneficiary_mbi) from v where valid_from = '2023-02-01'
                and change_type = 'NEW' and is_current),
             (select min(beneficiary_mbi) from v where valid_from = '2023-02-01'
                and change_type = 'UPDATE')""",
    )[0]
    _resend(raw, "beneficiary_demographics", "2023-02",
            lambda t: _drop_rows(t, "BENE_MBI_ID", {gone, history_changed}))  # fmt: skip

    def drop_and_reprice(table):
        claim_ids = table["CUR_CLM_UNIQ_ID"].to_pylist()
        table = _drop_rows(table, "CUR_CLM_UNIQ_ID", {claim_ids[0]})
        amounts = table["CLM_PMT_AMT"].to_pylist()
        first_original = table["CLM_ADJSMT_TYPE_CD"].to_pylist().index("0")
        amounts[first_original] = "1.23"  # an original, so it shows up as original_amount
        return table.set_column(table.schema.get_field_index("CLM_PMT_AMT"), "CLM_PMT_AMT",
                                pa.array(amounts, pa.string()))  # fmt: skip

    _resend(raw, "claims_header", "2023-04", drop_and_reprice)

    # Humana: a replacement re-sent away (its family must fall back to VOIDED) and one
    # member update removed from an eligibility re-send (history re-derived).
    replaced_family, replacement_id, action_month = _query(
        full_build,
        """select claim_family_key, replacement_claim_id, strftime(action_delivery_month, '%Y-%m')
           from intermediate.int_humana__claim_families
           where action_type = 'REPLACE' order by claim_family_key limit 1""",
    )[0]
    _resend(raw, "medical_claim_lines", action_month,
            lambda t: _drop_rows(t, "CLAIM_ID", {replacement_id}), feed="humana/ma")  # fmt: skip
    [(member_updated,)] = _query(
        full_build,
        """select min(member_id) from intermediate.int_humana__member_versions
           where valid_from = '2023-02-01' and change_type = 'UPDATE'""",
    )
    _resend(raw, "member_eligibility", "2023-02",
            lambda t: _drop_rows(t, "MEMBER_ID", {member_updated}), feed="humana/ma")  # fmt: skip
    _dbt(["run"], raw, db, work)

    rebuilt = root / "resend_full.duckdb"
    _dbt(["run", "--full-refresh"], raw, rebuilt, root / "work_resend_full")
    _assert_same_intermediate(rebuilt, db)

    # The scenario really exercised both paths.
    sql = (
        "select count(*) from intermediate.int_cclf__beneficiary_versions"
        " where beneficiary_mbi = '{}'"
    )
    assert _query(db, sql.format(gone)) == [(0,)]
    assert _query(db, sql.format(history_changed)) < _query(full_build, sql.format(history_changed))
    assert _query(db, "select count(*) from intermediate.int_cclf__claim_families where "
                      "original_amount = 1.23") == [(1,)]  # fmt: skip
    status = (
        "select family_status from intermediate.int_humana__claim_families"
        " where claim_family_key = '{}'"
    )
    assert _query(full_build, status.format(replaced_family)) == [("REPLACED",)]
    assert _query(db, status.format(replaced_family)) == [("VOIDED",)]
    versions = (
        "select count(*) from intermediate.int_humana__member_versions where member_id = '{}'"
    )
    assert _query(db, versions.format(member_updated)) < _query(
        full_build, versions.format(member_updated)
    )
