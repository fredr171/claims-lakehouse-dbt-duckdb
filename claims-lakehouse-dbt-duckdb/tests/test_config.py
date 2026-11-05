from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from claims_etl.ingestion.config import ConfigError, load_config
from tests.conftest import HUM_LANDING_KEY, LANDING_KEY

# file_type -> (CCLF number, column count, first column, last column, key columns),
# all taken from the delivered headers (identical in all 36 generated months).
REAL_FILE_TYPES = {
    "claims_header": (
        1,
        30,
        "CUR_CLM_UNIQ_ID",
        "CLM_ORG_CNTL_NUM",
        ("CUR_CLM_UNIQ_ID", "BENE_MBI_ID"),
    ),
    "claims_diagnosis": (
        4,
        12,
        "CUR_CLM_UNIQ_ID",
        "DGNS_PRCDR_ICD_IND",
        ("CUR_CLM_UNIQ_ID", "BENE_MBI_ID", "CLM_VAL_SQNC_NUM"),
    ),
    "part_b_physician_lines": (
        5,
        33,
        "CUR_CLM_UNIQ_ID",
        "DGNS_PRCDR_ICD_IND",
        ("CUR_CLM_UNIQ_ID", "CLM_LINE_NUM", "BENE_MBI_ID"),
    ),
    "beneficiary_demographics": (
        8,
        26,
        "BENE_MBI_ID",
        "RECORD_CHANGE_TYPE",
        ("BENE_MBI_ID",),
    ),
}


# Humana MA feed: file_type -> (file name prefix, column count, first, last, key columns).
HUMANA_FILE_TYPES = {
    "member_eligibility": (
        "HUM_ELIG",
        28,
        "MEMBER_ID",
        "RECORD_CHANGE_TYPE",
        ("MEMBER_ID",),
    ),
    "medical_claim_lines": (
        "HUM_MEDCLM",
        56,
        "CLAIM_ID",
        "PAID_DT",
        ("CLAIM_ID", "CLAIM_LINE_NUM", "CLAIM_FREQ_CD", "MEMBER_ID"),
    ),
}


def test_repo_config_has_exactly_the_cclf_and_humana_file_types() -> None:
    config = load_config()  # the real config/ folder
    assert sorted(config.file_types) == sorted([*REAL_FILE_TYPES, *HUMANA_FILE_TYPES])
    assert config.landing_prefixes == ("cms/cclf/", "humana/ma/")
    assert {n for n, ft in config.file_types.items() if ft.landing_prefix == "humana/ma/"} == set(
        HUMANA_FILE_TYPES
    )
    assert config.landing_bucket == "fredr-claims-landing-2026"
    assert config.region == "us-east-2"


@pytest.mark.parametrize("file_type", sorted(HUMANA_FILE_TYPES))
def test_repo_humana_config_matches_delivered_header(file_type: str) -> None:
    _, width, first, last, keys = HUMANA_FILE_TYPES[file_type]
    ft = load_config().file_types[file_type]
    assert len(ft.expected_columns) == width
    assert (ft.expected_columns[0], ft.expected_columns[-1]) == (first, last)
    assert ft.key_columns == keys


@pytest.mark.parametrize("file_type", sorted(HUMANA_FILE_TYPES))
def test_humana_name_resolves_to_delivery_month_partition(file_type: str) -> None:
    ft = load_config().file_types[file_type]
    groups = ft.match_file_name(f"{HUMANA_FILE_TYPES[file_type][0]}_202512.csv")
    assert groups == {"year": "2025", "month": "12"}
    assert ft.raw_key(groups) == f"humana/ma/{file_type}/delivery_month=2025-12/part-0.parquet"


@pytest.mark.parametrize("file_type", sorted(REAL_FILE_TYPES))
def test_repo_config_matches_delivered_header(file_type: str) -> None:
    _, width, first, last, keys = REAL_FILE_TYPES[file_type]
    ft = load_config().file_types[file_type]
    assert len(ft.expected_columns) == width
    assert (ft.expected_columns[0], ft.expected_columns[-1]) == (first, last)
    assert ft.key_columns == keys


@pytest.mark.parametrize("file_type", sorted(REAL_FILE_TYPES))
def test_real_name_resolves_to_delivery_month_partition(file_type: str) -> None:
    number = REAL_FILE_TYPES[file_type][0]
    ft = load_config().file_types[file_type]
    groups = ft.match_file_name(f"CCLF{number}_202512.csv")
    assert groups == {"year": "2025", "month": "12"}
    assert ft.raw_key(groups) == f"cms/cclf/{file_type}/delivery_month=2025-12/part-0.parquet"


def test_each_file_type_accepts_only_its_own_cclf_number() -> None:
    config = load_config()
    names = {ft: f"CCLF{number}_202301.csv" for ft, (number, *_) in REAL_FILE_TYPES.items()}
    names |= {ft: f"{prefix}_202301.csv" for ft, (prefix, *_) in HUMANA_FILE_TYPES.items()}
    for file_type, name in names.items():
        matching = [n for n, ft in config.file_types.items() if ft.match_file_name(name)]
        assert matching == [file_type]


def test_default_limit_applies_without_override() -> None:
    ft = load_config().file_types["claims_header"]
    assert ft.limits.max_file_bytes == 536_870_912


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        (LANDING_KEY, "claims_header"),
        (HUM_LANDING_KEY, "member_eligibility"),
        ("cms/cclf/x.csv", None),  # no file type folder
        ("humana/ma/x.csv", None),
        ("humana/member_eligibility/x.csv", None),  # not under a feed prefix
        ("cms/cclf/claims_header/sub/x.csv", None),  # nested too deep
        ("archive/cms/cclf/claims_header/x.csv", None),  # outside prefix
    ],
)
def test_file_type_for_key(config, key: str, expected: str | None) -> None:
    assert config.file_type_for_key(key) == expected


def test_file_types_for_key_are_only_the_keys_feed(config) -> None:
    assert config.file_types_for_key(LANDING_KEY) == {"claims_header"}
    assert config.file_types_for_key(HUM_LANDING_KEY) == {"member_eligibility"}
    # A CCLF folder under the Humana prefix is not a known file type there.
    assert "claims_header" not in config.file_types_for_key("humana/ma/claims_header/x.csv")
    assert config.file_types_for_key("elsewhere/x.csv") == set()


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        (LANDING_KEY, True),
        (HUM_LANDING_KEY, True),
        ("humana/other/member_eligibility/x.csv", False),
        ("cms/cclf/claims_header/x.zip", False),
        ("cms/cclf/claims_header/notes.json", False),
        ("quarantine/cms/cclf/claims_header/x.csv", False),
    ],
)
def test_is_landing_file(config, key: str, expected: bool) -> None:
    assert config.is_landing_file(key) is expected


def _edit_file_type(config_dir: Path, **changes) -> None:
    path = config_dir / "file_types" / "claims_header.yml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    data.update(changes)
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


def _edit_settings(config_dir: Path, **changes) -> None:
    path = config_dir / "ingestion.yml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    data.update(changes)
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


@pytest.mark.parametrize(
    ("prefixes", "message"),
    [
        ([], "non-empty list"),
        ("cms/cclf/", "non-empty list"),  # a string, not a list
        (["cms/cclf"], "ending in '/'"),
        (["cms/cclf/", "cms/cclf/"], "duplicates"),
        (["cms/", "cms/cclf/", "humana/ma/"], "overlap"),  # one inside another
        (["cms/cclf/", "humana/ma/", "quarantine/x/"], "overlap"),  # inside quarantine/
        (["cms/cclf/", "humana/ma/", "archive/"], "overlap"),
    ],
)
def test_invalid_landing_prefixes_are_rejected(config_dir: Path, prefixes, message: str) -> None:
    _edit_settings(config_dir, landing_prefixes=prefixes)
    with pytest.raises(ConfigError, match=message):
        load_config(config_dir)


def test_file_type_prefix_must_be_a_listed_feed(config_dir: Path) -> None:
    _edit_file_type(
        config_dir,
        landing_prefix="aetna/ma/",
        raw_key_template="aetna/ma/claims_header/delivery_month={year}-{month}/part-0.parquet",
    )
    with pytest.raises(ConfigError, match="not in landing_prefixes"):
        load_config(config_dir)


def test_raw_key_must_stay_in_the_file_types_folder(config_dir: Path) -> None:
    # IAM grants raw writes per feed prefix, so a raw key elsewhere would fail at upload.
    _edit_file_type(config_dir, raw_key_template="humana/ma/claims_header/{year}/part-0.parquet")
    with pytest.raises(ConfigError, match="must start with 'cms/cclf/claims_header/'"):
        load_config(config_dir)


def test_template_field_not_in_regex_is_rejected(config_dir: Path) -> None:
    _edit_file_type(config_dir, raw_key_template="x/sample={sample}/part-0.parquet")
    with pytest.raises(ConfigError, match="sample"):
        load_config(config_dir)


def test_key_column_must_be_an_expected_column(config_dir: Path) -> None:
    _edit_file_type(config_dir, key_columns=["NOT_A_COLUMN"])
    with pytest.raises(ConfigError, match="key_columns"):
        load_config(config_dir)


def test_columns_may_not_clash_with_metadata(config_dir: Path) -> None:
    _edit_file_type(config_dir, expected_columns=["CUR_CLM_UNIQ_ID", "_batch_id"])
    with pytest.raises(ConfigError, match="metadata"):
        load_config(config_dir)


def test_unknown_limit_is_rejected(config_dir: Path) -> None:
    _edit_file_type(config_dir, limits={"max_zip_bytes": 1})
    with pytest.raises(ConfigError, match="unknown limit"):
        load_config(config_dir)
