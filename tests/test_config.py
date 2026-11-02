from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from claims_etl.ingestion.config import ConfigError, load_config
from tests.conftest import LANDING_KEY


def test_repo_config_loads_real_beneficiary_header() -> None:
    config = load_config()  # the real config/ folder
    ft = config.file_types["beneficiary_summary"]
    assert len(ft.expected_columns) == 32
    assert ft.expected_columns[0] == "DESYNPUF_ID"
    assert ft.expected_columns[-1] == "PPPYMT_CAR"
    assert ft.key_columns == ("DESYNPUF_ID",)
    assert config.landing_bucket == "fredr-claims-landing-2026"
    assert config.region == "us-east-2"


def test_repo_config_templates_resolve_from_real_zip_name() -> None:
    ft = load_config().file_types["beneficiary_summary"]
    groups = ft.match_zip_name("DE1_0_2008_Beneficiary_Summary_File_Sample_1.zip")
    assert groups == {"year": "2008", "sample": "1"}
    assert ft.csv_member_name(groups) == "DE1_0_2008_Beneficiary_Summary_File_Sample_1.csv"
    assert ft.raw_key(groups) == (
        "cms/desynpuf/beneficiary_summary/year=2008/sample=1/part-0.parquet"
    )


def test_file_type_limits_override_defaults() -> None:
    ft = load_config().file_types["beneficiary_summary"]
    assert ft.limits.max_zip_bytes == 104_857_600  # overridden
    assert ft.limits.max_compression_ratio == 100  # inherited default


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        (LANDING_KEY, "beneficiary_summary"),
        ("cms/desynpuf/x.zip", None),  # no file type folder
        ("cms/desynpuf/beneficiary_summary/sub/x.zip", None),  # nested too deep
        ("archive/cms/desynpuf/beneficiary_summary/x.zip", None),  # outside prefix
    ],
)
def test_file_type_for_key(config, key: str, expected: str | None) -> None:
    assert config.file_type_for_key(key) == expected


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        (LANDING_KEY, True),
        ("cms/desynpuf/beneficiary_summary/x.csv", False),
        ("quarantine/cms/desynpuf/beneficiary_summary/x.zip", False),
    ],
)
def test_is_landing_zip(config, key: str, expected: bool) -> None:
    assert config.is_landing_zip(key) is expected


def _edit_file_type(config_dir: Path, **changes) -> None:
    path = config_dir / "file_types" / "beneficiary_summary.yml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    data.update(changes)
    path.write_text(yaml.safe_dump(data), encoding="utf-8")


def test_template_field_not_in_regex_is_rejected(config_dir: Path) -> None:
    _edit_file_type(config_dir, raw_key_template="x/month={month}/part-0.parquet")
    with pytest.raises(ConfigError, match="month"):
        load_config(config_dir)


def test_key_column_must_be_an_expected_column(config_dir: Path) -> None:
    _edit_file_type(config_dir, key_columns=["NOT_A_COLUMN"])
    with pytest.raises(ConfigError, match="key_columns"):
        load_config(config_dir)


def test_columns_may_not_clash_with_metadata(config_dir: Path) -> None:
    _edit_file_type(config_dir, expected_columns=["DESYNPUF_ID", "_batch_id"])
    with pytest.raises(ConfigError, match="metadata"):
        load_config(config_dir)


def test_unknown_limit_is_rejected(config_dir: Path) -> None:
    _edit_file_type(config_dir, limits={"max_zip_byts": 1})
    with pytest.raises(ConfigError, match="unknown limit"):
        load_config(config_dir)
