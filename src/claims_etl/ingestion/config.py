"""Load global ingestion settings and per-file-type configs from YAML.

Adding a new file type means adding config/file_types/<file_type>.yml; no code
changes. Configs are validated on load so mistakes fail fast, not mid-run.
"""

from __future__ import annotations

import os
import re
import string
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import Any

import yaml

from claims_etl.ingestion.models import METADATA_COLUMNS

CONFIG_DIR_ENV_VAR = "CLAIMS_ETL_CONFIG_DIR"
_REPO_CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"

_REQUIRED_FILE_TYPE_KEYS = (
    "file_type",
    "zip_name_regex",
    "csv_member_template",
    "raw_key_template",
    "expected_columns",
    "key_columns",
)


class ConfigError(ValueError):
    """Raised when a config file is missing, malformed, or inconsistent."""


@dataclass(frozen=True)
class Limits:
    """Size limits that guard against oversized archives and zip bombs."""

    max_zip_bytes: int
    max_uncompressed_bytes: int
    max_compression_ratio: float

    def with_overrides(self, overrides: dict[str, Any] | None) -> Limits:
        """Return a copy with any per-file-type overrides applied."""
        if not overrides:
            return self
        unknown = set(overrides) - {f.name for f in fields(Limits)}
        if unknown:
            raise ConfigError(f"unknown limit(s): {sorted(unknown)}")
        return replace(self, **overrides)


@dataclass(frozen=True)
class FileTypeConfig:
    """Everything needed to validate and land one file type."""

    file_type: str
    zip_name_regex: re.Pattern[str]
    csv_member_template: str
    raw_key_template: str
    expected_columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    limits: Limits

    def match_zip_name(self, zip_name: str) -> dict[str, str] | None:
        """Return the regex's named groups (e.g. year, sample), or None if no match."""
        match = self.zip_name_regex.fullmatch(zip_name)
        return match.groupdict() if match else None

    def csv_member_name(self, groups: dict[str, str]) -> str:
        """The single CSV file name expected inside the zip."""
        return self.csv_member_template.format(**groups)

    def raw_key(self, groups: dict[str, str]) -> str:
        """Deterministic output key in the raw bucket (re-runs overwrite)."""
        return self.raw_key_template.format(**groups)


@dataclass(frozen=True)
class IngestionConfig:
    """Global settings plus all known file types."""

    region: str
    landing_bucket: str
    raw_bucket: str
    landing_prefix: str
    archive_prefix: str
    quarantine_prefix: str
    manifest_prefix: str
    parquet_batch_rows: int
    file_types: dict[str, FileTypeConfig]

    def is_landing_zip(self, key: str) -> bool:
        """True if the key is a .zip under the landing prefix (the only keys we act on)."""
        return key.startswith(self.landing_prefix) and key.lower().endswith(".zip")

    def file_type_for_key(self, key: str) -> str | None:
        """Extract <file_type> from '<landing_prefix><file_type>/<zip_name>'.

        Returns None for keys outside the prefix or nested any deeper. The
        returned name may still be unknown; check it against file_types.
        """
        if not key.startswith(self.landing_prefix):
            return None
        parts = key[len(self.landing_prefix) :].split("/")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            return None
        return parts[0]


def default_config_dir() -> Path:
    """Config dir from $CLAIMS_ETL_CONFIG_DIR, else the repo's config/ folder."""
    return Path(os.environ.get(CONFIG_DIR_ENV_VAR, _REPO_CONFIG_DIR))


def load_config(config_dir: Path | str | None = None) -> IngestionConfig:
    """Load ingestion.yml and every file_types/*.yml under config_dir."""
    config_dir = Path(config_dir) if config_dir else default_config_dir()
    settings = _read_yaml(config_dir / "ingestion.yml")

    prefixes = {
        name: settings.get(name)
        for name in ("landing_prefix", "archive_prefix", "quarantine_prefix", "manifest_prefix")
    }
    for name, value in prefixes.items():
        if not isinstance(value, str) or not value.endswith("/"):
            raise ConfigError(f"{name} must be a string ending in '/', got {value!r}")

    try:
        default_limits = Limits(**settings["default_limits"])
    except (KeyError, TypeError) as exc:
        raise ConfigError(f"invalid default_limits in ingestion.yml: {exc}") from exc

    file_types: dict[str, FileTypeConfig] = {}
    for path in sorted((config_dir / "file_types").glob("*.yml")):
        file_type = _parse_file_type(_read_yaml(path), default_limits, path)
        if file_type.file_type != path.stem:
            raise ConfigError(f"{path.name}: file_type must equal the file name stem")
        file_types[file_type.file_type] = file_type

    return IngestionConfig(
        region=settings["region"],
        landing_bucket=settings["landing_bucket"],
        raw_bucket=settings["raw_bucket"],
        parquet_batch_rows=int(settings.get("parquet_batch_rows", 50_000)),
        file_types=file_types,
        **prefixes,
    )


def _read_yaml(path: Path) -> dict[str, Any]:
    """Read a YAML mapping, raising ConfigError if missing or not a mapping."""
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must contain a YAML mapping")
    return data


def _template_fields(template: str) -> set[str]:
    """Placeholder names used in a str.format template."""
    return {name for _, name, _, _ in string.Formatter().parse(template) if name}


def _parse_file_type(raw: dict[str, Any], default_limits: Limits, path: Path) -> FileTypeConfig:
    """Build and cross-check one FileTypeConfig."""
    missing = [k for k in _REQUIRED_FILE_TYPE_KEYS if k not in raw]
    if missing:
        raise ConfigError(f"{path.name}: missing keys {missing}")

    try:
        regex = re.compile(raw["zip_name_regex"])
    except re.error as exc:
        raise ConfigError(f"{path.name}: invalid zip_name_regex: {exc}") from exc

    groups = set(regex.groupindex)
    for key in ("csv_member_template", "raw_key_template"):
        unknown = _template_fields(raw[key]) - groups
        if unknown:
            raise ConfigError(f"{path.name}: {key} uses {sorted(unknown)} not captured by regex")

    columns = tuple(raw["expected_columns"])
    keys = tuple(raw["key_columns"])
    if not columns:
        raise ConfigError(f"{path.name}: expected_columns is empty")
    if len(set(columns)) != len(columns):
        raise ConfigError(f"{path.name}: expected_columns has duplicates")
    if clash := set(columns) & set(METADATA_COLUMNS):
        raise ConfigError(f"{path.name}: columns clash with metadata columns {sorted(clash)}")
    if not keys or not set(keys) <= set(columns):
        raise ConfigError(f"{path.name}: key_columns must be a non-empty subset of columns")

    return FileTypeConfig(
        file_type=raw["file_type"],
        zip_name_regex=regex,
        csv_member_template=raw["csv_member_template"],
        raw_key_template=raw["raw_key_template"],
        expected_columns=columns,
        key_columns=keys,
        limits=default_limits.with_overrides(raw.get("limits")),
    )
