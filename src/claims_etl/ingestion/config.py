"""Load global ingestion settings and per-file-type configs from YAML.

Adding a new file type means adding config/file_types/<file_type>.yml; no code
changes. A new feed (data source) also adds its prefix to landing_prefixes in
ingestion.yml and to the Terraform variable of the same name. Configs are
validated on load so mistakes fail fast, not mid-run.
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
    "landing_prefix",
    "file_name_regex",
    "raw_key_template",
    "expected_columns",
    "key_columns",
)


class ConfigError(ValueError):
    """Raised when a config file is missing, malformed, or inconsistent."""


@dataclass(frozen=True)
class Limits:
    """Size limits for a delivered file (guards against runaway or wrong uploads)."""

    max_file_bytes: int

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
    landing_prefix: str  # the feed this file type belongs to, e.g. "cms/cclf/"
    file_name_regex: re.Pattern[str]
    raw_key_template: str
    expected_columns: tuple[str, ...]
    key_columns: tuple[str, ...]
    limits: Limits

    def match_file_name(self, file_name: str) -> dict[str, str] | None:
        """Return the regex's named groups (e.g. year, month), or None if no match."""
        match = self.file_name_regex.fullmatch(file_name)
        return match.groupdict() if match else None

    def raw_key(self, groups: dict[str, str], checksum: str) -> str:
        """Output key in the raw bucket: one immutable key per delivered file.

        The template names the delivery (from the file name) and its content
        (`{checksum}`), so a corrected re-send lands NEXT TO the original instead of
        overwriting it; dbt staging decides which delivery is current.
        """
        return self.raw_key_template.format(**groups, checksum=_checked_checksum(checksum))


@dataclass(frozen=True)
class IngestionConfig:
    """Global settings plus all known file types."""

    region: str
    landing_bucket: str
    raw_bucket: str
    landing_prefixes: tuple[str, ...]  # one per feed; never overlapping
    archive_prefix: str
    quarantine_prefix: str
    manifest_prefix: str
    parquet_batch_rows: int
    file_types: dict[str, FileTypeConfig]
    publish_events: bool = False  # FileIngested events for the transform runner

    def landing_prefix_for_key(self, key: str) -> str | None:
        """The feed prefix the key sits under, or None (prefixes never overlap)."""
        return next((p for p in self.landing_prefixes if key.startswith(p)), None)

    def is_landing_file(self, key: str) -> bool:
        """True if the key is a .csv under a landing prefix (the only keys we act on)."""
        return self.landing_prefix_for_key(key) is not None and key.lower().endswith(".csv")

    def file_type_for_key(self, key: str) -> str | None:
        """Extract <file_type> from '<landing_prefix><file_type>/<file_name>'.

        Returns None for keys outside every prefix or nested any deeper. The
        returned name may still be unknown; check it against file_types_for_key.
        """
        prefix = self.landing_prefix_for_key(key)
        if prefix is None:
            return None
        parts = key[len(prefix) :].split("/")
        if len(parts) != 2 or not parts[0] or not parts[1]:
            return None
        return parts[0]

    def archive_key(self, source_key: str, checksum: str) -> str:
        """archive/<feed>/<file_type>/checksum=<sha256>/<file_name>: never overwritten."""
        return _delivery_key(self.archive_prefix, source_key, checksum)

    def quarantine_key(self, source_key: str, checksum: str) -> str:
        """quarantine/<feed>/<file_type>/checksum=<sha256>/<file_name> (+ .error.json)."""
        return _delivery_key(self.quarantine_prefix, source_key, checksum)

    def file_types_for_key(self, key: str) -> set[str]:
        """File types configured for the key's feed (a CCLF folder under ma_payer/ is unknown)."""
        prefix = self.landing_prefix_for_key(key)
        return {name for name, ft in self.file_types.items() if ft.landing_prefix == prefix}


def default_config_dir() -> Path:
    """Config dir from $CLAIMS_ETL_CONFIG_DIR, else the repo's config/ folder."""
    return Path(os.environ.get(CONFIG_DIR_ENV_VAR, _REPO_CONFIG_DIR))


def load_config(config_dir: Path | str | None = None) -> IngestionConfig:
    """Load ingestion.yml and every file_types/*.yml under config_dir."""
    config_dir = Path(config_dir) if config_dir else default_config_dir()
    settings = _read_yaml(config_dir / "ingestion.yml")

    internal = ("archive_prefix", "quarantine_prefix", "manifest_prefix")
    prefixes = {name: settings.get(name) for name in internal}
    for name, value in prefixes.items():
        if not isinstance(value, str) or not value.endswith("/"):
            raise ConfigError(f"{name} must be a string ending in '/', got {value!r}")
    landing_prefixes = _parse_landing_prefixes(settings.get("landing_prefixes"), prefixes)

    try:
        default_limits = Limits(**settings["default_limits"])
    except (KeyError, TypeError) as exc:
        raise ConfigError(f"invalid default_limits in ingestion.yml: {exc}") from exc

    file_types: dict[str, FileTypeConfig] = {}
    for path in sorted((config_dir / "file_types").glob("*.yml")):
        file_type = _parse_file_type(_read_yaml(path), default_limits, path)
        if file_type.file_type != path.stem:
            raise ConfigError(f"{path.name}: file_type must equal the file name stem")
        if file_type.landing_prefix not in landing_prefixes:
            raise ConfigError(
                f"{path.name}: landing_prefix {file_type.landing_prefix!r} "
                f"is not in landing_prefixes {list(landing_prefixes)}"
            )
        file_types[file_type.file_type] = file_type

    return IngestionConfig(
        region=settings["region"],
        landing_bucket=settings["landing_bucket"],
        raw_bucket=settings["raw_bucket"],
        landing_prefixes=landing_prefixes,
        parquet_batch_rows=int(settings.get("parquet_batch_rows", 50_000)),
        file_types=file_types,
        publish_events=_parse_bool(settings.get("publish_events", False), "publish_events"),
        **prefixes,
    )


def _parse_landing_prefixes(value: Any, other_prefixes: dict[str, str]) -> tuple[str, ...]:
    """A non-empty list of feed prefixes ending in '/', none inside another or an internal one."""
    if not isinstance(value, list) or not value:
        raise ConfigError(f"landing_prefixes must be a non-empty list, got {value!r}")
    for prefix in value:
        if not isinstance(prefix, str) or not prefix.endswith("/") or prefix == "/":
            raise ConfigError(f"each landing prefix must be a string ending in '/', got {prefix!r}")
    if len(set(value)) != len(value):
        raise ConfigError(f"landing_prefixes has duplicates: {value}")
    for i, prefix in enumerate(value):
        # Other feeds (each pair once) and the internal prefixes (archive/, quarantine/, ...).
        for other in [*value[i + 1 :], *other_prefixes.values()]:
            if prefix.startswith(other) or other.startswith(prefix):
                raise ConfigError(f"landing prefixes must not overlap: {prefix!r} and {other!r}")
    return tuple(value)


def _parse_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise ConfigError(f"{name} must be true or false, got {value!r}")
    return value


def _read_yaml(path: Path) -> dict[str, Any]:
    """Read a YAML mapping, raising ConfigError if missing or not a mapping."""
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ConfigError(f"{path} must contain a YAML mapping")
    return data


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
CHECKSUM_FIELD = "checksum"  # filled in by ingestion, never by the file name regex


def _checked_checksum(checksum: str) -> str:
    if not _SHA256.fullmatch(checksum or ""):
        raise ValueError("checksum must be a lowercase hex SHA-256")
    return checksum


def _delivery_key(prefix: str, source_key: str, checksum: str) -> str:
    """<prefix><source folder>/checksum=<sha256>/<file name>: one key per delivered file.

    The delivered file name is kept exactly; the checksum folder keeps every delivery of
    the same name (a corrected re-send never replaces the original).
    """
    folder, _, file_name = source_key.rpartition("/")
    return f"{prefix}{folder}/{CHECKSUM_FIELD}={_checked_checksum(checksum)}/{file_name}"


def _template_fields(template: str) -> set[str]:
    """Placeholder names used in a str.format template."""
    return {name for _, name, _, _ in string.Formatter().parse(template) if name}


def _parse_file_type(raw: dict[str, Any], default_limits: Limits, path: Path) -> FileTypeConfig:
    """Build and cross-check one FileTypeConfig."""
    missing = [k for k in _REQUIRED_FILE_TYPE_KEYS if k not in raw]
    if missing:
        raise ConfigError(f"{path.name}: missing keys {missing}")

    try:
        regex = re.compile(raw["file_name_regex"])
    except re.error as exc:
        raise ConfigError(f"{path.name}: invalid file_name_regex: {exc}") from exc

    fields = _template_fields(raw["raw_key_template"])
    if CHECKSUM_FIELD in regex.groupindex:
        raise ConfigError(f"{path.name}: the regex must not capture '{CHECKSUM_FIELD}'")
    if CHECKSUM_FIELD not in fields:
        # Immutable bronze: without the checksum a re-send would overwrite the original.
        raise ConfigError(
            f"{path.name}: raw_key_template must contain {{{CHECKSUM_FIELD}}} "
            "(one key per delivered file)"
        )
    unknown = fields - set(regex.groupindex) - {CHECKSUM_FIELD}
    if unknown:
        raise ConfigError(
            f"{path.name}: raw_key_template uses {sorted(unknown)} not captured by regex"
        )

    # Raw keys stay inside the file type's own folder (IAM grants raw writes per feed).
    folder = f"{raw['landing_prefix']}{raw['file_type']}/"
    if not raw["raw_key_template"].startswith(folder):
        raise ConfigError(f"{path.name}: raw_key_template must start with {folder!r}")

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
        landing_prefix=raw["landing_prefix"],
        file_name_regex=regex,
        raw_key_template=raw["raw_key_template"],
        expected_columns=columns,
        key_columns=keys,
        limits=default_limits.with_overrides(raw.get("limits")),
    )
