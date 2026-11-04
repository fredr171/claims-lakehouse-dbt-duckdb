"""Load config/notifications.yml (plus bucket settings from ingestion.yml)."""

from __future__ import annotations

import string
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from claims_etl.ingestion.config import ConfigError, default_config_dir, load_config

TITLE_FIELDS = {"file_type", "year", "sample", "zip_name", "failed_check"}
NEXT_STEP_FIELDS = {"file_type"}


@dataclass(frozen=True)
class LabelSpec:
    """A ticket label and how to create it if missing."""

    name: str
    color: str
    description: str


@dataclass(frozen=True)
class NotificationConfig:
    """Everything the notifier needs, tracker-agnostic."""

    region: str
    landing_bucket: str
    raw_bucket: str
    quarantine_prefix: str
    error_json_suffix: str
    ticket_prefix: str
    base_labels: tuple[str, ...]
    match_label: str
    default_label_color: str
    label_colors: dict[str, str]
    label_descriptions: dict[str, str]
    title_template: str
    title_template_no_partition: str
    max_title_chars: int
    max_details_chars: int
    next_steps: dict[str, str]

    def is_error_json_key(self, key: str) -> bool:
        """True for quarantine/.../<zip>.zip.error.json keys (the only keys we act on)."""
        return key.startswith(self.quarantine_prefix) and key.endswith(self.error_json_suffix)

    def labels_for(self, file_type: str) -> list[LabelSpec]:
        """Base labels plus the file type label, with colors/descriptions."""
        names = [*self.base_labels, file_type]
        return [
            LabelSpec(
                name=name,
                color=self.label_colors.get(name, self.default_label_color),
                description=self.label_descriptions.get(
                    name, self.label_descriptions.get("default", "")
                ),
            )
            for name in dict.fromkeys(names)  # de-duplicate, keep order
        ]

    def next_step_for(self, check: str | None, file_type: str) -> str:
        """Suggested next step text for a failed check."""
        template = self.next_steps.get(check or "", self.next_steps["default"])
        return template.format(file_type=file_type)


def _template_fields(template: str) -> set[str]:
    return {name for _, name, _, _ in string.Formatter().parse(template) if name}


def _require_fields(name: str, template: str, allowed: set[str]) -> None:
    unknown = _template_fields(template) - allowed
    if unknown:
        raise ConfigError(f"notifications.yml {name} uses unknown fields {sorted(unknown)}")


def load_notification_config(config_dir: Path | str | None = None) -> NotificationConfig:
    """Load and validate notifications.yml, taking buckets/prefixes from ingestion.yml."""
    config_dir = Path(config_dir) if config_dir else default_config_dir()
    ingestion = load_config(config_dir)
    path = config_dir / "notifications.yml"
    if not path.is_file():
        raise ConfigError(f"config file not found: {path}")
    raw: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    labels = raw.get("labels", {})
    base_labels = tuple(labels.get("base", []))
    match_label = labels.get("match", "")
    if match_label not in base_labels:
        raise ConfigError("notifications.yml labels.match must be one of labels.base")

    next_steps = dict(raw.get("next_steps", {}))
    if "default" not in next_steps:
        raise ConfigError("notifications.yml next_steps needs a 'default' entry")
    for check, text in next_steps.items():
        _require_fields(f"next_steps.{check}", text, NEXT_STEP_FIELDS)

    for key in ("title_template", "title_template_no_partition"):
        _require_fields(key, raw[key], TITLE_FIELDS)

    ticket_prefix = raw.get("ticket_prefix", "")
    if not ticket_prefix.endswith("/"):
        raise ConfigError("notifications.yml ticket_prefix must end with '/'")

    return NotificationConfig(
        region=ingestion.region,
        landing_bucket=ingestion.landing_bucket,
        raw_bucket=ingestion.raw_bucket,
        quarantine_prefix=ingestion.quarantine_prefix,
        error_json_suffix=raw["error_json_suffix"],
        ticket_prefix=ticket_prefix,
        base_labels=base_labels,
        match_label=match_label,
        default_label_color=labels.get("default_color", "c5def5"),
        label_colors=dict(labels.get("colors", {})),
        label_descriptions=dict(labels.get("descriptions", {})),
        title_template=raw["title_template"],
        title_template_no_partition=raw["title_template_no_partition"],
        max_title_chars=int(raw.get("max_title_chars", 200)),
        max_details_chars=int(raw.get("max_details_chars", 500)),
        next_steps=next_steps,
    )
