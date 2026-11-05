"""Load the `alerts:` section of config/notifications.yml (pipeline alerts -> issues).

Buckets and the region come from ingestion.yml and the run prefix from transform.yml,
so the notifier and the transform runner can never disagree about where reports live.
Kept free of pyarrow: the notifier Lambda has no pandas layer.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from claims_etl.ingestion.config import ConfigError, default_config_dir, load_config
from claims_etl.notifications.config import LabelSpec
from claims_etl.transform.config import load_transform_config

REQUIRED_LABELS = ("transform", "stopped", "error", "alarm")


@dataclass(frozen=True)
class AlertConfig:
    """Where transform reports live and how alert issues are labeled and worded."""

    region: str
    raw_bucket: str
    runs_prefix: str
    report_suffix: str
    record_prefix: str
    match_label: str
    labels: dict[str, LabelSpec]
    stopped_next_step: str
    error_next_step: str
    alarm_next_steps: dict[str, str]

    def is_report_key(self, key: str) -> bool:
        """True for <runs_prefix><run_id>/report.json (the only raw keys acted on)."""
        if not (key.startswith(self.runs_prefix) and key.endswith(self.report_suffix)):
            return False
        run_id = key[len(self.runs_prefix) : -len(self.report_suffix)]
        return bool(run_id) and "/" not in run_id

    def labels_for(self, *names: str) -> list[LabelSpec]:
        """The match label plus the given ones, de-duplicated, in order."""
        return [self.labels[name] for name in dict.fromkeys((self.match_label, *names))]

    def alarm_next_step(self, alarm_name: str) -> str:
        return self.alarm_next_steps.get(alarm_name, self.alarm_next_steps["default"])


def _text(section: dict[str, Any], key: str) -> str:
    value = section.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"notifications.yml alerts.{key} is required")
    return value


def load_alert_config(config_dir: Path | str | None = None) -> AlertConfig:
    """Load and validate notifications.yml `alerts:`."""
    config_dir = Path(config_dir) if config_dir else default_config_dir()
    ingestion = load_config(config_dir)
    transform = load_transform_config(config_dir)
    raw = yaml.safe_load((config_dir / "notifications.yml").read_text(encoding="utf-8")) or {}
    section = raw.get("alerts")
    if not isinstance(section, dict):
        raise ConfigError("notifications.yml needs an alerts: section")

    labels = {
        name: LabelSpec(name=name, color=str(spec.get("color", "c5def5")),
                        description=str(spec.get("description", "")))
        for name, spec in (section.get("labels") or {}).items()
    }  # fmt: skip
    match_label = _text(section, "match_label")
    missing = [n for n in (match_label, *REQUIRED_LABELS) if n not in labels]
    if missing:
        raise ConfigError(f"notifications.yml alerts.labels is missing {missing}")

    record_prefix = _text(section, "record_prefix")
    if not record_prefix.endswith("/"):
        raise ConfigError("notifications.yml alerts.record_prefix must end with '/'")
    report_suffix = _text(section, "report_suffix")
    if not report_suffix.startswith("/"):
        raise ConfigError("notifications.yml alerts.report_suffix must start with '/'")

    steps = section.get("next_steps") or {}
    alarms = dict(steps.get("alarms") or {})
    if "default" not in alarms:
        raise ConfigError("notifications.yml alerts.next_steps.alarms needs a 'default' entry")

    return AlertConfig(
        region=ingestion.region,
        raw_bucket=ingestion.raw_bucket,
        runs_prefix=transform.runs_prefix,
        report_suffix=report_suffix,
        record_prefix=record_prefix,
        match_label=match_label,
        labels=labels,
        stopped_next_step=_text(steps, "stopped"),
        error_next_step=_text(steps, "error"),
        alarm_next_steps=alarms,
    )
