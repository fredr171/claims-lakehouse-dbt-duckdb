"""Load config/transform.yml, plus the ingestion settings it builds on."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from claims_etl.ingestion.config import ConfigError, _read_yaml, default_config_dir, load_config

TARGET_ENV_VAR = "CLAIMS_DBT_TARGET"


@dataclass(frozen=True)
class TransformConfig:
    """Settings for one transform run (see config/transform.yml)."""

    region: str
    raw_bucket: str
    state_key: str
    backup_key: str
    runs_prefix: str
    dbt_target: str
    feed_sources: dict[str, str]  # landing prefix -> dbt source name

    def source_for(self, feed: str) -> str:
        try:
            return self.feed_sources[feed]
        except KeyError:
            raise ConfigError(f"feed {feed!r} has no dbt source in transform.yml") from None


def load_transform_config(config_dir: Path | str | None = None) -> TransformConfig:
    """Load transform.yml; feeds must map exactly the landing prefixes in ingestion.yml."""
    config_dir = Path(config_dir) if config_dir else default_config_dir()
    ingestion = load_config(config_dir)
    settings = _read_yaml(config_dir / "transform.yml")

    feeds = settings.get("feeds")
    if not isinstance(feeds, dict) or set(feeds) != set(ingestion.landing_prefixes):
        raise ConfigError(
            f"transform.yml feeds {sorted(feeds or {})} must map exactly the landing "
            f"prefixes {sorted(ingestion.landing_prefixes)}"
        )
    for key in ("state_key", "backup_key", "runs_prefix"):
        if not isinstance(settings.get(key), str) or not settings[key]:
            raise ConfigError(f"transform.yml: {key} is required")
    if not settings["runs_prefix"].endswith("/"):
        raise ConfigError("transform.yml: runs_prefix must end in '/'")

    return TransformConfig(
        region=ingestion.region,
        raw_bucket=ingestion.raw_bucket,
        state_key=settings["state_key"],
        backup_key=settings["backup_key"],
        runs_prefix=settings["runs_prefix"],
        dbt_target=os.environ.get(TARGET_ENV_VAR) or settings.get("dbt_target", "dev"),
        feed_sources=dict(feeds),
    )
