"""Secret/setting lookup behind one function, so the backing store can change.

Locally, settings come from environment variables (filled from a gitignored
.env by the CLI). In Lambda, CLAIMS_ETL_SETTINGS_SOURCE=ssm selects SSM
Parameter Store (/claims-etl/<NAME>). Values are never logged.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from functools import cache

import boto3

SSM_PATH_PREFIX = "/claims-etl/"  # e.g. /claims-etl/GITHUB_TOKEN (SecureString)


class MissingSettingError(RuntimeError):
    """A required setting is not configured. The message names it, never its value."""


def env_setting(name: str) -> str | None:
    """Read a setting from the environment."""
    return os.environ.get(name)


@cache
def _ssm_client():  # pragma: no cover - exercised via ssm_setting tests
    return boto3.client("ssm")


def ssm_setting(name: str) -> str | None:
    """Read a setting from SSM Parameter Store (decrypted), or None if absent."""
    client = _ssm_client()
    try:
        response = client.get_parameter(Name=f"{SSM_PATH_PREFIX}{name}", WithDecryption=True)
    except client.exceptions.ParameterNotFound:
        return None
    return response["Parameter"]["Value"]


SOURCE_ENV_VAR = "CLAIMS_ETL_SETTINGS_SOURCE"  # "ssm" in Lambda (set by Terraform)


def _select_lookup() -> Callable[[str], str | None]:
    """SSM when CLAIMS_ETL_SETTINGS_SOURCE=ssm, else environment variables."""
    return ssm_setting if os.environ.get(SOURCE_ENV_VAR, "").lower() == "ssm" else env_setting


_lookup: Callable[[str], str | None] = _select_lookup()


def get_setting(name: str) -> str:
    """Return a required setting, raising MissingSettingError if unset or empty."""
    value = _lookup(name)
    if not value:
        raise MissingSettingError(f"required setting {name} is not set")
    return value
