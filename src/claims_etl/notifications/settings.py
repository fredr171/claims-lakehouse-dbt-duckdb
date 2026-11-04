"""Secret/setting lookup behind one function, so the backing store can change.

Today settings come from environment variables (filled from a gitignored .env
by the CLI). In Lambda they will come from SSM Parameter Store; the switch is
the single ``_lookup = ...`` line below. Values are never logged.
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


# Swap to `ssm_setting` when the notifier runs in Lambda.
_lookup: Callable[[str], str | None] = env_setting


def get_setting(name: str) -> str:
    """Return a required setting, raising MissingSettingError if unset or empty."""
    value = _lookup(name)
    if not value:
        raise MissingSettingError(f"required setting {name} is not set")
    return value
