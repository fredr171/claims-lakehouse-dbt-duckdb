"""claims_etl.transform.dbt_cli: dbt must run where POSIX semaphores are missing (Lambda).

AWS Lambda has no /dev/shm, so creating a multiprocessing lock raises FileNotFoundError.
Each test simulates that host in a subprocess by making SemLock creation fail the same
way, then runs `dbt parse` (which registers the adapter, where the live Lambda crashed).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.dbt

DBT_DIR = Path(__file__).resolve().parents[1] / "dbt"

# Make every multiprocessing lock fail as it does on Lambda, then run `module` as __main__.
SIMULATE_LAMBDA = """
import errno, runpy, sys
import multiprocessing.synchronize as synchronize

def no_semaphores(*args, **kwargs):
    raise FileNotFoundError(errno.ENOENT, "No such file or directory")

synchronize.SemLock.__init__ = no_semaphores
module = sys.argv.pop(1)
sys.argv[0] = module
runpy.run_module(module, run_name="__main__", alter_sys=True)
"""


def _parse_without_semaphores(module: str, tmp_path: Path) -> subprocess.CompletedProcess:
    command = [
        sys.executable, "-c", SIMULATE_LAMBDA, module, "parse",
        "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
        "--target-path", str(tmp_path / "target"), "--log-path", str(tmp_path / "logs"),
    ]  # fmt: skip
    env = {
        **os.environ,
        "CLAIMS_DUCKDB_PATH": str(tmp_path / "parse.duckdb"),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "PYTHONIOENCODING": "utf-8",
    }
    return subprocess.run(command, env=env, capture_output=True, text=True, encoding="utf-8")


def test_plain_dbt_crashes_without_semaphores(tmp_path: Path) -> None:
    """The simulation reproduces the live Lambda failure."""
    result = _parse_without_semaphores("dbt.cli.main", tmp_path)
    assert result.returncode != 0
    assert "FileNotFoundError" in result.stdout + result.stderr


def test_entry_point_runs_dbt_without_semaphores(tmp_path: Path) -> None:
    result = _parse_without_semaphores("claims_etl.transform.dbt_cli", tmp_path)
    assert result.returncode == 0, (result.stdout + result.stderr)[-2000:]
    assert (tmp_path / "target" / "manifest.json").is_file()
