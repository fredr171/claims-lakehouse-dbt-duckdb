"""claims_etl.transform.dbt_cli: dbt must run where POSIX semaphores are missing (Lambda).

AWS Lambda has no /dev/shm, so creating a multiprocessing lock raises FileNotFoundError.
Each test simulates that host in a subprocess:
  - SemLock creation fails the same way;
  - the DEFAULT multiprocessing context is not spawn, as on Linux (fork). This matters:
    ThreadPool takes its locks from the default context, and on Windows the default IS
    spawn, so without this a spawn-only fix would pass here and still crash on Lambda
    (it did, 2026-11-05).
Then it runs a real `dbt build` of a one-model project, so dbt registers the adapter
AND executes nodes on its ThreadPool (where the second live crash happened).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.dbt

SIMULATE_LAMBDA = """
import errno, runpy, sys
import multiprocessing.context as context
import multiprocessing.synchronize as synchronize

def no_semaphores(*args, **kwargs):
    raise FileNotFoundError(errno.ENOENT, "No such file or directory")

synchronize.SemLock.__init__ = no_semaphores


class LinuxLikeContext(context.BaseContext):
    # Stands in for Linux's fork context: a default that is not SpawnContext.
    _name = "linux-like"


# multiprocessing.get_context is bound to this object at import, so change what it
# resolves to rather than replacing the object.
context._default_context._actual_context = LinuxLikeContext()
module = sys.argv.pop(1)
sys.argv[0] = module
runpy.run_module(module, run_name="__main__", alter_sys=True)
"""

PROFILE = """\
tiny:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{db}"
      threads: 2
"""


@pytest.fixture
def tiny_project(tmp_path: Path) -> Path:
    """A one-model dbt-duckdb project with a test (needs no raw data)."""
    project = tmp_path / "project"
    (project / "models").mkdir(parents=True)
    (project / "dbt_project.yml").write_text(
        "name: tiny\nversion: '1.0'\nprofile: tiny\nmodel-paths: [models]\n", encoding="utf-8"
    )
    (project / "profiles.yml").write_text(
        PROFILE.format(db=(tmp_path / "tiny.duckdb").as_posix()), encoding="utf-8"
    )
    (project / "models" / "one.sql").write_text("select 1 as id", encoding="utf-8")
    (project / "models" / "one.yml").write_text(
        "models:\n  - name: one\n    columns:\n      - name: id\n        data_tests: [not_null]\n",
        encoding="utf-8",
    )
    return project


def _build_without_semaphores(module: str, project: Path) -> subprocess.CompletedProcess:
    command = [
        sys.executable, "-c", SIMULATE_LAMBDA, module, "build",
        "--project-dir", str(project), "--profiles-dir", str(project),
        "--target-path", str(project / "target"), "--log-path", str(project / "logs"),
    ]  # fmt: skip
    env = {**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "PYTHONIOENCODING": "utf-8"}
    return subprocess.run(command, env=env, capture_output=True, text=True, encoding="utf-8")


def test_plain_dbt_crashes_without_semaphores(tiny_project: Path) -> None:
    """The simulation reproduces the live Lambda failure."""
    result = _build_without_semaphores("dbt.cli.main", tiny_project)
    assert result.returncode != 0
    assert "FileNotFoundError" in result.stdout + result.stderr


def test_entry_point_builds_without_semaphores(tiny_project: Path) -> None:
    result = _build_without_semaphores("claims_etl.transform.dbt_cli", tiny_project)
    output = result.stdout + result.stderr
    assert result.returncode == 0, output[-3000:]
    assert "PASS=2" in output  # the model ran and its test passed, on dbt's ThreadPool
