"""dbt's own CLI, made safe for hosts without POSIX semaphores (AWS Lambda).

dbt asks multiprocessing for Lock/RLock objects (the adapter's connection manager, the
manifest). On Linux those are POSIX semaphores backed by /dev/shm, which Lambda does
not have, so dbt crashes while registering the adapter, before it reads a model
(FileNotFoundError from SemLock). dbt never starts worker processes (models run on a
ThreadPool), so threading locks give the same guarantees. This module swaps them in,
then hands over to dbt's CLI unchanged.

The runner calls it as `python -m claims_etl.transform.dbt_cli build ...` everywhere
(Lambda, CLI, tests), so local runs exercise the same entry point as the Lambda.
"""

from __future__ import annotations

import threading
from multiprocessing.context import SpawnContext


def use_thread_locks() -> None:
    """Make dbt's spawn context hand out threading locks instead of semaphores."""
    SpawnContext.Lock = lambda self: threading.Lock()  # type: ignore[method-assign]
    SpawnContext.RLock = lambda self: threading.RLock()  # type: ignore[method-assign]


def main() -> None:
    use_thread_locks()
    from dbt.cli.main import cli  # imported after the patch, like a fresh dbt process

    cli()


if __name__ == "__main__":
    main()
