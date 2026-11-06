"""dbt's own CLI, made safe for hosts without POSIX semaphores (AWS Lambda).

dbt asks multiprocessing for Lock/RLock objects in two places: through its spawn context
(the adapter's connection manager, the manifest) and, indirectly, through the platform's
DEFAULT context, which multiprocessing.pool.ThreadPool uses for its internal
change-notifier queue (fork on Linux, so on Lambda). On Linux those locks are POSIX
semaphores backed by /dev/shm, which Lambda does not have: dbt crashes with
FileNotFoundError from SemLock. dbt never starts worker processes (models run on that
ThreadPool), so threading locks give the same guarantees. This module swaps them in on
the base context class, so EVERY context (spawn, fork, forkserver, default) gets them,
then hands over to dbt's CLI unchanged.

The runner calls it as `python -m claims_etl.transform.dbt_cli build ...` everywhere
(Lambda, CLI, tests), so local runs exercise the same entry point as the Lambda.
"""

from __future__ import annotations

import threading
from multiprocessing.context import BaseContext


def use_thread_locks() -> None:
    """Make every multiprocessing context hand out threading locks instead of semaphores."""
    BaseContext.Lock = lambda self: threading.Lock()  # type: ignore[method-assign]
    BaseContext.RLock = lambda self: threading.RLock()  # type: ignore[method-assign]


def main() -> None:
    use_thread_locks()
    from dbt.cli.main import cli  # imported after the patch, like a fresh dbt process

    cli()


if __name__ == "__main__":
    main()
