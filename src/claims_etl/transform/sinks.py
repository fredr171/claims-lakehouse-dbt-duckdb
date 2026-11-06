"""Run artifact sinks: <runs_prefix><run_id>/report.json and run_results.json."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from claims_etl.ingestion.s3_store import S3Store
from claims_etl.ingestion.utils import s3_uri


class S3RunSink:
    """Writes a run's artifacts under s3://<bucket>/<runs_prefix><run_id>/."""

    def __init__(self, store: S3Store, bucket: str, runs_prefix: str, run_id: str) -> None:
        self._store, self._bucket = store, bucket
        self._prefix = f"{runs_prefix}{run_id}/"

    def put_json(self, name: str, data: dict[str, Any]) -> str:
        self._store.put_json(self._bucket, self._prefix + name, data)
        return s3_uri(self._bucket, self._prefix + name)

    def put_file(self, name: str, path: Path) -> str:
        self._store.upload(path, self._bucket, self._prefix + name)
        return s3_uri(self._bucket, self._prefix + name)


class LocalRunSink:
    """Writes a run's artifacts under <root>/<run_id>/ (CLI with --db, tests)."""

    def __init__(self, root: Path, run_id: str) -> None:
        self._dir = root / run_id

    def put_json(self, name: str, data: dict[str, Any]) -> str:
        self._dir.mkdir(parents=True, exist_ok=True)
        (self._dir / name).write_text(json.dumps(data, indent=2), encoding="utf-8")
        return str(self._dir / name)

    def put_file(self, name: str, path: Path) -> str:
        self._dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, self._dir / name)
        return str(self._dir / name)
