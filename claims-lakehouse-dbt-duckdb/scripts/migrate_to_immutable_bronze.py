"""One-off: move deliveries ingested before immutable bronze to checksum keys.

Before (one key per file NAME, a re-send overwrote the original):
    raw:     <feed>/<file_type>/delivery_month=<m>/part-0.parquet
    archive: archive/<feed>/<file_type>/<file_name>
After (one key per DELIVERY):
    raw:     <feed>/<file_type>/delivery_month=<m>/checksum=<sha256>/part-0.parquet
    archive: archive/<feed>/<file_type>/checksum=<sha256>/<file_name>

Driven by the manifests (_manifests/<sha256>.json, outcome success). Each object is
moved (copy -> verify -> delete) only after its CONTENT is shown to belong to that
manifest: the Parquet's _source_checksum, or the archived CSV's SHA-256, must equal the
manifest checksum. Anything else is reported and left alone (e.g. an original that a
pre-migration re-send already overwrote). The manifest is then updated with the new
locations and a `layout_migration` note keeping the old ones.

Dry run by default (prints the plan, writes nothing). Re-running is safe: objects
already at their new key are skipped. Moving archive objects does not trigger ingestion
(archive/ is outside the feed prefixes) and raw has no trigger on data keys.

    python scripts/migrate_to_immutable_bronze.py            # plan only
    python scripts/migrate_to_immutable_bronze.py --apply    # do it
Exit code 0 = nothing left to do / all moved, 1 = problems reported.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from claims_etl.ingestion.config import IngestionConfig, load_config
from claims_etl.ingestion.s3_store import S3Store
from claims_etl.ingestion.utils import sha256_file, utc_now


@dataclass
class Move:
    bucket: str
    old: str
    new: str
    kind: str  # raw | archive


@dataclass
class Plan:
    checksum: str
    manifest_key: str
    moves: list[Move] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)
    already_done: bool = False


def _key(uri: str | None, bucket: str) -> str | None:
    prefix = f"s3://{bucket}/"
    return uri[len(prefix) :] if uri and uri.startswith(prefix) else None


def _content_checksum(store: S3Store, bucket: str, key: str, kind: str, tmp: Path) -> str | None:
    """The checksum the object's content belongs to (raw: _source_checksum; csv: SHA-256)."""
    local = tmp / ("object.parquet" if kind == "raw" else "object.csv")
    store.download(bucket, key, local)
    if kind == "archive":
        return sha256_file(local)
    values = set(pq.read_table(local, columns=["_source_checksum"])["_source_checksum"].to_pylist())
    return values.pop() if len(values) == 1 else None


def plan_manifest(
    store: S3Store, config: IngestionConfig, manifest_key: str, record: dict[str, Any], tmp: Path
) -> Plan | None:
    """What to move for one manifest (None for non-success records)."""
    if record.get("outcome") != "success":
        return None
    checksum = record["checksum_sha256"]
    plan = Plan(checksum, manifest_key)
    ft = config.file_types.get(record.get("file_type") or "")
    if ft is None:
        plan.problems.append(f"unknown file type {record.get('file_type')!r}")
        return plan
    targets = [
        ("raw", config.raw_bucket, _key(record.get("output_uri"), config.raw_bucket),
         ft.raw_key(record["name_fields"], checksum)),
        ("archive", config.landing_bucket,
         _key(record.get("landing_destination_uri"), config.landing_bucket),
         config.archive_key(record["source_key"], checksum)),
    ]  # fmt: skip
    for kind, bucket, old, new in targets:
        if old == new:
            continue  # manifest already points at the new key
        if old is None:
            plan.problems.append(f"{kind}: manifest has no location")
        elif store.exists(bucket, new) and not store.exists(bucket, old):
            continue  # moved before; only the manifest still needs updating
        elif not store.exists(bucket, old):
            plan.problems.append(f"{kind}: {old} is missing")
        elif (found := _content_checksum(store, bucket, old, kind, tmp)) != checksum:
            # e.g. a re-send overwrote this original before immutable bronze: its bytes
            # are gone, and the object belongs to the other delivery's manifest.
            plan.problems.append(f"{kind}: {old} holds checksum {found}, not this delivery")
        else:
            plan.moves.append(Move(bucket, old, new, kind))
    plan.already_done = (
        not plan.moves and not plan.problems and all(old == new for _, _, old, new in targets)
    )
    return plan


def apply_plan(store: S3Store, config: IngestionConfig, plan: Plan, record: dict) -> None:
    """Move the objects, then point the manifest at them (old locations kept in a note)."""
    for move in plan.moves:
        store.move(move.bucket, move.old, move.new)  # copy -> verify -> delete
    ft = config.file_types[record["file_type"]]
    new_raw = f"s3://{config.raw_bucket}/{ft.raw_key(record['name_fields'], plan.checksum)}"
    new_archive = (
        f"s3://{config.landing_bucket}/{config.archive_key(record['source_key'], plan.checksum)}"
    )
    updated = {
        **record,
        "output_uri": new_raw,
        "landing_destination_uri": new_archive,
        "layout_migration": {
            "at": utc_now().isoformat(),
            "previous_output_uri": record.get("output_uri"),
            "previous_landing_destination_uri": record.get("landing_destination_uri"),
        },
    }
    store.put_json(config.raw_bucket, plan.manifest_key, updated)


def migrate(store: S3Store, config: IngestionConfig, *, apply: bool) -> dict[str, Any]:
    """Plan (and with apply=True carry out) the migration of every success manifest."""
    summary: dict[str, Any] = {"apply": apply, "moved": 0, "already_done": 0, "problems": []}
    with tempfile.TemporaryDirectory(prefix="claims_migrate_") as tmp:
        for manifest_key in sorted(store.list_keys(config.raw_bucket, config.manifest_prefix)):
            if not manifest_key.endswith(".json"):
                continue
            record = store.get_json(config.raw_bucket, manifest_key) or {}
            plan = plan_manifest(store, config, manifest_key, record, Path(tmp))
            if plan is None:
                continue
            for move in plan.moves:
                print(json.dumps({"move": move.kind, "from": move.old, "to": move.new}))
            if plan.problems:
                summary["problems"] += [
                    f"{record.get('file_name')} ({plan.checksum[:12]}): {p}" for p in plan.problems
                ]
                continue  # never half-migrate a delivery
            if plan.already_done:
                summary["already_done"] += 1
                continue
            if apply:
                apply_plan(store, config, plan, record)
            summary["moved"] += 1
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--apply", action="store_true", help="move objects (default: plan only)")
    parser.add_argument("--config-dir", help="config folder (default: the repo's config/)")
    args = parser.parse_args(argv)
    config = load_config(args.config_dir)
    summary = migrate(S3Store.for_region(config.region), config, apply=args.apply)
    print(json.dumps(summary, indent=2))
    return 1 if summary["problems"] else 0


if __name__ == "__main__":
    sys.exit(main())
