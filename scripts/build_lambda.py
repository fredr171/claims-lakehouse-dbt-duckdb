"""Build build/lambda.zip for the ingestion Lambda.

Contents: the claims_etl package, the config/ folder, and PyYAML + requests
installed for the Lambda platform (Linux x86_64, CPython 3.13). pyarrow and numpy come from
the AWS SDK for pandas layer; boto3 comes from the Lambda runtime.

The zip is deterministic (sorted entries, fixed timestamps), so rebuilding
unchanged code yields the same hash and Terraform does not redeploy.

    python scripts/build_lambda.py
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILD_DIR = ROOT / "build"
ZIP_PATH = BUILD_DIR / "lambda.zip"

LAMBDA_PYTHON_VERSION = "3.13"  # keep in sync with infra/terraform (runtime + layer)
LAMBDA_PLATFORM = "manylinux2014_x86_64"
VENDORED_REQUIREMENTS = ["pyyaml>=6,<7", "requests>=2.31,<3"]

FIXED_ZIP_TIME = (2020, 1, 1, 0, 0, 0)
EXCLUDED_DIRS = {"__pycache__", ".pytest_cache"}
EXCLUDED_SUFFIXES = {".pyc", ".pyo"}


def stage_sources(staging: Path) -> None:
    """Copy the package and config into the staging folder."""
    ignore = shutil.ignore_patterns(*EXCLUDED_DIRS, "*.pyc", "*.pyo", "*.egg-info")
    shutil.copytree(ROOT / "src" / "claims_etl", staging / "claims_etl", ignore=ignore)
    shutil.copytree(ROOT / "config", staging / "config", ignore=ignore)


def install_vendored_dependencies(staging: Path) -> None:
    """pip-install Linux wheels for the Lambda runtime into the staging folder."""
    subprocess.run(
        [
            sys.executable, "-m", "pip", "install",
            "--quiet",
            "--target", str(staging),
            "--platform", LAMBDA_PLATFORM,
            "--python-version", LAMBDA_PYTHON_VERSION,
            "--implementation", "cp",
            "--only-binary=:all:",
            "--no-compile",
            *VENDORED_REQUIREMENTS,
        ],
        check=True,
    )  # fmt: skip


def write_deterministic_zip(staging: Path) -> int:
    """Zip the staging folder with sorted entries and fixed timestamps; return file count."""
    files = sorted(
        p
        for p in staging.rglob("*")
        if p.is_file()
        and not (set(p.relative_to(staging).parts) & EXCLUDED_DIRS)
        and p.suffix not in EXCLUDED_SUFFIXES
    )
    BUILD_DIR.mkdir(exist_ok=True)
    ZIP_PATH.unlink(missing_ok=True)
    with zipfile.ZipFile(ZIP_PATH, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            info = zipfile.ZipInfo(path.relative_to(staging).as_posix(), FIXED_ZIP_TIME)
            info.external_attr = 0o644 << 16  # rw-r--r--, readable by the Lambda user
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, path.read_bytes())
    return len(files)


def main() -> None:
    """Build the zip and print a short summary."""
    # Stage outside the repo: OneDrive locks folders in the project tree mid-sync.
    with tempfile.TemporaryDirectory(prefix="claims_etl_lambda_") as tmp:
        staging = Path(tmp)
        stage_sources(staging)
        install_vendored_dependencies(staging)
        count = write_deterministic_zip(staging)
    size_kb = ZIP_PATH.stat().st_size / 1024
    print(f"built {ZIP_PATH.relative_to(ROOT)}: {count} files, {size_kb:.0f} KiB")


if __name__ == "__main__":
    main()
