"""Build the Lambda deployment packages.

    python scripts/build_lambda.py              # build/lambda.zip
    python scripts/build_lambda.py --transform  # build/transform_lambda.zip

build/lambda.zip is shared by claims-etl-ingest and claims-etl-notify (same zip,
different handlers): the claims_etl package, the config/ folder, and PyYAML +
requests installed for the Lambda platform (Linux x86_64, CPython 3.13). boto3
comes from the Lambda runtime. pyarrow and numpy come from the AWS SDK for pandas
layer, which only the ingestion function attaches.

build/transform_lambda.zip (claims-etl-transform) adds the dbt project (as dbt_project/:
dbt-core's own Python package is named dbt/),
dbt-core + dbt-duckdb + duckdb at exactly the versions installed locally (the ones
the tests ran), and the DuckDB httpfs + aws extensions (duckdb_extensions/), so
nothing is downloaded at run time. Two trims keep it under Lambda's 250 MB unzipped
limit: dbt-core-experimental-parser (a 275 MB native binary used only with
--use-v2-parser) is replaced by an empty placeholder wheel, and Babel's locale data
is cut to English (agate parses numbers in en_US). The zip is over 50 MB, so
Terraform deploys it through S3.

Both zips are deterministic (sorted entries, fixed timestamps), so rebuilding
unchanged code yields the same hash and Terraform does not redeploy.
"""

from __future__ import annotations

import argparse
import gzip
import importlib.metadata
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILD_DIR = ROOT / "build"
ZIP_PATH = BUILD_DIR / "lambda.zip"
TRANSFORM_ZIP_PATH = BUILD_DIR / "transform_lambda.zip"
CACHE_DIR = BUILD_DIR / ".cache"

LAMBDA_PYTHON_VERSION = "3.13"  # keep in sync with infra/terraform (runtime + layer)
LAMBDA_PLATFORM = "manylinux2014_x86_64"
# duckdb 2.x ships manylinux_2_28 wheels; the python3.13 runtime (Amazon Linux 2023,
# glibc 2.34) runs them. Only the transform package allows the newer tag.
TRANSFORM_PLATFORMS = [LAMBDA_PLATFORM, "manylinux_2_28_x86_64"]
VENDORED_REQUIREMENTS = ["pyyaml>=6,<7", "requests>=2.31,<3"]

# Transform package: pinned to the locally installed (tested) versions at build time.
DBT_DISTRIBUTIONS = ["dbt-core", "dbt-duckdb", "duckdb"]
PLACEHOLDER_DISTRIBUTION = "dbt-core-experimental-parser"
DUCKDB_EXTENSIONS = ["httpfs", "aws"]  # parquet is built in
DUCKDB_PLATFORM = "linux_amd64"
EXTENSION_URL = "https://extensions.duckdb.org/{version}/{platform}/{name}.duckdb_extension.gz"
DBT_PROJECT_DIR_IN_PACKAGE = "dbt_project"  # CLAIMS_DBT_PROJECT_DIR in transform.tf
DBT_PROJECT_PARTS = ["dbt_project.yml", "profiles.yml", "models", "macros", "tests"]
KEPT_BABEL_LOCALES = ("root.dat", "en.dat", "en_")
LAMBDA_UNZIPPED_LIMIT = 250 * 1024 * 1024

FIXED_ZIP_TIME = (2020, 1, 1, 0, 0, 0)
EXCLUDED_DIRS = {"__pycache__", ".pytest_cache"}
EXCLUDED_SUFFIXES = {".pyc", ".pyo"}


def stage_sources(staging: Path) -> None:
    """Copy the package and config into the staging folder."""
    ignore = shutil.ignore_patterns(*EXCLUDED_DIRS, "*.pyc", "*.pyo", "*.egg-info")
    shutil.copytree(ROOT / "src" / "claims_etl", staging / "claims_etl", ignore=ignore)
    shutil.copytree(ROOT / "config", staging / "config", ignore=ignore)


def install_vendored_dependencies(
    staging: Path,
    requirements: list[str],
    find_links: Path | None = None,
    platforms: list[str] | None = None,
) -> None:
    """pip-install Linux wheels for the Lambda runtime into the staging folder."""
    extra = ["--find-links", str(find_links)] if find_links else []
    for platform in platforms or [LAMBDA_PLATFORM]:
        extra += ["--platform", platform]
    subprocess.run(
        [
            sys.executable, "-m", "pip", "install",
            "--quiet",
            "--target", str(staging),
            "--python-version", LAMBDA_PYTHON_VERSION,
            "--implementation", "cp",
            "--only-binary=:all:",
            "--no-compile",
            *extra,
            *requirements,
        ],
        check=True,
    )  # fmt: skip


def drop_console_scripts(staging: Path) -> None:
    """Remove bin/ (console-script launchers pip writes for the BUILD host).

    On Windows they are .exe files embedding the temp path, so they differ on every build,
    and Lambda never runs them. Their lines are removed from each RECORD too.
    """
    shutil.rmtree(staging / "bin", ignore_errors=True)
    for record in staging.glob("*.dist-info/RECORD"):
        lines = record.read_text(encoding="utf-8").splitlines(keepends=True)
        record.write_text("".join(x for x in lines if not x.startswith("../../bin/")),
                          encoding="utf-8")  # fmt: skip


def stage_dbt_project(staging: Path) -> None:
    """Copy the dbt project (no target/, logs/ or .duckdb files) to dbt_project/."""
    for part in DBT_PROJECT_PARTS:
        src, dst = ROOT / "dbt" / part, staging / DBT_PROJECT_DIR_IN_PACKAGE / part
        if src.is_dir():
            shutil.copytree(src, dst, ignore=shutil.ignore_patterns(*EXCLUDED_DIRS))
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)


def build_placeholder_wheel(out_dir: Path) -> None:
    """An empty py3-none-any wheel satisfying dbt-core's experimental-parser dependency."""
    version = importlib.metadata.version(PLACEHOLDER_DISTRIBUTION)
    name = PLACEHOLDER_DISTRIBUTION.replace("-", "_")
    dist_info = f"{name}-{version}.dist-info"
    files = {
        f"{dist_info}/METADATA": f"Metadata-Version: 2.1\nName: {PLACEHOLDER_DISTRIBUTION}\n"
        f"Version: {version}\nSummary: empty placeholder (Lambda build)\n",
        f"{dist_info}/WHEEL": "Wheel-Version: 1.0\nGenerator: build_lambda\n"
        "Root-Is-Purelib: true\nTag: py3-none-any\n",
    }
    files[f"{dist_info}/RECORD"] = "".join(f"{n},,\n" for n in files) + f"{dist_info}/RECORD,,\n"
    with zipfile.ZipFile(out_dir / f"{name}-{version}-py3-none-any.whl", "w") as zf:
        for entry, text in files.items():
            zf.writestr(zipfile.ZipInfo(entry, FIXED_ZIP_TIME), text)


def trim_babel_locales(staging: Path) -> None:
    """Keep only the root and English locale data (agate only parses en_US numbers)."""
    for path in (staging / "babel" / "locale-data").glob("*.dat"):
        if not path.name.startswith(KEPT_BABEL_LOCALES):
            path.unlink()


def stage_duckdb_extensions(staging: Path) -> None:
    """Linux extensions for the bundled DuckDB, in extension_directory layout (cached)."""
    version = "v" + importlib.metadata.version("duckdb")
    for name in DUCKDB_EXTENSIONS:
        cached = CACHE_DIR / "duckdb_extensions" / version / DUCKDB_PLATFORM / f"{name}.gz"
        if not cached.is_file():
            url = EXTENSION_URL.format(version=version, platform=DUCKDB_PLATFORM, name=name)
            # The extension CDN rejects urllib's default User-Agent. Extensions are
            # signed; DuckDB verifies the signature when it loads them.
            request = urllib.request.Request(url, headers={"User-Agent": "claims-etl-build"})
            with urllib.request.urlopen(request, timeout=300) as response:
                data = response.read()
            cached.parent.mkdir(parents=True, exist_ok=True)
            cached.write_bytes(data)
        out = staging / "duckdb_extensions" / version / DUCKDB_PLATFORM
        out.mkdir(parents=True, exist_ok=True)
        (out / f"{name}.duckdb_extension").write_bytes(gzip.decompress(cached.read_bytes()))


def write_deterministic_zip(staging: Path, zip_path: Path = ZIP_PATH) -> int:
    """Zip the staging folder with sorted entries and fixed timestamps; return file count."""
    files = sorted(
        p
        for p in staging.rglob("*")
        if p.is_file()
        and not (set(p.relative_to(staging).parts) & EXCLUDED_DIRS)
        and p.suffix not in EXCLUDED_SUFFIXES
    )
    BUILD_DIR.mkdir(exist_ok=True)
    zip_path.unlink(missing_ok=True)
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for path in files:
            info = zipfile.ZipInfo(path.relative_to(staging).as_posix(), FIXED_ZIP_TIME)
            info.external_attr = 0o644 << 16  # rw-r--r--, readable by the Lambda user
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, path.read_bytes())
    return len(files)


def build_transform(staging: Path) -> int:
    """Stage and zip the transform package; refuse to exceed Lambda's unzipped limit."""
    stage_sources(staging)
    stage_dbt_project(staging)
    with tempfile.TemporaryDirectory(prefix="claims_etl_wheels_") as wheels:
        build_placeholder_wheel(Path(wheels))
        pins = [f"{d}=={importlib.metadata.version(d)}" for d in DBT_DISTRIBUTIONS]
        install_vendored_dependencies(
            staging, [*VENDORED_REQUIREMENTS, *pins], Path(wheels), TRANSFORM_PLATFORMS
        )
    drop_console_scripts(staging)
    trim_babel_locales(staging)
    stage_duckdb_extensions(staging)
    unzipped = sum(p.stat().st_size for p in staging.rglob("*") if p.is_file())
    if unzipped >= LAMBDA_UNZIPPED_LIMIT:
        raise SystemExit(f"transform package is {unzipped / 2**20:.0f} MiB unzipped (limit 250)")
    print(f"transform package: {unzipped / 2**20:.0f} MiB unzipped")
    return write_deterministic_zip(staging, TRANSFORM_ZIP_PATH)


def main() -> None:
    """Build the requested zip and print a short summary."""
    parser = argparse.ArgumentParser(description="Build the Lambda deployment packages.")
    parser.add_argument("--transform", action="store_true", help="build the transform package")
    args = parser.parse_args()
    zip_path = TRANSFORM_ZIP_PATH if args.transform else ZIP_PATH
    # Stage outside the repo: OneDrive locks folders in the project tree mid-sync.
    with tempfile.TemporaryDirectory(prefix="claims_etl_lambda_") as tmp:
        staging = Path(tmp)
        if args.transform:
            count = build_transform(staging)
        else:
            stage_sources(staging)
            install_vendored_dependencies(staging, VENDORED_REQUIREMENTS)
            drop_console_scripts(staging)
            count = write_deterministic_zip(staging)
    size_kb = zip_path.stat().st_size / 1024
    print(f"built {zip_path.relative_to(ROOT)}: {count} files, {size_kb:.0f} KiB")


if __name__ == "__main__":
    main()
