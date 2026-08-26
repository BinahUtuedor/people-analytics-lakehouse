"""Build a reproducible, self-contained Python bundle for the Bronze EMR job."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIRECTORIES = ("config", "spark")
ENTRY_POINT = Path("spark/bronze/job.py")
DEFAULT_OUTPUT = PROJECT_ROOT / "dist" / "emr" / "bronze"
REQUIREMENTS = PROJECT_ROOT / "requirements-emr.txt"
ZIP_TIMESTAMP = (2020, 1, 1, 0, 0, 0)


def _source_files() -> list[Path]:
    """Return application source files in stable archive order."""

    files: list[Path] = []
    for directory in SOURCE_DIRECTORIES:
        files.extend(
            path
            for path in (PROJECT_ROOT / directory).rglob("*.py")
            if "__pycache__" not in path.parts
        )
    return sorted(files, key=lambda path: path.relative_to(PROJECT_ROOT).as_posix())


def _write_file(archive: zipfile.ZipFile, source: Path, name: str) -> None:
    """Write one file with stable metadata for reproducible hashes."""

    info = zipfile.ZipInfo(name, ZIP_TIMESTAMP)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o644 << 16
    archive.writestr(info, source.read_bytes())


def _install_dependencies(target: Path) -> None:
    """Install only EMR runtime dependencies into a staging directory."""

    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--disable-pip-version-check",
            "--no-compile",
            "--requirement",
            str(REQUIREMENTS),
            "--target",
            str(target),
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )


def build_bundle(output_directory: Path, *, include_dependencies: bool) -> dict[str, object]:
    """Build the entry point, Python archive, and checksum manifest."""

    output_directory = output_directory.resolve()
    output_directory.mkdir(parents=True, exist_ok=True)
    bundle_path = output_directory / "people-analytics-bronze.zip"
    entry_point_path = output_directory / "bronze-job.py"

    with tempfile.TemporaryDirectory(prefix="bronze-emr-") as temporary:
        dependency_root = Path(temporary) / "python"
        dependency_root.mkdir()
        if include_dependencies:
            _install_dependencies(dependency_root)

        with zipfile.ZipFile(bundle_path, "w") as archive:
            for source in _source_files():
                _write_file(
                    archive,
                    source,
                    source.relative_to(PROJECT_ROOT).as_posix(),
                )
            for dependency in sorted(
                dependency_root.rglob("*"),
                key=lambda path: path.relative_to(dependency_root).as_posix(),
            ):
                if dependency.is_file() and "__pycache__" not in dependency.parts:
                    _write_file(
                        archive,
                        dependency,
                        dependency.relative_to(dependency_root).as_posix(),
                    )

    shutil.copyfile(PROJECT_ROOT / ENTRY_POINT, entry_point_path)
    artifacts = (entry_point_path, bundle_path)
    manifest: dict[str, object] = {
        "entry_point": entry_point_path.name,
        "python_bundle": bundle_path.name,
        "dependencies_included": include_dependencies,
        "artifacts": {
            path.name: {
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size_bytes": path.stat().st_size,
            }
            for path in artifacts
        },
    }
    manifest_path = output_directory / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def parse_args() -> argparse.Namespace:
    """Parse build options."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--skip-dependencies",
        action="store_true",
        help="Build a source-only archive for offline structural validation.",
    )
    return parser.parse_args()


def main() -> None:
    """Build and report the deployment artifact without publishing it."""

    args = parse_args()
    manifest = build_bundle(
        args.output,
        include_dependencies=not args.skip_dependencies,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
