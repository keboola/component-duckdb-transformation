"""Validates DuckDB version consistency across uv.lock, src/versions.py, and Dockerfile.

Run locally : PYTHONPATH=src uv run python scripts/check_versions.py
Run in CI   : pre-check job (no Docker needed)
"""

import subprocess
import sys
from pathlib import Path

from versions import DEFAULT_VERSION, SUPPORTED_VERSIONS, venv_name


def get_locked_versions() -> set[str]:
    """Query uv for all duckdb versions resolved across all dependency groups."""
    result = subprocess.run(
        ["uv", "tree", "--package", "duckdb", "--universal", "--all-groups"],
        capture_output=True,
        text=True,
        check=True,
    )
    return {
        line.split()[1].lstrip("v")
        for line in result.stdout.splitlines()
        if line.split() and line.split()[0] == "duckdb"
    }


def main() -> int:
    locked = get_locked_versions()
    dockerfile = Path("Dockerfile").read_text()
    fail = False

    print("Checking version consistency...")
    print(f"  uv.lock duckdb versions : {' '.join(sorted(locked))}")
    print(f"  versions.py supported   : {' '.join(sorted(SUPPORTED_VERSIONS))}")
    print(f"  versions.py default     : {DEFAULT_VERSION}")

    # Every version in versions.py must be in the lockfile.
    for version in sorted(SUPPORTED_VERSIONS - locked):
        print(f"FAIL: duckdb=={version} is in versions.py but not in uv.lock")
        print("      Run 'uv lock' to update the lockfile.")
        fail = True

    # Every locked version must be registered in versions.py.
    for version in sorted(locked - SUPPORTED_VERSIONS):
        print(f"FAIL: duckdb=={version} is in uv.lock but not in versions.py")
        print("      Add it to VENV_NAMES in src/versions.py.")
        fail = True

    # Every locked version must have a venv RUN block in the Dockerfile.
    for version in sorted(locked):
        name = venv_name(version)
        if name not in dockerfile:
            print(f"FAIL: venv '{name}' for duckdb=={version} has no RUN block in Dockerfile")
            print(f"      Add UV_PROJECT_ENVIRONMENT=.../{name} to the base stage.")
            fail = True

    if DEFAULT_VERSION not in SUPPORTED_VERSIONS:
        print(f"FAIL: DEFAULT_VERSION={DEFAULT_VERSION} is not in SUPPORTED_VERSIONS")
        fail = True

    if fail:
        return 1

    print(f"OK: all {len(SUPPORTED_VERSIONS)} duckdb version references are consistent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
