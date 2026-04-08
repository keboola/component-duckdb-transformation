"""Launcher that selects the correct DuckDB venv before running the component."""

import json
import os
import sys

SUPPORTED_VERSIONS = {"1.5.1", "1.4.4"}
DEFAULT_VERSION = "1.5.1"
VENV_BASE = "/opt/venvs"


def get_duckdb_version():
    """Read duckdb_version from config.json, return DEFAULT_VERSION on any failure."""
    data_dir = os.environ.get("KBC_DATADIR", "/data")
    config_path = os.path.join(data_dir, "config.json")
    try:
        with open(config_path) as f:
            version = json.load(f).get("parameters", {}).get("duckdb_version", DEFAULT_VERSION)
        if version in SUPPORTED_VERSIONS:
            return version
    except (OSError, json.JSONDecodeError, TypeError, KeyError):
        pass
    return DEFAULT_VERSION


def main():
    version = get_duckdb_version()
    venv_python = os.path.join(VENV_BASE, f"duckdb-{version}", "bin", "python")
    component = os.path.join(os.path.dirname(os.path.abspath(__file__)), "component.py")
    target = venv_python if os.path.isfile(venv_python) else sys.executable
    os.execv(target, [target, "-u", component])


if __name__ == "__main__":
    main()
