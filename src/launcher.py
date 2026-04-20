"""Launcher that selects the correct DuckDB venv before running the component."""

import json
import os
import sys
from pathlib import Path

from versions import LATEST_ALIAS, SUPPORTED_VERSIONS, VENV_BASE, resolve_version, venv_name


def get_duckdb_version() -> str:
    """Read duckdb_version from config.json, return LATEST_ALIAS on any failure."""
    data_dir = os.environ.get("KBC_DATADIR", "/data")
    config_path = Path(data_dir) / "config.json"
    try:
        with open(config_path) as f:
            version = json.load(f).get("parameters", {}).get("duckdb_version", LATEST_ALIAS)
        if version == LATEST_ALIAS or version in SUPPORTED_VERSIONS:
            return version
    except (OSError, json.JSONDecodeError, TypeError, KeyError):
        pass
    return LATEST_ALIAS


def main() -> None:
    version = resolve_version(get_duckdb_version())
    venv_python = Path(VENV_BASE) / venv_name(version) / "bin" / "python"
    component = Path(__file__).parent / "component.py"
    target = str(venv_python) if venv_python.is_file() else sys.executable
    os.execv(target, [target, "-u", str(component)])


if __name__ == "__main__":
    main()
