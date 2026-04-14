import os
import subprocess
import sys
from pathlib import Path

import pytest
from freezegun import freeze_time
from keboola.datadirtest import DataDirTester, TestDataDir

from versions import SUPPORTED_VERSIONS, VENV_BASE, venv_name


class LauncherTestDataDir(TestDataDir):
    """Runs the component through launcher.py via subprocess.

    This ensures the full launcher -> version routing -> component code path is tested.
    In dev (no /code/.venvs), the launcher falls back to sys.executable (DuckDB 1.5.1).
    In Docker, the launcher routes to the correct DuckDB venv.
    """

    def run_component(self):
        # Warn when version-specific venvs are missing (local dev without Docker)
        missing = [
            venv_name(v)
            for v in SUPPORTED_VERSIONS
            if not (Path(VENV_BASE) / venv_name(v) / "bin" / "python").is_file()
        ]
        if missing:
            pytest.skip(f"Version venv(s) not found (run inside Docker): {missing}")

        env = os.environ.copy()
        env["KBC_DATADIR"] = self.source_data_dir
        result = subprocess.run(
            [sys.executable, "-u", self.component_script],
            env=env,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Component failed with exit code {result.returncode}:\n{result.stderr}")


class TestFunctional:
    @freeze_time("2023-04-02")
    def test_functional(self):
        os.environ["KBC_DATA_TYPE_SUPPORT"] = "none"
        base_dir = Path(__file__).parent
        launcher_script = (base_dir / ".." / "src" / "launcher.py").resolve()
        functional_tests = DataDirTester(
            data_dir=str(base_dir / "functional"),
            component_script=str(launcher_script),
            test_data_dir_class=LauncherTestDataDir,
        )
        functional_tests.run()
