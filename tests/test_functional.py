import os
import subprocess
import sys
from os import path
import unittest

from keboola.datadirtest import DataDirTester, TestDataDir
from freezegun import freeze_time


class LauncherTestDataDir(TestDataDir):
    """Runs the component through launcher.py via subprocess.

    This ensures the full launcher → version routing → component code path is tested.
    In dev (no /opt/venvs), the launcher falls back to sys.executable (DuckDB 1.5.1).
    In Docker, the launcher routes to the correct DuckDB venv.
    """

    def run_component(self):
        env = os.environ.copy()
        env["KBC_DATADIR"] = self.source_data_dir
        result = subprocess.run(
            [sys.executable, "-u", self.component_script],
            env=env,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"Component failed with exit code {result.returncode}:\n{result.stderr}"
            )


class TestComponent(unittest.TestCase):
    @freeze_time("2023-04-02")
    def test_functional(self):
        os.environ["KBC_DATA_TYPE_SUPPORT"] = "none"
        base_dir = path.dirname(__file__)
        launcher_script = path.abspath(path.join(base_dir, "..", "src", "launcher.py"))
        functional_tests = DataDirTester(
            data_dir=path.join(base_dir, "functional"),
            component_script=launcher_script,
            test_data_dir_class=LauncherTestDataDir,
        )
        functional_tests.run()


if __name__ == "__main__":
    unittest.main()