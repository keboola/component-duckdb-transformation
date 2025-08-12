import os
from os import path
import unittest

from datadirtest import DataDirTester
from freezegun import freeze_time


class TestComponent(unittest.TestCase):
    @freeze_time("2023-04-02")
    def test_functional_simple(self):
        os.environ["KBC_DATA_TYPE_SUPPORT"] = "none"
        base_dir = path.dirname(__file__)
        functional_tests = DataDirTester(data_dir=path.join(base_dir, "functional", "simple"))
        functional_tests.run()

    @freeze_time("2023-04-02")
    def test_functional_types(self):
        os.environ["KBC_DATA_TYPE_SUPPORT"] = "authoritative"
        base_dir = path.dirname(__file__)
        functional_tests = DataDirTester(data_dir=path.join(base_dir, "functional", "dtypes"))
        functional_tests.run()
    
    @freeze_time("2023-04-02")
    def test_functional_execution_plan(self):
        os.environ["KBC_DATA_TYPE_SUPPORT"] = "none"
        base_dir = path.dirname(__file__)
        functional_tests = DataDirTester(data_dir=path.join(base_dir, "functional", "sync_action_execution_plan"))
        functional_tests.run()

    @freeze_time("2023-04-02")
    def test_functional_parquet(self):
        os.environ["KBC_DATA_TYPE_SUPPORT"] = "none"
        base_dir = path.dirname(__file__)
        functional_tests = DataDirTester(data_dir=path.join(base_dir, "functional", "simple_parquet"))
        functional_tests.run()


if __name__ == "__main__":
    unittest.main()