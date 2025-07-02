import os
import unittest

from datadirtest import DataDirTester
from freezegun import freeze_time


class TestComponent(unittest.TestCase):
    @freeze_time("2023-04-02")
    def test_functional(self):
        os.environ["KBC_DATA_TYPE_SUPPORT"] = "none"
        functional_tests = DataDirTester()
        functional_tests.run()

    @freeze_time("2023-04-02")
    def test_functional_types(self):
        os.environ["KBC_DATA_TYPE_SUPPORT"] = "authoritative"
        functional_tests = DataDirTester(data_dir="./tests/functional_dtypes")
        functional_tests.run()


if __name__ == "__main__":
    unittest.main()
