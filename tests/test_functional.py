import os
from os import path
import unittest

from datadirtest import DataDirTester
from freezegun import freeze_time


class TestComponent(unittest.TestCase):
    @freeze_time("2023-04-02")
    def test_functional(self):
        os.environ["KBC_DATA_TYPE_SUPPORT"] = "none"
        base_dir = path.dirname(__file__)
        functional_tests = DataDirTester(data_dir=path.join(base_dir, "functional"))
        functional_tests.run()


if __name__ == "__main__":
    unittest.main()