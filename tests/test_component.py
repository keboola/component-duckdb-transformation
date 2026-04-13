import os
import unittest
from unittest import mock

from freezegun import freeze_time

from component import Component


class TestComponent(unittest.TestCase):
    @freeze_time("2010-10-10")
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()
