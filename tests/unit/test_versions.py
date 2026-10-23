import unittest

from configuration import Configuration
from versions import DEFAULT_VERSION, LATEST_ALIAS, resolve_version


class TestVersions(unittest.TestCase):
    def test_latest_resolves_to_highest_version(self):
        self.assertEqual(DEFAULT_VERSION, "2.0.0")
        self.assertEqual(resolve_version(LATEST_ALIAS), "2.0.0")

    def test_pinned_versions_are_selectable(self):
        for version in ("2.0.0", "1.5.2", "1.4.4"):
            self.assertEqual(resolve_version(version), version)
            self.assertEqual(Configuration(duckdb_version=version).duckdb_version, version)
