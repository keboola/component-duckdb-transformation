import unittest

from configuration import Configuration
from versions import DEFAULT_VERSION, EXPERIMENTAL_VERSIONS, LATEST_ALIAS, SUPPORTED_VERSIONS, resolve_version


class TestVersions(unittest.TestCase):
    def test_latest_resolves_to_highest_stable_version(self):
        self.assertEqual(resolve_version(LATEST_ALIAS), "1.5.2")
        self.assertEqual(DEFAULT_VERSION, "1.5.2")

    def test_experimental_version_is_never_default(self):
        self.assertIn("2.0.0", EXPERIMENTAL_VERSIONS)
        self.assertNotIn(DEFAULT_VERSION, EXPERIMENTAL_VERSIONS)

    def test_experimental_version_is_supported_and_selectable(self):
        self.assertLessEqual(EXPERIMENTAL_VERSIONS, SUPPORTED_VERSIONS)
        self.assertEqual(resolve_version("2.0.0"), "2.0.0")
        self.assertEqual(Configuration(duckdb_version="2.0.0").duckdb_version, "2.0.0")
