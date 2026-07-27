import unittest

from keboola.component.dao import SupportedDataTypes

from component import Component


class TestDuckdbTypeToBaseType(unittest.TestCase):
    """Verify that DuckDB output column types (from DESCRIBE) are mapped to Keboola
    BaseType with their precision/scale (and length) propagated into the manifest."""

    def _assert(self, duckdb_type: str, expected_dtype: SupportedDataTypes, expected_length):
        base_type = Component.duckdb_type_to_base_type(duckdb_type)
        data_type = base_type["base"]
        self.assertEqual(data_type.dtype, expected_dtype, msg=f"dtype for {duckdb_type!r}")
        self.assertEqual(data_type.length, expected_length, msg=f"length for {duckdb_type!r}")

    def test_decimal_precision_scale_propagated(self):
        # The customer's exact case: NUMERIC(1,0) must not collapse to NUMBER(38,9).
        self._assert("DECIMAL(1,0)", SupportedDataTypes.NUMERIC, "1,0")

    def test_decimal_with_scale(self):
        self._assert("DECIMAL(10,2)", SupportedDataTypes.NUMERIC, "10,2")

    def test_numeric_alias(self):
        self._assert("NUMERIC(5,3)", SupportedDataTypes.NUMERIC, "5,3")

    def test_varchar_length_parsed_when_present(self):
        # Parser correctness even though DuckDB rarely emits VARCHAR(n).
        self._assert("VARCHAR(2)", SupportedDataTypes.STRING, "2")

    def test_plain_varchar_has_no_length(self):
        self._assert("VARCHAR", SupportedDataTypes.STRING, None)

    def test_integer_family(self):
        self._assert("INTEGER", SupportedDataTypes.INTEGER, None)
        self._assert("BIGINT", SupportedDataTypes.INTEGER, None)

    def test_double_maps_to_float(self):
        self._assert("DOUBLE", SupportedDataTypes.FLOAT, None)

    def test_boolean(self):
        self._assert("BOOLEAN", SupportedDataTypes.BOOLEAN, None)

    def test_date(self):
        self._assert("DATE", SupportedDataTypes.DATE, None)

    def test_timestamp(self):
        self._assert("TIMESTAMP", SupportedDataTypes.TIMESTAMP, None)
        self._assert("TIMESTAMP WITH TIME ZONE", SupportedDataTypes.TIMESTAMP, None)

    def test_timestamp_precision_variants(self):
        # The customer's case: TIMESTAMP_S came out as VARCHAR(16777216) because the
        # sub-second precision variants were not recognized and fell back to STRING.
        self._assert("TIMESTAMP_S", SupportedDataTypes.TIMESTAMP, None)
        self._assert("TIMESTAMP_MS", SupportedDataTypes.TIMESTAMP, None)
        self._assert("TIMESTAMP_NS", SupportedDataTypes.TIMESTAMP, None)

    def test_complex_type_falls_back_to_string_without_length(self):
        # STRUCT(...) / arrays must not leak their inner definition into `length`.
        self._assert("STRUCT(a INTEGER)", SupportedDataTypes.STRING, None)
        self._assert("INTEGER[]", SupportedDataTypes.STRING, None)


if __name__ == "__main__":
    unittest.main()
