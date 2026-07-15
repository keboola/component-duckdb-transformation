import unittest
from collections import OrderedDict

from keboola.component.dao import ColumnDefinition

from in_tables_creator import LocalTableCreator


class FakeTable:
    def __init__(self, schema):
        self.schema = schema


def _column(data_type: dict | None = None) -> ColumnDefinition:
    col: dict = {"name": "c"}
    if data_type is not None:
        col["data_type"] = data_type
    return ColumnDefinition().from_dict(col)


class TestGetDataTypes(unittest.TestCase):
    def setUp(self):
        self.creator = LocalTableCreator(connection=None, dtypes_infer=False)

    def test_infer_returns_none(self):
        creator = LocalTableCreator(connection=None, dtypes_infer=True)
        schema = OrderedDict(a=_column({"base": {"type": "INTEGER"}}))
        self.assertIsNone(creator._get_data_types(FakeTable(schema)))

    def test_all_base_types(self):
        schema = OrderedDict(
            a=_column({"base": {"type": "INTEGER"}}),
            b=_column({"base": {"type": "STRING"}}),
        )
        self.assertEqual(self.creator._get_data_types(FakeTable(schema)), {"a": "INTEGER", "b": "STRING"})

    def test_missing_base_column_defaults_to_varchar(self):
        # Regression: column without a "base" data type must not raise
        # "'NoneType' object has no attribute 'dtype'"; it defaults to VARCHAR.
        schema = OrderedDict(
            a=_column({"base": {"type": "INTEGER"}}),
            b=_column(),  # no data_type -> data_types == {}
            c=_column({"snowflake": {"type": "VARCHAR"}}),  # backend-only, no base
        )
        self.assertEqual(
            self.creator._get_data_types(FakeTable(schema)),
            {"a": "INTEGER", "b": "VARCHAR", "c": "VARCHAR"},
        )

    def test_non_typed_table_all_varchar(self):
        # Non-typed table (all columns without a base type) must load as VARCHAR,
        # matching how Keboola stores non-typed columns.
        schema = OrderedDict(
            a=_column(),
            b=_column({"snowflake": {"type": "VARCHAR"}}),
        )
        self.assertEqual(
            self.creator._get_data_types(FakeTable(schema)),
            {"a": "VARCHAR", "b": "VARCHAR"},
        )


if __name__ == "__main__":
    unittest.main()
