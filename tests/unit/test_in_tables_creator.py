import json
import shutil
import tempfile
import unittest
from collections import OrderedDict
from pathlib import Path

import duckdb
from keboola.component import CommonInterface
from keboola.component.dao import ColumnDefinition
from keboola.component.exceptions import UserException

from in_tables_creator import FileType, LocalTableCreator


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


class TestEmptyParquetTable(unittest.TestCase):
    """An input table with 0 rows is exported without any Parquet file (SUPPORT-18012)."""

    MANIFEST = {
        "id": "in.c-test.empty",
        "columns": ["id", "name", "qty", "price", "flag", "day", "updated", "untyped"],
        "column_metadata": {
            "id": [{"key": "KBC.datatype.basetype", "value": "INTEGER"}],
            "name": [{"key": "KBC.datatype.basetype", "value": "STRING"}],
            "qty": [
                {"key": "KBC.datatype.basetype", "value": "NUMERIC"},
                {"key": "KBC.datatype.length", "value": "38,9"},
            ],
            "price": [{"key": "KBC.datatype.basetype", "value": "FLOAT"}],
            "flag": [{"key": "KBC.datatype.basetype", "value": "BOOLEAN"}],
            "day": [{"key": "KBC.datatype.basetype", "value": "DATE"}],
            "updated": [{"key": "KBC.datatype.basetype", "value": "TIMESTAMP"}],
            "untyped": [],
        },
    }

    def _table_definition(self, manifest: dict, table_dir: str = "empty_table", as_file: bool = False):
        """Build a real TableDefinition through CommonInterface from a temporary data dir."""
        data_dir = Path(tempfile.mkdtemp())
        tables_dir = data_dir / "in" / "tables"
        tables_dir.mkdir(parents=True)
        (data_dir / "out" / "tables").mkdir(parents=True)
        if as_file:
            (tables_dir / table_dir).write_text("")  # non-sliced single file, not a folder
        else:
            (tables_dir / table_dir).mkdir()  # sliced folder with no Parquet file inside
        (tables_dir / f"{table_dir}.manifest").write_text(json.dumps(manifest))
        (data_dir / "config.json").write_text(json.dumps({"parameters": {}, "storage": {}}))
        self.addCleanup(shutil.rmtree, data_dir, True)
        return CommonInterface(data_folder_path=str(data_dir)).get_input_tables_definitions()[0]

    def test_empty_parquet_folder_creates_empty_table_from_manifest(self):
        in_table = self._table_definition(self.MANIFEST)
        connection = duckdb.connect()
        self.addCleanup(connection.close)

        result = LocalTableCreator(connection, dtypes_infer=True).create_table(
            in_table, table_name="empty_table", file_type=FileType.PARQUET
        )

        self.assertFalse(result.is_view)
        schema = {row[0]: row[1] for row in connection.execute("DESCRIBE TABLE 'empty_table'").fetchall()}
        self.assertEqual(
            schema,
            {
                "id": "BIGINT",  # INTEGER -> BIGINT, same as the cast applied to non-empty Parquet
                "name": "VARCHAR",
                "qty": "DECIMAL(38,9)",
                "price": "DOUBLE",
                "flag": "BOOLEAN",
                "day": "DATE",
                "updated": "TIMESTAMP",
                "untyped": "VARCHAR",
            },
        )
        self.assertEqual(connection.execute("SELECT count(*) FROM 'empty_table'").fetchone()[0], 0)

    def test_empty_parquet_folder_without_columns_raises_user_exception(self):
        in_table = self._table_definition({"id": "in.c-test.empty", "columns": [], "column_metadata": {}})
        connection = duckdb.connect()
        self.addCleanup(connection.close)

        with self.assertRaises(UserException):
            LocalTableCreator(connection, dtypes_infer=True).create_table(
                in_table, table_name="empty_table", file_type=FileType.PARQUET
            )

    def test_parquet_folder_with_data_is_unchanged(self):
        """Regression: the non-empty Parquet path must keep reading the files."""
        in_table = self._table_definition(self.MANIFEST, table_dir="full_table")
        writer = duckdb.connect()
        writer.execute(
            f"COPY (SELECT 1 AS id, 'a' AS name, 1.5 AS qty, 2.5 AS price, true AS flag, "
            f"DATE '2024-01-01' AS day, TIMESTAMP '2024-01-01 00:00:00' AS updated, 'x' AS untyped) "
            f"TO '{in_table.full_path}/part.parquet' (FORMAT PARQUET)"
        )
        writer.close()
        connection = duckdb.connect()
        self.addCleanup(connection.close)

        LocalTableCreator(connection, dtypes_infer=True).create_table(
            in_table, table_name="full_table", file_type=FileType.PARQUET
        )

        self.assertEqual(connection.execute("SELECT count(*) FROM 'full_table'").fetchone()[0], 1)
        schema = {row[0]: row[1] for row in connection.execute("DESCRIBE TABLE 'full_table'").fetchall()}
        self.assertEqual(schema["id"], "BIGINT")
        self.assertEqual(schema["name"], "VARCHAR")

    def test_hidden_parquet_file_is_still_read(self):
        """The existence check must use DuckDB glob semantics, which include hidden files."""
        in_table = self._table_definition(self.MANIFEST, table_dir="hidden_table")
        writer = duckdb.connect()
        writer.execute(
            f"COPY (SELECT 1 AS id, 'a' AS name, 1.5 AS qty, 2.5 AS price, true AS flag, "
            f"DATE '2024-01-01' AS day, TIMESTAMP '2024-01-01 00:00:00' AS updated, 'x' AS untyped) "
            f"TO '{in_table.full_path}/.part.parquet' (FORMAT PARQUET)"
        )
        writer.close()
        connection = duckdb.connect()
        self.addCleanup(connection.close)

        LocalTableCreator(connection, dtypes_infer=True).create_table(
            in_table, table_name="hidden_table", file_type=FileType.PARQUET
        )

        self.assertEqual(connection.execute("SELECT count(*) FROM 'hidden_table'").fetchone()[0], 1)

    def test_non_sliced_parquet_input_still_fails_loudly(self):
        """A single-file (non-sliced) Parquet input must not be turned into an empty table."""
        in_table = self._table_definition(self.MANIFEST, table_dir="single_file", as_file=True)
        connection = duckdb.connect()
        self.addCleanup(connection.close)

        with self.assertRaises(UserException):
            LocalTableCreator(connection, dtypes_infer=True).create_table(
                in_table, table_name="single_file", file_type=FileType.PARQUET
            )

    def test_rows_count_above_zero_without_files_fails_loudly(self):
        """A non-empty table with no data files is a broken staging, not an empty table."""
        manifest = dict(self.MANIFEST, rows_count=5)
        in_table = self._table_definition(manifest, table_dir="missing_data")
        connection = duckdb.connect()
        self.addCleanup(connection.close)

        with self.assertRaises(UserException):
            LocalTableCreator(connection, dtypes_infer=True).create_table(
                in_table, table_name="missing_data", file_type=FileType.PARQUET
            )

    def test_rows_count_zero_creates_empty_table(self):
        in_table = self._table_definition(dict(self.MANIFEST, rows_count=0), table_dir="zero_rows")
        connection = duckdb.connect()
        self.addCleanup(connection.close)

        LocalTableCreator(connection, dtypes_infer=True).create_table(
            in_table, table_name="zero_rows", file_type=FileType.PARQUET
        )

        self.assertEqual(connection.execute("SELECT count(*) FROM 'zero_rows'").fetchone()[0], 0)

    def test_column_name_with_quote_is_escaped(self):
        """A manifest column name must never be able to inject SQL."""
        manifest = {
            "id": "in.c-test.empty",
            "columns": ['evil" INTEGER); DROP TABLE victim; --'],
            "column_metadata": {'evil" INTEGER); DROP TABLE victim; --': []},
        }
        in_table = self._table_definition(manifest, table_dir="injection")
        connection = duckdb.connect()
        self.addCleanup(connection.close)
        connection.execute("CREATE TABLE victim (x INTEGER)")

        LocalTableCreator(connection, dtypes_infer=True).create_table(
            in_table, table_name="injection", file_type=FileType.PARQUET
        )

        self.assertEqual(connection.execute("SELECT count(*) FROM victim").fetchone()[0], 0)
        schema = [row[0] for row in connection.execute("DESCRIBE TABLE 'injection'").fetchall()]
        self.assertEqual(schema, ['evil" INTEGER); DROP TABLE victim; --'])

    def test_decimal_length_handling(self):
        """Lengths DuckDB cannot represent fall back to the default DECIMAL."""
        manifest = {
            "id": "in.c-test.empty",
            "columns": ["too_wide", "no_length", "lowercase_type"],
            "column_metadata": {
                "too_wide": [
                    {"key": "KBC.datatype.basetype", "value": "NUMERIC"},
                    {"key": "KBC.datatype.length", "value": "76,38"},
                ],
                "no_length": [{"key": "KBC.datatype.basetype", "value": "NUMERIC"}],
                "lowercase_type": [{"key": "KBC.datatype.basetype", "value": "integer"}],
            },
        }
        in_table = self._table_definition(manifest, table_dir="decimals")
        connection = duckdb.connect()
        self.addCleanup(connection.close)

        LocalTableCreator(connection, dtypes_infer=True).create_table(
            in_table, table_name="decimals", file_type=FileType.PARQUET
        )

        schema = {row[0]: row[1] for row in connection.execute("DESCRIBE TABLE 'decimals'").fetchall()}
        self.assertEqual(
            schema,
            {"too_wide": "DECIMAL(38,9)", "no_length": "DECIMAL(38,9)", "lowercase_type": "BIGINT"},
        )
