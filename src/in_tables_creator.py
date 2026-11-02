"""Local file table creator."""

import logging
import os
import re
from dataclasses import dataclass
from enum import StrEnum

import duckdb
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException


class FileType(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"


# Keboola base data type -> DuckDB type used when an input table has no data file.
# INTEGER maps to BIGINT to match the cast applied to Parquet files that do contain data
# (see _get_columns_to_cast).
BASE_TYPE_TO_DUCKDB: dict[str, str] = {
    "INTEGER": "BIGINT",
    "STRING": "VARCHAR",
    "NUMERIC": "DECIMAL",
    "FLOAT": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
}
DEFAULT_DUCKDB_TYPE = "VARCHAR"
# Keboola's default precision/scale for NUMERIC, used when the manifest carries no length.
DEFAULT_DECIMAL_TYPE = "DECIMAL(38,9)"
# Accepts "38" or "38,9" only, so a manifest value can never reach SQL unchecked.
DECIMAL_LENGTH_RE = re.compile(r"^(\d+)(?:,(\d+))?$")
# DuckDB rejects a DECIMAL wider than 38 digits.
MAX_DECIMAL_WIDTH = 38


@dataclass
class CreatedTable:
    """Information about created table/view."""

    name: str
    is_view: bool


class LocalTableCreator:
    """Create tables from local files (CSV, Parquet)."""

    def __init__(self, connection, dtypes_infer=True):
        self.connection = connection
        self.logger = logging.getLogger(self.__class__.__name__)
        self.dtypes_infer = dtypes_infer

    def create_table(
        self, in_table: TableDefinition, table_name: str, file_type: FileType = FileType.CSV
    ) -> CreatedTable:
        """Create table from local file.

        Args:
            in_table: Table definition with file path and metadata.
            table_name: Name for the DuckDB table/view (from input mapping destination).
            file_type: File type from input mapping.
        """
        self.logger.debug(f"Processing local file for table: {table_name}")
        # Get data types
        dtype = self._get_data_types(in_table)
        # Get local file path
        path = self._get_local_file_path(in_table, file_type)
        # Create table based on file_type from input mapping
        if file_type == FileType.PARQUET:
            return self._create_table_from_parquet(table_name, in_table, path)
        else:
            try:
                return self._create_view_from_csv(table_name, in_table, path, dtype)
            except duckdb.IOException as e:
                raise UserException(f"Unsupported file type for table {table_name}, error: {e}")

    def _get_local_file_path(self, in_table: TableDefinition, file_type: FileType = FileType.CSV) -> str:
        """Get the appropriate file path for local file processing."""
        if file_type == FileType.PARQUET:
            path = f"{in_table.full_path}/*.parquet"
            self.logger.debug(f"Using hive-partitioned parquet path pattern: {path}")
        elif in_table.is_sliced:
            path = f"{in_table.full_path}/*.csv"
            self.logger.debug(f"Using sliced path pattern: {path}")
        else:
            path = in_table.full_path
            self.logger.debug(f"Using direct path: {path}")
        return path

    def _get_data_types(self, in_table: TableDefinition) -> dict | None:
        """Get data types for table creation.

        Columns that expose a "base" data type are pinned to it. Columns without a
        usable base type (e.g. non-typed tables, or manifests that only carry
        backend-specific types) default to VARCHAR, mirroring how Keboola stores
        non-typed columns, instead of being force-cast or left to DuckDB's
        value-based inference (which could, for example, read a text column of
        date-like values as DATE and break downstream string handling).
        """
        if self.dtypes_infer:
            self.logger.debug("Using automatic dtype inference")
            return None

        dtype = {}
        for key, value in in_table.schema.items():
            data_types = value.data_types if isinstance(value.data_types, dict) else {}
            base_type = data_types.get("base")
            if base_type is not None and base_type.dtype:
                dtype[key] = base_type.dtype
            else:
                dtype[key] = "VARCHAR"

        if not dtype:
            self.logger.debug("Empty schema, using automatic dtype inference")
            return None

        self.logger.debug(f"Using custom dtypes: {dtype}")
        return dtype

    def _create_table_from_parquet(self, table_name: str, in_table: TableDefinition, path) -> CreatedTable:
        """Create table from Parquet files with optional type casting."""
        self.logger.debug(f"Creating table from Parquet files: {table_name}")
        # Check if type casting is needed for Snowflake INTEGER columns
        to_cast = self._get_columns_to_cast(in_table)
        # A table with 0 rows is exported without any Parquet file, so read_parquet would
        # fail with "No files found that match the pattern". Build the empty table from the
        # manifest columns instead. (CSV is not affected: an empty table still gets a
        # header-only CSV file.)
        if not self._parquet_files_exist(path):
            if not self._input_is_empty(in_table):
                # Not the empty-table case (for example a partial or failed staging):
                # keep failing loudly instead of silently producing a table with no rows.
                raise UserException(
                    f"No Parquet files found for input table {table_name} at {in_table.full_path}, "
                    f"but the table is not empty. The input data is incomplete, please run the job again."
                )
            return self._create_empty_table_from_manifest(table_name, in_table, to_cast)
        if to_cast:
            return self._create_parquet_table_with_casting(table_name, path, to_cast)
        else:
            return self._create_parquet_table_without_casting(table_name, path)

    def _parquet_files_exist(self, path) -> bool:
        """Report whether any file matches the Parquet path pattern.

        Uses DuckDB's own glob() so the result matches exactly what read_parquet() sees
        (Python's glob, for example, skips hidden files that DuckDB would read).
        """
        matched = self.connection.execute("SELECT 1 FROM glob(?) LIMIT 1", [str(path)]).fetchone()
        self.logger.debug(f"Files matching {path}: {matched is not None}")
        return matched is not None

    def _input_is_empty(self, in_table: TableDefinition) -> bool:
        """Report whether the input table holds 0 rows and therefore has no data file.

        The manifest row count is authoritative. Manifests that do not carry one fall back
        to "the sliced folder exists but holds no Parquet file".
        """
        rows_count = getattr(in_table, "rows_count", None)
        if rows_count is not None:
            try:
                return int(rows_count) == 0
            except (TypeError, ValueError):
                self.logger.warning(f"Manifest row count {rows_count!r} is not a number, ignoring it.")
        return os.path.isdir(in_table.full_path)

    def _get_columns_to_cast(self, in_table: TableDefinition) -> list[str]:
        """Get list of columns that need to be cast to BIGINT."""
        """
        Snowflake keeps integers as NUMBER(38,0): https://docs.snowflake.com/en/sql-reference/data-types-numeric#int-integer-bigint-smallint-tinyint-byteint  # noqa: E501
        and exports them to Parquet as DECIMAL(38,0) which negatively impacts performance in DuckDB: https://duckdb.org/docs/stable/sql/data_types/numeric.html#fixed-point-decimals  # noqa: E501
        based on the KBC column metadata we are casting such columns to BIGINT.
        """
        to_cast = [
            k for k, v in in_table.table_metadata.column_metadata.items() if v.get("KBC.datatype.basetype") == "INTEGER"
        ]
        self.logger.debug(f"Columns to cast to BIGINT: {to_cast}")
        return to_cast

    def _create_parquet_table_with_casting(self, table_name: str, path, to_cast: list[str]) -> CreatedTable:
        """Create Parquet table with type casting for INTEGER columns."""
        self.logger.debug("Processing Parquet with type casting")
        rel = self.connection.sql(f"FROM read_parquet('{path}')")
        cast_exprs = []
        for col in rel.columns:
            if col in to_cast:
                cast_exprs.append(f'CAST("{col}" AS BIGINT) AS "{col}"')
            else:
                cast_exprs.append(f'"{col}"')
        select_clause = ", ".join(cast_exprs)
        self.connection.execute(
            f"CREATE OR REPLACE TABLE '{table_name}' AS SELECT {select_clause} FROM read_parquet('{path}')"
        )
        return CreatedTable(name=table_name, is_view=False)

    def _create_parquet_table_without_casting(self, table_name: str, path) -> CreatedTable:
        """Create Parquet table without type casting."""
        self.logger.debug("Processing Parquet without type casting")
        self.connection.execute(f"CREATE OR REPLACE TABLE '{table_name}' AS FROM read_parquet('{path}')")
        return CreatedTable(name=table_name, is_view=False)

    def _create_empty_table_from_manifest(
        self, table_name: str, in_table: TableDefinition, to_cast: list[str]
    ) -> CreatedTable:
        """Create an empty table from the manifest column definitions.

        Used when the input table has no Parquet file because it holds 0 rows.
        """
        columns = self._get_duckdb_column_types(in_table, to_cast)
        if not columns:
            raise UserException(
                f"Input table {table_name} has no Parquet files and no columns in its manifest, "
                f"so the table cannot be created."
            )
        self.logger.warning(
            f"Input table {table_name} contains no Parquet files (the source table is empty). "
            f"Creating an empty table from the manifest columns: {list(columns)}"
        )
        column_clause = ", ".join(f'"{self._quote_identifier(name)}" {dtype}' for name, dtype in columns.items())
        self.connection.execute(f"CREATE OR REPLACE TABLE '{table_name}' ({column_clause})")
        return CreatedTable(name=table_name, is_view=False)

    def _get_duckdb_column_types(self, in_table: TableDefinition, to_cast: list[str]) -> dict[str, str]:
        """Map the manifest columns to DuckDB column types."""
        column_metadata = in_table.table_metadata.column_metadata or {}
        columns: dict[str, str] = {}
        for name, value in in_table.schema.items():
            data_types = value.data_types if isinstance(value.data_types, dict) else {}
            base_type = data_types.get("base")
            dtype = base_type.dtype if base_type is not None else None
            duckdb_type = (
                BASE_TYPE_TO_DUCKDB.get(str(dtype).upper(), DEFAULT_DUCKDB_TYPE) if dtype else DEFAULT_DUCKDB_TYPE
            )
            if duckdb_type == "DECIMAL":
                length = getattr(base_type, "length", None) if base_type is not None else None
                if not length:
                    # Legacy manifests carry the length only in column_metadata.
                    length = column_metadata.get(name, {}).get("KBC.datatype.length")
                duckdb_type = self._decimal_type(name, length)
            if name in to_cast:
                duckdb_type = "BIGINT"
            columns[name] = duckdb_type
        return columns

    def _decimal_type(self, column_name: str, length) -> str:
        """Build a valid DuckDB DECIMAL type from the manifest length, or fall back."""
        match = DECIMAL_LENGTH_RE.match(str(length)) if length else None
        if match:
            width = int(match.group(1))
            scale = int(match.group(2)) if match.group(2) is not None else 0
            if 1 <= width <= MAX_DECIMAL_WIDTH and scale <= width:
                return f"DECIMAL({width},{scale})"
            self.logger.warning(
                f"Column {column_name} declares a length of {length} that DuckDB cannot represent. "
                f"Using {DEFAULT_DECIMAL_TYPE}."
            )
        return DEFAULT_DECIMAL_TYPE

    @staticmethod
    def _quote_identifier(name: str) -> str:
        """Escape a column name so it is safe inside a double-quoted SQL identifier."""
        return name.replace('"', '""')

    def _create_view_from_csv(
        self, table_name: str, in_table: TableDefinition, path: str, dtype: dict | None
    ) -> CreatedTable:
        """Create view from CSV file with error handling."""
        try:
            quote_char = in_table.enclosure or '"'
            self.logger.debug(
                f"Reading CSV file with parameters: delimiter='{in_table.delimiter or ','}',"
                f" quotechar='{quote_char}', header={in_table.has_header}"
            )
            self.connection.read_csv(
                path_or_buffer=path,
                delimiter=in_table.delimiter or ",",
                quotechar=in_table.enclosure or '"',
                header=in_table.has_header,
                names=in_table.column_names or None,
                dtype=dtype,
            ).to_view(table_name, replace=True)
            return CreatedTable(name=table_name, is_view=True)

        except duckdb.IOException as e:
            self.logger.error(f"DuckDB IO error importing table {table_name}: {e}")
            raise UserException(f"Error importing table {table_name}: {e}")

        except Exception as e:
            self.logger.error(f"Unexpected error importing table {table_name}: {e}")
            raise UserException(f"Unexpected error importing table {table_name}: {e}")
