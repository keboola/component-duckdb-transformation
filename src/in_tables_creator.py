"""Local file table creator."""

import logging
from csv import DictReader
from dataclasses import dataclass

import duckdb
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException


@dataclass
class CreatedTable:
    """Information about created table/view."""

    name: str
    is_view: bool


class LocalTableCreator:
    """Create tables from local files (CSV, Parquet)."""

    def __init__(self, connection, file_type, dtypes_infer=True):
        self.connection = connection
        self.logger = logging.getLogger(self.__class__.__name__)
        self.dtypes_infer = dtypes_infer
        self.file_type = file_type.lower()

    def create_table(self, in_table: TableDefinition) -> CreatedTable:
        """Create table from local file."""
        self.logger.debug(f"Processing local file for table: {in_table.name}")
        # Get data types
        dtype = self._get_data_types(in_table)

        # Create table
        if self.file_type == "parquet":
            return self._create_table_from_parquet(in_table)
        else:
            return self._create_view_from_csv(in_table, dtype)

    def _get_local_csv_file_path(self, in_table: TableDefinition) -> str:
        """Get the appropriate file path for local file processing."""
        if in_table.is_sliced:
            path = f"{in_table.full_path}/*.csv"
            self.logger.debug(f"Using sliced path pattern: {path}")
        else:
            path = in_table.full_path
            self.logger.debug(f"Using direct path: {path}")
        return path

    def _get_data_types(self, in_table: TableDefinition) -> dict:
        """Get data types for table creation."""
        if not self.dtypes_infer:
            #  TODO: solve properly handling the NUMBER(38,0) from Snowflake with integer base type
            dtype = {
                k: ("BIGINT" if v.data_types.get("base").dtype == "INTEGER" else v.data_types.get("base").dtype)
                for k, v in in_table.schema.items()
            }
            self.logger.debug(f"Using custom dtypes: {dtype}")
        else:
            dtype = None
            self.logger.debug("Using automatic dtype inference")
        return dtype

    def _create_table_from_parquet(self, in_table: TableDefinition) -> CreatedTable:
        """Create table from Parquet files in S3 with optional type casting."""
        self.logger.debug(f"Creating table from Parquet files: {in_table.name}")
        # Check if type casting is needed for Snowflake INTEGER columns
        to_cast = self._get_columns_to_cast(in_table)
        if to_cast:
            return self._create_parquet_table_with_casting(in_table, to_cast)
        else:
            return self._create_parquet_table_without_casting(in_table)

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

    def _create_parquet_table_with_casting(self, in_table: TableDefinition, to_cast: list[str]) -> CreatedTable:
        """Create Parquet table with type casting for INTEGER columns."""
        self.logger.debug("Processing Parquet with type casting")
        path = in_table.full_path
        rel = self.connection.sql(f"FROM read_parquet('{path}/*.parquet')")
        # Use table name without parquet extension to avoid schema parsing (e.g., 'pq.parquet' -> 'pq')
        table_name = in_table.name.removesuffix(".parquet").removesuffix(".parq")
        columns = []
        for col in rel.columns:
            if col in to_cast:
                columns.append(duckdb.ColumnExpression(col).cast(duckdb.typing.BIGINT).alias(col))
            else:
                columns.append(duckdb.ColumnExpression(col))
        self.connection.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        rel.select(*columns).to_table(table_name)
        return CreatedTable(
            name=table_name,
            is_view=False,
        )

    def _create_parquet_table_without_casting(self, in_table: TableDefinition) -> CreatedTable:
        """Create Parquet table without type casting."""
        self.logger.debug("Processing Parquet without type casting")
        path = in_table.full_path
        self.connection.execute(
            f"""
                CREATE OR REPLACE TABLE '{in_table.destination}' AS
                FROM read_parquet('{path}/*.parquet')
            """
        )
        return CreatedTable(
            name=in_table.destination,
            is_view=False,
        )

    def _create_view_from_csv(self, in_table: TableDefinition, dtype: dict) -> CreatedTable:
        """Create table from local file with error handling."""
        try:
            path = self._get_local_csv_file_path(in_table)
            # Table name should already be clean (without .csv) from get_input_tables_definitions
            table_name = in_table.destination.removesuffix(".csv")
            quote_char = in_table.enclosure or '"'
            self.logger.debug(
                f"Reading CSV file with parameters: delimiter='{in_table.delimiter or ','}',"
                f" quotechar='{quote_char}', header={self._has_header_in_file(in_table)}"
            )
            self.connection.read_csv(
                path_or_buffer=path,
                delimiter=in_table.delimiter or ",",
                quotechar=in_table.enclosure or '"',
                header=self._has_header_in_file(in_table),
                names=self._get_column_names(in_table),
                dtype=dtype,
            ).to_view(table_name, replace=True)
            return CreatedTable(
                name=table_name,
                is_view=True,
            )
        except duckdb.IOException as e:
            self.logger.error(f"DuckDB IO error importing table {in_table.name}: {e}")
            raise UserException(f"Error importing table {in_table.name}: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error importing table {in_table.name}: {e}")
            raise UserException(f"Unexpected error importing table {in_table.name}: {e}")

    @staticmethod
    def _has_header_in_file(t: TableDefinition) -> bool:
        """Determine if file has header."""
        is_input_mapping_manifest = t.stage == "in"
        if t.is_sliced:
            has_header = False
        elif t.column_names and not is_input_mapping_manifest:
            has_header = False
        else:
            has_header = True
        return has_header

    @staticmethod
    def _get_column_names(t: TableDefinition) -> list[str]:
        """Get table header from the file or from the manifest."""
        header = None
        if t.is_sliced or t.column_names:
            header = t.column_names
        else:
            with open(t.full_path, encoding="utf-8") as f:
                reader = DictReader(f, lineterminator="\n", delimiter=t.delimiter, quotechar=t.enclosure)
                header = reader.fieldnames
        return header
