import logging
import os
import shutil
import time

from keboola.component.base import ComponentBase, sync_action
from keboola.component.dao import (
    BaseType,
    ColumnDefinition,
    SupportedDataTypes,
)
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import MessageType

import duckdb_client
from actions.execution_plan_visualization import ExecutionPlanVisualizationAction
from actions.expected_input_tables import ExpectedInputTablesAction
from actions.lineage_visualization import LineageVisualizationAction
from configuration import Configuration
from in_tables_creator import FileType, LocalTableCreator
from query_orchestrator import BlockOrchestrator
from validators import SQLValidator


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        # Setup database connection
        self._setup_database_path()
        # Initialize connection
        self._connection = duckdb_client.init_connection(
            self.params.threads, self.params.max_memory_mb, self._db_out_path
        )

    def run(self):
        original_cwd = os.getcwd()
        try:
            os.chdir(self.data_folder_path)
            start_time = time.time()
            # Perform startup syntax check if enabled
            self._perform_startup_syntax_check()
            self._create_input_tables()
            self._process_queries()
            self._export_tables()
            self._export_files()
            if self.params.debug:
                duckdb_client.debug_log(self._connection)
            self._connection.close()
            total_time = time.time() - start_time
            logging.info(f"Total component execution time: {total_time:.2f}s")
        finally:
            try:
                os.chdir(original_cwd)
            except Exception as e:
                logging.warning(f"Failed to restore original working directory: {e}")

    def _setup_database_path(self):
        """Setup database paths and move existing database if needed."""
        db_in_path = os.path.join(self.data_folder_path, "in", "files", ".duck.db")
        self._db_out_path = os.path.join(self.data_folder_path, "out", "files", ".duck.db")
        # Ensure the output directory exists so DuckDB can create the database file
        out_dir = os.path.dirname(self._db_out_path)
        os.makedirs(out_dir, exist_ok=True)
        if os.path.exists(db_in_path):
            shutil.move(db_in_path, self._db_out_path)

    def _perform_startup_syntax_check(self) -> None:
        """
        Perform syntax check on all SQL queries at component startup.
        Raises UserException if syntax check fails and is enabled.
        """
        if not self.params.syntax_check_on_startup:
            logging.info("Skipping startup syntax check (disabled)")
            return
        logging.info("🔍 Performing syntax check on startup...")
        sql_validator = SQLValidator()
        syntax_result = sql_validator.validate_queries(self.params.blocks)
        if syntax_result.type == MessageType.DANGER:
            raise UserException(f"Syntax check failed on startup: {syntax_result.message}")
        else:
            logging.info(syntax_result.message)

    def _process_queries(self):
        """Process all SQL queries with timing."""
        start_time = time.time()
        # Block-based orchestration with consecutive blocks and parallel scripts
        orchestrator = BlockOrchestrator(connection=self._connection, max_workers=self.params.threads)
        orchestrator.add_queries_from_blocks(self.params.blocks)
        orchestrator.execute()
        logging.debug(f"All queries processed in {time.time() - start_time:.2f} seconds")

    @sync_action("syntax_check")
    def syntax_check(self):
        """
        Perform syntax check on all SQL queries without executing them.
        Returns ValidationResult with validation results.
        """
        sql_validator = SQLValidator()
        return sql_validator.validate_queries(self.params.blocks)

    @sync_action("lineage_visualization")
    def lineage_visualization(self):
        """
        Generate data lineage visualization from SQL queries.
        Returns ValidationResult with markdown lineage diagram.
        """
        action = LineageVisualizationAction()
        return action.lineage_visualization(self.params.blocks)

    @sync_action("execution_plan_visualization")
    def execution_plan_visualization(self):
        """
        Generate execution plan visualization showing block order and parallel execution.
        Returns ValidationResult with markdown execution plan.
        """
        action = ExecutionPlanVisualizationAction(self.params.threads)
        return action.execution_plan_visualization(self.params.blocks)

    @sync_action("expected_input_tables")
    def expected_input_tables(self):
        """
        Returns expected input tables with validation.
        If input tables are available in configuration, validates against them and returns detailed report.
        Otherwise returns a comma-separated list of required external input tables.
        """
        action = ExpectedInputTablesAction()

        # Try to get available input tables - if they exist, do validation
        available_tables = self.get_input_tables_definitions()
        if available_tables:
            # Do validation with detailed report
            return action.expected_input_tables(blocks=self.params.blocks, available_tables=available_tables)
        else:
            # Fall back to simple comma-separated list
            self.get_input_tables_definitions()
            return action.expected_input_tables(self.params.blocks)

    def _create_input_tables(self):
        """Create input tables from detected sources."""
        start_time = time.time()
        # Map storage table ID -> desired DuckDB table name from input mapping
        source_to_destination = {m.source: m.destination for m in self.configuration.tables_input_mapping}
        source_to_file_type = {m.source: m.file_type for m in self.configuration.tables_input_mapping}
        creator = LocalTableCreator(self._connection, self.params.dtypes_infer)
        for in_table in self.get_input_tables_definitions():
            # Input mapping destination overrides the table definition name.
            # For input tables, the storage ID is in in_table.id (not in_table.destination).
            table_name = source_to_destination.get(in_table.id) or in_table.name
            file_type = FileType(source_to_file_type.get(in_table.id, FileType.CSV))
            result = creator.create_table(in_table, table_name=table_name, file_type=file_type)
            logging.info(f"Input table created: {result.name} (is_view={result.is_view})")
        logging.debug(f"Input tables created in {time.time() - start_time:.2f} seconds")

    def _export_tables(self):
        """Export tables to KBC output with timing."""
        start_time = time.time()
        for table in self.configuration.tables_output_mapping:
            try:
                # Get table schema
                table_meta = self._connection.execute(f"DESCRIBE TABLE '{table.source}';").fetchall()
                schema = {c[0]: ColumnDefinition(data_types=self.duckdb_type_to_base_type(c[1])) for c in table_meta}
                # Create output table definition
                out_table = self.create_out_table_definition(
                    name=table.source,
                    schema=schema,
                    primary_key=table.primary_key,
                    incremental=table.incremental,
                    destination=table.destination,
                    has_header=True,
                )
                # Export table to CSV
                self._connection.execute(
                    f"COPY '{table.source}' TO '{out_table.full_path}' (HEADER, DELIMITER ',', FORCE_QUOTE *)"
                )
                # Write manifest
                self.write_manifest(out_table)
            except Exception as e:
                raise UserException(f"Error exporting table {table.source}: {e}")
        logging.debug(f"Output tables exported in {time.time() - start_time:.2f} seconds")

    def _export_files(self):
        """Export files to KBC output with timing."""
        start_time = time.time()
        for file in self.configuration.files_output_mapping:
            out_file = self.create_out_file_definition(
                name=file.source,
                is_permanent=file.is_permanent,
                tags=file.tags,
            )
            self.write_manifest(out_file)
        logging.debug(f"Output files exported in {time.time() - start_time:.2f} seconds")

    # DuckDB base-type names whose parenthesized modifier is a meaningful length /
    # precision-scale (as opposed to complex types like STRUCT(...) that also use parentheses).
    _LENGTH_BEARING_TYPES = frozenset({"DECIMAL", "NUMERIC", "VARCHAR", "CHAR", "BPCHAR", "STRING", "TEXT"})

    # DuckDB encodes sub-second timestamp precision in the type NAME, not a TIMESTAMP(p)
    # modifier, so the fractional-seconds precision can't be parsed from parentheses like
    # DECIMAL(p,s) — it has to be looked up from the name. Map each variant to the Keboola
    # TIMESTAMP length (= fractional-seconds precision) so Storage creates TIMESTAMP_NTZ(p)
    # instead of defaulting to (9): TIMESTAMP_S = second, _MS = millisecond, _NS = nanosecond.
    # Plain TIMESTAMP (microsecond) and TIMESTAMP WITH TIME ZONE are deliberately left with no
    # explicit length (Storage default) — the common case is unchanged to keep the blast radius
    # off existing configs; only the explicit precision variants get a tightened width.
    _TIMESTAMP_PRECISION = {"TIMESTAMP_S": "0", "TIMESTAMP_MS": "3", "TIMESTAMP_NS": "9"}

    @staticmethod
    def _map_base_type(dtype: str) -> SupportedDataTypes:
        """Map a DuckDB base type name (without any length/precision modifier) to a Keboola type."""
        if dtype in [
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "UTINYINT",
            "USMALLINT",
            "UINTEGER",
            "UBIGINT",
            "UHUGEINT",
        ]:
            return SupportedDataTypes.INTEGER
        elif dtype in ["REAL", "DECIMAL", "NUMERIC"]:
            return SupportedDataTypes.NUMERIC
        elif dtype == "DOUBLE":
            return SupportedDataTypes.FLOAT
        elif dtype == "BOOLEAN":
            return SupportedDataTypes.BOOLEAN
        elif dtype in [
            "TIMESTAMP",
            "TIMESTAMP WITH TIME ZONE",
            "TIMESTAMP_S",
            "TIMESTAMP_MS",
            "TIMESTAMP_NS",
        ]:
            # DuckDB exposes sub-second precision as distinct named types
            # (TIMESTAMP_S/_MS/_NS) rather than a TIMESTAMP(p) modifier. They all
            # map to Keboola TIMESTAMP; without these they fell through to STRING.
            return SupportedDataTypes.TIMESTAMP
        elif dtype == "DATE":
            return SupportedDataTypes.DATE
        else:
            return SupportedDataTypes.STRING

    @staticmethod
    def duckdb_type_to_base_type(duckdb_type: str) -> BaseType:
        """Convert a DuckDB column type (from DESCRIBE) into a Keboola BaseType.

        The parenthesized modifier is preserved so that Storage reproduces the intended
        type instead of a max default: DuckDB ``DECIMAL(p,s)`` -> ``length="p,s"`` (Keboola's
        ``precision,scale`` format) and ``VARCHAR(n)`` -> ``length="n"``. The length is only
        read for base-type names that genuinely carry one, so complex types that fall through
        to STRING (e.g. ``STRUCT(a INTEGER)``, ``INTEGER[]``) never leak their inner definition
        into ``length``.

        Sub-second timestamp precision is carried in the DuckDB type *name*
        (``TIMESTAMP_S`` / ``_MS`` / ``_NS``) rather than a ``TIMESTAMP(p)`` modifier, so it is
        looked up from ``_TIMESTAMP_PRECISION`` and emitted as the Keboola ``length`` -> Storage
        creates ``TIMESTAMP_NTZ(0/3/9)`` instead of defaulting to ``(9)``.

        Note: DuckDB does not retain VARCHAR length internally, so casts like ``VARCHAR(2)``
        arrive here already collapsed to plain ``VARCHAR`` and cannot be recovered.
        """
        base_name = duckdb_type.split("(")[0].strip().upper()
        dtype = Component._map_base_type(base_name)

        length = None
        if "(" in duckdb_type and base_name in Component._LENGTH_BEARING_TYPES:
            length = duckdb_type[duckdb_type.index("(") + 1 : duckdb_type.rindex(")")].strip()
        elif base_name in Component._TIMESTAMP_PRECISION:
            length = Component._TIMESTAMP_PRECISION[base_name]

        return BaseType(dtype=dtype, length=length)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
