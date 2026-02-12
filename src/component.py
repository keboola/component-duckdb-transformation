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
from in_tables_creator import LocalTableCreator
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
        available_tables = self._get_input_tables_definitions()
        if available_tables:
            # Do validation with detailed report
            return action.expected_input_tables(blocks=self.params.blocks, available_tables=available_tables)
        else:
            # Fall back to simple comma-separated list
            return action.expected_input_tables(self.params.blocks)

    def _get_input_tables_definitions(self):
        """
        Override parent method to add destination_table_name attribute from configuration.

        Returns:
            List of TableDefinition objects with added destination_table_name attribute and updated names
        """
        base_definitions = self.get_input_tables_definitions()

        for table_def in base_definitions:
            # Find mapping from source to destination names from config
            destination_table_name = None
            for table in self.configuration.tables_input_mapping:
                if table_def.id:
                    if table.source == table_def.id:
                        destination_table_name = table.destination
                        break

            # Fallback: use original name without .csv
            if not destination_table_name:
                destination_table_name = table_def.name

            # Add attribute and update name
            table_def.destination = destination_table_name

        return base_definitions

    def _create_input_tables(self):
        """Create input tables from detected sources."""
        start_time = time.time()

        for in_table in self._get_input_tables_definitions():
            creator = LocalTableCreator(self._connection, self.params.dtypes_infer)
            result = creator.create_table(in_table)
            logging.info(f"Input table created: {result.name} (is_view={result.is_view})")
        logging.debug(f"Input tables created in {time.time() - start_time:.2f} seconds")

    def _export_tables(self):
        """Export tables to KBC output with timing."""
        start_time = time.time()
        for table in self.configuration.tables_output_mapping:
            try:
                # Get table schema
                table_meta = self._connection.execute(f"""DESCRIBE TABLE '{table.source}';""").fetchall()
                schema = {
                    c[0]: ColumnDefinition(data_types=BaseType(dtype=self.convert_base_types(c[1]))) for c in table_meta
                }
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
                self._connection.execute(f'''COPY "{table.source}" TO "{out_table.full_path}"
                                            (HEADER, DELIMITER ',', FORCE_QUOTE *)''')
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

    @staticmethod
    def convert_base_types(dtype: str):
        dtype = dtype.split("(")[0]

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
        elif dtype in ["REAL", "DECIMAL"]:
            return SupportedDataTypes.NUMERIC
        elif dtype == "DOUBLE":
            return SupportedDataTypes.FLOAT
        elif dtype == "BOOLEAN":
            return SupportedDataTypes.BOOLEAN
        elif dtype in ["TIMESTAMP", "TIMESTAMP WITH TIME ZONE"]:
            return SupportedDataTypes.TIMESTAMP
        elif dtype == "DATE":
            return SupportedDataTypes.DATE
        else:
            return SupportedDataTypes.STRING


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
