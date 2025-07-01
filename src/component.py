import logging
import os
import shutil
from collections import OrderedDict

import duckdb
from keboola.component.base import ComponentBase
from keboola.component.dao import ColumnDefinition, BaseType, SupportedDataTypes, TableMetadata
from keboola.component.exceptions import UserException

import duckdb_client
from configuration import Configuration

MANDATORY_PARS = ["queries"]

class Component(ComponentBase):

        def __init__(self):
            super().__init__()
            self.validate_configuration_parameters(MANDATORY_PARS)
            self._connection = duckdb_client.init_connection()
            self.params = Configuration(**self.configuration.parameters)

        def run(self):
            logging.info("Running DuckDB transformation...")

            # Execute SQL queries
            queries=self.params.queries
            for i, query in enumerate(queries):
                logging.info(f"Executing query {i + 1}/{len(queries)}")
                try:
                    self._connection.execute(query)
                except Exception as e:
                    raise UserException(f"Error in query {i + 1}: {str(e)}")

            self.export_tables()
            # TODO tady se to musí domyslet, je potřeba nechat možnost exportovat cokoliv do jakéhokoliv souboru a typu možná necháme export na query?
            self.export_files()

            logging.info("DuckDB transformation completed successfully.")

        def export_tables(self) -> None:
            out_tables = self.configuration.tables_output_mapping

            for table_params in out_tables:

                table_meta = self._connection.execute(f"""DESCRIBE TABLE '{table_params.source}';""").fetchall()
                schema = OrderedDict((c[0], ColumnDefinition(data_types=BaseType(dtype=self.convert_base_types(c[1]))))
                                     for c in table_meta)

                tm = TableMetadata()
                tm.add_column_data_types({c[0]: self.convert_base_types(c[1]) for c in table_meta})

                out_table = self.create_out_table_definition(f"{table_params.source}.csv",
                                                             schema=schema,
                                                             primary_key=table_params.primary_key,
                                                             incremental=table_params.incremental,
                                                             destination=table_params.destination,
                                                             table_metadata=tm
                                                             )

                try:
                    self._connection.execute(f'''COPY "{table_params.source}" TO "{out_table.full_path}"
                                                        (HEADER, DELIMITER ',', FORCE_QUOTE *)''')
                except duckdb.duckdb.ConversionException as e:
                    raise UserException(f"Error during query execution: {e}")

                self.write_manifest(out_table)
                self._connection.close()

        def export_files(self) -> None:
            out_files = self.configuration.files_output_mapping

            for files_params in out_files:

                out_file = self.create_out_file_definition(f"{files_params.source}")

                try:
                    self._connection.execute(f'''COPY "{files_params.source}" TO "{out_file.full_path}"
                                                        (HEADER, DELIMITER ',', FORCE_QUOTE *)''')
                except duckdb.duckdb.ConversionException as e:
                    raise UserException(f"Error during query execution: {e}")

                self._connection.close()

        def convert_base_types(self, dtype: str) -> SupportedDataTypes:
            if dtype in ['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT',
                         'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT', 'UHUGEINT']:
                return SupportedDataTypes.INTEGER
            elif dtype in ['REAL', 'DECIMAL']:
                return SupportedDataTypes.NUMERIC
            elif dtype == 'DOUBLE':
                return SupportedDataTypes.FLOAT
            elif dtype == 'BOOLEAN':
                return SupportedDataTypes.BOOLEAN
            elif dtype in ['TIMESTAMP', 'TIMESTAMP WITH TIME ZONE']:
                return SupportedDataTypes.TIMESTAMP
            elif dtype == 'DATE':
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
