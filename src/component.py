import logging
import time
from collections import OrderedDict
from csv import DictReader

import duckdb
from keboola.component.base import ComponentBase
from keboola.component.dao import BaseType, ColumnDefinition, SupportedDataTypes, TableDefinition
from keboola.component.exceptions import UserException

import duckdb_client
from configuration import Configuration


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        self._connection = duckdb_client.init_connection(self.params.threads, self.params.max_memory_mb)

    def run(self):
        for table in self.get_input_tables_definitions():
            self.create_view(table)

        self.process_queries()
        self.export_tables()
        self.generate_out_files_manifests()
        self._connection.close()

    def process_queries(self):
        total_scripts = sum(len(code.script) for block in self.params.blocks for code in block.codes)
        counter = 1
        for block in self.params.blocks:
            logging.info(f"Processing block: {block.name}")
            for code in block.codes:
                logging.info(f"Executing code: {code.name}")
                for script in code.script:
                    try:
                        start_time = time.time()
                        self._connection.execute(script)
                        logging.info(
                            f"Query {counter} / {total_scripts} finished in {time.time() - start_time:.2f} seconds"
                        )
                        counter += 1
                    except Exception as e:
                        raise UserException(f"Error during executing the query '{code.name}': {str(e)}")

    def create_view(self, in_table: TableDefinition) -> None:
        if in_table.is_sliced:
            path = f"{in_table.full_path}/*.csv"
        else:
            path = in_table.full_path

        try:
            self._connection.read_csv(
                path_or_buffer=path,
                delimiter=in_table.delimiter or ",",
                quotechar=in_table.enclosure or '"',
                header=self._has_header_in_file(in_table),
                names=self._get_column_names(in_table),
                dtype={key: value.data_types.get("base").dtype for key, value in in_table.schema.items()},
            ).to_view(in_table.name.removesuffix(".csv"))

            logging.debug(f"Table {in_table.name} created.")
        except duckdb.IOException as e:
            raise UserException(f"Error creating view for table {in_table.name}: {e}")
        except Exception as e:
            raise UserException(f"Unexpected error creating view for table {in_table.name}: {e}")

    @staticmethod
    def _has_header_in_file(t: TableDefinition):
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
        """
        Get table header from the file or from the manifest
        """
        header = None
        if t.is_sliced or t.column_names:
            header = t.column_names
        else:
            with open(t.full_path, encoding="utf-8") as f:
                reader = DictReader(f, lineterminator="\n", delimiter=t.delimiter, quotechar=t.enclosure)
                header = reader.fieldnames

        return header

    def export_tables(self) -> None:
        for table in self.configuration.tables_output_mapping:
            try:
                table_meta = self._connection.execute(f"""DESCRIBE TABLE '{table.source}';""").fetchall()
                schema = OrderedDict(
                    {
                        c[0]: ColumnDefinition(data_types=BaseType(dtype=self.convert_base_types(c[1])))
                        for c in table_meta
                    }
                )

                out_table = self.create_out_table_definition(
                    name=f"{table.source}.csv",
                    schema=schema,
                    primary_key=table.primary_key,
                    incremental=table.incremental,
                    destination=table.destination,
                )

                self._connection.execute(f'''COPY "{table.source}" TO "{out_table.full_path}"
                                                        (HEADER, DELIMITER ',', FORCE_QUOTE *)''')

            except duckdb.CatalogException as e:
                raise UserException(f"Can't find table defined in output mapping {table.source}: {e}")

            except duckdb.IOException as e:
                raise UserException(f"Error exporting table {table.source}: {e}")

            except Exception as e:
                raise UserException(f"Unexpected error exporting table {table.source}: {e}")

            self.write_manifest(out_table)

    def generate_out_files_manifests(self) -> None:
        for file in self.configuration.files_output_mapping:
            out_file = self.create_out_file_definition(
                name=file.source,
                is_permanent=file.is_permanent,
                tags=file.tags,
            )
            self.write_manifest(out_file)

    @staticmethod
    def convert_base_types(dtype: str) -> SupportedDataTypes:
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
