import os
import shutil
import json
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
        db_in_path = os.path.join(self.data_folder_path, "in", "files", ".duck.db")
        db_out_path = os.path.join(self.data_folder_path, "out", "files", ".duck.db")
        if os.path.exists(db_in_path):
            shutil.move(db_in_path, db_out_path)
        self._conn = duckdb_client.init_connection(self.params.threads, self.params.max_memory_mb, db_out_path)

    def run(self):
        start_time = time.time()
        for table in self.get_input_tables_definitions(orphaned_manifests=True):
            self.load_in_table(table)
        logging.debug(f"Input tables loaded in {time.time() - start_time:.2f} seconds")
        self.debug_log()

        start_time = time.time()
        self.process_queries()
        logging.debug(f"All queries processed in {time.time() - start_time:.2f} seconds")
        self.debug_log()

        start_time = time.time()
        self.export_tables()
        logging.debug(f"Output tables exported in {time.time() - start_time:.2f} seconds")

        self.generate_out_files_manifests()
        self._conn.close()

    def debug_log(self):
        if self.params.debug:
            q = [
                "SELECT database_name, table_name, has_primary_key, estimated_size, index_count FROM duckdb_tables();",
                "SELECT path, round(size/10**6)::INT as 'size_MB' FROM duckdb_temporary_files();",
                """SELECT tag, round(memory_usage_bytes/10**6)::INT as 'mem_MB',
                       round(temporary_storage_bytes/10**6)::INT as 'storage_MB' FROM duckdb_memory();""",
            ]

            for query in q:
                logging.debug(self._conn.sql(query).show())

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
                        res = self._conn.execute(script).fetchall()
                        logging.debug(
                            f"Query {counter} / {total_scripts} finished in {time.time() - start_time:.2f} seconds\n"
                            f"Result:\n{res}"
                        )
                        counter += 1
                    except AttributeError:
                        logging.info("Query did not return any result.")
                    except Exception as e:
                        raise UserException(f"Error during executing the query '{code.name}': {str(e)}")

    def load_in_table(self, in_table: TableDefinition) -> None:
        if not in_table.full_path:  # if the full_path is missing, it's s3 staging
            in_mapping = self.configuration.tables_input_mapping
            path = [table for table in in_mapping if table.source == in_table.id][0].full_path

            if os.path.exists(f"{path}.manifest"):
                s3 = json.load(open(f"{path}.manifest")).get("s3", {})

                self._conn.execute(
                    f"""CREATE OR REPLACE SECRET (
                                TYPE S3,
                                REGION '{s3.get("region")}',
                                KEY_ID '{s3.get("credentials", {}).get("access_key_id")}',
                                SECRET '{s3.get("credentials", {}).get("secret_access_key")}',
                                SESSION_TOKEN '{s3.get("credentials", {}).get("session_token")}'
                                );
                           """
                )

                manifest = self._conn.sql(f"FROM read_json('s3://{s3.get('bucket')}/{s3.get('key')}')").fetchone()[0]

                files = [f.get("url") for f in manifest]

                suffix = files[0].split(".")[-1]

                if suffix in ["csv", "gz"]:
                    self._conn.execute(f"""
                                    CREATE OR REPLACE TABLE '{in_table.name}' AS
                                    FROM read_csv({files}, column_names={in_table.column_names})""")
                elif suffix == "parquet":
                    """
                    Snowflake keeps integers as NUMBER(38,0): https://docs.snowflake.com/en/sql-reference/data-types-numeric#int-integer-bigint-smallint-tinyint-byteint  # noqa: E501
                    and exports them to Parquet as DECIMAL(38,0) which negatively impacts performance in DuckDB: https://duckdb.org/docs/stable/sql/data_types/numeric.html#fixed-point-decimals  # noqa: E501
                    based on the KBC column metadata we are casting such columns to BIGINT.
                    """
                    to_cast = [
                        k
                        for k, v in in_table.table_metadata.column_metadata.items()
                        if v.get("KBC.datatype.basetype") == "INTEGER"
                    ]
                    if to_cast:
                        rel = self._conn.sql(f"""FROM read_parquet({files})""")

                        columns = []
                        for col in rel.columns:
                            if col in to_cast:
                                columns.append(duckdb.ColumnExpression(col).cast(duckdb.typing.BIGINT).alias(col))
                            else:
                                columns.append(duckdb.ColumnExpression(col))

                        self._conn.execute(f'DROP TABLE IF EXISTS "{in_table.name}"')
                        rel.select(*columns).to_table(in_table.name)
                    else:
                        self._conn.execute(f"""
                                        CREATE OR REPLACE TABLE '{in_table.name}' AS
                                        FROM read_parquet({files})""")

                else:
                    raise UserException(f"Unsupported file format: {suffix}")

                table_meta = self._conn.execute(f"""DESCRIBE TABLE '{in_table.name}';""").fetchall()
                logging.debug(f"Table {in_table.name} created with following dtypes: {[c[1] for c in table_meta]}")

                return

        path = in_table.full_path
        if in_table.is_sliced:
            path = f"{in_table.full_path}/*.csv"

        dtype = None
        if not self.params.dtypes_infer:
            dtype = {key: value.data_types.get("base").dtype for key, value in in_table.schema.items()}

        try:
            self._conn.execute(f'DROP TABLE IF EXISTS "{in_table.name.removesuffix(".csv")}"')
            self._conn.read_csv(
                path_or_buffer=path,
                delimiter=in_table.delimiter or ",",
                quotechar=in_table.enclosure or '"',
                header=self._has_header_in_file(in_table),
                names=self._get_column_names(in_table),
                dtype=dtype,
            ).to_table(in_table.name.removesuffix(".csv"))

            logging.debug(f"Table {in_table.name} created.")
        except duckdb.IOException as e:
            raise UserException(f"Error importing table {in_table.name}: {e}")
        except Exception as e:
            raise UserException(f"Unexpected error importing table table {in_table.name}: {e}")

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
                table_meta = self._conn.execute(f"""DESCRIBE TABLE '{table.source}';""").fetchall()
                schema = OrderedDict(
                    {
                        c[0]: ColumnDefinition(data_types=BaseType(dtype=self.convert_base_types(c[1])))
                        for c in table_meta
                    }
                )

                out_table = self.create_out_table_definition(
                    name=table.source,
                    schema=schema,
                    primary_key=table.primary_key,
                    incremental=table.incremental,
                    destination=table.destination,
                    has_header=True,
                )

                self._conn.execute(f'''COPY "{table.source}" TO "{out_table.full_path}"
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
