import logging
import os

import duckdb
from duckdb import DuckDBPyConnection

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


def init_connection(threads, max_memory, db_path) -> DuckDBPyConnection:
    """
    Returns connection to temporary DuckDB database with advanced optimizations.
    DuckDB supports thread-safe access to a single connection.
    """
    os.makedirs(DUCK_DB_DIR, exist_ok=True)
    # Enhanced configuration with performance optimizations
    # Using only definitely valid DuckDB configuration parameters
    config = {
        # Basic settings
        "temp_directory": DUCK_DB_DIR,
        "threads": threads,
        "max_memory": f"{max_memory}MB",
        "extension_directory": os.path.join(DUCK_DB_DIR, "extensions"),
        # Performance optimizations
        "preserve_insertion_order": False,  # Faster inserts
    }

    logging.info(f"Initializing DuckDB connection with config: {config}")
    conn = duckdb.connect(database=db_path, config=config)
    return conn


def debug_log(connection) -> None:
    """Debug logging for DuckDB connection."""
    try:
        q = [
            "SELECT database_name, table_name, has_primary_key, estimated_size, index_count FROM duckdb_tables();",
            "SELECT path, round(size/10**6)::INT as 'size_MB' FROM duckdb_temporary_files();",
            """SELECT tag,
                      round(memory_usage_bytes / 10 * 6)::INT as 'mem_MB',
                      round(temporary_storage_bytes / 10 * 6) ::INT as 'storage_MB'
               FROM duckdb_memory();""",
        ]
        for query in q:
            connection.sql(query).show()
    except Exception as e:
        logging.error(f"Failed to execute debug query: {e}")
