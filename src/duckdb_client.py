import logging
import os
import threading
from typing import Dict, Optional

import duckdb
from duckdb import DuckDBPyConnection

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")

# Thread-local storage for connections
_thread_local_storage = threading.local()
_main_db_path: Optional[str] = None
_main_config: Optional[Dict] = None


def init_connection(threads, max_memory, db_path) -> DuckDBPyConnection:
    """
    Returns connection to temporary DuckDB database with advanced optimizations
    """
    global _main_db_path, _main_config

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

    # Store config globally for thread-safe connections
    _main_db_path = db_path
    _main_config = config

    logging.info(f"Initializing DuckDB connection with config: {config}")
    conn = duckdb.connect(database=db_path, config=config)
    return conn


def get_thread_connection() -> DuckDBPyConnection:
    """
    Get thread-local DuckDB connection. Each thread gets its own connection
    to the same database to ensure thread-safety.
    """
    if not hasattr(_thread_local_storage, 'connection'):
        if _main_db_path is None or _main_config is None:
            raise RuntimeError("Main connection must be initialized first")

        thread_id = threading.current_thread().ident
        logging.debug(f"Creating thread-local DuckDB connection for thread {thread_id}")
        _thread_local_storage.connection = duckdb.connect(
            database=_main_db_path,
            config=_main_config
        )

    return _thread_local_storage.connection


def close_thread_connection():
    """Close thread-local connection if it exists."""
    if hasattr(_thread_local_storage, 'connection'):
        try:
            _thread_local_storage.connection.close()
            delattr(_thread_local_storage, 'connection')
        except Exception as e:
            logging.warning(f"Error closing thread connection: {e}")


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
