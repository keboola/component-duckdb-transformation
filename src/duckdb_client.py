import os

import duckdb
from duckdb import DuckDBPyConnection

DUCK_DB_DIR = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb")


def init_connection(threads, max_memory, db_path) -> DuckDBPyConnection:
    """
    Returns connection to temporary DuckDB database
    """
    os.makedirs(DUCK_DB_DIR, exist_ok=True)
    config = {"temp_directory": DUCK_DB_DIR, "threads": threads, "max_memory": f"{max_memory}MB"}
    conn = duckdb.connect(database=db_path, config=config)

    return conn
