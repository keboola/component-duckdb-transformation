import os
import duckdb
from duckdb.duckdb import DuckDBPyConnection

DUCK_DB_DIR = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'duckdb')


def init_connection() -> DuckDBPyConnection:
    """
            Returns connection to temporary DuckDB database
            """
    os.makedirs(DUCK_DB_DIR, exist_ok=True)
    config = dict(temp_directory=DUCK_DB_DIR,
                  threads="4",
                  memory_limit="512MB",
                  max_memory="512MB")
    conn = duckdb.connect(config=config)

    return conn
