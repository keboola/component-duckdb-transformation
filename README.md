keboola.duckdb-transformation
=============

DuckDB SQL transformation component for Keboola platform with block-based orchestration.

**Features:**
- **Consecutive Blocks**: Blocks execute in order, ensuring logical separation of processing phases
- **Parallel Scripts**: Scripts within each block run in parallel when dependencies allow
- **Automatic DAG**: Component creates its own dependency graph based on SQL analysis
- **SQLGlot Integration**: Advanced SQL parsing and dependency detection
- **Performance Optimization**: Parallel execution with configurable thread limits
- **System Resource Detection**: Automatic detection of CPU and memory limits for optimal DuckDB settings
- **Local File Support**: Support for CSV and Parquet files from local storage
- **Data Type Inference**: Optional automatic data type detection for CSV files
- **SQL Validation**: Startup and on-demand SQL syntax validation
- **Visualization Actions**: Execution plan and data lineage visualization

**Table of Contents:**


Functionality Notes
===================

Prerequisites
=============

Ensure you have the necessary API token, register the application, etc.

Features
========

| **Feature**             | **Description**                               |
|-------------------------|-----------------------------------------------|
| Block-Based Orchestration | Consecutive blocks with parallel scripts execution |
| Automatic DAG Creation | SQL dependency analysis and execution planning |
| SQLGlot Integration    | Advanced SQL parsing and syntax validation    |
| Parallel Processing     | Configurable thread limits for performance    |
| Memory Management       | Configurable memory limits for DuckDB         |
| Syntax Checking         | Startup and on-demand SQL validation          |
| System Resource Detection | Automatic CPU and memory detection for optimal settings |
| Local File Support      | Support for CSV and Parquet files from local storage |
| Data Type Inference     | Optional automatic data type detection for CSV files |
| Execution Visualization | Visualize execution plan and data lineage |

Supported Endpoints
===================

If you need additional endpoints, please submit your request to
[ideas.keboola.com](https://ideas.keboola.com/).

Configuration
=============

The component uses a block-based configuration structure:

```json
{
  "parameters": {
    "blocks": [
      {
        "name": "Data Preparation",
        "codes": [
          {
            "name": "Clean Data",
            "script": [
              "CREATE VIEW 'clean_table' AS SELECT * FROM input_table WHERE valid = true;"
            ]
          }
        ]
      }
    ],
    "threads": 4,
    "max_memory_mb": 2048,
    "dtypes_infer": false,
    "debug": false,
    "syntax_check_on_startup": false
  }
}
```

**Parameters:**
- `blocks`: Array of processing blocks (executed consecutively)
- `threads`: Number of parallel threads for query execution (None for auto-detection)
- `max_memory_mb`: Memory limit for DuckDB in MB (None for auto-detection)
- `dtypes_infer`: Enable automatic data type inference for CSV files (default: false)
- `debug`: Enable debug logging (default: false)
- `syntax_check_on_startup`: Validate SQL syntax before execution (default: false)

**Input Sources:**
- **Local Files**: CSV and Parquet files from local storage

**Sync Actions:**
- `syntax_check`: Validate SQL syntax without execution
- `lineage_visualization`: Generate data lineage visualization
- `execution_plan_visualization`: Visualize execution plan
- `expected_input_tables`: Show expected input tables

Output
======

Exports tables to CSV files with manifests into `out/tables` and file manifests into `out/files`.

SQL Syntax and Naming Conventions
==================================

**Table Name Case Sensitivity:**
- **Unquoted table names** are automatically converted to lowercase by DuckDB
  - Example: `SELECT * FROM MyTable` references table `mytable`
- **Quoted table names** are case-sensitive
  - Example: `SELECT * FROM "MyTable"` references exactly `MyTable` (case-sensitive)

**Column Name Case Sensitivity:**
- **Columns are always case-sensitive** regardless of quoting
  - Example: `SELECT columnName` and `SELECT ColumnName` refer to different columns

**Best Practices:**
- Use consistent casing for table and column names
- When referencing tables with mixed case (or any non-alphanumeric characters) always use quotes: `"TaBlE-stage"`
- Be aware that input table names are typically lowercase unless explicitly quoted

Supported DuckDB Versions
-------------------------

Each supported DuckDB version gets its own isolated venv built into the Docker image.
The `latest` UI option always resolves to the most recent version at runtime.

To add a new version, update these five files in order:

**1. `pyproject.toml`** — add a dependency group and extend the conflicts list:

```toml
[dependency-groups]
"duckdb-X.Y.Z" = ["duckdb==X.Y.Z"]

[tool.uv]
conflicts = [
    [
        { group = "duckdb-1.5.1" },
        { group = "duckdb-1.4.4" },
        { group = "duckdb-X.Y.Z" },
    ],
]
```

**2. `src/versions.py`** — add the version mapping (order does not matter, the highest version is detected automatically):

```python
VENV_NAMES: dict[str, str] = {
    "1.5.1": "duckdb-1.5.1",
    "1.4.4": "duckdb-1.4.4",
    "X.Y.Z": "duckdb-X.Y.Z",
}
```

**3. `Dockerfile`** — add a venv build step in the `base` stage:

```dockerfile
RUN UV_PROJECT_ENVIRONMENT=$VENV_BASE/duckdb-X.Y.Z \
    uv sync --group duckdb-X.Y.Z --no-group dev --no-group duckdb-1.5.1 --no-group duckdb-1.4.4 --frozen
```

**4. `component_config/configSchema.json`** — add the version to the enum:

```json
"enum": ["latest", "X.Y.Z", "1.5.1", "1.4.4"]
```

**5. Regenerate the lockfile:**

```sh
uv lock
```

The CI `pre-check` job runs before any Docker build and validates that the lockfile
and `versions.py` are consistent. If any file is out of sync it fails immediately
with a message pointing to the exact fix needed. To run the check locally:

```sh
PYTHONPATH=src uv run python scripts/check_versions.py
```

Development
-----------

To customize the local data folder path, replace the `CUSTOM_FOLDER` placeholder with your desired path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, initialize the workspace, and run the component using the following
commands:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone git@github.com:keboola/component-duckdb-transformation.git keboola.duckdb_transformation
cd keboola.duckdb_transformation
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and perform lint checks using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer
documentation](https://developers.keboola.com/extend/component/deployment/).
