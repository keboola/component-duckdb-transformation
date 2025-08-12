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

[TOC]

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
    "syntax_check_on_startup": true
  }
}
```

**Parameters:**
- `blocks`: Array of processing blocks (executed consecutively)
- `threads`: Number of parallel threads for query execution (None for auto-detection)
- `max_memory_mb`: Memory limit for DuckDB in MB (None for auto-detection)
- `dtypes_infer`: Enable automatic data type inference for CSV files (default: false)
- `debug`: Enable debug logging (default: false)
- `syntax_check_on_startup`: Validate SQL syntax before execution (default: true)

**Input Sources:**
- **Local Files**: CSV and Parquet files from local storage

**Sync Actions:**
- `syntax_check`: Validate SQL syntax without execution
- `lineage_visualization`: Generate data lineage visualization
- `execution_plan_visualization`: Visualize execution plan
- `expected_input_tables`: Show expected input tables

Output
======

Provides a list of tables, foreign keys, and schema.

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
