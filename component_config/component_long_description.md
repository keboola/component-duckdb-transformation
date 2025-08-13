Advanced DuckDB SQL transformation component for Keboola platform that provides sophisticated data processing capabilities with intelligent execution planning.

**Key Features:**

**Block-Based Orchestration**
- Execute SQL queries in consecutive blocks for logical separation
- Parallel execution of independent queries within each block
- Automatic dependency detection and execution planning

**Performance Optimization**
- Configurable parallel thread execution
- Automatic system resource detection (CPU/memory)
- Optimized DuckDB configuration for maximum performance

**SQL Processing**
- Advanced SQL parsing with SQLGlot integration
- Automatic dependency analysis between queries
- Syntax validation with detailed error reporting
- Support for complex SQL operations and CTEs

**Data Handling**
- Local CSV and Parquet file support
- Automatic data type inference for CSV files
- Snowflake INTEGER column optimization for Parquet files
- Flexible input/output table mapping

**Development Tools**
- SQL syntax checking without execution
- Data lineage visualization
- Execution plan visualization
- Expected input tables analysis

**System Integration**
- Automatic resource detection from cgroup limits
- Configurable memory and thread limits
- Debug logging and performance monitoring
- Comprehensive error handling and reporting

This component is designed for high-performance data transformations with intelligent execution planning, making it ideal for complex ETL workflows requiring parallel processing and dependency management.