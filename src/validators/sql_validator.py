"""SQL validation module."""

import logging
from typing import Dict, List

import sqlglot
from keboola.component.sync_actions import MessageType, ValidationResult
from sqlglot.errors import ParseError

from sql_parser import SQLParser


class SQLValidator:
    """SQL syntax and semantic validation."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sql_parser = SQLParser()

    def validate_queries(self, blocks) -> ValidationResult:
        """
        Validate all SQL queries in blocks.
        Args:
            blocks: List of Block objects (Pydantic models) with codes containing SQL scripts
        Returns:
            ValidationResult with validation results
        """
        total_queries = 0
        valid_queries = 0
        errors = []
        try:
            for block, code, script, script_index in self.sql_parser.iterate_blocks(blocks):
                query_name = self.sql_parser.get_query_name(code, script_index)
                total_queries += 1
                try:
                    # Parse SQL with sqlglot
                    parsed = sqlglot.parse(script, read="duckdb")
                    if not parsed:
                        errors.append(f"Block '{block.name}' > Query '{query_name}': Empty or invalid SQL query")
                    else:
                        # Additional validation for common errors
                        warnings = self._check_common_sql_errors(script)
                        if warnings:
                            errors.extend(
                                [f"Block '{block.name}' > Query '{query_name}': {w['error']}" for w in warnings]
                            )
                        else:
                            valid_queries += 1
                except ParseError as e:
                    errors.append(f"Block '{block.name}' > Query '{query_name}': Syntax error: {str(e)}")
                except Exception as e:
                    errors.append(f"Block '{block.name}' > Query '{query_name}': Unexpected error: {str(e)}")
            # Create appropriate message based on results
            if total_queries == 0:
                message = "No SQL queries found to validate."
                message_type = MessageType.WARNING
            elif len(errors) == 0:
                message = f"✅ All {total_queries} SQL queries are syntactically valid."
                message_type = MessageType.SUCCESS
            else:
                error_summary = (
                    f"❌ Found {len(errors)} syntax errors in {total_queries} queries "
                    f"({valid_queries} valid, {len(errors)} invalid):\n"
                )
                error_summary += "\n".join(f"• {error}" for error in errors[:10])
                if len(errors) > 10:
                    error_summary += f"\n... and {len(errors) - 10} more errors"
                message = error_summary
                message_type = MessageType.DANGER
            return ValidationResult(message=message, type=message_type)
        except Exception as e:
            error_message = f"System error during syntax check: {str(e)}"
            return ValidationResult(message=error_message, type=MessageType.DANGER)

    def validate_single_query(self, sql: str, query_name: str = "query") -> ValidationResult:
        """
        Validate a single SQL query.
        Args:
            sql: SQL query to validate
            query_name: Name of the query for error reporting
        Returns:
            ValidationResult with validation result
        """
        try:
            # Parse SQL with sqlglot
            parsed = sqlglot.parse(sql, read="duckdb")
            if not parsed:
                message = f"❌ Query '{query_name}': Empty or invalid SQL query"
                return ValidationResult(message=message, type=MessageType.DANGER)
            # Check for common errors
            warnings = self._check_common_sql_errors(sql)
            if warnings:
                error_messages = [w["error"] for w in warnings]
                message = f"❌ Query '{query_name}': {'; '.join(error_messages)}"
                return ValidationResult(message=message, type=MessageType.DANGER)
            message = f"✅ Query '{query_name}': SQL is syntactically valid"
            return ValidationResult(message=message, type=MessageType.SUCCESS)
        except ParseError as e:
            message = f"❌ Query '{query_name}': Syntax error: {str(e)}"
            return ValidationResult(message=message, type=MessageType.DANGER)
        except Exception as e:
            message = f"❌ Query '{query_name}': Unexpected error: {str(e)}"
            return ValidationResult(message=message, type=MessageType.DANGER)

    @staticmethod
    def _check_common_sql_errors(sql: str) -> List[Dict[str, str]]:
        """
        Check for common SQL errors that sqlglot might not catch.
        Args:
            sql: SQL query to check
        Returns:
            List of error messages
        """
        errors = []
        sql_upper = sql.upper()
        # Check for common typos in keywords
        if "CREATE OR RE PLACE" in sql_upper:
            errors.append({"error": "Syntax error: 'RE PLACE' should be 'REPLACE'"})
        if "CREATE OR REPLACE VIEW" in sql_upper and "AS" not in sql_upper:
            errors.append({"error": "Syntax error: CREATE VIEW missing 'AS' keyword"})
        if "SELECT" in sql_upper and "FROM" not in sql_upper:
            errors.append({"error": "Syntax error: SELECT statement missing 'FROM' clause"})
        # Check for WHERE clause without comparison operators
        where_ops = ["=", ">", "<", "!=", "LIKE", "IN", "BETWEEN", "IS"]
        if "WHERE" in sql_upper and not any(op in sql_upper for op in where_ops):
            errors.append({"error": "Syntax error: WHERE clause missing comparison operator"})
        # Check for unmatched parentheses
        if sql.count("(") != sql.count(")"):
            errors.append({"error": "Syntax error: Unmatched parentheses"})
        # Check for DuckDB-specific function issues
        if "PERCENTILE(" in sql_upper and "WITHIN GROUP" in sql_upper:
            errors.append({"error": "DuckDB Error: Use PERCENTILE_CONT() or PERCENTILE_DISC() instead of PERCENTILE()"})
        # Check for unsupported window functions
        if "PERCENTILE_CONT(" in sql_upper and "OVER (" in sql_upper and "WITHIN GROUP" not in sql_upper:
            errors.append(
                {"error": "DuckDB Error: PERCENTILE_CONT() cannot be used as window function, use WITHIN GROUP instead"}
            )
        # Check for common type casting issues
        if (
            "CAST(" in sql_upper
            and "AS VARCHAR" in sql_upper
            and any(op in sql_upper for op in ["+", "-", "*", "/", ">", "<", "="])
            and "||" not in sql_upper
        ):
            errors.append({"error": "Warning: Arithmetic operations on VARCHAR columns may cause Binder Errors"})
        return errors

    def extract_table_dependencies(self, sql: str) -> Dict[str, List[str]]:
        """
        Extract table dependencies from SQL query.
        Args:
            sql: SQL query to analyze
        Returns:
            Dict with 'dependencies' (tables read) and 'outputs' (tables created)
        """
        try:
            parsed = sqlglot.parse(sql, read="duckdb")
            dependencies = set()
            outputs = set()
            for statement in parsed:
                if statement is None:
                    continue
                # Find table references (dependencies)
                for table in statement.find_all(sqlglot.exp.Table):
                    table_name = table.name
                    if table_name:
                        dependencies.add(table_name)
                # Find CREATE statements (outputs)
                if isinstance(statement, sqlglot.exp.Create):
                    if statement.this and hasattr(statement.this, "name"):
                        outputs.add(statement.this.name)
            return {"dependencies": list(dependencies), "outputs": list(outputs)}
        except Exception as e:
            self.logger.warning(f"Failed to extract dependencies from SQL: {e}")
            return {"dependencies": [], "outputs": []}
