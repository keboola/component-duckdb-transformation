"""
SQL parsing utilities for dependency analysis and query processing.
"""

import logging
from typing import List, Set, Tuple

import sqlglot
from sqlglot import exp

from configuration import Block, Code


class SQLParser:
    """Utility class for parsing SQL and extracting dependencies and outputs."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def extract_dependencies_and_outputs(self, sql: str) -> Tuple[Set[str], Set[str]]:
        """
        Extract table dependencies and outputs from SQL query.

        Args:
            sql: SQL query string

        Returns:
            Tuple of (dependencies, outputs) sets
        """
        try:
            parsed = sqlglot.parse(sql, read="duckdb")
            dependencies = set()
            outputs = set()

            for statement in parsed:
                if statement is None:
                    continue

                # Find all table references (dependencies)
                for table in statement.find_all(exp.Table):
                    table_name = table.name
                    if table_name:
                        dependencies.add(table_name)

                # Find CREATE statements (outputs)
                if isinstance(statement, exp.Create):
                    if statement.this and hasattr(statement.this, "name"):
                        outputs.add(statement.this.name)

                # Remove CTEs from dependencies (they're defined in the same query)
                for cte in statement.find_all(exp.CTE):
                    if cte.alias:
                        dependencies.discard(cte.alias)

            # Remove outputs from dependencies (can't depend on what you create)
            dependencies = dependencies - outputs

            return dependencies, outputs

        except Exception as e:
            self.logger.warning(f"Failed to parse SQL: {e}")
            return set(), set()

    @staticmethod
    def iterate_blocks(blocks: List[Block]):
        """
        Generator that yields (block, code, script, index) tuples.

        Args:
            blocks: List of blocks from configuration

        Yields:
            Tuple of (block, code, script, script_index)
        """
        for block in blocks:
            for code in block.codes:
                for i, script in enumerate(code.script):
                    yield block, code, script, i

    @staticmethod
    def get_query_name(code: Code, script_index: int) -> str:
        """
        Generate query name from code and script index.

        Args:
            code: Code object
            script_index: Index of script in code.script list

        Returns:
            Query name string
        """
        if len(code.script) > 1:
            return f"{code.name}_{script_index}"
        return code.name
