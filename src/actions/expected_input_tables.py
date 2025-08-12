"""
Expected input tables action for dependency analysis.
"""

import logging

from keboola.component.sync_actions import MessageType, ValidationResult

from sql_parser import SQLParser


class ExpectedInputTablesAction:
    """Handles expected input tables sync action."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sql_parser = SQLParser()

    def expected_input_tables(self, blocks):
        """
        Returns a comma-separated list of required external input tables (filtering out likely CTE aliases).
        """
        try:
            all_dependencies = set()
            all_outputs = set()

            # Use SQLParser to iterate through blocks and extract dependencies
            for block, code, script, _ in self.sql_parser.iterate_blocks(blocks):
                dependencies, outputs = self.sql_parser.extract_dependencies_and_outputs(script)
                all_dependencies.update(dependencies)
                all_outputs.update(outputs)

            # External tables are dependencies that are never created anywhere
            # CTE aliases are already excluded during dependency extraction
            external_tables = all_dependencies - all_outputs

            message = ", ".join(sorted(external_tables)) if external_tables else ""
            return ValidationResult(message=message, type=MessageType.SUCCESS)
        except Exception as e:
            error_message = f"Error analyzing expected input tables: {str(e)}"
            return ValidationResult(message=error_message, type=MessageType.DANGER)
