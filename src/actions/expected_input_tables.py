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

    def expected_input_tables(self, blocks, available_tables=None):
        """
        Returns a comma-separated list of required external input tables (filtering out likely CTE aliases).
        If available_tables is provided, validates against them and returns detailed report.
        """
        try:
            # Two-pass analysis for better accuracy
            all_dependencies = set()
            all_outputs = set()

            # First pass: collect ALL outputs from ALL queries
            # This ensures we know about tables created in any block/query
            for block, code, script, _ in self.sql_parser.iterate_blocks(blocks):
                _, outputs = self.sql_parser.extract_dependencies_and_outputs(script)
                all_outputs.update(outputs)

            # Second pass: collect dependencies, now with full knowledge of outputs
            for block, code, script, _ in self.sql_parser.iterate_blocks(blocks):
                dependencies, _ = self.sql_parser.extract_dependencies_and_outputs(script)
                all_dependencies.update(dependencies)

            # External tables are dependencies that are never created anywhere in the entire pipeline
            external_tables = all_dependencies - all_outputs

            # If no validation requested, return simple comma-separated list
            if available_tables is None:
                message = ", ".join(sorted(external_tables)) if external_tables else ""
                return ValidationResult(message=message, type=MessageType.SUCCESS)

            # Perform validation against available tables
            return self._validate_against_available_tables(external_tables, available_tables)

        except Exception as e:
            error_message = f"Error analyzing expected input tables: {str(e)}"
            return ValidationResult(message=error_message, type=MessageType.DANGER)

    def _validate_against_available_tables(self, expected_tables, available_tables):
        """Validate expected tables against available input tables."""
        # Get available table names (remove .csv suffix)
        available_table_names = {
            table.destination.removesuffix(".csv").removesuffix(".parquet").removesuffix(".parq")
            for table in available_tables
        }

        # Compare expected vs available
        missing_tables = expected_tables - available_table_names
        extra_tables = available_table_names - expected_tables

        # Build detailed message
        message = self._build_validation_message(expected_tables, available_table_names, missing_tables, extra_tables)

        # Determine message type
        if missing_tables:
            message_type = MessageType.DANGER
        elif extra_tables:
            message_type = MessageType.WARNING
        else:
            message_type = MessageType.SUCCESS

        return ValidationResult(message=message, type=message_type)

    def _build_validation_message(self, expected_tables, available_table_names, missing_tables, extra_tables):
        """Build detailed validation message."""
        lines = []

        # Header with summary
        if not expected_tables:
            lines.append("✅ No input tables required by SQL queries")
            return "\n".join(lines)

        lines.append("📋 **Input Tables Validation Report**")
        lines.append("")

        # Expected tables section
        lines.append(f"**Required tables ({len(expected_tables)}):**")
        for table in sorted(expected_tables):
            status = "✅" if table in available_table_names else "❌"
            lines.append(f"  {status} `{table}`")
        lines.append("")

        # Available tables section
        lines.append(f"**Available tables ({len(available_table_names)}):**")
        for table in sorted(available_table_names):
            status = "✅" if table in expected_tables else "⚠️"
            lines.append(f"  {status} `{table}`")
        lines.append("")

        # Issues section
        if missing_tables or extra_tables:
            lines.append("**Issues found:**")

            if missing_tables:
                missing_table_list = ", ".join(f"`{t}`" for t in sorted(missing_tables))
                lines.append(f"❌ **Missing tables ({len(missing_tables)}):** {missing_table_list}")

            if extra_tables:
                extra_table_list = ", ".join(f"`{t}`" for t in sorted(extra_tables))
                lines.append(f"⚠️ **Extra tables ({len(extra_tables)}):** {extra_table_list}")
        else:
            lines.append("✅ **All required tables are available!**")

        return "\n".join(lines)
