"""
Lineage visualization action for data lineage analysis.
"""

import logging
from keboola.component.sync_actions import ValidationResult, MessageType

from sql_parser import SQLParser


class LineageVisualizationAction:
    """Handles lineage visualization sync action."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.sql_parser = SQLParser()

    def lineage_visualization(self, blocks):
        """
        Generate data lineage visualization from SQL queries.
        Returns ValidationResult with markdown lineage diagram.
        """
        try:
            # Collect all queries and their dependencies
            queries = []
            for block, code, script, script_index in self.sql_parser.iterate_blocks(blocks):
                query_name = self.sql_parser.get_query_name(code, script_index)
                # Parse SQL to extract dependencies and outputs
                dependencies, outputs = self.sql_parser.extract_dependencies_and_outputs(script)
                queries.append(
                    {
                        "name": query_name,
                        "block": block.name,
                        "code": code.name,
                        "dependencies": dependencies,
                        "outputs": outputs,
                        "sql": script,
                    }
                )
            # Generate markdown lineage diagram
            markdown = self._generate_lineage_markdown(queries)
            return ValidationResult(message=markdown, type=MessageType.SUCCESS)
        except Exception as e:
            error_message = f"Error generating lineage visualization: {str(e)}"
            return ValidationResult(message=error_message, type=MessageType.DANGER)

    @staticmethod
    def _generate_lineage_markdown(queries: list) -> str:
        """Generate markdown lineage diagram from queries."""
        markdown = "# 📊 Data Lineage Visualization\n\n"
        # Collect all tables
        all_tables = set()
        for query in queries:
            all_tables.update(query["dependencies"])
            all_tables.update(query["outputs"])
        # Group by blocks
        blocks = {}
        for query in queries:
            block_name = query["block"]
            if block_name not in blocks:
                blocks[block_name] = []
            blocks[block_name].append(query)
        markdown += "## 🔗 Table Dependencies\n\n"
        # Show table dependencies
        for table in sorted(all_tables):
            markdown += f"### 📋 {table}\n\n"
            # Find queries that read this table
            readers = [q for q in queries if table in q["dependencies"]]
            if readers:
                markdown += "**Read by:**\n"
                for reader in readers:
                    markdown += f"- `{reader['name']}` (Block: {reader['block']}, Code: {reader['code']})\n"
                markdown += "\n"
            # Find queries that create this table
            creators = [q for q in queries if table in q["outputs"]]
            if creators:
                markdown += "**Created by:**\n"
                for creator in creators:
                    markdown += f"- `{creator['name']}` (Block: {creator['block']}, Code: {creator['code']})\n"
                markdown += "\n"
        markdown += "## 📈 Query Flow\n\n"
        # Show query flow by blocks
        for block_name, block_queries in blocks.items():
            markdown += f"### 🧱 {block_name}\n\n"
            for query in block_queries:
                markdown += f"#### 🔧 {query['name']}\n\n"
                markdown += f"**Code:** {query['code']}\n\n"
                if query["dependencies"]:
                    markdown += "**Inputs:**\n"
                    for dep in sorted(query["dependencies"]):
                        markdown += f"- `{dep}`\n"
                    markdown += "\n"
                if query["outputs"]:
                    markdown += "**Outputs:**\n"
                    for output in sorted(query["outputs"]):
                        markdown += f"- `{output}`\n"
                    markdown += "\n"
                markdown += "**SQL:**\n```sql\n"
                markdown += query["sql"]
                markdown += "\n```\n\n"
        return markdown
