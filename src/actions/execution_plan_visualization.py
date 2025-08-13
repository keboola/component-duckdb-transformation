"""
Execution plan visualization action for query execution planning.
"""

import logging
from keboola.component.sync_actions import ValidationResult, MessageType
from query_orchestrator import BlockOrchestrator


class ExecutionPlanVisualizationAction:
    """Handles execution plan visualization sync action."""

    def __init__(self, max_workers: int):
        self.max_workers = max_workers
        self.logger = logging.getLogger(self.__class__.__name__)

    def execution_plan_visualization(self, blocks):
        """
        Generate execution plan visualization showing block order and parallel execution.
        Returns ValidationResult with markdown execution plan.
        """
        try:
            # Create orchestrator to build execution plan
            orchestrator = BlockOrchestrator(
                connection=None,  # We don't need actual connection for planning
                max_workers=self.max_workers,
            )
            orchestrator.add_queries_from_blocks(blocks)
            # Generate markdown execution plan
            return ValidationResult(
                message=self._generate_execution_plan_markdown(orchestrator), type=MessageType.SUCCESS
            )
        except Exception as e:
            return ValidationResult(
                message=f"Error generating execution plan visualization: {str(e)}", type=MessageType.DANGER
            )

    @staticmethod
    def _generate_execution_plan_markdown(orchestrator: BlockOrchestrator) -> str:
        """Generate markdown execution plan from orchestrator."""
        markdown = "# 🚀 Execution Plan Visualization\n\n"
        # Build execution plan
        batches = orchestrator.build_block_execution_plan()
        markdown += "## 📊 Execution Summary\n\n"
        markdown += f"- **Total Queries:** {len(orchestrator.queries)}\n"
        markdown += f"- **Total Batches:** {len(batches)}\n"
        markdown += f"- **Max Parallel Workers:** {orchestrator.max_workers}\n\n"
        markdown += "## 🔄 Execution Flow\n\n"
        # Group queries by block for display
        block_queries = {}
        for query in orchestrator.queries:
            if query.block_name not in block_queries:
                block_queries[query.block_name] = []
            block_queries[query.block_name].append(query)
        for block_name in block_queries.keys():
            markdown += f"### 🧱 Block: {block_name}\n\n"
            # Find batches for this block
            block_batches = []
            for batch in batches:
                if any(q.block_name == block_name for q in batch):
                    block_batches.append(batch)
            for batch in block_batches:
                markdown += "#### ⚡ Batch (Parallel Execution)\n\n"
                for query in batch:
                    if query.block_name == block_name:
                        markdown += f"- **{query.name}** (Code: {query.code_name})\n"
                        if query.dependencies:
                            deps = ", ".join(sorted(query.dependencies))
                            markdown += f"  - Dependencies: `{deps}`\n"
                        if query.outputs:
                            outputs = ", ".join(sorted(query.outputs))
                            markdown += f"  - Outputs: `{outputs}`\n"
                        markdown += "\n"
            markdown += "---\n\n"
        markdown += "## 🔍 Dependency Analysis\n\n"
        # Show dependency graph
        for query in orchestrator.queries:
            markdown += f"### 📋 {query.name}\n\n"
            markdown += f"**Block:** {query.block_name}\n"
            markdown += f"**Code:** {query.code_name}\n\n"
            if query.dependencies:
                markdown += "**Dependencies:**\n"
                for dep in sorted(query.dependencies):
                    markdown += f"- `{dep}`\n"
                markdown += "\n"
            if query.outputs:
                markdown += "**Outputs:**\n"
                for output in sorted(query.outputs):
                    markdown += f"- `{output}`\n"
                markdown += "\n"
        return markdown
