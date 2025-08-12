import unittest
import os
import sys

# Ensure src is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

from keboola.component.sync_actions import MessageType
from actions.execution_plan_visualization import ExecutionPlanVisualizationAction
from configuration import Block, Code


def _make_blocks():
    return [
        Block(
            name="B1",
            codes=[
                Code(
                    name="C1",
                    script=[
                        "CREATE TABLE t1 AS SELECT * FROM in_a;",
                        "CREATE TABLE t2 AS SELECT * FROM t1 JOIN in_b USING(id);",
                        "CREATE VIEW v_t2 AS SELECT * FROM t2;",
                    ],
                )
            ],
        ),
        Block(
            name="B2",
            codes=[
                Code(
                    name="C2",
                    script=[
                        "SELECT * FROM v_t2 JOIN in_c USING(id);",
                    ],
                )
            ],
        ),
    ]


class TestExecutionPlanVisualizationAction(unittest.TestCase):
    def test_execution_plan_success(self):
        sys.stderr.write("🚀 Starting test: test_execution_plan_success\n")
        sys.stderr.flush()
        action = ExecutionPlanVisualizationAction(max_workers=4)
        res = action.execution_plan_visualization(_make_blocks())

        self.assertEqual(res.type, MessageType.SUCCESS)
        expected_start = (
            "# 🚀 Execution Plan Visualization\n\n"
            "## 📊 Execution Summary\n\n"
            "- **Total Queries:** 4\n"
            "- **Total Batches:** 4\n"
            "- **Max Parallel Workers:** 4\n\n"
            "## 🔄 Execution Flow\n\n"
            "### 🧱 Block: B1\n\n"
            "#### ⚡ Batch (Parallel Execution)\n\n"
            "- **C1_0** (Code: C1)\n"
        )
        # Only verify the start to keep the test robust to ordering of later sections
        self.assertTrue(res.message.startswith(expected_start))

    def test_execution_plan_error(self):
        sys.stderr.write("🚀 Starting test: test_execution_plan_error\n")
        sys.stderr.flush()
        import actions.execution_plan_visualization as mod
        
        original_orchestrator = mod.BlockOrchestrator

        def boom(*_, **__):
            raise RuntimeError("boom")

        try:
            mod.BlockOrchestrator = boom
            action = ExecutionPlanVisualizationAction(max_workers=2)
            res = action.execution_plan_visualization([])

            self.assertEqual(res.type, MessageType.DANGER)
            self.assertIn("Error generating execution plan visualization", res.message)
        finally:
            mod.BlockOrchestrator = original_orchestrator


