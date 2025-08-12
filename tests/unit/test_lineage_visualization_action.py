import os
import sys

# Ensure src is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

from keboola.component.sync_actions import MessageType
from actions.lineage_visualization import LineageVisualizationAction
from configuration import Block, Code
import textwrap


def test_lineage_visualization_success():
    # More complex lineage with CTE, CREATE VIEW, JOINs and multiple blocks
    sql1 = textwrap.dedent(
        """
        WITH base_data AS (
            SELECT * FROM in_x
        )
        CREATE TABLE out_x AS
        SELECT * FROM base_data
        JOIN in_y USING(id);
        """
    ).strip()
    sql2 = "CREATE VIEW v_x AS SELECT id FROM out_x;"
    sql3 = "SELECT * FROM v_x JOIN in_z USING(id);"

    blocks = [
        Block(
            name="B1",
            codes=[
                Code(
                    name="C1",
                    script=[sql1, sql2],
                )
            ],
        ),
        Block(
            name="B2",
            codes=[
                Code(
                    name="C2",
                    script=[sql3],
                )
            ],
        ),
    ]
    action = LineageVisualizationAction()
    res = action.lineage_visualization(blocks)

    assert res.type == MessageType.SUCCESS
    expected = (
        "# 📊 Data Lineage Visualization\n\n"
        "## 🔗 Table Dependencies\n\n"
        "### 📋 in_x\n\n"
        "**Read by:**\n"
        "- `C1_0` (Block: B1, Code: C1)\n\n"
        "### 📋 in_y\n\n"
        "**Read by:**\n"
        "- `C1_0` (Block: B1, Code: C1)\n\n"
        "### 📋 in_z\n\n"
        "**Read by:**\n"
        "- `C2` (Block: B2, Code: C2)\n\n"
        "### 📋 out_x\n\n"
        "**Read by:**\n"
        "- `C1_1` (Block: B1, Code: C1)\n\n"
        "**Created by:**\n"
        "- `C1_0` (Block: B1, Code: C1)\n\n"
        "### 📋 v_x\n\n"
        "**Read by:**\n"
        "- `C2` (Block: B2, Code: C2)\n\n"
        "**Created by:**\n"
        "- `C1_1` (Block: B1, Code: C1)\n\n"
        "## 📈 Query Flow\n\n"
        "### 🧱 B1\n\n"
        "#### 🔧 C1_0\n\n"
        "**Code:** C1\n\n"
        "**Inputs:**\n"
        "- `in_x`\n"
        "- `in_y`\n\n"
        "**Outputs:**\n"
        "- `out_x`\n\n"
        "**SQL:**\n```sql\n"
        f"{sql1}\n"
        "```\n\n"
        "#### 🔧 C1_1\n\n"
        "**Code:** C1\n\n"
        "**Inputs:**\n"
        "- `out_x`\n\n"
        "**Outputs:**\n"
        "- `v_x`\n\n"
        "**SQL:**\n```sql\n"
        f"{sql2}\n"
        "```\n\n"
        "### 🧱 B2\n\n"
        "#### 🔧 C2\n\n"
        "**Code:** C2\n\n"
        "**Inputs:**\n"
        "- `in_z`\n"
        "- `v_x`\n\n"
        "**SQL:**\n```sql\n"
        f"{sql3}\n"
        "```\n\n"
    )
    # Normalize whitespace to avoid false negatives from indentation/spacing
    def _normalize(s: str) -> str:
        return "\n".join(line.rstrip() for line in s.strip().splitlines())

    assert _normalize(res.message) == _normalize(expected)


def test_lineage_visualization_handles_no_tables():
    # SQL that has no FROM (constant select) should not break
    blocks = [Block(name="B", codes=[Code(name="C", script=["SELECT 1 AS x;"])])]
    action = LineageVisualizationAction()
    res = action.lineage_visualization(blocks)
    assert res.type == MessageType.SUCCESS
    # Should still include the query section and SQL
    assert "SELECT 1 AS x;" in res.message


def test_lineage_visualization_error_path(monkeypatch):
    action = LineageVisualizationAction()
    def boom(_):
        raise RuntimeError("boom")
    monkeypatch.setattr(action.sql_parser, "iterate_blocks", boom)
    res = action.lineage_visualization([])
    assert res.type == MessageType.DANGER


def test_lineage_visualization_empty():
    action = LineageVisualizationAction()
    res = action.lineage_visualization([])

    assert res.type == MessageType.SUCCESS


def test_lineage_visualization_error(monkeypatch):
    action = LineageVisualizationAction()

    def boom(_):
        raise RuntimeError("boom")

    monkeypatch.setattr(action.sql_parser, "iterate_blocks", boom)
    res = action.lineage_visualization([])

    assert res.type == MessageType.DANGER
    assert "Error generating lineage visualization" in res.message


