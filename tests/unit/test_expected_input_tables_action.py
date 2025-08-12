import unittest
import os
import sys

# Ensure src is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../src")))

from keboola.component.sync_actions import MessageType
from actions.expected_input_tables import ExpectedInputTablesAction
from configuration import Block, Code, Configuration


class TestExpectedInputTablesAction(unittest.TestCase):
    def test_expected_input_tables_success(self):
        action = ExpectedInputTablesAction()
        cfg = Configuration(
            blocks=[
                Block(
                    name="B1",
                    codes=[
                        Code(
                            name="C1",
                            script=[
                                # CTE alias should be filtered, external deps in_a and in_b should remain
                                """
                                WITH base_data AS (
                                    SELECT * FROM in_a
                                )
                                CREATE TABLE out_a AS
                                SELECT *
                                FROM base_data bd
                                JOIN in_b USING(id);
                                """.strip(),
                                # Create a view that reads out_a, so out_a is not external
                                "CREATE VIEW v1 AS SELECT * FROM out_a;",
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
                                # Another CTE alias filtered out, selects from v1 and in_c
                                """
                                WITH temp_data AS (
                                    SELECT * FROM in_c
                                )
                                SELECT * FROM v1 JOIN temp_data USING(id);
                                """.strip(),
                            ],
                        )
                    ],
                ),
            ]
        )
        res = action.expected_input_tables(cfg.blocks)

        self.assertEqual(res.type, MessageType.SUCCESS)
        # external tables = dependencies - outputs => {in_a, in_b, in_c}
        self.assertEqual(res.message, "in_a, in_b, in_c")

    def test_expected_input_tables_empty(self):
        action = ExpectedInputTablesAction()
        res = action.expected_input_tables([])

        self.assertEqual(res.type, MessageType.SUCCESS)
        self.assertEqual(res.message, "")

    def test_expected_input_tables_error(self):
        action = ExpectedInputTablesAction()
        original_iterate_blocks = action.sql_parser.iterate_blocks

        def boom(_):
            raise RuntimeError("boom")

        try:
            # Force error path
            action.sql_parser.iterate_blocks = boom
            res = action.expected_input_tables([])

            self.assertEqual(res.type, MessageType.DANGER)
            self.assertIn("Error analyzing expected input tables", res.message)
        finally:
            action.sql_parser.iterate_blocks = original_iterate_blocks

    def test_expected_input_tables_filters_cte_aliases_only(self):
        # If all identifiers are typical CTE aliases, result should be empty
        action = ExpectedInputTablesAction()
        cfg = Configuration(
            blocks=[
                Block(
                    name="B",
                    codes=[
                        Code(
                            name="C",
                            script=[
                                """
                                WITH base_data AS (SELECT * FROM raw_data),
                                     temp_data AS (SELECT * FROM base_data)
                                SELECT * FROM temp_data;
                                """.strip()
                            ],
                        )
                    ],
                )
            ]
        )
        res = action.expected_input_tables(cfg.blocks)
        self.assertEqual(res.type, MessageType.SUCCESS)
        # Without heuristic filtering, raw_data remains as true external dependency
        self.assertEqual(res.message, "raw_data")

    def test_expected_input_tables_create_without_inputs(self):
        # CREATE from constant has no external dependencies
        action = ExpectedInputTablesAction()
        cfg = Configuration(
            blocks=[
                Block(
                    name="B",
                    codes=[
                        Code(
                            name="C",
                            script=[
                                "CREATE TABLE t AS SELECT 1 AS id;",
                            ],
                        )
                    ],
                )
            ]
        )
        res = action.expected_input_tables(cfg.blocks)
        self.assertEqual(res.type, MessageType.SUCCESS)
        self.assertEqual(res.message, "")