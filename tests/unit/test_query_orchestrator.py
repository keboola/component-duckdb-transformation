import pytest
from keboola.component.exceptions import UserException

from configuration import Block, Code
from query_orchestrator import BlockOrchestrator


def _plan_for(blocks):
    orchestrator = BlockOrchestrator(connection=None)
    orchestrator.add_queries_from_blocks(blocks)
    return orchestrator.build_block_execution_plan()


def _flat_order(plan):
    """Query names flattened in execution order across all batches of block 0."""
    return [query.name for batch in plan.blocks[0].batches for query in batch]


def test_recreating_same_table_in_block_is_not_a_cycle():
    # build -> adjust (reads build's bsm_03) -> final (recreates bsm_03).
    # Last-writer-wins wrongly binds `adjust` to `final`, producing a false cycle.
    blocks = [
        Block(
            name="reconciliation block",
            codes=[
                Code(name="build", script=["CREATE OR REPLACE TABLE bsm_03 AS SELECT * FROM bsm_02"]),
                Code(
                    name="adjust",
                    script=["CREATE OR REPLACE TABLE bsm_03_adj AS SELECT * FROM bsm_03 JOIN refh USING (pc)"],
                ),
                Code(name="final", script=["CREATE OR REPLACE TABLE bsm_03 AS SELECT * FROM bsm_03_adj"]),
            ],
        )
    ]

    plan = _plan_for(blocks)  # must not raise

    order = _flat_order(plan)
    assert order.index("build") < order.index("adjust") < order.index("final")


def test_single_producer_written_after_reader_is_reordered():
    # The DAG must still reorder a reader after its (only) producer even when
    # the producer is written later in the block.
    blocks = [
        Block(
            name="B",
            codes=[
                Code(name="reader", script=["CREATE OR REPLACE TABLE b AS SELECT * FROM a"]),
                Code(name="creator", script=["CREATE OR REPLACE TABLE a AS SELECT 1 AS id"]),
            ],
        )
    ]
    order = _flat_order(_plan_for(blocks))
    assert order.index("creator") < order.index("reader")


def test_insert_runs_after_create_of_same_table():
    blocks = [
        Block(
            name="B",
            codes=[
                Code(name="create_t", script=["CREATE TABLE t (id INTEGER)"]),
                Code(name="insert_t", script=["INSERT INTO t SELECT 1"]),
            ],
        )
    ]
    order = _flat_order(_plan_for(blocks))
    assert order.index("create_t") < order.index("insert_t")


def test_case_variant_dependency_is_wired_across_statements():
    # Reader references BSM (upper) of a table created as bsm (lower);
    # the planner must still schedule the creator first (separate batch).
    blocks = [
        Block(
            name="B",
            codes=[
                Code(name="create", script=["CREATE OR REPLACE TABLE bsm AS SELECT 1 AS id"]),
                Code(name="read", script=["CREATE OR REPLACE TABLE report AS SELECT * FROM BSM"]),
            ],
        )
    ]
    batches = _plan_for(blocks).blocks[0].batches
    assert [q.name for q in batches[0]] == ["create"]
    assert "read" in [q.name for q in batches[1]]


def test_genuine_cycle_still_raises():
    # a depends on b, b depends on a -> unresolvable, must still raise.
    blocks = [
        Block(
            name="B",
            codes=[
                Code(name="q1", script=["CREATE TABLE a AS SELECT * FROM b"]),
                Code(name="q2", script=["CREATE TABLE b AS SELECT * FROM a"]),
            ],
        )
    ]
    with pytest.raises(UserException, match="Circular dependency detected among queries in block"):
        _plan_for(blocks)


def test_multi_output_producer_read_by_multiple_dependencies():
    # One query creates two tables (single script, two statements); a later
    # query reads both. This produces two producer->reader edges with the same
    # source; the planner must still schedule correctly (edges are self-balancing).
    blocks = [
        Block(
            name="B",
            codes=[
                Code(name="make_ab", script=["CREATE TABLE a AS SELECT 1 AS id; CREATE TABLE b AS SELECT 1 AS id;"]),
                Code(name="join_ab", script=["CREATE TABLE c AS SELECT * FROM a JOIN b USING (id)"]),
            ],
        )
    ]
    order = _flat_order(_plan_for(blocks))
    assert order.index("make_ab") < order.index("join_ab")


def test_statement_reading_and_recreating_same_table_is_not_a_cycle():
    # A statement whose table appears in BOTH its dependencies and its outputs
    # (here an UPDATE that reads the table it updates) after a prior CREATE of
    # that table produces a duplicate producer->reader edge; it must not be
    # reported as a circular dependency.
    blocks = [
        Block(
            name="B",
            codes=[
                Code(name="seed", script=["CREATE TABLE t AS SELECT 1 AS id, 5 AS v"]),
                Code(name="bump", script=["UPDATE t SET v = (SELECT max(v) FROM t)"]),
            ],
        )
    ]
    order = _flat_order(_plan_for(blocks))
    assert order.index("seed") < order.index("bump")


def test_reader_before_all_producers_of_recreated_table_waits_for_final():
    # A reader written before a table that is CREATEd twice later must still wait
    # until the table is fully built (bind to the last producer), not run before
    # the table exists.
    blocks = [
        Block(
            name="B",
            codes=[
                Code(name="reader", script=["CREATE TABLE report AS SELECT * FROM x"]),
                Code(name="p1", script=["CREATE TABLE x AS SELECT 1 AS id"]),
                Code(name="p2", script=["CREATE OR REPLACE TABLE x AS SELECT 2 AS id"]),
            ],
        )
    ]
    order = _flat_order(_plan_for(blocks))
    assert order.index("p1") < order.index("p2") < order.index("reader")


def _batch_index(plan, name):
    """Index of the batch (within block 0) that contains query ``name``."""
    for i, batch in enumerate(plan.blocks[0].batches):
        if name in [q.name for q in batch]:
            return i
    raise AssertionError(f"{name} not scheduled")


def test_reader_between_producers_runs_before_the_recreation():
    # create t -> read t (into an independent table) -> recreate t.
    # The reader is bound after the first producer, but it must also be ordered
    # BEFORE the second producer; otherwise the re-creation can land in the same
    # parallel batch as the read and race it (reader sees the wrong version).
    blocks = [
        Block(
            name="B",
            codes=[
                Code(name="create_t", script=["CREATE TABLE t AS SELECT 1 AS id"]),
                Code(name="read_t", script=["CREATE TABLE u AS SELECT * FROM t"]),
                Code(name="recreate_t", script=["CREATE OR REPLACE TABLE t AS SELECT 2 AS id"]),
            ],
        )
    ]
    plan = _plan_for(blocks)
    # The read must complete in an earlier batch than the re-creation, never
    # alongside it.
    assert _batch_index(plan, "create_t") < _batch_index(plan, "read_t")
    assert _batch_index(plan, "read_t") < _batch_index(plan, "recreate_t")
