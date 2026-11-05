import pytest

from sql_parser import SQLParser


def test_case_variant_self_reference_is_not_a_dependency():
    # A statement that creates "BSM_03" and reads bsm_03 must not list bsm_03
    # as a dependency (DuckDB identifiers are case-insensitive), while the
    # output name keeps its original case.
    deps, outputs = SQLParser().extract_dependencies_and_outputs(
        'CREATE OR REPLACE TABLE "BSM_03" AS SELECT * FROM bsm_03'
    )
    assert deps == set()
    assert any(o.casefold() == "bsm_03" for o in outputs)


def test_external_dependency_is_still_reported_with_original_case():
    deps, outputs = SQLParser().extract_dependencies_and_outputs(
        "CREATE OR REPLACE TABLE bsm_03 AS SELECT * FROM bsm_02"
    )
    assert "bsm_02" in deps
    assert "bsm_03" in outputs
    assert "bsm_03" not in deps


@pytest.mark.parametrize(
    "sql",
    [
        "INSTALL httpfs",
        "FORCE INSTALL spatial FROM core_nightly",
        "LOAD httpfs",
        "SET threads = 4",
        "SET VARIABLE x = 1",
        "RESET threads",
        "PRAGMA enable_profiling",
        "USE db2",
        "ATTACH ':memory:' AS db2",
        "DETACH db2",
        "CREATE SECRET (TYPE gcs, KEY_ID 'a', SECRET 'b')",
        "CREATE OR REPLACE PERSISTENT SECRET s (TYPE s3)",
        "DROP SECRET s",
        "CREATE MACRO add_one(a) AS a + 1",
        "CREATE SCHEMA s1",
        "CREATE TYPE mood AS ENUM ('a', 'b')",
        "DROP MACRO add_one",
        "DROP SCHEMA s1 CASCADE",
        "DROP SEQUENCE seq",
        "CALL dbgen(sf = 0.1)",
        "SELEC 1",
    ],
)
def test_session_and_unparseable_statements_are_order_barriers(sql):
    assert SQLParser().is_order_barrier(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1",
        "CREATE OR REPLACE TABLE t AS SELECT 1 AS id",
        "CREATE TEMP TABLE t (id INTEGER)",
        "CREATE VIEW v AS SELECT 1 AS id",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET id = 2",
        "DELETE FROM t",
        "DROP TABLE IF EXISTS t",
        "DROP VIEW v",
        "COPY (SELECT 1) TO '/data/out/files/x.csv'",
    ],
)
def test_table_statements_are_not_order_barriers(sql):
    assert not SQLParser().is_order_barrier(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "SET VARIABLE snapshot_date = DATE '2026-10-20'",
        "SET TimeZone = 'UTC'",
        "SET SESSION TimeZone = 'UTC'",
        "SET GLOBAL TimeZone = 'UTC'",
        "-- comment\nset variable x = 1",
        "RESET VARIABLE x",
        "reset TimeZone",
    ],
)
def test_session_statements_are_detected(sql):
    assert SQLParser().is_session_statement(sql)


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE t AS SELECT getvariable('x') AS x",
        "UPDATE t SET v = 1",
        "SELECT current_setting('TimeZone')",
    ],
)
def test_other_statements_are_not_session_statements(sql):
    assert not SQLParser().is_session_statement(sql)
