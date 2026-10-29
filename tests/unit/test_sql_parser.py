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
