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
