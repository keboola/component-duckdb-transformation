# CFTL-715 False-Positive Circular Dependency Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop `keboola.duckdb-transformation` from raising a false "Circular dependency detected" error when a block creates the same table more than once.

**Architecture:** The in-block execution planner currently resolves table producers with a global *last-writer-wins* map, so a query reading an early version of a table gets wired to a later re-creation of it — a backward edge that the topological sort reports as a cycle. Replace that with **order-aware, in-block producer resolution**: within a block (queries are in author order), a reader binds to the most recent producer that *precedes* it, and repeated producers of the same table are serialized in author order. Blocks already run sequentially, so cross-block dependencies need no explicit edges. A separate, smaller change makes the parser's self-reference removal case-insensitive (DuckDB identifiers are case-insensitive).

**Tech Stack:** Python 3.13 (`.python-version`), `sqlglot` (parsed with `read="duckdb"`), `pydantic` config models, `pytest` (+ `keboola.datadirtest` for functional tests), `uv` for env/deps, `ruff` for lint/format.

## Global Constraints

- **Preserve original-case table names** in the return value of `SQLParser.extract_dependencies_and_outputs`. Two sync actions (`actions/expected_input_tables.py:31`, `actions/lineage_visualization.py:30`) and `actions/execution_plan_visualization.py` render those names to users and compare them against real storage destinations (`expected_input_tables.py:59-60`). Case-normalization is allowed only as a **comparison key** (via `.casefold()`), never by mutating the stored names.
- **Blocks execute strictly in author order** (`build_block_execution_plan` iterates `block_queries.keys()` in insertion order; `execute()` runs blocks consecutively). Rely on this — do **not** add cross-block dependency edges.
- **DuckDB identifiers are case-insensitive and case-preserving.** Use `str.casefold()` for identifier comparison.
- **All code must pass `uv run ruff check` and `uv run ruff format --check`.**
- Functional (datadir) tests run the component via subprocess and **skip locally** when the per-version DuckDB venvs are absent (`tests/test_functional.py` `pytest.skip(...)`); they are validated in Docker/CI. Unit tests need neither Docker nor a DuckDB connection.

---

## Getting started

- [ ] **Create the working branch**

```bash
git checkout main
git pull
git checkout -b fix/CFTL-715
```

(Linear pre-generated `matyasjirat-cftl-715-support-16838-circular-dependency-error`; `fix/CFTL-715` is shorter and matches the repo's `fix/CFTL-580` convention. Use either.)

---

## Task 1: Order-aware in-block producer resolution (core fix)

**Files:**
- Modify: `src/query_orchestrator.py` — rewrite `_create_parallel_batches_for_block` (currently lines 104-163) and simplify `BlockOrchestrator.build_block_execution_plan` (currently lines 216-269).
- Test: `tests/unit/test_query_orchestrator.py` (create)

**Interfaces:**
- Consumes: `Query` dataclass (`name: str`, `sql: str`, `dependencies: set[str]`, `outputs: set[str]`, `block_name: str`), `Batch`, `Block`, `ExecutionPlan` (all already in `src/query_orchestrator.py`); `BlockOrchestrator(connection, max_workers=4)` and `BlockOrchestrator.add_queries_from_blocks(blocks)`; `configuration.Block`, `configuration.Code`.
- Produces: `_create_parallel_batches_for_block(block_queries: list[Query]) -> list[Batch]` (note: the `producers` parameter is **removed**); `BlockOrchestrator.build_block_execution_plan() -> ExecutionPlan` (unchanged signature, simplified body). Ordering guarantees other tasks rely on: within a block, a producer is always scheduled before the queries that read it, repeated producers of one table run in author order, and a genuinely cyclic block still raises `keboola.component.exceptions.UserException` with message containing `"Circular dependency detected among queries in block"`.

- [ ] **Step 1: Write the failing regression test (the exact reproducer)**

Create `tests/unit/test_query_orchestrator.py`:

```python
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
            name="MAINCON OFFSET RECONCILIATION FINAL",
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
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_query_orchestrator.py::test_recreating_same_table_in_block_is_not_a_cycle -v`
Expected: FAIL — raises `UserException: Circular dependency detected among queries in block: ...`.

- [ ] **Step 3: Rewrite `_create_parallel_batches_for_block`**

In `src/query_orchestrator.py`, replace the whole function (lines 104-163) with:

```python
def _create_parallel_batches_for_block(block_queries: list[Query]) -> list[Batch]:
    """
    Create parallel batches for the queries of a single block using an
    order-aware topological sort.

    ``block_queries`` is in author order, so a query's list index is its
    position. A reader of table T binds to the most recent producer of T that
    *precedes* it (never a later re-creation), and multiple producers of the
    same table are serialized in author order. This prevents a false "circular
    dependency" when a block creates the same table more than once (e.g.
    build -> adjust -> re-build under one name). Table identifiers are compared
    case-insensitively because DuckDB identifiers are case-insensitive.
    """
    remaining = {q.name: q for q in block_queries}
    local_graph: dict[str, list[str]] = defaultdict(list)
    local_in_degree = {q.name: 0 for q in block_queries}

    def add_edge(src_index: int, dst_index: int) -> None:
        """Register 'src must run before dst'; ignore self-edges."""
        src = block_queries[src_index]
        dst = block_queries[dst_index]
        if src.name == dst.name:
            return
        local_graph[src.name].append(dst.name)
        local_in_degree[dst.name] += 1

    # Producers of each table within this block, in author order (ascending index).
    producers_by_table: dict[str, list[int]] = defaultdict(list)
    for index, query in enumerate(block_queries):
        for output in query.outputs:
            producers_by_table[output.casefold()].append(index)

    # Serialize repeated producers of the same table: p0 -> p1 -> p2 ...
    for indices in producers_by_table.values():
        for earlier, later in zip(indices, indices[1:]):
            add_edge(earlier, later)

    # Bind each reader to the correct producer, respecting statement order.
    for index, query in enumerate(block_queries):
        for dep in query.dependencies:
            producer_indices = producers_by_table.get(dep.casefold())
            if not producer_indices:
                continue  # produced in an earlier block, or a runtime-missing table
            preceding = [i for i in producer_indices if i < index]
            if preceding:
                add_edge(preceding[-1], index)
            elif len(producer_indices) == 1:
                # Single producer written after the reader: keep author-order-agnostic reordering.
                add_edge(producer_indices[0], index)
            # else: several producers, all at/after the reader -> treat as external.

    batches: list[Batch] = []
    while remaining:
        # Find queries with no remaining in-block dependencies.
        ready = [remaining[name] for name in remaining if local_in_degree[name] == 0]
        if not ready:
            remaining_names = list(remaining.keys())
            logging.error("Circular dependency detected in block!")
            logging.error(f"Remaining queries: {remaining_names}")
            for name in remaining_names:
                logging.error(f"Query '{name}' depends on: {remaining[name].dependencies}")
            raise UserException(
                f"Circular dependency detected among queries in block: {', '.join(remaining_names)}. "
                f"Check your SQL dependencies."
            )
        batches.append(Batch(queries=ready))
        # Remove processed queries and relax dependents.
        for query in ready:
            del remaining[query.name]
            for dependent in local_graph[query.name]:
                if dependent in local_in_degree:
                    local_in_degree[dependent] -= 1
    return batches
```

- [ ] **Step 4: Simplify `build_block_execution_plan` to drop the global producer map**

In `src/query_orchestrator.py`, replace the body of `build_block_execution_plan` (lines 216-269) with:

```python
    def build_block_execution_plan(self) -> ExecutionPlan:
        """
        Build an execution plan that runs blocks consecutively while allowing
        parallel execution of independent queries within each block.

        Blocks execute in author order, so any table produced in an earlier
        block is available to later blocks without an explicit edge. Ordering
        *within* a block is resolved by ``_create_parallel_batches_for_block``
        using each query's position, which is why a block may safely create the
        same table more than once.
        """
        if not self.queries:
            return ExecutionPlan(blocks=[])

        # Group queries by block, preserving author order.
        block_queries: dict[str, list[Query]] = defaultdict(list)
        for query in self.queries:
            block_queries[query.block_name].append(query)

        blocks = [
            Block(name=block_name, batches=_create_parallel_batches_for_block(queries))
            for block_name, queries in block_queries.items()
        ]
        return ExecutionPlan(blocks=blocks)
```

This removes the now-unused global `producers` / `create_producers` / `insert_producers` maps and the dead `graph` / `in_degree` locals. The `StatementType` import and `Query.statement_type` field are no longer used by the planner but stay in place (harmless; they still classify statements and are cheap to keep).

- [ ] **Step 5: Run the reproducer to verify it passes**

Run: `uv run pytest tests/unit/test_query_orchestrator.py::test_recreating_same_table_in_block_is_not_a_cycle -v`
Expected: PASS.

- [ ] **Step 6: Add the guard tests (feature-preservation + genuine-cycle + case)**

Append to `tests/unit/test_query_orchestrator.py`:

```python
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
```

- [ ] **Step 7: Run the full unit test file**

Run: `uv run pytest tests/unit/test_query_orchestrator.py -v`
Expected: all 5 tests PASS.

- [ ] **Step 8: Lint**

Run: `uv run ruff check src/query_orchestrator.py tests/unit/test_query_orchestrator.py && uv run ruff format src/query_orchestrator.py tests/unit/test_query_orchestrator.py`
Expected: no errors; formatting clean.

- [ ] **Step 9: Commit**

```bash
git add src/query_orchestrator.py tests/unit/test_query_orchestrator.py
git commit -m "fix: order-aware in-block dependency resolution (CFTL-715)

Resolve table producers per-position within a block instead of with a
global last-writer-wins map, so re-creating the same table in one block no
longer produces a false circular-dependency error. Blocks still run
consecutively; genuine cycles still raise."
```

---

## Task 2: Functional (datadir) regression test

**Files:**
- Create: `tests/functional/circular_dependency_repro/source/data/config.json`
- Create: `tests/functional/circular_dependency_repro/expected/data/out/tables/bsm` (generated — see steps)
- Create: `tests/functional/circular_dependency_repro/expected/data/out/tables/bsm.manifest` (generated)

**Interfaces:**
- Consumes: the full launcher → `BlockOrchestrator.execute()` path and the ordering guarantee produced by Task 1. Uses the datadir harness in `tests/test_functional.py` (auto-discovers every directory under `tests/functional/`).
- Produces: an end-to-end fixture proving a block that recreates a table runs to completion and yields the correct data.

- [ ] **Step 1: Create the source config (a block that recreates a table)**

Create `tests/functional/circular_dependency_repro/source/data/config.json`:

```json
{
  "parameters": {
    "blocks": [
      {
        "name": "Recon",
        "codes": [
          {
            "name": "build",
            "script": ["CREATE OR REPLACE TABLE bsm AS SELECT 1 AS id, 10 AS amount;"]
          },
          {
            "name": "adjust",
            "script": ["CREATE OR REPLACE TABLE bsm_adj AS SELECT id, amount * 2 AS amount FROM bsm;"]
          },
          {
            "name": "final",
            "script": ["CREATE OR REPLACE TABLE bsm AS SELECT * FROM bsm_adj;"]
          }
        ]
      }
    ]
  },
  "storage": {
    "input": { "tables": [] },
    "output": { "tables": [{ "destination": "out.bsm", "source": "bsm" }] }
  }
}
```

- [ ] **Step 2: Run the datadir test to confirm it no longer errors and to generate expected output**

Run (in Docker/CI, or locally if the version venvs exist): `uv run pytest "tests/test_functional.py" -v -k circular_dependency_repro`
- Before generating `expected/`, the test fails only on the missing expected files, **not** on a circular-dependency error. If you still see `Circular dependency detected`, Task 1 is incomplete — stop and fix it.
- The datadir runner writes actual output under the test's data dir. Inspect the produced `out/tables/bsm`: it must be a single row where `amount = 20` (10 → ×2 → re-materialized). Copy the produced `bsm` and `bsm.manifest` into `tests/functional/circular_dependency_repro/expected/data/out/tables/` (mirror the exact bytes the component wrote, as done for the existing `query_dependencies` fixture — headers quoted, `.manifest` schema matching the produced columns `id`, `amount`).

If you cannot run in Docker locally, commit the `config.json` now and let CI generate/verify; note in the PR that expected output was produced in CI.

- [ ] **Step 3: Re-run to verify green**

Run: `uv run pytest "tests/test_functional.py" -v -k circular_dependency_repro`
Expected: PASS (or SKIP locally with "Version venv(s) not found" — then it is validated in CI).

- [ ] **Step 4: Commit**

```bash
git add tests/functional/circular_dependency_repro
git commit -m "test: datadir regression for recreating a table within one block (CFTL-715)"
```

---

## Task 3: Case-insensitive self-reference removal in the parser (secondary latent fix)

**Files:**
- Modify: `src/sql_parser.py` — the `dependencies - create_outputs` step in `extract_dependencies_and_outputs` (currently line 122).
- Test: `tests/unit/test_sql_parser.py` (create)

**Interfaces:**
- Consumes: `SQLParser().extract_dependencies_and_outputs(sql) -> tuple[set[str], set[str]]`.
- Produces: same signature and **same original-case names**; only the self-reference filter becomes case-insensitive. No change visible to the two sync actions beyond correctly dropping a table from its own dependency set.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_sql_parser.py`:

```python
from sql_parser import SQLParser


def test_case_variant_self_reference_is_not_a_dependency():
    # A statement that creates "BSM_03" and reads bsm_03 must not list bsm_03
    # as a dependency (DuckDB identifiers are case-insensitive), while the
    # output name keeps its original case.
    deps, outputs = SQLParser().extract_dependencies_and_outputs(
        'CREATE OR REPLACE TABLE "BSM_03" AS SELECT * FROM bsm_03'
    )
    assert not any(d.casefold() == "bsm_03" for d in deps)
    assert any(o.casefold() == "bsm_03" for o in outputs)


def test_external_dependency_is_still_reported_with_original_case():
    deps, outputs = SQLParser().extract_dependencies_and_outputs(
        "CREATE OR REPLACE TABLE bsm_03 AS SELECT * FROM bsm_02"
    )
    assert "bsm_02" in deps
    assert "bsm_03" in outputs
    assert "bsm_03" not in deps
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/unit/test_sql_parser.py -v`
Expected: `test_case_variant_self_reference_is_not_a_dependency` FAILS (self-reference `bsm_03` leaks into deps because the subtraction is case-sensitive). The second test passes already.

- [ ] **Step 3: Make the self-reference removal case-insensitive**

In `src/sql_parser.py`, replace the single line (currently line 122):

```python
            dependencies = dependencies - create_outputs
```

with:

```python
            # A query cannot depend on a table it creates in the same statement.
            # Compare case-insensitively (DuckDB identifiers are case-insensitive)
            # but keep the original-case names for any remaining dependencies.
            create_outputs_normalized = {output.casefold() for output in create_outputs}
            dependencies = {dep for dep in dependencies if dep.casefold() not in create_outputs_normalized}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/unit/test_sql_parser.py -v`
Expected: both tests PASS.

- [ ] **Step 5: Lint**

Run: `uv run ruff check src/sql_parser.py tests/unit/test_sql_parser.py && uv run ruff format src/sql_parser.py tests/unit/test_sql_parser.py`
Expected: no errors; formatting clean.

- [ ] **Step 6: Commit**

```bash
git add src/sql_parser.py tests/unit/test_sql_parser.py
git commit -m "fix: case-insensitive self-reference removal in dependency extractor (CFTL-715)"
```

---

## Task 4: Confirm against the real customer block (verification — needs project-10464 access)

This closes the one MED-confidence gap: the fix was proven against a faithful reproduction, not the customer's actual SQL. The strongest confirmation is the block that failed *"regardless of restructuring."*

**Files:**
- Create (if fetched): `tests/functional/circular_dependency_aje_case3/source/data/config.json` + generated `expected/…`

**Interfaces:**
- Consumes: the customer configuration on the AWS-US stack (`connection.keboola.com`), project **10464**, branch **1307562**, config **`01kwmevsar2xp4qhjvrqwcdkdc`** ("BI_SALES_MARGIN_DUCK_TEST V2"), block **"AJE CASE 3 DETAIL"**.

- [ ] **Step 1: Obtain the real config**

Get a Storage API token for project 10464 (AWS-US) — via the customer/AE or an internal admin — or an exported config JSON for branch 1307562. Then fetch `parameters.blocks` for config `01kwmevsar2xp4qhjvrqwcdkdc` (and, if useful, `01kw08d88kgdqrvstqmfr7akd6`, block "MAINCON OFFSET RECONCILIATION FINAL"). `kbagent` can pull it once the project is reachable: `kbagent --help` → add/connect the project → fetch the config detail.

If no token is obtainable, **skip this task** — the fix stands on Task 1's reproducer — and note in the PR that real-config confirmation is pending.

- [ ] **Step 2: Reproduce the failure on `main`, confirm the fix on the branch**

```bash
git stash   # or check out main in a scratch worktree
```
Build a throwaway script (in `/tmp`, not the repo) that imports `BlockOrchestrator` and feeds it the real block(s); confirm `build_block_execution_plan()` raises `Circular dependency detected` on `main` and succeeds on `fix/CFTL-715`. This proves the mechanism matches reality.

- [ ] **Step 3: Add the real block as a datadir fixture (anonymized)**

Anonymize table/column names if needed (no customer identifiers in committed fixtures — see repo/global rules), keep the SQL *shape* (the repeated CREATE of one table). Generate `expected/` as in Task 2. Run `uv run pytest "tests/test_functional.py" -v -k aje_case3`.

- [ ] **Step 4: Commit (only if a fixture was added)**

```bash
git add tests/functional/circular_dependency_aje_case3
git commit -m "test: datadir regression from real CFTL-715 block (anonymized)"
```

---

## Task 5: Finalize — full suite, lint, PR

**Files:** none (verification + PR).

- [ ] **Step 1: Run the full test suite**

Run: `uv run pytest -v`
Expected: all unit tests pass; existing functional tests (`query_dependencies`, `sync_action_execution_plan`, `simple`, `dtypes`, `simple_parquet`, version dirs) stay green or SKIP locally (validated in CI). No `Circular dependency` regressions.

- [ ] **Step 2: Lint the whole tree**

Run: `uv run ruff check && uv run ruff format --check`
Expected: clean.

- [ ] **Step 3: Push and open the PR**

```bash
git push -u origin fix/CFTL-715
gh pr create --fill --title "fix: false-positive circular dependency in block planner (CFTL-715)"
```

PR body should state: root cause (last-writer-wins producer map), the fix (order-aware in-block resolution + case-insensitive self-reference removal), that it **complements** CFTL-580's classification fix (different trigger — no overlap), and whether real-config confirmation (Task 4) was done or is pending. Do not name the customer in the PR.

- [ ] **Step 4: Review loop**

Use `component-developer:babysit-pr` (or `/review`) to run review → fix → re-review until clean.

---

## Notes for the implementer

- **Why not just detect and ignore the cycle?** The graph edge is genuinely wrong (it points at the wrong producer). Fixing producer resolution is correct at the source; the cycle-detection raise stays as a backstop for *real* cycles (Task 1, `test_genuine_cycle_still_raises`).
- **Behavior change to be aware of:** a reader positioned *between* a `CREATE` and a later `INSERT` of the same table now binds to the `CREATE` (reads pre-insert state) instead of the `INSERT`. This matches author position and is what removes the false cycle; it is covered by `test_insert_runs_after_create_of_same_table`.
- **Sync actions get fixed for free:** `execution_plan_visualization` / `lineage_visualization` build on the same planner, so they stop failing on these configs without extra work.
