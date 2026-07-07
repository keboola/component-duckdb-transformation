"""Block-based SQL query orchestrator with consecutive blocks and parallel scripts."""

import logging
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from keboola.component.exceptions import UserException

from sql_parser import SQLParser, StatementType


@dataclass
class Query:
    """Simple query representation."""

    name: str
    sql: str
    dependencies: set[str]  # tables this query reads
    outputs: set[str]  # tables this query creates
    block_name: str  # Add block information
    code_name: str  # Add code information
    statement_type: StatementType = StatementType.OTHER


@dataclass
class Batch:
    """A batch of queries that can be executed in parallel."""

    queries: list[Query]

    def __len__(self) -> int:
        return len(self.queries)

    def __iter__(self):
        return iter(self.queries)

    def __getitem__(self, index):
        return self.queries[index]


@dataclass
class Block:
    """A block containing batches that must be executed sequentially."""

    name: str
    batches: list[Batch]

    def __len__(self) -> int:
        return len(self.batches)

    def __iter__(self):
        return iter(self.batches)

    @property
    def total_queries(self) -> int:
        return sum(len(batch) for batch in self.batches)


@dataclass
class ExecutionPlan:
    """Complete execution plan with blocks that must be executed consecutively."""

    blocks: list[Block]

    def __len__(self) -> int:
        return len(self.blocks)

    def __iter__(self):
        return iter(self.blocks)

    @property
    def total_queries(self) -> int:
        return sum(block.total_queries for block in self.blocks)

    @property
    def total_batches(self) -> int:
        return sum(len(block) for block in self.blocks)


@dataclass
class ExecutionStats:
    """Statistics from query execution."""

    total_queries: int
    total_batches: int
    total_execution_time: float
    batch_times: list[float]
    query_times: list[float]
    fastest_query: float
    slowest_query: float

    @property
    def average_query_time(self) -> float:
        return sum(self.query_times) / len(self.query_times) if self.query_times else 0.0

    @property
    def average_batch_time(self) -> float:
        return sum(self.batch_times) / len(self.batch_times) if self.batch_times else 0.0


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
                # Bind to the most recent producer before this reader.
                add_edge(preceding[-1], index)
            else:
                # Reader precedes all in-block producers of this table: bind to the
                # last producer so it waits until the table is fully built (its final
                # materialized version) instead of racing an earlier re-creation.
                add_edge(producer_indices[-1], index)

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


class BlockOrchestrator:
    """
    Orchestrator that executes blocks consecutively, but scripts within each block in parallel.
    Creates its own DAG based on SQL query analysis.
    """

    def __init__(self, connection, max_workers: int = 4):
        self.connection = connection
        self.max_workers = max_workers
        self.queries: list[Query] = []
        self.query_times: list[float] = []
        self.batch_times: list[float] = []
        self.sql_parser = SQLParser()

    def add_queries_from_blocks(self, blocks):
        """Add queries from Keboola blocks structure with block and code information."""
        for block, code, script, script_index in self.sql_parser.iterate_blocks(blocks):
            name = self.sql_parser.get_query_name(code, script_index)
            # Parse and create query with block information
            query = self._parse_sql(name, script, block.name, code.name)
            self.queries.append(query)

    def _parse_sql(self, name: str, sql: str, block_name: str, code_name: str) -> Query:
        """Parse SQL and create Query with extracted dependencies using SQLGlot."""
        try:
            # Use SQLParser to extract dependencies and outputs
            dependencies, outputs = self.sql_parser.extract_dependencies_and_outputs(sql)
            statement_type = self.sql_parser.classify_statement(sql)
            return Query(
                name=name,
                sql=sql,
                dependencies=dependencies,
                outputs=outputs,
                block_name=block_name,
                code_name=code_name,
                statement_type=statement_type,
            )
        except Exception as e:
            # Fallback to empty sets if parsing fails
            logging.warning(f"Failed to parse SQL for query '{name}': {e}")
            return Query(
                name=name,
                sql=sql,
                dependencies=set(),
                outputs=set(),
                block_name=block_name,
                code_name=code_name,
                statement_type=StatementType.OTHER,
            )

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

    def execute(self) -> ExecutionStats:
        """Execute queries with block-based parallelization and return statistics."""
        execution_start = time.time()
        # Reset statistics
        self.query_times.clear()
        self.batch_times.clear()
        execution_plan = self.build_block_execution_plan()

        block_count = len(execution_plan)
        block_text = "block" if block_count == 1 else "blocks"
        logging.info(
            f"Executing {execution_plan.total_queries} queries in "
            f"{execution_plan.total_batches} batches across {block_count} {block_text}"
        )

        batch_counter = 0
        # Execute blocks consecutively
        for block_index, block in enumerate(execution_plan):
            if not block.batches:
                continue

            block_start_time = time.time()
            logging.info(f"Starting block '{block.name}' ({block_index + 1}/{len(execution_plan)})")

            # Execute all batches within this block
            for batch in block:
                batch_counter += 1
                batch_start = time.time()
                if len(batch) == 1:
                    logging.info(
                        f"Batch {batch_counter}/{execution_plan.total_batches}: Executing 1 query sequentially"
                    )
                    try:
                        query_time = self._execute_query(batch[0])
                        self.query_times.append(query_time)
                    except Exception as e:
                        raise UserException(f"Query '{batch[0].name}' failed: {e}")
                else:
                    logging.info(
                        f"Batch {batch_counter}/{execution_plan.total_batches}: "
                        f"Executing {len(batch)} queries in parallel"
                    )
                    query_times = self._execute_batch_parallel(batch)
                    self.query_times.extend(query_times)
                batch_time = time.time() - batch_start
                self.batch_times.append(batch_time)

            # Block completed
            block_time = time.time() - block_start_time
            logging.info(f"Block '{block.name}' completed in {block_time:.2f}s")

        total_time = time.time() - execution_start
        # Create statistics
        stats = ExecutionStats(
            total_queries=execution_plan.total_queries,
            total_batches=execution_plan.total_batches,
            total_execution_time=total_time,
            batch_times=self.batch_times.copy(),
            query_times=self.query_times.copy(),
            fastest_query=min(self.query_times) if self.query_times else 0.0,
            slowest_query=max(self.query_times) if self.query_times else 0.0,
        )
        return stats

    @staticmethod
    def _get_sql_preview(sql: str, max_length: int = 10) -> str:
        """Get a preview of SQL query for logging purposes."""
        cleaned_sql = sql.replace("\n", " ").strip()
        if len(cleaned_sql) <= max_length:
            return cleaned_sql
        return cleaned_sql[:max_length] + "..."

    def _execute_query(self, query: Query) -> float:
        """Execute single query and return execution time."""
        thread_id = threading.current_thread().ident
        start = time.time()
        # Each thread must use its own cursor for proper isolation and exception propagation.
        # Using connection.execute() directly shares an internal cursor which is not thread-safe.
        cursor = self.connection.cursor()
        try:
            cursor.execute(query.sql)
        finally:
            cursor.close()
        duration = time.time() - start
        sql_preview = BlockOrchestrator._get_sql_preview(query.sql)
        logging.info(f"Query '{query.name}' completed in {duration:.2f}s [Thread {thread_id}] - SQL: {sql_preview}")
        return duration

    def _execute_batch_parallel(self, batch: Batch) -> list[float]:
        """Execute batch of queries in parallel and return list of execution times."""
        max_workers = min(self.max_workers, len(batch))

        if max_workers == 1:
            query_times = []
            for query in batch:
                try:
                    execution_time = self._execute_query(query)
                    query_times.append(execution_time)
                except Exception as e:
                    raise UserException(f"Query '{query.name}' failed: {e}")
            return query_times
        else:
            logging.info(f"Using {max_workers} threads")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all queries
                future_to_query = {executor.submit(self._execute_query, query): query for query in batch}
                query_times = []
                failed_queries = []
                completed_futures = set()

                # Wait for completion with better error handling
                for future in as_completed(future_to_query):
                    completed_futures.add(future)
                    try:
                        execution_time = future.result()
                        query_times.append(execution_time)
                    except Exception as e:
                        failed_query = future_to_query[future]
                        failed_queries.append(f"{failed_query.name}: {str(e)}")

                if failed_queries:
                    if len(completed_futures) < len(future_to_query):
                        self._cancel_remaining_futures(future_to_query, completed_futures)
                    successful_count = len(query_times)
                    separator = "\n  - "
                    raise UserException(
                        f"Query execution failed after {successful_count} successful "
                        f"quer{'y' if successful_count == 1 else 'ies'}:"
                        f"{separator}{separator.join(failed_queries)}"
                    )

                return query_times

    @staticmethod
    def _cancel_remaining_futures(future_to_query: dict, completed_futures: set) -> int:
        """Cancel all futures that haven't completed yet. Returns number of cancelled futures."""
        cancelled_count = 0
        for future in future_to_query:
            if future not in completed_futures and not future.done():
                try:
                    if future.cancel():
                        cancelled_count += 1
                except Exception:
                    pass

        # Give a brief moment for cancellations to take effect
        if cancelled_count > 0:
            time.sleep(0.1)

        return cancelled_count
