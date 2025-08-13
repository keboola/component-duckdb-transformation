"""Block-based SQL query orchestrator with consecutive blocks and parallel scripts."""

import logging
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

from keboola.component.exceptions import UserException

from sql_parser import SQLParser


@dataclass
class Query:
    """Simple query representation."""

    name: str
    sql: str
    dependencies: set[str]  # tables this query reads
    outputs: set[str]  # tables this query creates
    block_name: str  # Add block information
    code_name: str  # Add code information


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


def _create_parallel_batches_for_block(block_queries: list[Query], producers: dict) -> list[list[Query]]:
    """
    Create parallel batches for queries within a single block.
    Uses topological sort to respect SQL dependencies.
    """
    batches = []
    remaining = {q.name: q for q in block_queries}
    # Create local dependency graph and in-degree for this block only
    local_graph = defaultdict(list)
    local_in_degree = {q.name: 0 for q in block_queries}
    # Build local dependency graph for this block
    for query in block_queries:
        for dep in query.dependencies:
            # Check if dependency is produced by another query in this block
            if dep in producers:
                producer = producers[dep]
                # Only add edge if producer is in the same block
                if producer.name in remaining:
                    local_graph[producer.name].append(query.name)
                    local_in_degree[query.name] += 1
                    logging.debug(f"Local dependency: {producer.name} -> {query.name}")
                else:
                    logging.debug(
                        f"External dependency: {query.name} depends on {dep}"
                        f" (produced by {producer.name} in different block)"
                    )
            else:
                logging.debug(f"Input dependency: {query.name} depends on {dep} (input table)")
    # Debug: print local in-degree for this block
    logging.debug("=== Local In-Degree for Block ===")
    for name, degree in local_in_degree.items():
        logging.debug(f"  {name}: {degree}")
    while remaining:
        # Find queries with no local dependencies
        ready = [remaining[name] for name in remaining if local_in_degree[name] == 0]
        logging.debug(f"Ready queries: {[q.name for q in ready]}")
        logging.debug(f"Remaining queries: {list(remaining.keys())}")
        if not ready:
            # Check for circular dependencies within block
            remaining_names = list(remaining.keys())
            logging.error("Circular dependency detected in block!")
            logging.error(f"Remaining queries: {remaining_names}")
            for name in remaining_names:
                remaining_query = remaining[name]
                logging.error(f"Query '{name}' depends on: {remaining_query.dependencies}")
            raise UserException(
                f"Circular dependency detected among queries in block: {', '.join(remaining_names)}. "
                f"Check your SQL dependencies."
            )
        batches.append(ready)
        # Remove processed queries and update local dependencies
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
            return Query(
                name=name,
                sql=sql,
                dependencies=dependencies,
                outputs=outputs,
                block_name=block_name,
                code_name=code_name,
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
            )

    def build_block_execution_plan(self) -> list[list[Query]]:
        """
        Build execution plan that respects block order but allows parallel execution within blocks.
        Creates DAG based on SQL dependencies.
        """
        if not self.queries:
            return []
        # Group queries by block
        block_queries = defaultdict(list)
        for query in self.queries:
            block_queries[query.block_name].append(query)
        # Build producer mapping across all queries
        producers = {}
        for query in self.queries:
            for output in query.outputs:
                producers[output] = query
        # Build dependency graph
        graph = defaultdict(list)
        in_degree = {q.name: 0 for q in self.queries}
        for query in self.queries:
            for dep in query.dependencies:
                if dep in producers:
                    producer = producers[dep]
                    graph[producer.name].append(query.name)
                    in_degree[query.name] += 1
                else:
                    # If dependency is not produced by any query in this block,
                    # it's an external dependency (input table) and doesn't create circular dependency
                    logging.debug(f"Query '{query.name}' depends on external table '{dep}'")
        # Debug: print global in-degree
        logging.debug("=== Global In-Degree ===")
        for name, degree in in_degree.items():
            logging.debug(f"  {name}: {degree}")
        # Create execution plan: blocks in order, queries within blocks in parallel
        execution_plan = []
        for block_name in block_queries.keys():
            block_queries_list = block_queries[block_name]
            # For each block, create batches of queries that can run in parallel
            block_batches = _create_parallel_batches_for_block(block_queries_list, producers)
            execution_plan.extend(block_batches)
        return execution_plan

    def execute(self) -> ExecutionStats:
        """Execute queries with block-based parallelization and return statistics."""
        execution_start = time.time()
        # Reset statistics
        self.query_times.clear()
        self.batch_times.clear()
        # Debug: print all detected dependencies
        logging.debug("=== Detected Dependencies ===")
        for query in self.queries:
            logging.debug(f"Query '{query.name}' (Block: {query.block_name}, Code: {query.code_name}):")
            logging.debug(f"  Dependencies: {query.dependencies}")
            logging.debug(f"  Outputs: {query.outputs}")
            logging.debug(f"  SQL: {query.sql[:100]}...")
        # Debug: print producer mapping
        logging.debug("=== Producer Mapping ===")
        producers = {}
        for query in self.queries:
            for output in query.outputs:
                producers[output] = query
        for output, producer in producers.items():
            logging.debug(f"  {output} -> {producer.name}")
        batches = self.build_block_execution_plan()
        logging.info(f"Executing {len(self.queries)} queries in {len(batches)} batches across blocks")
        current_block = None
        block_start_time = None
        for i, batch in enumerate(batches, 1):
            # Check if we're starting a new block
            if batch and batch[0].block_name != current_block:
                if current_block and block_start_time:
                    block_time = time.time() - block_start_time
                    logging.info(f"Block '{current_block}' completed in {block_time:.2f}s")
                current_block = batch[0].block_name
                block_start_time = time.time()
                logging.info(f"Starting block '{current_block}'")
            logging.info(f"Batch {i}: {[q.name for q in batch]} ({len(batch)} queries)")
            batch_start = time.time()
            if len(batch) == 1:
                # Single query - execute directly
                logging.info(f"Batch {i}: Executing 1 query sequentially")
                query_time = self._execute_query(batch[0])
                self.query_times.append(query_time)
            else:
                # Multiple queries - execute in parallel
                query_times = self._execute_batch_parallel(batch)
                self.query_times.extend(query_times)
            batch_time = time.time() - batch_start
            self.batch_times.append(batch_time)
        # Handle last block
        if current_block and block_start_time:
            block_time = time.time() - block_start_time
            logging.info(f"Block '{current_block}' completed in {block_time:.2f}s")
        total_time = time.time() - execution_start
        # Create statistics
        stats = ExecutionStats(
            total_queries=len(self.queries),
            total_batches=len(batches),
            total_execution_time=total_time,
            batch_times=self.batch_times.copy(),
            query_times=self.query_times.copy(),
            fastest_query=min(self.query_times) if self.query_times else 0.0,
            slowest_query=max(self.query_times) if self.query_times else 0.0,
        )
        return stats

    def _execute_query(self, query: Query) -> float:
        """Execute single query and return execution time."""
        thread_id = threading.current_thread().ident
        start = time.time()
        try:
            logging.info(f"Starting query '{query.name}' (Block: {query.block_name}) [Thread-{thread_id}]")
            self.connection.execute(query.sql)
            duration = time.time() - start
            logging.info(
                f"Query '{query.name}' (Block: {query.block_name}) completed in {duration:.2f}s [Thread-{thread_id}]"
            )
            return duration
        except Exception as e:
            # Don't log here - let the caller handle logging
            raise UserException(f"Query '{query.name}' failed: {e}")

    def _execute_batch_parallel(self, batch: list[Query]) -> list[float]:
        """Execute batch of queries in parallel and return list of execution times."""
        max_workers = min(self.max_workers, len(batch))
        if max_workers == 1:
            logging.info(f"Batch: Executing {len(batch)} queries sequentially (1 thread)")
            # For single thread, execute sequentially to avoid race conditions
            query_times = []
            for query in batch:
                try:
                    execution_time = self._execute_query(query)
                    query_times.append(execution_time)
                except UserException as e:
                    # Log the error and stop immediately
                    thread_id = threading.current_thread().ident
                    logging.error(f"Query '{query.name}' failed: {e} [Thread-{thread_id}]")
                    logging.error(f"Query '{query.name}' failed, stopping batch execution")
                    raise UserException(f"Query '{query.name}' failed")
            return query_times
        else:
            logging.info(f"Batch: Executing {len(batch)} queries in parallel using {max_workers} threads")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all queries
                future_to_query = {executor.submit(self._execute_query, query): query for query in batch}
                query_times = []
                failed_query = None
                # Wait for completion with better error handling
                try:
                    for future in as_completed(future_to_query):
                        try:
                            execution_time = future.result()  # Get execution time
                            query_times.append(execution_time)
                        except UserException as e:
                            # Get the failed query name and log the error
                            failed_query = future_to_query[future]
                            thread_id = threading.current_thread().ident
                            logging.error(f"Query '{failed_query.name}' failed: {e} [Thread-{thread_id}]")
                            logging.error(
                                f"Query '{failed_query.name}' failed, cancelling remaining "
                                f"{len(future_to_query) - len(query_times)} queries"
                            )
                            # Cancel ALL remaining futures immediately
                            cancelled_count = 0
                            for f in future_to_query:
                                if not f.done():
                                    f.cancel()
                                    cancelled_count += 1
                                    logging.debug(f"Cancelled future for query: {future_to_query[f].name}")
                            logging.info(f"Cancelled {cancelled_count} remaining queries")
                            # Wait a bit for cancellations to take effect
                            time.sleep(0.5)
                            # Re-raise the exception
                            raise UserException(f"Query '{failed_query.name}' failed")
                except UserException:
                    # Make sure all futures are cancelled before re-raising
                    for f in future_to_query:
                        if not f.done():
                            f.cancel()
                    raise
                return query_times
