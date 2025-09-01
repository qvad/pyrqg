"""
Filtered PostgreSQL executor that applies a compatibility filter before execution.
This module provides a minimal, compatible wrapper over PostgreSQLExecutor.
"""

import time

from .executor import PostgreSQLExecutor
from .result import Result, Status
from ..filters.postgres_filter import PostgreSQLFilter


class FilteredPostgreSQLExecutor(PostgreSQLExecutor):
    """PostgreSQL executor with automatic query filtering.
    
    Notes:
    - Uses the existing Executor API (dsn string) rather than legacy configs.
    - If the filter decides to skip a query, returns a Result with Status.SKIP.
    """
    def __init__(self, dsn: str, aggressive_mode: bool = True):
        super().__init__(dsn)
        self.filter = PostgreSQLFilter(aggressive_mode=aggressive_mode)
        self.filter_stats = {
            'queries_filtered': 0,
            'queries_skipped': 0,
        }

    def execute(self, query: str) -> Result:
        start_time = time.time()
        original_query = query
        filtered_query = self.filter.filter_query(query)

        # If filter suggests skipping the query
        if filtered_query is None:
            self.filter_stats['queries_skipped'] += 1
            res = Result(query=original_query, status=Status.SKIP, errstr="Skipped by PostgreSQL filter")
            res.start_time = start_time
            res.end_time = time.time()
            return res

        if filtered_query != original_query:
            self.filter_stats['queries_filtered'] += 1

        # Delegate execution to base class on filtered query (strip trailing semicolon already handled by filter)
        result = super().execute(filtered_query)
        return result


def create_filtered_executor(dsn: str, aggressive_mode: bool = True) -> FilteredPostgreSQLExecutor:
    """Factory function to create filtered PostgreSQL executor.
    
    Example:
        exec = create_filtered_executor("postgresql://user:pass@localhost:5432/db")
        res = exec.execute("SELECT 1;")
    """
    executor = FilteredPostgreSQLExecutor(dsn, aggressive_mode=aggressive_mode)
    # Connect immediately to mirror create_executor behavior
    executor.connect()
    return executor