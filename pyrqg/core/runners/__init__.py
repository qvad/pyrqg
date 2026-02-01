"""
PyRQG Pluggable Runner Layer

This module provides a pluggable architecture for executing generated queries
against different database systems. Users can define custom runners for any
database by implementing the Runner interface.

Example usage:
    from pyrqg.core.runners import RunnerRegistry, PostgreSQLRunner

    # Get a runner by name
    runner = RunnerRegistry.get("postgresql")

    # Or create directly
    runner = PostgreSQLRunner(dsn="postgresql://localhost:5432/testdb")

    # Execute queries
    stats = runner.execute_queries(queries)

Supported runners:
    - postgresql: Standard PostgreSQL
    - ysql: YugabyteDB YSQL (PostgreSQL-compatible)
    - ycql: YugabyteDB YCQL (Cassandra-compatible)
"""

from pyrqg.core.runners.base import Runner, RunnerConfig, ExecutionStats
from pyrqg.core.runners.registry import RunnerRegistry
from pyrqg.core.runners.postgresql import PostgreSQLRunner
from pyrqg.core.runners.ysql import YSQLRunner

# Optional YCQL runner (requires cassandra-driver)
try:
    from pyrqg.core.runners.ycql import YCQLRunner
    YCQL_AVAILABLE = True
except ImportError:
    YCQL_AVAILABLE = False
    YCQLRunner = None

__all__ = [
    "Runner",
    "RunnerConfig",
    "ExecutionStats",
    "RunnerRegistry",
    "PostgreSQLRunner",
    "YSQLRunner",
    "YCQLRunner",
    "YCQL_AVAILABLE",
]
