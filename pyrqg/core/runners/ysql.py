"""
YugabyteDB YSQL Runner

Provides query execution against YugabyteDB's PostgreSQL-compatible YSQL API.
Inherits from PostgreSQLRunner with YSQL-specific defaults.
"""

from __future__ import annotations

from typing import Optional

from pyrqg.core.runners.base import RunnerConfig
from pyrqg.core.runners.postgresql import PostgreSQLRunner
from pyrqg.core.runners.registry import RunnerRegistry


class YSQLRunner(PostgreSQLRunner):
    """YugabyteDB YSQL query execution runner.

    Inherits all functionality from PostgreSQLRunner with YSQL-specific
    defaults (port 5433 instead of 5432).

    Example:
        runner = YSQLRunner(host="localhost", port=5433, database="yugabyte")
        stats = runner.execute_queries(queries)
    """

    name = "ysql"
    description = "YugabyteDB YSQL (PostgreSQL-compatible API)"
    target_api = "ysql"

    def __init__(self, config: Optional[RunnerConfig] = None, **kwargs):
        # Default to YSQL port if not specified
        if config is None and 'port' not in kwargs and 'dsn' not in kwargs:
            kwargs.setdefault('port', 5433)
            kwargs.setdefault('database', 'yugabyte')
            kwargs.setdefault('username', 'yugabyte')
            kwargs.setdefault('password', 'yugabyte')

        super().__init__(config, **kwargs)

    def is_ddl(self, query: str) -> bool:
        """Check if query is DDL. Includes YugabyteDB-specific statements."""
        q = query.strip().upper()
        # Standard DDL plus YugabyteDB-specific statements
        return q.startswith((
            "CREATE", "ALTER", "DROP", "TRUNCATE",
            "REINDEX", "REFRESH MATERIALIZED VIEW",
        ))


# Register the runner
RunnerRegistry.register(YSQLRunner)
