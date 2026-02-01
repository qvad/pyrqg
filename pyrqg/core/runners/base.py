"""
Base Runner Interface

Defines the abstract interface that all database runners must implement.
This allows PyRQG to support multiple database backends with a unified API.
"""

from __future__ import annotations

import re
import sys
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional, Dict, Iterator, List, Set, Any


_STRING_LITERAL_RE = re.compile(r"'(?:''|[^'])*'")
_NUMERIC_LITERAL_RE = re.compile(r"\b\d+(?:\.\d+)?\b")


def query_shape(query: str) -> str:
    """Normalize query by replacing literals with placeholders."""
    q = query.strip()
    q = _STRING_LITERAL_RE.sub("'?'", q)
    q = _NUMERIC_LITERAL_RE.sub('?', q)
    q = " ".join(q.split())
    return q


@dataclass
class RunnerConfig:
    """Configuration for a database runner."""

    # Connection settings
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    username: Optional[str] = None
    password: Optional[str] = None
    dsn: Optional[str] = None  # Full DSN overrides individual settings

    # Execution settings
    threads: int = 10
    statement_timeout: int = 1000  # milliseconds
    continue_on_error: bool = True
    progress_interval: int = 10000  # Print summary every N queries

    # YCQL-specific
    keyspace: Optional[str] = None
    replication_factor: int = 1

    def get_dsn(self) -> str:
        """Build DSN from config or return provided DSN."""
        if self.dsn:
            return self.dsn
        auth = ""
        if self.username:
            auth = self.username
            if self.password:
                auth += f":{self.password}"
            auth += "@"
        return f"postgresql://{auth}{self.host}:{self.port}/{self.database}"


@dataclass
class ExecutionStats:
    """Statistics collected during query execution."""

    total: int = 0
    success: int = 0
    failed: int = 0
    syntax_errors: int = 0
    timeouts: int = 0
    connection_errors: int = 0

    symbols: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    errors: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    shapes: Set[str] = field(default_factory=set)
    start_time: float = field(default_factory=time.time)

    def elapsed(self) -> float:
        """Return elapsed time in seconds."""
        return time.time() - self.start_time

    def qps(self) -> float:
        """Return queries per second."""
        elapsed = self.elapsed()
        return self.total / elapsed if elapsed > 0 else 0.0

    def summary(self) -> str:
        """Return a formatted summary string."""
        lines = [
            f"Total: {self.total:,} queries in {self.elapsed():.2f}s ({self.qps():.1f} QPS)",
            f"  Success: {self.success:,}",
            f"  Failed: {self.failed:,}",
            f"  Syntax Errors: {self.syntax_errors:,}",
            f"  Timeouts: {self.timeouts:,}",
            f"  Connection Errors: {self.connection_errors:,}",
            f"  Unique Query Shapes: {len(self.shapes):,}",
        ]
        if self.errors:
            lines.append("  Top Errors:")
            sorted_errors = sorted(self.errors.items(), key=lambda x: x[1], reverse=True)[:5]
            for err, cnt in sorted_errors:
                lines.append(f"    - {err}: {cnt:,}")
        return "\n".join(lines)


class Runner(ABC):
    """Abstract base class for database runners.

    To implement a custom runner, subclass this and implement:
    - connect(): Establish database connection
    - close(): Close database connection
    - execute_one(query): Execute a single query
    - is_ddl(query): Check if query is DDL

    Optionally override:
    - execute_queries(): Custom execution loop
    - setup_schema(): Initialize schema before execution
    """

    # Class-level metadata
    name: str = "base"
    description: str = "Abstract base runner"
    target_api: str = "sql"  # 'sql', 'ysql', 'ycql', 'postgres', etc.

    def __init__(self, config: Optional[RunnerConfig] = None, **kwargs):
        """Initialize runner with config or keyword arguments."""
        if config:
            self.config = config
        else:
            self.config = RunnerConfig(**kwargs)
        self._connected = False

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the database."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the database connection."""
        pass

    @abstractmethod
    def execute_one(self, query: str) -> tuple[str, Optional[str]]:
        """Execute a single query.

        Returns:
            Tuple of (symbol, error_type) where:
            - symbol is a single char: '.' for success, 'S' for syntax error,
              't' for timeout, 'C' for connection error, 'e' for other error
            - error_type is the error class name or None on success
        """
        pass

    def is_ddl(self, query: str) -> bool:
        """Check if query is a DDL statement requiring serialization."""
        q = query.strip().upper()
        return q.startswith(("CREATE", "ALTER", "DROP", "TRUNCATE"))

    def setup_schema(self, ddl_statements: List[str]) -> None:
        """Execute DDL statements to set up schema."""
        self.connect()
        try:
            for stmt in ddl_statements:
                self.execute_one(stmt)
        finally:
            self.close()

    def execute_queries(
        self,
        queries: Iterator[str],
        progress_callback: Optional[callable] = None
    ) -> ExecutionStats:
        """Execute queries and collect statistics.

        Args:
            queries: Iterator of SQL queries to execute
            progress_callback: Optional callback(stats) called periodically

        Returns:
            ExecutionStats with execution results
        """
        stats = ExecutionStats()
        progress_chars_on_line = 0

        self.connect()
        try:
            for query in queries:
                if not query or not query.strip() or query.strip().startswith("--"):
                    continue

                stats.total += 1
                stats.shapes.add(query_shape(query))

                symbol, error = self.execute_one(query)
                stats.symbols[symbol] += 1

                if symbol == '.':
                    stats.success += 1
                elif symbol == 'S':
                    stats.syntax_errors += 1
                    stats.failed += 1
                elif symbol == 't':
                    stats.timeouts += 1
                    stats.failed += 1
                elif symbol == 'C':
                    stats.connection_errors += 1
                    stats.failed += 1
                else:
                    stats.failed += 1

                if error:
                    stats.errors[error] += 1

                # Progress output
                sys.stdout.write(symbol)
                progress_chars_on_line += 1
                if progress_chars_on_line % 80 == 0:
                    sys.stdout.write('\n')
                sys.stdout.flush()

                # Periodic summary
                if stats.total % self.config.progress_interval == 0:
                    self._print_progress(stats, progress_chars_on_line)
                    progress_chars_on_line = 0
                    if progress_callback:
                        progress_callback(stats)

                # Stop on error if configured
                if not self.config.continue_on_error and stats.failed > 0:
                    break

            # Final newline and summary
            sys.stdout.write("\n")
            self._print_progress(stats, 0)

        except KeyboardInterrupt:
            sys.stdout.write("\n[Interrupted by user]\n")
        finally:
            self.close()

        return stats

    def _print_progress(self, stats: ExecutionStats, chars_on_line: int) -> None:
        """Print progress summary."""
        if chars_on_line > 0:
            sys.stdout.write("\n")
        sys.stdout.write(f"\n[{time.strftime('%H:%M:%S')}] {stats.summary()}\n")
        sys.stdout.write("-" * 80 + "\n")
        sys.stdout.flush()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False
