"""
YugabyteDB YCQL Runner

Provides query execution against YugabyteDB's Cassandra-compatible YCQL API.
Requires the cassandra-driver package.
"""

from __future__ import annotations

import sys
import time
import logging
from typing import Optional, Iterator

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False
    Cluster = None
    PlainTextAuthProvider = None

from pyrqg.core.runners.base import Runner, RunnerConfig, ExecutionStats, query_shape
from pyrqg.core.runners.registry import RunnerRegistry

logger = logging.getLogger(__name__)


class YCQLRunner(Runner):
    """YugabyteDB YCQL query execution runner.

    Executes CQL queries against YugabyteDB's Cassandra-compatible API.
    Requires cassandra-driver package.

    Example:
        runner = YCQLRunner(host="localhost", port=9042, keyspace="test_keyspace")
        stats = runner.execute_queries(queries)
    """

    name = "ycql"
    description = "YugabyteDB YCQL (Cassandra-compatible API)"
    target_api = "ycql"

    def __init__(self, config: Optional[RunnerConfig] = None, **kwargs):
        if not CASSANDRA_AVAILABLE:
            raise RuntimeError(
                "cassandra-driver is required for YCQLRunner. "
                "Install with: pip install cassandra-driver"
            )

        # Set YCQL-specific defaults
        if config is None:
            kwargs.setdefault('port', 9042)
            kwargs.setdefault('keyspace', 'test_keyspace')

        super().__init__(config, **kwargs)

        self._cluster = None
        self._session = None

    def connect(self, keyspace: Optional[str] = None) -> None:
        """Connect to YugabyteDB YCQL."""
        if self._session is not None:
            return  # Already connected

        auth_provider = None
        if self.config.username and self.config.password:
            auth_provider = PlainTextAuthProvider(
                username=self.config.username,
                password=self.config.password
            )

        self._cluster = Cluster(
            contact_points=[self.config.host],
            port=self.config.port,
            auth_provider=auth_provider
        )

        # Connect without keyspace first, then switch if needed
        self._session = self._cluster.connect()
        logger.info("Connected to YCQL at %s:%d", self.config.host, self.config.port)

        # Switch to keyspace if provided
        target_keyspace = keyspace or self.config.keyspace
        if target_keyspace:
            try:
                self._session.execute(f"USE {target_keyspace}")
            except Exception:
                # Keyspace might not exist yet
                pass

    def close(self) -> None:
        """Close the YCQL connection."""
        if self._session:
            try:
                self._session.shutdown()
            except Exception:
                pass
            self._session = None

        if self._cluster:
            try:
                self._cluster.shutdown()
            except Exception:
                pass
            self._cluster = None

    def execute_one(self, query: str) -> tuple[str, Optional[str]]:
        """Execute a single YCQL query."""
        if not self._session:
            return "C", "NotConnected"

        try:
            self._session.execute(query)
            return ".", None
        except Exception as e:
            error_str = str(e).lower()
            if "syntax" in error_str:
                return "S", "SyntaxError"
            elif "timeout" in error_str:
                return "t", "Timeout"
            elif "connection" in error_str or "unavailable" in error_str:
                return "C", "ConnectionError"
            else:
                return "e", type(e).__name__

    def is_ddl(self, query: str) -> bool:
        """Check if query is a DDL statement in YCQL."""
        q = query.strip().upper()
        return q.startswith((
            "CREATE", "ALTER", "DROP", "TRUNCATE",
            "USE",  # Keyspace switch
        ))

    def create_keyspace(self, keyspace: str, replication_factor: Optional[int] = None) -> None:
        """Create a keyspace if it doesn't exist."""
        if not self._session:
            # Connect without keyspace to create it
            if self._cluster is None:
                auth_provider = None
                if self.config.username and self.config.password:
                    auth_provider = PlainTextAuthProvider(
                        username=self.config.username,
                        password=self.config.password
                    )
                self._cluster = Cluster(
                    contact_points=[self.config.host],
                    port=self.config.port,
                    auth_provider=auth_provider
                )
            self._session = self._cluster.connect()

        rf = replication_factor or self.config.replication_factor
        query = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {rf}}}
        """
        self._session.execute(query)
        logger.info("Created keyspace: %s", keyspace)

    def setup_schema(self, ddl_statements: list[str], keyspace: Optional[str] = None) -> None:
        """Set up schema including keyspace creation."""
        target_keyspace = keyspace or self.config.keyspace

        # Connect without keyspace first
        self.connect(keyspace=None)

        try:
            # Create keyspace
            if target_keyspace:
                self.create_keyspace(target_keyspace)
                # Switch to keyspace
                self._session.execute(f"USE {target_keyspace}")

            # Execute DDL statements
            for stmt in ddl_statements:
                try:
                    self._session.execute(stmt)
                except Exception as e:
                    logger.warning("DDL error: %s - %s", stmt[:50], e)
        finally:
            self.close()

    def execute_queries(
        self,
        queries: Iterator[str],
        progress_callback: Optional[callable] = None,
        keyspace: Optional[str] = None
    ) -> ExecutionStats:
        """Execute YCQL queries.

        Note: YCQL execution is single-threaded since cassandra-driver
        handles connection pooling internally.
        """
        stats = ExecutionStats()
        progress_chars_on_line = 0
        target_keyspace = keyspace or self.config.keyspace

        # Ensure keyspace exists and connect
        self.connect(keyspace=None)
        try:
            if target_keyspace:
                self.create_keyspace(target_keyspace)
                # Reconnect with keyspace
                self.close()
                self.connect(keyspace=target_keyspace)

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

            # Final summary
            sys.stdout.write("\n")
            self._print_progress(stats, 0)

        except KeyboardInterrupt:
            sys.stdout.write("\n[Interrupted by user]\n")
        finally:
            self.close()

        return stats


# Register the runner
RunnerRegistry.register(YCQLRunner)
