"""
YCQL (Cassandra-compatible) Execution Support for PyRQG.

Provides execution capabilities for YCQL grammars against YugabyteDB's
Cassandra-compatible API.
"""

import logging
import os
from dataclasses import dataclass, field
from typing import Optional, List, Any, Iterator

logger = logging.getLogger(__name__)

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    CASSANDRA_AVAILABLE = True
except ImportError:
    CASSANDRA_AVAILABLE = False
    Cluster = None
    PlainTextAuthProvider = None


@dataclass
class YCQLStats:
    """Statistics for YCQL execution."""
    success: int = 0
    failed: int = 0
    syntax_errors: int = 0
    errors: List[str] = field(default_factory=list)


class YCQLExecutor:
    """Execute YCQL queries against YugabyteDB.

    Example:
        executor = YCQLExecutor(host="localhost", port=9042)
        stats = executor.run_queries(queries, keyspace="test_keyspace")
    """

    def __init__(self, host: str = "localhost", port: int = 9042,
                 username: Optional[str] = None, password: Optional[str] = None):
        if not CASSANDRA_AVAILABLE:
            raise RuntimeError(
                "cassandra-driver is not installed. "
                "Install it with: pip install cassandra-driver"
            )

        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self._cluster = None
        self._session = None

    def connect(self, keyspace: Optional[str] = None):
        """Connect to YugabyteDB YCQL."""
        auth_provider = None
        if self.username and self.password:
            auth_provider = PlainTextAuthProvider(
                username=self.username, password=self.password
            )

        self._cluster = Cluster(
            contact_points=[self.host],
            port=self.port,
            auth_provider=auth_provider
        )
        self._session = self._cluster.connect(keyspace)
        logger.info("Connected to YCQL at %s:%d", self.host, self.port)

    def close(self):
        """Close the connection."""
        if self._session:
            self._session.shutdown()
        if self._cluster:
            self._cluster.shutdown()

    def execute(self, query: str) -> bool:
        """Execute a single YCQL query. Returns True on success."""
        if not self._session:
            raise RuntimeError("Not connected. Call connect() first.")
        try:
            self._session.execute(query)
            return True
        except Exception as e:
            logger.debug("Query failed: %s", e)
            raise

    def run_queries(self, queries: Iterator[str], keyspace: Optional[str] = None,
                    continue_on_error: bool = True) -> YCQLStats:
        """Run multiple YCQL queries and collect statistics."""
        stats = YCQLStats()

        try:
            self.connect(keyspace)

            for query in queries:
                try:
                    self.execute(query)
                    stats.success += 1
                    print(".", end="", flush=True)
                except Exception as e:
                    error_str = str(e).lower()
                    if "syntax" in error_str:
                        stats.syntax_errors += 1
                        print("S", end="", flush=True)
                    else:
                        stats.failed += 1
                        print("e", end="", flush=True)

                    error_type = type(e).__name__
                    if error_type not in stats.errors:
                        stats.errors.append(error_type)

                    if not continue_on_error:
                        break

            print()  # Newline after progress dots

        finally:
            self.close()

        return stats

    def create_keyspace(self, keyspace: str, replication_factor: int = 1):
        """Create a keyspace if it doesn't exist."""
        if not self._session:
            self.connect()

        query = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}}
        """
        self._session.execute(query)
        logger.info("Created keyspace: %s", keyspace)


def create_ycql_executor_from_env() -> Optional[YCQLExecutor]:
    """Create a YCQLExecutor from environment variables.

    Environment variables:
        YCQL_HOST: YCQL host (default: localhost)
        YCQL_PORT: YCQL port (default: 9042)
        YCQL_USERNAME: Optional username
        YCQL_PASSWORD: Optional password
    """
    if not CASSANDRA_AVAILABLE:
        logger.warning("cassandra-driver not installed, YCQL execution unavailable")
        return None

    host = os.environ.get("YCQL_HOST", "localhost")
    port = int(os.environ.get("YCQL_PORT", "9042"))
    username = os.environ.get("YCQL_USERNAME")
    password = os.environ.get("YCQL_PASSWORD")

    return YCQLExecutor(host=host, port=port, username=username, password=password)
