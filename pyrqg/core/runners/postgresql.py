"""
PostgreSQL Runner

Provides query execution against PostgreSQL databases.
Supports multi-threaded execution with connection pooling.
"""

from __future__ import annotations

import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, Future
from typing import Optional, Iterator, List, Any

try:
    import psycopg2
    import psycopg2.errors
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    PSYCOPG2_AVAILABLE = True
except ImportError:
    psycopg2 = None
    PSYCOPG2_AVAILABLE = False

from pyrqg.core.runners.base import Runner, RunnerConfig, ExecutionStats, query_shape
from pyrqg.core.runners.registry import RunnerRegistry


class PostgreSQLRunner(Runner):
    """PostgreSQL query execution runner.

    Features:
    - Multi-threaded execution with thread-local connections
    - Backpressure to prevent memory issues
    - DDL barrier for schema changes
    - Automatic reconnection on connection loss

    Example:
        runner = PostgreSQLRunner(dsn="postgresql://localhost:5432/testdb")
        stats = runner.execute_queries(queries)
    """

    name = "postgresql"
    description = "Standard PostgreSQL database"
    target_api = "postgres"

    def __init__(self, config: Optional[RunnerConfig] = None, **kwargs):
        super().__init__(config, **kwargs)
        if not PSYCOPG2_AVAILABLE:
            raise RuntimeError("psycopg2 is required for PostgreSQLRunner")

        self._local = threading.local()
        self._main_conn = None
        self._executor = None
        self._max_futures = self.config.threads * 10

    def connect(self) -> None:
        """Establish main connection."""
        if self._main_conn is None or self._main_conn.closed:
            self._main_conn = psycopg2.connect(self.config.get_dsn())
            self._main_conn.autocommit = True

    def close(self) -> None:
        """Close all connections."""
        if self._main_conn:
            try:
                self._main_conn.close()
            except Exception:
                pass
            self._main_conn = None

        if self._executor:
            self._executor.shutdown(wait=False)
            self._executor = None

    def _get_thread_connection(self):
        """Get or create a thread-local connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None or self._local.conn.closed:
            try:
                conn = psycopg2.connect(self.config.get_dsn())
                conn.autocommit = True
                if self.config.statement_timeout:
                    with conn.cursor() as cur:
                        cur.execute(f"SET statement_timeout = {self.config.statement_timeout}")
                self._local.conn = conn
            except Exception as e:
                self._local.conn = None
                raise e
        return self._local.conn

    def _force_reconnect(self):
        """Close and clear the current thread's connection."""
        if hasattr(self._local, "conn") and self._local.conn:
            try:
                self._local.conn.close()
            except Exception:
                pass
        self._local.conn = None

    def execute_one(self, query: str) -> tuple[str, Optional[str]]:
        """Execute a single query."""
        try:
            conn = self._get_thread_connection()
            with conn.cursor() as cur:
                cur.execute(query)
            return ".", None
        except psycopg2.errors.SyntaxError:
            return "S", "SyntaxError"
        except psycopg2.errors.QueryCanceled:
            return "t", "Timeout"
        except psycopg2.OperationalError:
            self._force_reconnect()
            return "C", "ConnectionError"
        except psycopg2.Error as e:
            return "e", type(e).__name__
        except Exception as e:
            return "e", type(e).__name__

    def _execute_ddl_with_retry(self, query: str, stats: ExecutionStats, retries: int = 5) -> None:
        """Execute DDL with retries for serialization failures."""
        for i in range(retries):
            try:
                with self._main_conn.cursor() as cur:
                    cur.execute(query)
                stats.success += 1
                stats.symbols['.'] += 1
                sys.stdout.write('.')
                return
            except (psycopg2.errors.SerializationFailure, psycopg2.errors.OperationalError) as e:
                if i == retries - 1:
                    stats.failed += 1
                    sym = 'C' if isinstance(e, psycopg2.errors.OperationalError) else 'e'
                    stats.symbols[sym] += 1
                    stats.errors[type(e).__name__] += 1
                    sys.stdout.write(sym)
                else:
                    time.sleep(1)
            except Exception as e:
                stats.failed += 1
                stats.symbols['e'] += 1
                stats.errors[type(e).__name__] += 1
                sys.stdout.write('e')
                return

    def execute_queries(
        self,
        queries: Iterator[str],
        progress_callback: Optional[callable] = None
    ) -> ExecutionStats:
        """Execute queries with multi-threaded execution and DDL barriers."""
        stats = ExecutionStats()
        progress_chars_on_line = 0

        self.connect()
        self._executor = ThreadPoolExecutor(max_workers=self.config.threads)
        futures: List[Future] = []

        def _process_futures(timeout: Optional[float] = 0) -> int:
            nonlocal progress_chars_on_line
            if not futures:
                return 0

            done, pending = wait(futures, timeout=timeout, return_when=FIRST_COMPLETED)
            processed = 0

            for f in done:
                symbol, error = f.result()
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

                sys.stdout.write(symbol)
                progress_chars_on_line += 1
                if progress_chars_on_line % 80 == 0:
                    sys.stdout.write('\n')
                sys.stdout.flush()
                processed += 1

            futures[:] = list(pending)
            return processed

        try:
            for query in queries:
                if not query or not query.strip() or query.strip().startswith("--"):
                    continue

                stats.total += 1
                stats.shapes.add(query_shape(query))

                # Backpressure
                if len(futures) >= self._max_futures:
                    _process_futures(timeout=None)
                elif len(futures) >= self.config.threads:
                    _process_futures(timeout=0)

                # Periodic summary
                if stats.total > 0 and stats.total % self.config.progress_interval == 0:
                    self._print_progress(stats, progress_chars_on_line)
                    progress_chars_on_line = 0
                    if progress_callback:
                        progress_callback(stats)

                if self.is_ddl(query):
                    # DDL Barrier: Drain all pending
                    while futures:
                        _process_futures(timeout=None)

                    # Execute DDL on main thread with retry
                    self._execute_ddl_with_retry(query, stats)

                    progress_chars_on_line += 1
                    if progress_chars_on_line % 80 == 0:
                        sys.stdout.write('\n')
                    sys.stdout.flush()
                else:
                    # Submit to thread pool
                    fut = self._executor.submit(self.execute_one, query)
                    futures.append(fut)

                # Stop on error if configured
                if not self.config.continue_on_error and stats.failed > 0:
                    break

            # Drain remaining futures
            while futures:
                _process_futures(timeout=None)

            # Final summary
            sys.stdout.write("\n")
            self._print_progress(stats, 0)

        except KeyboardInterrupt:
            sys.stdout.write("\n[Interrupted by user]\n")
        finally:
            self.close()

        return stats


# Register the runner
RunnerRegistry.register(PostgreSQLRunner)
