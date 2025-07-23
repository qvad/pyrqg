"""
Robust Multi-threaded Query Execution Engine

This module provides a production-grade execution engine for running
generated SQL workloads against a database. It features:
- Thread pooling with backpressure (to avoid OOM).
- Connection pooling/recovery (per-thread connections).
- DDL barriers (pausing concurrency for schema changes).
- Real-time progress reporting (SQLsmith style).
- Robust error handling and retry logic.
"""
from __future__ import annotations

import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED, Future
from collections import defaultdict
from typing import Optional, Dict, Any, List, Tuple, Set

try:
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
except ImportError:
    psycopg2 = None

from pyrqg.api import RQG

_STRING_LITERAL_RE = re.compile(r"'(?:''|[^'])*'")
_NUMERIC_LITERAL_RE = re.compile(r"\b\d+(?:\.\d+)?\b")


def _query_shape(query: str) -> str:
    """Normalize query by replacing literals with placeholders."""
    q = query.strip()
    q = _STRING_LITERAL_RE.sub("'?'", q)
    q = _NUMERIC_LITERAL_RE.sub('?', q)
    q = " ".join(q.split())
    return q


class ExecutionStats:
    def __init__(self):
        self.total = 0
        self.success = 0
        self.failed = 0
        self.symbols: Dict[str, int] = defaultdict(int)
        self.errors: Dict[str, int] = defaultdict(int)
        self.shapes: Set[str] = set()
        self.start_time = time.time()

class WorkloadExecutor:
    """Executes a grammar-based workload against a database."""

    def __init__(self, dsn: str, threads: int = 10, statement_timeout: int = 1000):
        if psycopg2 is None:
            raise RuntimeError("psycopg2 is required for WorkloadExecutor")
        self.dsn = dsn
        self.threads = threads
        self.statement_timeout = statement_timeout
        self._local = threading.local()
        self._max_futures = threads * 10  # Backpressure limit (was 100 in testing)

    def _get_connection(self):
        """Get or create a thread-local connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None or self._local.conn.closed:
            try:
                conn = psycopg2.connect(self.dsn)
                conn.autocommit = True
                if self.statement_timeout:
                    with conn.cursor() as cur:
                        cur.execute(f"SET statement_timeout = {self.statement_timeout}")
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

    def _execute_task(self, query: str) -> Tuple[str, Optional[str]]:
        """Task executed by worker threads."""
        try:
            conn = self._get_connection()
            with conn.cursor() as cur:
                cur.execute(query)
            return ".", None
        except psycopg2.errors.SyntaxError:
            return "S", "SyntaxError"
        except psycopg2.errors.QueryCanceled:
            return "t", "Timeout"
        except psycopg2.OperationalError:
            self._force_reconnect()
            return "C", "Crash/ConnectionLost"
        except psycopg2.Error as e:
            return "e", type(e).__name__
        except Exception as e:
            return "e", type(e).__name__

    def run(self, rqg: RQG, grammar_name: str, count: int, seed: Optional[int] = None, 
            context: Any = None, progress_interval: int = 10000) -> ExecutionStats:
        """Run the workload.
        
        Args:
            rqg: The RQG instance.
            grammar_name: Name of the grammar to run.
            count: Number of queries to generate.
            seed: Random seed.
            context: The context (e.g. SchemaAwareContext) for generation.
            progress_interval: How often to print stats summary.
        """
        stats = ExecutionStats()
        executor = ThreadPoolExecutor(max_workers=self.threads)
        
        # Connect main thread for DDL execution
        main_conn = psycopg2.connect(self.dsn)
        main_conn.autocommit = True

        futures: List[Future] = []
        progress_chars_on_line = 0

        def _process_futures(timeout: Optional[float] = 0) -> int:
            nonlocal progress_chars_on_line
            done, pending = wait(futures, timeout=timeout, return_when=FIRST_COMPLETED)
            
            # Update the futures list reference (remove done items)
            # We can't modify 'futures' list in-place while waiting on it safely, 
            # but 'wait' returns sets. We reconstruct the list from pending.
            # However, we need to process 'done'.
            
            # Note: We must clear the processed futures from the main list.
            # The efficient way is to rebuild the list or remove indices.
            # Since 'wait' returns objects, we can just keep 'pending'.
            # BUT we need to update the outer 'futures' variable.
            # Using nonlocal/mutable list.
            
            processed = 0
            for f in done:
                symbol, error = f.result()
                stats.symbols[symbol] += 1
                if symbol == '.':
                    stats.success += 1
                else:
                    stats.failed += 1
                    if error:
                        stats.errors[error] += 1
                
                # Output
                sys.stdout.write(symbol)
                progress_chars_on_line += 1
                if progress_chars_on_line % 80 == 0:
                    sys.stdout.write('\n')
                sys.stdout.flush()
                processed += 1
            
            # Update the main list to only hold pending futures
            futures[:] = list(pending)
            return processed

        def _print_summary():
            nonlocal progress_chars_on_line
            sys.stdout.write(f"\n\n[{time.strftime('%H:%M:%S')}] Progress: {stats.total:,}/{count} queries.\n")
            sys.stdout.write(f"  OK (.): {stats.symbols['.']:,}\n")
            sys.stdout.write(f"  Syntax (S): {stats.symbols['S']:,}\n")
            sys.stdout.write(f"  Timeout (t): {stats.symbols['t']:,}\n")
            sys.stdout.write(f"  Crash (C): {stats.symbols['C']:,}\n")
            sys.stdout.write(f"  Error (e): {stats.symbols['e']:,}\n")
            sys.stdout.write(f"  Unique Shapes: {len(stats.shapes):,}\n")
            if stats.errors:
                sys.stdout.write("  Top Errors:\n")
                sorted_errors = sorted(stats.errors.items(), key=lambda x: x[1], reverse=True)[:5]
                for err, cnt in sorted_errors:
                    sys.stdout.write(f"    - {err}: {cnt:,}\n")
            sys.stdout.write("-" * 80 + "\n")
            sys.stdout.flush()
            progress_chars_on_line = 0

        try:
            gen = rqg.generate_from_grammar(grammar_name, count=count, seed=seed, context=context)

            for query_str in gen:
                if not query_str or not query_str.strip() or query_str.strip().startswith("--"):
                    continue

                stats.total += 1
                stats.shapes.add(_query_shape(query_str))
                
                # Backpressure
                if len(futures) >= self._max_futures:
                    _process_futures(timeout=None) # Block until at least one finishes
                elif len(futures) >= self.threads:
                    _process_futures(timeout=0) # Poll
                
                if stats.total > 0 and stats.total % progress_interval == 0:
                    _print_summary()

                is_ddl = query_str.strip().upper().startswith(("CREATE", "ALTER", "DROP", "TRUNCATE"))
                
                if is_ddl:
                    # DDL Barrier: Drain all pending
                    while futures:
                        _process_futures(timeout=None)
                    
                    # Execute DDL (Main Thread) with retry
                    _execute_ddl_with_retry(main_conn, query_str, stats)
                    
                    progress_chars_on_line += 1
                    if progress_chars_on_line % 80 == 0:
                        sys.stdout.write('\n')
                    sys.stdout.flush()
                else:
                    # Submit standard query
                    fut = executor.submit(self._execute_task, query_str)
                    futures.append(fut)
            
            # Drain at the end
            while futures:
                _process_futures(timeout=None)
            
            # Final summary
            sys.stdout.write("\n")
            _print_summary()
            
        except KeyboardInterrupt:
            sys.stdout.write("\n[Warn] Interrupted by user.\n")
        finally:
            if main_conn:
                main_conn.close()
            executor.shutdown(wait=False)
        
        return stats

def _execute_ddl_with_retry(conn, query, stats):
    """Execute DDL with retries for serialization failures."""
    for i in range(5):
        try:
            with conn.cursor() as cur:
                cur.execute(query)
            stats.success += 1
            stats.symbols['.'] += 1
            sys.stdout.write('.')
            return
        except (psycopg2.errors.SerializationFailure, psycopg2.errors.OperationalError) as e:
            if i == 4:
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
