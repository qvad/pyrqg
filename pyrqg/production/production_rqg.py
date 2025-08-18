"""
Production-ready Random Query Generator with billion-scale capabilities.

This is the main entry point for the production PyRQG system, integrating
all components for high-performance, unique query generation.
"""

import os
import sys
import time
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Generator, Tuple
from dataclasses import dataclass
from concurrent.futures import Future
import signal
import atexit

# Remove path manipulation - use proper package imports

from pyrqg.dsl.core import Grammar
from pyrqg.api import RQG as BaseRQG

from .config import ProductionConfig
from .entropy import EntropyManager
from .threading import ThreadPoolManager, QueryBatch
from .data_generator import DynamicDataGenerator
from .uniqueness import UniquenessTracker


@dataclass
class ProductionStats:
    """Aggregate statistics for production run"""
    start_time: float
    total_queries_generated: int = 0
    unique_queries: int = 0
    duplicate_queries: int = 0
    failed_queries: int = 0
    
    @property
    def runtime(self) -> float:
        return time.time() - self.start_time
    
    @property
    def overall_qps(self) -> float:
        if self.runtime > 0:
            return self.total_queries_generated / self.runtime
        return 0.0
    
    @property
    def uniqueness_rate(self) -> float:
        if self.total_queries_generated > 0:
            return self.unique_queries / self.total_queries_generated
        return 1.0


class ProductionRQG:
    """
    Production-ready Random Query Generator.
    
    Features:
    - Billion-scale unique query generation
    - Configurable multithreading
    - High-entropy randomization
    - Dynamic data generation
    - Query uniqueness tracking
    - Performance monitoring
    - Checkpoint and resume support
    """
    
    def __init__(self, config: ProductionConfig):
        self.config = config
        self.logger = self._setup_logging()
        
        # Validate configuration
        errors = config.validate()
        if errors:
            raise ValueError(f"Configuration errors: {errors}")
            
        # Create directories
        config.create_directories()
        
        # Initialize components
        self.logger.info("Initializing production RQG components...")
        
        self.entropy_manager = EntropyManager(config.entropy)
        self.uniqueness_tracker = UniquenessTracker(config.uniqueness)
        self.stats = ProductionStats(start_time=time.time())
        
        # Load grammars
        self.grammars = self._load_grammars()
        
        # Initialize thread pool with generator factory
        self.thread_pool = ThreadPoolManager(
            config.threading,
            self._create_generator
        )
        
        # Monitoring
        self.last_monitor_time = time.time()
        self.last_monitor_queries = 0
        
        # Checkpoint support
        self.checkpoint_path = Path(config.output_dir) / f"{config.name}_checkpoint.json"
        self.last_checkpoint_queries = 0
        
        # Duplicate collection (optional; set from CLI via attributes)
        self.collect_duplicates: bool = getattr(self, 'collect_duplicates', False)
        self.duplicates_output: Optional[str] = getattr(self, 'duplicates_output', None)
        self.duplicates: List[str] = []
        
        # Setup signal handlers for graceful shutdown
        
        # Internal: helper regexes for function call detection (skip collecting duplicates of pure calls)
        import re as _re  # local alias to avoid top-level cost
        self._re_select_func = _re.compile(r"^\s*SELECT\s+([A-Za-z_][\w\.]*?)\s*\(", _re.IGNORECASE | _re.DOTALL)
        self._re_has_from = _re.compile(r"\bFROM\b", _re.IGNORECASE)
        self._re_call = _re.compile(r"^\s*CALL\s+", _re.IGNORECASE)
        self._setup_signal_handlers()
        self.logger.info("Production RQG initialized successfully")
        
    def _is_function_call_query(self, query: str) -> bool:
        """Heuristic: return True for plain function/procedure invocations that are normal to repeat.
        - CALL proc(...);
        - SELECT func(...); (and there is no FROM clause)
        Does not treat SELECT ... FROM func(...) as a plain call (that's a full SELECT).
        """
        q = (query or "").strip()
        if not q:
            return False
        # Procedure call
        if self._re_call.search(q) is not None:
            return True
        # Bare function call via SELECT (no FROM clause)
        if self._re_select_func.search(q) is not None and self._re_has_from.search(q) is None:
            return True
        return False
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger("ProductionRQG")
        logger.setLevel(self.config.log_level)
        
        # Console handler
        console = logging.StreamHandler()
        console.setLevel(self.config.log_level)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console.setFormatter(formatter)
        logger.addHandler(console)
        
        # File handler (ensure directory exists before creating file)
        output_dir_path = Path(self.config.output_dir)
        try:
            output_dir_path.mkdir(parents=True, exist_ok=True)
        except Exception:
            # If directory creation fails, continue with console-only logging
            pass
        log_file = output_dir_path / f"{self.config.name}.log"
        try:
            file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
            file_handler.setLevel(self.config.log_level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        except FileNotFoundError:
            # In rare race conditions on some filesystems, directory may not be ready; skip file logging
            logger.warning(f"Could not open log file at {log_file}, continuing without file logging")
        
        return logger
    
    def _load_grammars(self) -> Dict[str, Grammar]:
        """Load configured grammars"""
        grammars = {}
        
        # Import grammar modules dynamically
        import importlib
        
        for grammar_name in self.config.grammars:
            try:
                # Try to import from grammars package
                module = importlib.import_module(f"grammars.{grammar_name}")
                if hasattr(module, 'g'):
                    grammars[grammar_name] = module.g
                    self.logger.info(f"Loaded grammar: {grammar_name}")
                else:
                    self.logger.warning(f"Grammar module {grammar_name} has no 'g' attribute")
            except ImportError as e:
                self.logger.error(f"Failed to load grammar {grammar_name}: {e}")
                
        if not grammars:
            raise RuntimeError("No grammars loaded successfully")
            
        return grammars
    
    def _create_generator(self, thread_id: int):
        """Factory function to create generator for thread"""
        # Get thread-specific RNG
        rng = self.entropy_manager.get_generator(thread_id)
        
        # Create data generator
        data_gen = DynamicDataGenerator(self.config.data, rng)
        
        # Create base RQG with dynamic tables
        from pyrqg.api import TableMetadata
        base_rqg = BaseRQG()
        
        # Add dynamic tables
        for i in range(10):  # Start with 10 tables
            schema = data_gen.generate_schema("medium")
            # Convert to TableMetadata format
            columns = [{"name": col.name, "type": col.data_type} for col in schema.columns]
            table = TableMetadata(
                name=schema.name,
                columns=columns
            )
            base_rqg.add_table(table)
        
        # Add grammars
        for name, grammar in self.grammars.items():
            base_rqg.add_grammar(name, grammar)
            
        return base_rqg
    
    def generate_batch(self, count: int) -> Generator[str, None, None]:
        """Generate a batch of queries (pre-submits all batches)."""
        # Determine grammar distribution
        if self.config.grammar_weights:
            # Weighted selection
            grammar_names = list(self.config.grammar_weights.keys())
            weights = list(self.config.grammar_weights.values())
        else:
            # Uniform distribution
            grammar_names = list(self.grammars.keys())
            weights = [1.0 / len(grammar_names)] * len(grammar_names)
        
        # Uniqueness mode check (allow bypass to reduce overhead)
        no_uniqueness = False
        try:
            from .uniqueness import UniquenessMode  # type: ignore
            no_uniqueness = getattr(self.config.uniqueness, 'mode', None) == UniquenessMode.NONE
        except Exception:
            no_uniqueness = False
        
        # Submit batches to thread pool
        batch_size = self.config.threading.batch_size
        futures = []
        
        remaining = count
        while remaining > 0:
            current_batch_size = min(remaining, batch_size)
            
            # Select grammar for this batch
            # In production, might want to mix within batch
            import random
            grammar = random.choices(grammar_names, weights=weights)[0]
            
            future = self.thread_pool.submit_batch(grammar, current_batch_size)
            futures.append(future)
            remaining -= current_batch_size
        
        # Collect results
        queries_generated = 0
        # Use as_completed to avoid head-of-line blocking if the first batch is slow
        from concurrent.futures import as_completed
        for future in as_completed(futures):
            try:
                batch = future.result()
                
                for query in batch.queries:
                    if no_uniqueness:
                        # Update stats and yield directly
                        self.stats.total_queries_generated += 1
                        self.stats.unique_queries += 1
                        yield query
                    else:
                        # Check uniqueness
                        is_unique = self.uniqueness_tracker.check_and_add(query)
                        
                        # Update stats
                        self.stats.total_queries_generated += 1
                        if is_unique:
                            self.stats.unique_queries += 1
                            yield query
                        else:
                            self.stats.duplicate_queries += 1
                            if self.collect_duplicates and not self._is_function_call_query(query):
                                self.duplicates.append(query)
                            # Could regenerate here if needed
                        
                    queries_generated += 1
                    
                    # Monitor progress
                    if queries_generated % self.config.monitoring.interval == 0:
                        self._monitor_progress()
                        
                    # Checkpoint
                    if queries_generated % self.config.checkpoint_interval == 0:
                        self._save_checkpoint()
                        
            except Exception as e:
                self.logger.error(f"Batch generation failed: {e}")
                # We don't know exactly how many queries were in this batch here; count as batch_size
                self.stats.failed_queries += batch_size
    
    def generate_stream(self, count: int, max_outstanding_batches: Optional[int] = None, end_time: Optional[float] = None) -> Generator[str, None, None]:
        """Generate queries in a streaming fashion with a small sliding window of outstanding batches.
        This avoids pre-submitting all batches and reduces memory/backlog, improving real-time behavior.
        """
        # Determine grammar distribution
        if self.config.grammar_weights:
            grammar_names = list(self.config.grammar_weights.keys())
            weights = list(self.config.grammar_weights.values())
        else:
            grammar_names = list(self.grammars.keys())
            weights = [1.0 / len(grammar_names)] * len(grammar_names)
        
        # Uniqueness mode check
        no_uniqueness = False
        try:
            from .uniqueness import UniquenessMode  # type: ignore
            no_uniqueness = getattr(self.config.uniqueness, 'mode', None) == UniquenessMode.NONE
        except Exception:
            no_uniqueness = False
        
        batch_size = self.config.threading.batch_size
        window = max_outstanding_batches or max(1, int(self.config.threading.num_threads))
        from collections import deque
        pending: deque = deque()
        remaining = count
        queries_generated = 0
        import random
        from concurrent.futures import as_completed
        
        # Prime initial window
        while remaining > 0 and len(pending) < window:
            current_batch_size = min(remaining, batch_size)
            grammar = random.choices(grammar_names, weights=weights)[0]
            pending.append(self.thread_pool.submit_batch(grammar, current_batch_size))
            remaining -= current_batch_size
        
        # Process as futures complete, and keep window filled
        while pending:
            future = pending.popleft()
            try:
                batch = future.result()
                for query in batch.queries:
                    if end_time is not None and time.time() >= end_time:
                        return
                    if no_uniqueness:
                        self.stats.total_queries_generated += 1
                        self.stats.unique_queries += 1
                        yield query
                    else:
                        is_unique = self.uniqueness_tracker.check_and_add(query)
                        self.stats.total_queries_generated += 1
                        if is_unique:
                            self.stats.unique_queries += 1
                            yield query
                        else:
                            self.stats.duplicate_queries += 1
                            if self.collect_duplicates and not self._is_function_call_query(query):
                                self.duplicates.append(query)
                    
                    queries_generated += 1
                    if queries_generated % self.config.monitoring.interval == 0:
                        self._monitor_progress()
                    if queries_generated % self.config.checkpoint_interval == 0:
                        self._save_checkpoint()
            except Exception as e:
                self.logger.error(f"Batch generation failed: {e}")
                self.stats.failed_queries += batch_size
            
            # Refill window
            if remaining > 0:
                current_batch_size = min(remaining, batch_size)
                grammar = random.choices(grammar_names, weights=weights)[0]
                pending.append(self.thread_pool.submit_batch(grammar, current_batch_size))
                remaining -= current_batch_size
        
    def generate(self, count: int, output_file: Optional[str] = None) -> int:
        """
        Generate queries and optionally write to file.
        
        Returns number of unique queries generated.
        """
        self.logger.info(f"Starting generation of {count:,} queries...")
        
        output_handle = None
        if output_file:
            output_handle = open(output_file, 'w')
            
        try:
            unique_count = 0
            for query in self.generate_batch(count):
                if output_handle:
                    output_handle.write(query + ';\n')
                unique_count += 1
                
                # Stop if we've reached target
                if unique_count >= count:
                    break
                    
            self.logger.info(f"Generated {unique_count:,} unique queries")
            return unique_count
            
        finally:
            if output_handle:
                output_handle.close()
    
    def execute_stream(self, count: int, dsn: str, use_filter: bool = False, print_errors: bool = False, error_samples: int = 10, output_file: Optional[str] = None, progress_every: int = 0, echo_queries: bool = False, end_time: Optional[float] = None) -> int:
        """Generate queries and execute each one in real time against a live DB.
        Returns number of queries attempted (executed or skipped) and writes optional output.
        """
        self.logger.info(f"Starting real-time execution of {count:,} queries...")
        self.logger.info(f"Execution settings: dsn={dsn}, use_filter={use_filter}, echo={echo_queries}, progress_every={progress_every}, threads={self.config.threading.num_threads}")

        # Prepare optional output file
        out_handle = None
        if output_file:
            out_handle = open(output_file, 'w', encoding='utf-8')

        # Create executor (optionally filtered)
        try:
            from pyrqg.core.executor import create_executor
            try:
                from pyrqg.core.filtered_executor import create_filtered_executor  # type: ignore
            except Exception:
                create_filtered_executor = None  # type: ignore

            executor = (create_filtered_executor(dsn) if (use_filter and create_filtered_executor is not None)
                        else create_executor(dsn))

            from pyrqg.core.constants import Status
            executed = 0
            syntax_errors = 0
            samples = []
            start_time = time.time()

            try:
                # Use streaming generator with small outstanding window to avoid caching/queuing
                window = max(1, int(self.config.threading.num_threads))
                for query in self.generate_stream(count, max_outstanding_batches=window, end_time=end_time):
                    if out_handle:
                        out_handle.write((query if query.strip().endswith(';') else (query + ';')) + '\n')
                    if echo_queries:
                        print(f"[{executed + 1}] {query}", flush=True)
                    res = executor.execute(query)
                    executed += 1
                    if res.status == Status.SYNTAX_ERROR:
                        syntax_errors += 1
                        if print_errors and len(samples) < error_samples:
                            samples.append((query, res.errstr))
                    if progress_every and executed % progress_every == 0:
                        elapsed = max(1e-6, time.time() - start_time)
                        qps = executed / elapsed
                        print(f"Progress: executed={executed}, syntax_errors={syntax_errors}, qps={qps:.1f}", flush=True)

                self.logger.info(f"Executed {executed:,} queries. Syntax errors: {syntax_errors:,}")
                if print_errors and samples:
                    print("\nSample syntax errors (showing up to", len(samples), "):")
                    for i, (q, err) in enumerate(samples, 1):
                        print(f"[{i}] Error: {err}")
                        print(f"    Query: {q}")
                return executed
            finally:
                try:
                    executor.close()
                except Exception:
                    pass
        finally:
            if out_handle:
                out_handle.close()

    def _monitor_progress(self):
        """Monitor and log progress"""
        current_time = time.time()
        current_queries = self.stats.total_queries_generated
        
        # Calculate interval QPS
        interval_time = current_time - self.last_monitor_time
        interval_queries = current_queries - self.last_monitor_queries
        interval_qps = interval_queries / interval_time if interval_time > 0 else 0
        
        # Get component stats
        entropy_stats = self.entropy_manager.get_statistics()
        thread_stats = self.thread_pool.get_statistics()
        uniqueness_stats = self.uniqueness_tracker.get_statistics()
        
        # Memory usage (optional if psutil not installed)
        memory_mb = None
        try:
            import psutil  # type: ignore
            process = psutil.Process()
            memory_mb = process.memory_info().rss / (1024 * 1024)
        except Exception:
            memory_mb = None
        
        # Log progress
        mem_str = f"{memory_mb:.1f} MB" if isinstance(memory_mb, (int, float)) else "n/a"
        self.logger.info(
            f"Progress: {current_queries:,} queries | "
            f"QPS: {interval_qps:.1f} (interval), {self.stats.overall_qps:.1f} (overall) | "
            f"Unique: {self.stats.uniqueness_rate:.2%} | "
            f"Memory: {mem_str} | "
            f"Threads: {thread_stats.active_threads} active"
        )
        
        # Export metrics if configured
        if self.config.monitoring.export_path:
            metrics = {
                "timestamp": current_time,
                "total_queries": current_queries,
                "unique_queries": self.stats.unique_queries,
                "duplicate_queries": self.stats.duplicate_queries,
                "failed_queries": self.stats.failed_queries,
                "interval_qps": interval_qps,
                "overall_qps": self.stats.overall_qps,
                "uniqueness_rate": self.stats.uniqueness_rate,
                "memory_mb": memory_mb,
                "entropy": entropy_stats,
                "threading": thread_stats.__dict__,
                "uniqueness": uniqueness_stats
            }
            
            with open(self.config.monitoring.export_path, 'a') as f:
                json.dump(metrics, f)
                f.write('\n')
        
        # Check alerts
        if self.stats.uniqueness_rate < (1 - self.config.monitoring.alert_on_duplicate_rate):
            self.logger.warning(f"High duplicate rate: {1 - self.stats.uniqueness_rate:.2%}")
            # If duplicate collection is enabled, print duplicates and optionally dump to file
            if self.collect_duplicates and self.duplicates:
                try:
                    print("\nDuplicate queries detected (showing all collected so far):")
                    for i, dq in enumerate(self.duplicates, 1):
                        print(f"[{i}] {dq}")
                    if self.duplicates_output:
                        with open(self.duplicates_output, 'a', encoding='utf-8') as f:
                            for dq in self.duplicates:
                                f.write(dq.rstrip(';') + ';\n')
                        print(f"[info] Duplicates appended to {self.duplicates_output}")
                except Exception as de:
                    self.logger.error(f"Failed to print/write duplicates: {de}")
            
        # Update for next interval
        self.last_monitor_time = current_time
        self.last_monitor_queries = current_queries
    
    def _save_checkpoint(self):
        """Save checkpoint for resume support"""
        checkpoint = {
            "timestamp": time.time(),
            "stats": {
                "total_queries_generated": self.stats.total_queries_generated,
                "unique_queries": self.stats.unique_queries,
                "duplicate_queries": self.stats.duplicate_queries,
                "failed_queries": self.stats.failed_queries,
            },
            "entropy_stats": self.entropy_manager.get_statistics(),
            "uniqueness_stats": self.uniqueness_tracker.get_statistics()
        }
        
        with open(self.checkpoint_path, 'w') as f:
            json.dump(checkpoint, f, indent=2)
            
        self.logger.info(f"Checkpoint saved at {self.stats.total_queries_generated:,} queries")
        self.last_checkpoint_queries = self.stats.total_queries_generated
    
    def load_checkpoint(self, checkpoint_file: str):
        """Load from checkpoint"""
        with open(checkpoint_file, 'r') as f:
            checkpoint = json.load(f)
            
        # Restore stats
        for key, value in checkpoint["stats"].items():
            setattr(self.stats, key, value)
            
        self.logger.info(f"Loaded checkpoint: {self.stats.total_queries_generated:,} queries generated")
        
        # Note: Full state restoration would require more work
        # This is simplified for demonstration
    
    def _setup_signal_handlers(self):
        """Setup graceful shutdown handlers"""
        def shutdown_handler(signum, frame):
            self.logger.info("Received shutdown signal, cleaning up...")
            self.shutdown()
            sys.exit(0)
            
        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)
        
        # Also register cleanup on exit
        atexit.register(self.shutdown)
    
    def shutdown(self):
        """Graceful shutdown"""
        self.logger.info("Shutting down production RQG...")
        
        # Save final checkpoint
        if self.stats.total_queries_generated > self.last_checkpoint_queries:
            self._save_checkpoint()
            
        # Shutdown thread pool
        self.thread_pool.shutdown(wait=True, timeout=30.0)
        
        # Final stats
        self.logger.info(f"Final statistics:")
        self.logger.info(f"  Total queries: {self.stats.total_queries_generated:,}")
        self.logger.info(f"  Unique queries: {self.stats.unique_queries:,}")
        self.logger.info(f"  Duplicate queries: {self.stats.duplicate_queries:,}")
        self.logger.info(f"  Failed queries: {self.stats.failed_queries:,}")
        self.logger.info(f"  Runtime: {self.stats.runtime:.1f} seconds")
        self.logger.info(f"  Overall QPS: {self.stats.overall_qps:.1f}")
        self.logger.info(f"  Uniqueness rate: {self.stats.uniqueness_rate:.2%}")
        
        # If duplicates were collected, present at shutdown as well
        if self.collect_duplicates and self.duplicates:
            try:
                print("\nDuplicate queries collected during run:")
                for i, dq in enumerate(self.duplicates, 1):
                    print(f"[{i}] {dq}")
                if self.duplicates_output:
                    with open(self.duplicates_output, 'a', encoding='utf-8') as f:
                        for dq in self.duplicates:
                            f.write(dq.rstrip(';') + ';\n')
                    print(f"[info] Duplicates appended to {self.duplicates_output}")
            except Exception as de:
                self.logger.error(f"Failed to print/write duplicates at shutdown: {de}")


def main():
    """Command-line interface for production RQG"""
    import argparse
    from .configs import get_config, CONFIGS, custom_config
    
    parser = argparse.ArgumentParser(
        description="Production PyRQG - Billion-scale query generation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Available configurations:
  billion     - Generate 1 billion queries with maximum performance
  test        - Test configuration with 10k queries  
  performance - Benchmark configuration (no uniqueness checking)
  minimal     - Minimal configuration for simple use
  yugabyte    - YugabyteDB-specific configuration

Examples:
  # Use predefined configuration
  python run_production.py --config billion
  
  # Custom configuration
  python run_production.py --custom --queries 100000 --grammars dml_unique,functions_ddl
  
  # Override settings
  python run_production.py --config test --count 50000 --threads 8
        """
    )
    
    # Config selection
    config_group = parser.add_mutually_exclusive_group(required=True)
    config_group.add_argument("--config", choices=list(CONFIGS.keys()), 
                             help="Use predefined configuration")
    config_group.add_argument("--custom", action="store_true",
                             help="Create custom configuration")
    
    # Custom config options
    parser.add_argument("--queries", type=int, default=10000,
                       help="Number of queries to generate (for custom config)")
    parser.add_argument("--grammars", type=str,
                       help="Comma-separated list of grammars (for custom config)")
    
    # Common options
    parser.add_argument("--count", type=int, help="Override number of queries")
    parser.add_argument("--threads", type=int, help="Override thread count")
    parser.add_argument("--output", help="Output file for queries (also used to optionally mirror executed queries)")
    parser.add_argument("--checkpoint", help="Resume from checkpoint file")
    parser.add_argument("--no-uniqueness", action="store_true",
                       help="Disable uniqueness checking for speed")

    # Live execution options
    parser.add_argument("--dsn", help="PostgreSQL-compatible DSN for real-time execution (e.g., postgresql://user:pass@host:port/db)")
    parser.add_argument("--use-filter", dest="use_filter", action="store_true", help="Use PostgreSQL compatibility filter before executing queries")
    parser.add_argument("--print-errors", dest="print_errors", action="store_true", help="Print sample SQL syntax errors encountered during execution")
    parser.add_argument("--error-samples", dest="error_samples", type=int, default=10, help="Max number of error samples to print with --print-errors")
    parser.add_argument("--echo-queries", dest="echo_queries", action="store_true", help="Echo each executed query to stdout (real-time exec)")
    parser.add_argument("--progress-every", dest="progress_every", type=int, default=0, help="Print a progress line every N executed queries (real-time exec; 0=disable)")
    
    # Time-based execution/generation
    parser.add_argument("--duration", type=int, default=0, help="Run for N seconds instead of a fixed count (0=disabled)")
    
    # Duplicate visibility (optional)
    parser.add_argument("--print-duplicates", dest="print_duplicates", action="store_true", help="Collect and print duplicate queries when high duplicate rate is detected (memory-intensive for long runs)")
    parser.add_argument("--duplicates-output", dest="duplicates_output", help="Optional file to write duplicates when collected")

    # Verbosity
    parser.add_argument("--verbose", action="store_true", help="Enable INFO level logging with timestamps")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG level logging (very verbose)")
    
    args = parser.parse_args()
    
    # Ensure immediate terminal visibility when echo/progress are used
    try:
        if getattr(args, 'echo_queries', False) or (getattr(args, 'progress_every', 0) or 0) > 0:
            if hasattr(sys.stdout, 'reconfigure'):
                sys.stdout.reconfigure(line_buffering=True)
    except Exception:
        pass
    
    # Get configuration
    if args.custom:
        if not args.grammars:
            parser.error("--grammars required for custom configuration")
        grammars = args.grammars.split(",")
        config = custom_config(
            name="custom_run",
            queries=args.queries,
            grammars=grammars,
            threads=args.threads,
            uniqueness=not args.no_uniqueness
        )
    else:
        config = get_config(args.config)
    
    # Apply overrides
    if args.count:
        config.target_queries = args.count
    if args.threads:
        config.threading.num_threads = args.threads
    if args.no_uniqueness:
        from .uniqueness import UniquenessMode
        config.uniqueness.mode = UniquenessMode.NONE
        
    # Adjust log level from verbosity flags (affects ProductionRQG logger)
    if args.debug:
        config.log_level = logging.DEBUG
    elif args.verbose:
        config.log_level = logging.INFO

    # Create generator
    generator = ProductionRQG(config)
    
    # Configure duplicate collection settings on generator from CLI flags
    if getattr(args, 'print_duplicates', False):
        generator.collect_duplicates = True
        generator.duplicates_output = getattr(args, 'duplicates_output', None)
    
    # Load checkpoint if specified
    if args.checkpoint:
        generator.load_checkpoint(args.checkpoint)
        
    # Generate or execute queries
    try:
        to_run = config.target_queries - generator.stats.total_queries_generated
        # Determine end_time if duration requested
        end_time = None
        if getattr(args, 'duration', 0):
            end_time = time.time() + max(0, int(args.duration))
        
        if args.dsn:
            executed = generator.execute_stream(
                count=to_run,
                dsn=args.dsn,
                use_filter=getattr(args, 'use_filter', False),
                print_errors=getattr(args, 'print_errors', False),
                error_samples=getattr(args, 'error_samples', 10),
                output_file=args.output,
                progress_every=getattr(args, 'progress_every', 0),
                echo_queries=getattr(args, 'echo_queries', False),
                end_time=end_time
            )
            print(f"\nReal-time execution complete: executed {executed:,} queries")
        else:
            # If duration is set, stream-generate until time is up; otherwise use count-based generate()
            if end_time is not None:
                out_handle = open(args.output, 'w', encoding='utf-8') if args.output else None
                try:
                    unique_count = 0
                    window = max(1, int(generator.config.threading.num_threads))
                    for query in generator.generate_stream(to_run, max_outstanding_batches=window, end_time=end_time):
                        if out_handle:
                            out_handle.write((query if query.strip().endswith(';') else (query + ';')) + '\n')
                        else:
                            print(query)
                        unique_count += 1
                    print(f"\nSuccessfully generated {unique_count:,} unique queries (time-based)")
                finally:
                    if out_handle:
                        out_handle.close()
            else:
                unique_count = generator.generate(
                    to_run,
                    args.output
                )
                print(f"\nSuccessfully generated {unique_count:,} unique queries")
        
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        raise
    finally:
        generator.shutdown()


if __name__ == "__main__":
    main()