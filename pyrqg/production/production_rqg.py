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
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        self.logger.info("Production RQG initialized successfully")
    
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
        
        # File handler
        log_file = Path(self.config.output_dir) / f"{self.config.name}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(self.config.log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
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
        """Generate a batch of queries"""
        # Determine grammar distribution
        if self.config.grammar_weights:
            # Weighted selection
            grammar_names = list(self.config.grammar_weights.keys())
            weights = list(self.config.grammar_weights.values())
        else:
            # Uniform distribution
            grammar_names = list(self.grammars.keys())
            weights = [1.0 / len(grammar_names)] * len(grammar_names)
        
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
        for future in futures:
            try:
                batch = future.result(timeout=30.0)
                
                for query in batch.queries:
                    # Check uniqueness
                    is_unique = self.uniqueness_tracker.check_and_add(query)
                    
                    # Update stats
                    self.stats.total_queries_generated += 1
                    if is_unique:
                        self.stats.unique_queries += 1
                        yield query
                    else:
                        self.stats.duplicate_queries += 1
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
                self.stats.failed_queries += current_batch_size
    
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
        
        # Memory usage
        import psutil
        process = psutil.Process()
        memory_mb = process.memory_info().rss / (1024 * 1024)
        
        # Log progress
        self.logger.info(
            f"Progress: {current_queries:,} queries | "
            f"QPS: {interval_qps:.1f} (interval), {self.stats.overall_qps:.1f} (overall) | "
            f"Unique: {self.stats.uniqueness_rate:.2%} | "
            f"Memory: {memory_mb:.1f} MB | "
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
    parser.add_argument("--output", help="Output file for queries")
    parser.add_argument("--checkpoint", help="Resume from checkpoint file")
    parser.add_argument("--no-uniqueness", action="store_true",
                       help="Disable uniqueness checking for speed")
    
    args = parser.parse_args()
    
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
        
    # Create generator
    generator = ProductionRQG(config)
    
    # Load checkpoint if specified
    if args.checkpoint:
        generator.load_checkpoint(args.checkpoint)
        
    # Generate queries
    try:
        unique_count = generator.generate(
            config.target_queries - generator.stats.total_queries_generated,
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