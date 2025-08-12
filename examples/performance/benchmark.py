#!/usr/bin/env python3
"""
benchmark.py - PyRQG Performance Benchmarking

This example provides comprehensive performance benchmarking:
- Query generation speed tests
- Grammar complexity impact
- Memory usage profiling
- Thread scaling analysis
- Optimization comparisons

To run:
    python benchmark.py --all
    python benchmark.py --grammar-comparison
"""

import sys
import time
import psutil
import threading
import gc
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Callable
import statistics
import json
import matplotlib.pyplot as plt
from dataclasses import dataclass, asdict

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.api import RQG
from pyrqg.production import ProductionRQG
from pyrqg.production.configs import custom_config
from pyrqg.dsl.core import Grammar, choice, template, repeat, number


@dataclass
class BenchmarkResult:
    """Result of a benchmark test."""
    test_name: str
    queries_per_second: float
    total_queries: int
    duration: float
    memory_used_mb: float
    cpu_percent: float
    thread_count: int = 1
    grammar: str = "default"
    
    @property
    def efficiency(self) -> float:
        """Calculate efficiency score."""
        # Higher QPS with lower resource usage is better
        if self.memory_used_mb > 0 and self.cpu_percent > 0:
            return self.queries_per_second / (self.memory_used_mb * self.cpu_percent / 100)
        return 0.0


class PerformanceBenchmark:
    """Main benchmarking class."""
    
    def __init__(self):
        self.results: List[BenchmarkResult] = []
        self.process = psutil.Process()
        
    def measure_performance(self, 
                          test_func: Callable,
                          test_name: str,
                          duration: float = 10.0,
                          **kwargs) -> BenchmarkResult:
        """Measure performance of a test function."""
        
        # Force garbage collection
        gc.collect()
        
        # Get initial memory
        initial_memory = self.process.memory_info().rss / 1024 / 1024
        
        # CPU usage tracking
        cpu_samples = []
        stop_monitoring = threading.Event()
        
        def monitor_cpu():
            while not stop_monitoring.is_set():
                cpu_samples.append(self.process.cpu_percent(interval=0.1))
        
        monitor_thread = threading.Thread(target=monitor_cpu)
        monitor_thread.start()
        
        # Run test
        start_time = time.time()
        query_count = 0
        
        while time.time() - start_time < duration:
            test_func()
            query_count += 1
        
        elapsed = time.time() - start_time
        
        # Stop monitoring
        stop_monitoring.set()
        monitor_thread.join()
        
        # Get final memory
        final_memory = self.process.memory_info().rss / 1024 / 1024
        memory_used = final_memory - initial_memory
        
        # Calculate average CPU
        avg_cpu = statistics.mean(cpu_samples) if cpu_samples else 0.0
        
        result = BenchmarkResult(
            test_name=test_name,
            queries_per_second=query_count / elapsed,
            total_queries=query_count,
            duration=elapsed,
            memory_used_mb=max(0, memory_used),
            cpu_percent=avg_cpu,
            **kwargs
        )
        
        self.results.append(result)
        return result
    
    def benchmark_basic_generation(self):
        """Benchmark basic query generation."""
        print("\n1. Basic Query Generation Benchmark")
        print("-" * 50)
        
        rqg = RQG()
        
        def generate_simple():
            rqg.generate_query("dml_basic")
        
        result = self.measure_performance(
            generate_simple,
            "Basic Generation",
            duration=10.0,
            grammar="dml_basic"
        )
        
        print(f"Queries/sec: {result.queries_per_second:,.0f}")
        print(f"Memory used: {result.memory_used_mb:.1f} MB")
        print(f"CPU usage:   {result.cpu_percent:.1f}%")
    
    def benchmark_grammar_complexity(self):
        """Benchmark impact of grammar complexity."""
        print("\n2. Grammar Complexity Impact")
        print("-" * 50)
        
        # Create grammars of increasing complexity
        grammars = []
        
        # Simple grammar
        simple = Grammar("simple")
        simple.rule("query", template("SELECT * FROM users WHERE id = {id}"))
        simple.rule("id", number(1, 1000))
        grammars.append(("Simple", simple))
        
        # Medium grammar
        medium = Grammar("medium")
        medium.rule("query", choice(
            template("SELECT {columns} FROM {table} WHERE {condition}"),
            template("INSERT INTO {table} ({columns}) VALUES ({values})"),
            template("UPDATE {table} SET {assignments} WHERE {condition}")
        ))
        medium.rule("columns", choice("*", "id, name", "id, email, status"))
        medium.rule("table", choice("users", "products", "orders"))
        medium.rule("condition", template("{column} = {value}"))
        medium.rule("column", choice("id", "status", "type"))
        medium.rule("value", choice("1", "'active'", "'pending'"))
        medium.rule("assignments", "status = 'updated'")
        medium.rule("values", "1, 'test', 'active'")
        grammars.append(("Medium", medium))
        
        # Complex grammar
        complex_gram = Grammar("complex")
        complex_gram.rule("query", choice(
            ref("select_complex"),
            ref("insert_complex"),
            ref("update_complex")
        ))
        complex_gram.rule("select_complex", template(
            "SELECT {columns} FROM {table} {joins} WHERE {conditions} {group_by} {order_by}"
        ))
        complex_gram.rule("columns", repeat(
            choice("id", "name", "COUNT(*)", "SUM(amount)"),
            min=1, max=5, separator=", "
        ))
        complex_gram.rule("table", choice("users", "orders", "products"))
        complex_gram.rule("joins", repeat(
            template("JOIN {table} ON {condition}"),
            min=0, max=3, separator=" "
        ))
        complex_gram.rule("conditions", repeat(
            template("{column} {op} {value}"),
            min=1, max=5, separator=" AND "
        ))
        complex_gram.rule("column", choice("id", "status", "created_at"))
        complex_gram.rule("op", choice("=", ">", "<", "LIKE"))
        complex_gram.rule("value", choice("1", "'test'", "CURRENT_DATE"))
        complex_gram.rule("group_by", maybe("GROUP BY id"))
        complex_gram.rule("order_by", maybe("ORDER BY created_at DESC"))
        complex_gram.rule("insert_complex", "INSERT INTO users VALUES (1, 'test')")
        complex_gram.rule("update_complex", "UPDATE users SET name = 'test'")
        grammars.append(("Complex", complex_gram))
        
        # Benchmark each grammar
        rqg = RQG()
        
        for name, grammar in grammars:
            rqg.grammars[name.lower()] = grammar
            
            def generate():
                grammar.generate("query")
            
            result = self.measure_performance(
                generate,
                f"Grammar: {name}",
                duration=5.0,
                grammar=name.lower()
            )
            
            print(f"\n{name} Grammar:")
            print(f"  Queries/sec: {result.queries_per_second:,.0f}")
            print(f"  Efficiency:  {result.efficiency:.2f}")
    
    def benchmark_thread_scaling(self):
        """Benchmark multi-threaded performance scaling."""
        print("\n3. Thread Scaling Analysis")
        print("-" * 50)
        
        thread_counts = [1, 2, 4, 8, 16]
        scaling_results = []
        
        for threads in thread_counts:
            config = custom_config(
                name="thread_test",
                queries=100000,
                threads=threads,
                batch_size=1000
            )
            
            prod_rqg = ProductionRQG(config)
            
            def generate_batch():
                # Generate a batch
                batch = list(prod_rqg.generate_batch(100))
                return len(batch)
            
            result = self.measure_performance(
                generate_batch,
                f"Threads: {threads}",
                duration=10.0,
                thread_count=threads
            )
            
            # Scale by batch size
            result.queries_per_second *= 100  # Batch size
            scaling_results.append((threads, result))
            
            print(f"\nThreads: {threads}")
            print(f"  Queries/sec: {result.queries_per_second:,.0f}")
            print(f"  Speedup:     {result.queries_per_second / scaling_results[0][1].queries_per_second:.2f}x")
    
    def benchmark_memory_efficiency(self):
        """Benchmark memory usage patterns."""
        print("\n4. Memory Efficiency Analysis")
        print("-" * 50)
        
        # Test different generation patterns
        tests = [
            ("Streaming", self._streaming_generation),
            ("Batch Small", lambda: self._batch_generation(100)),
            ("Batch Large", lambda: self._batch_generation(10000)),
            ("Accumulated", self._accumulated_generation)
        ]
        
        for test_name, test_func in tests:
            gc.collect()
            initial_mem = self.process.memory_info().rss / 1024 / 1024
            
            # Run test
            start = time.time()
            test_func()
            elapsed = time.time() - start
            
            final_mem = self.process.memory_info().rss / 1024 / 1024
            mem_used = final_mem - initial_mem
            
            print(f"\n{test_name}:")
            print(f"  Memory used:  {mem_used:.1f} MB")
            print(f"  Time:         {elapsed:.2f}s")
            print(f"  Final memory: {final_mem:.1f} MB")
    
    def _streaming_generation(self):
        """Stream queries without accumulation."""
        rqg = RQG()
        count = 0
        for i in range(50000):
            query = rqg.generate_query("dml_basic", seed=i)
            count += len(query)  # Use query to prevent optimization
        return count
    
    def _batch_generation(self, batch_size: int):
        """Generate in batches."""
        rqg = RQG()
        count = 0
        for batch_start in range(0, 50000, batch_size):
            batch = []
            for i in range(batch_start, min(batch_start + batch_size, 50000)):
                batch.append(rqg.generate_query("dml_basic", seed=i))
            count += sum(len(q) for q in batch)
            batch.clear()  # Clear batch
        return count
    
    def _accumulated_generation(self):
        """Accumulate all queries (memory intensive)."""
        rqg = RQG()
        queries = []
        for i in range(50000):
            queries.append(rqg.generate_query("dml_basic", seed=i))
        return sum(len(q) for q in queries)
    
    def benchmark_optimization_techniques(self):
        """Compare different optimization techniques."""
        print("\n5. Optimization Techniques Comparison")
        print("-" * 50)
        
        rqg = RQG()
        
        # Test different optimizations
        optimizations = [
            ("Baseline", lambda: self._baseline_generation(rqg)),
            ("Cached Grammar", lambda: self._cached_generation(rqg)),
            ("Pre-compiled", lambda: self._precompiled_generation(rqg)),
            ("Optimized Random", lambda: self._optimized_random(rqg))
        ]
        
        for name, test_func in optimizations:
            result = self.measure_performance(
                test_func,
                name,
                duration=5.0
            )
            
            print(f"\n{name}:")
            print(f"  Queries/sec: {result.queries_per_second:,.0f}")
            print(f"  Improvement: {result.queries_per_second / self.results[0].queries_per_second:.2f}x")
    
    def _baseline_generation(self, rqg):
        """Standard generation."""
        return rqg.generate_query("dml_basic")
    
    def _cached_generation(self, rqg):
        """Cache grammar lookups."""
        grammar = rqg.grammars["dml_basic"]
        return grammar.generate("query")
    
    def _precompiled_generation(self, rqg):
        """Use pre-compiled patterns."""
        # Simulate pre-compiled query patterns
        patterns = [
            "SELECT * FROM users WHERE id = {}",
            "INSERT INTO products (name, price) VALUES ('{}', {})",
            "UPDATE orders SET status = '{}' WHERE id = {}"
        ]
        import random
        pattern = random.choice(patterns)
        return pattern.format(random.randint(1, 1000), random.randint(1, 100))
    
    def _optimized_random(self, rqg):
        """Use optimized random generation."""
        # Use faster random for non-cryptographic needs
        import random
        grammar = rqg.grammars["dml_basic"]
        # Cache random instance
        if not hasattr(self, '_fast_random'):
            self._fast_random = random.Random()
        grammar.context._rng = self._fast_random
        return grammar.generate("query")
    
    def generate_report(self, output_file: Optional[str] = None):
        """Generate benchmark report."""
        print("\n\nBenchmark Summary")
        print("=" * 80)
        
        # Sort by efficiency
        sorted_results = sorted(self.results, key=lambda r: r.efficiency, reverse=True)
        
        print(f"\n{'Test Name':<30} {'QPS':>10} {'Memory (MB)':>12} {'CPU %':>8} {'Efficiency':>10}")
        print("-" * 80)
        
        for result in sorted_results:
            print(f"{result.test_name:<30} "
                  f"{result.queries_per_second:>10,.0f} "
                  f"{result.memory_used_mb:>12.1f} "
                  f"{result.cpu_percent:>8.1f} "
                  f"{result.efficiency:>10.2f}")
        
        # Save detailed results
        if output_file:
            results_data = {
                "timestamp": time.time(),
                "system_info": {
                    "cpu_count": psutil.cpu_count(),
                    "memory_total_gb": psutil.virtual_memory().total / 1024**3,
                    "python_version": sys.version
                },
                "results": [asdict(r) for r in self.results]
            }
            
            with open(output_file, 'w') as f:
                json.dump(results_data, f, indent=2)
            
            print(f"\nDetailed results saved to: {output_file}")
    
    def plot_results(self):
        """Create performance visualization."""
        try:
            # Thread scaling plot
            thread_results = [r for r in self.results if "Threads:" in r.test_name]
            if thread_results:
                threads = [r.thread_count for r in thread_results]
                qps = [r.queries_per_second for r in thread_results]
                
                plt.figure(figsize=(10, 6))
                plt.subplot(1, 2, 1)
                plt.plot(threads, qps, 'bo-')
                plt.xlabel('Number of Threads')
                plt.ylabel('Queries per Second')
                plt.title('Thread Scaling Performance')
                plt.grid(True)
                
                # Efficiency comparison
                plt.subplot(1, 2, 2)
                names = [r.test_name for r in self.results[:5]]
                efficiency = [r.efficiency for r in self.results[:5]]
                
                plt.bar(names, efficiency)
                plt.xlabel('Test')
                plt.ylabel('Efficiency Score')
                plt.title('Performance Efficiency Comparison')
                plt.xticks(rotation=45)
                
                plt.tight_layout()
                plt.savefig('benchmark_results.png')
                print("\nPerformance plots saved to: benchmark_results.png")
                
        except ImportError:
            print("\nMatplotlib not available for plotting")


def main():
    """Main benchmark runner."""
    parser = argparse.ArgumentParser(
        description="PyRQG Performance Benchmarking"
    )
    
    parser.add_argument('--all', action='store_true',
                       help='Run all benchmarks')
    parser.add_argument('--basic', action='store_true',
                       help='Run basic generation benchmark')
    parser.add_argument('--grammar-complexity', action='store_true',
                       help='Test grammar complexity impact')
    parser.add_argument('--thread-scaling', action='store_true',
                       help='Test thread scaling')
    parser.add_argument('--memory', action='store_true',
                       help='Test memory efficiency')
    parser.add_argument('--optimizations', action='store_true',
                       help='Compare optimization techniques')
    parser.add_argument('--output', help='Save results to JSON file')
    parser.add_argument('--plot', action='store_true',
                       help='Generate performance plots')
    
    args = parser.parse_args()
    
    # Default to all if nothing specified
    if not any([args.all, args.basic, args.grammar_complexity,
                args.thread_scaling, args.memory, args.optimizations]):
        args.all = True
    
    benchmark = PerformanceBenchmark()
    
    print("PyRQG Performance Benchmark")
    print("=" * 50)
    print(f"CPU cores: {psutil.cpu_count()}")
    print(f"Total RAM: {psutil.virtual_memory().total / 1024**3:.1f} GB")
    print(f"Python:    {sys.version.split()[0]}")
    
    # Run selected benchmarks
    if args.all or args.basic:
        benchmark.benchmark_basic_generation()
    
    if args.all or args.grammar_complexity:
        benchmark.benchmark_grammar_complexity()
    
    if args.all or args.thread_scaling:
        benchmark.benchmark_thread_scaling()
    
    if args.all or args.memory:
        benchmark.benchmark_memory_efficiency()
    
    if args.all or args.optimizations:
        benchmark.benchmark_optimization_techniques()
    
    # Generate report
    benchmark.generate_report(args.output)
    
    # Create plots if requested
    if args.plot:
        benchmark.plot_results()


if __name__ == "__main__":
    main()