#!/usr/bin/env python3
"""
billion_scale.py - Billion-Scale Query Generation Example

This example demonstrates PyRQG's ability to generate billions of unique queries:
- Multi-threaded generation
- Memory-efficient streaming
- Uniqueness tracking with Bloom filters
- Progress tracking and checkpointing
- Performance optimization techniques

To run:
    python billion_scale.py --queries 1000000000 --threads 8 --output queries.sql
"""

import sys
import time
import threading
import queue
import argparse
from pathlib import Path
from typing import Generator, Optional
import hashlib
import struct
import mmap
import os

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.production import ProductionRQG
from pyrqg.production.configs import billion_scale_config, custom_config
from pyrqg.production.uniqueness import UniquenessTracker
from pyrqg.production.entropy import EntropyManager


class BillionScaleGenerator:
    """Optimized generator for billion-scale query generation."""
    
    def __init__(self, target_queries: int, num_threads: int = 8):
        self.target_queries = target_queries
        self.num_threads = num_threads
        self.queries_generated = 0
        self.start_time = None
        self.output_queue = queue.Queue(maxsize=10000)
        self.stats_lock = threading.Lock()
        
        # Performance tracking
        self.checkpoint_interval = 1000000  # Save progress every 1M queries
        self.last_checkpoint = 0
        
        # Initialize production RQG with custom config
        self.config = custom_config(
            name="billion_scale_demo",
            queries=target_queries,
            grammars=["dml_unique", "complex_queries"],
            threads=num_threads,
            uniqueness=True,
            uniqueness_false_positive_rate=0.001,
            batch_size=5000,  # Large batches for efficiency
            entropy_seed=42
        )
        
        self.production_rqg = ProductionRQG(self.config)
    
    def generate_batch(self, batch_size: int) -> Generator[str, None, None]:
        """Generate a batch of queries."""
        return self.production_rqg.generate_batch(batch_size)
    
    def worker_thread(self, thread_id: int, queries_per_thread: int):
        """Worker thread for query generation."""
        local_count = 0
        batch_size = self.config['batch_size']
        
        while local_count < queries_per_thread:
            # Calculate next batch size
            remaining = queries_per_thread - local_count
            current_batch = min(batch_size, remaining)
            
            # Generate batch
            for query in self.generate_batch(current_batch):
                self.output_queue.put(query)
                local_count += 1
            
            # Update global counter
            with self.stats_lock:
                self.queries_generated += current_batch
                
                # Checkpoint if needed
                if self.queries_generated - self.last_checkpoint >= self.checkpoint_interval:
                    self.save_checkpoint()
                    self.last_checkpoint = self.queries_generated
    
    def save_checkpoint(self):
        """Save generation progress for resumption."""
        checkpoint = {
            'queries_generated': self.queries_generated,
            'elapsed_time': time.time() - self.start_time,
            'entropy_state': self.production_rqg.entropy_manager.get_state(),
            'uniqueness_stats': self.production_rqg.uniqueness_tracker.get_stats()
        }
        
        # Save to file
        checkpoint_file = f"billion_scale_checkpoint_{self.queries_generated}.json"
        import json
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint, f)
        
        print(f"\nCheckpoint saved: {checkpoint_file}")
    
    def print_progress(self):
        """Print generation progress."""
        while self.queries_generated < self.target_queries:
            time.sleep(10)  # Update every 10 seconds
            
            with self.stats_lock:
                current = self.queries_generated
            
            elapsed = time.time() - self.start_time
            rate = current / elapsed if elapsed > 0 else 0
            eta = (self.target_queries - current) / rate if rate > 0 else 0
            
            progress = current / self.target_queries * 100
            
            print(f"\rProgress: {current:,}/{self.target_queries:,} "
                  f"({progress:.1f}%) | "
                  f"Rate: {rate:,.0f} q/s | "
                  f"ETA: {eta/3600:.1f}h", end='', flush=True)
    
    def run(self, output_file: Optional[str] = None):
        """Run billion-scale generation."""
        print(f"Starting billion-scale generation")
        print(f"Target: {self.target_queries:,} queries")
        print(f"Threads: {self.num_threads}")
        print(f"Batch size: {self.config['batch_size']}")
        print()
        
        self.start_time = time.time()
        
        # Start worker threads
        threads = []
        queries_per_thread = self.target_queries // self.num_threads
        extra_queries = self.target_queries % self.num_threads
        
        for i in range(self.num_threads):
            # Distribute extra queries to first few threads
            thread_queries = queries_per_thread + (1 if i < extra_queries else 0)
            
            t = threading.Thread(
                target=self.worker_thread,
                args=(i, thread_queries)
            )
            t.start()
            threads.append(t)
        
        # Start progress printer
        progress_thread = threading.Thread(target=self.print_progress)
        progress_thread.daemon = True
        progress_thread.start()
        
        # Handle output
        if output_file:
            self.write_to_file(output_file)
        else:
            self.write_to_stdout()
        
        # Wait for completion
        for t in threads:
            t.join()
        
        # Final statistics
        self.print_final_stats()
    
    def write_to_file(self, output_file: str):
        """Write queries to file efficiently."""
        written = 0
        buffer = []
        buffer_size = 1000  # Write in chunks
        
        with open(output_file, 'w', buffering=1024*1024) as f:  # 1MB buffer
            while written < self.target_queries:
                try:
                    query = self.output_queue.get(timeout=1)
                    buffer.append(query + ';\n')
                    written += 1
                    
                    if len(buffer) >= buffer_size:
                        f.writelines(buffer)
                        buffer.clear()
                        
                except queue.Empty:
                    if self.queries_generated >= self.target_queries:
                        # Flush remaining
                        if buffer:
                            f.writelines(buffer)
                        break
            
        print(f"\n\nWrote {written:,} queries to {output_file}")
    
    def write_to_stdout(self):
        """Write queries to stdout."""
        written = 0
        
        while written < self.target_queries:
            try:
                query = self.output_queue.get(timeout=1)
                print(query + ';')
                written += 1
                
            except queue.Empty:
                if self.queries_generated >= self.target_queries:
                    break
    
    def print_final_stats(self):
        """Print final generation statistics."""
        elapsed = time.time() - self.start_time
        rate = self.queries_generated / elapsed if elapsed > 0 else 0
        
        # Get uniqueness statistics
        uniqueness_stats = self.production_rqg.uniqueness_tracker.get_stats()
        
        print("\n\nGeneration Complete!")
        print("=" * 60)
        print(f"Total queries:     {self.queries_generated:,}")
        print(f"Time elapsed:      {elapsed/3600:.2f} hours")
        print(f"Average rate:      {rate:,.0f} queries/second")
        print(f"Unique queries:    {uniqueness_stats['unique_count']:,}")
        print(f"Duplicates found:  {uniqueness_stats['duplicate_count']:,}")
        print(f"Uniqueness rate:   {uniqueness_stats['uniqueness_rate']:.4%}")
        print(f"Memory used:       {uniqueness_stats['memory_usage_mb']:.1f} MB")


class MemoryEfficientWriter:
    """Memory-mapped file writer for extreme scale."""
    
    def __init__(self, filename: str, size_estimate: int):
        self.filename = filename
        self.size_estimate = size_estimate
        self.position = 0
        
        # Create file with estimated size
        with open(filename, 'wb') as f:
            f.seek(size_estimate - 1)
            f.write(b'\0')
        
        # Memory map the file
        self.file = open(filename, 'r+b')
        self.mmap = mmap.mmap(self.file.fileno(), 0)
    
    def write(self, data: bytes):
        """Write data to memory-mapped file."""
        data_len = len(data)
        if self.position + data_len > self.size_estimate:
            # Extend file if needed
            self.mmap.close()
            self.file.close()
            
            with open(self.filename, 'r+b') as f:
                f.seek(0, 2)  # End of file
                f.write(b'\0' * data_len)
            
            self.file = open(self.filename, 'r+b')
            self.mmap = mmap.mmap(self.file.fileno(), 0)
        
        self.mmap[self.position:self.position + data_len] = data
        self.position += data_len
    
    def close(self):
        """Close memory-mapped file."""
        # Truncate to actual size
        self.mmap.close()
        self.file.truncate(self.position)
        self.file.close()


def demonstrate_optimization_techniques():
    """Demonstrate various optimization techniques."""
    
    print("Optimization Techniques for Billion-Scale Generation")
    print("=" * 60)
    
    # 1. Memory efficiency
    print("\n1. Memory Efficiency:")
    print("   - Streaming generation (no list accumulation)")
    print("   - Bloom filter for uniqueness (probabilistic)")
    print("   - Memory-mapped files for large outputs")
    print("   - Garbage collection tuning")
    
    # 2. CPU optimization
    print("\n2. CPU Optimization:")
    print("   - Multi-threading with thread-local entropy")
    print("   - Batch processing to reduce overhead")
    print("   - JIT compilation with PyPy (if available)")
    print("   - Minimal string operations")
    
    # 3. I/O optimization
    print("\n3. I/O Optimization:")
    print("   - Buffered writing with large buffers")
    print("   - Asynchronous I/O where possible")
    print("   - Compression for storage efficiency")
    print("   - Parallel writing to multiple files")
    
    # Example: Parallel file writing
    print("\n4. Example: Parallel File Writing")
    
    class ParallelWriter:
        def __init__(self, base_filename: str, num_files: int):
            self.files = []
            self.current_file = 0
            
            for i in range(num_files):
                filename = f"{base_filename}.part{i}"
                self.files.append(open(filename, 'w', buffering=1024*1024))
        
        def write(self, query: str):
            self.files[self.current_file].write(query + ';\n')
            self.current_file = (self.current_file + 1) % len(self.files)
        
        def close(self):
            for f in self.files:
                f.close()
    
    print("   Created ParallelWriter for distributed I/O")


def benchmark_performance():
    """Benchmark generation performance."""
    
    print("\nPerformance Benchmark")
    print("=" * 60)
    
    # Test different configurations
    configs = [
        ("Single-threaded", 1, 1000),
        ("Multi-threaded (4)", 4, 1000),
        ("Multi-threaded (8)", 8, 1000),
        ("Large batch", 4, 5000),
        ("Small batch", 4, 100)
    ]
    
    for name, threads, batch_size in configs:
        config = custom_config(
            name="benchmark",
            queries=10000,
            threads=threads,
            batch_size=batch_size,
            uniqueness=True
        )
        
        rqg = ProductionRQG(config)
        
        start = time.time()
        count = 0
        
        for query in rqg.generate():
            count += 1
            if count >= 10000:
                break
        
        elapsed = time.time() - start
        rate = count / elapsed
        
        print(f"{name:20s}: {rate:8,.0f} queries/sec")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Generate billions of unique SQL queries"
    )
    
    parser.add_argument(
        '--queries',
        type=int,
        default=1000000,
        help='Number of queries to generate (default: 1M)'
    )
    
    parser.add_argument(
        '--threads',
        type=int,
        default=8,
        help='Number of threads (default: 8)'
    )
    
    parser.add_argument(
        '--output',
        help='Output file (default: stdout)'
    )
    
    parser.add_argument(
        '--benchmark',
        action='store_true',
        help='Run performance benchmark'
    )
    
    parser.add_argument(
        '--demo',
        action='store_true',
        help='Demonstrate optimization techniques'
    )
    
    args = parser.parse_args()
    
    if args.demo:
        demonstrate_optimization_techniques()
    elif args.benchmark:
        benchmark_performance()
    else:
        # Run billion-scale generation
        generator = BillionScaleGenerator(
            target_queries=args.queries,
            num_threads=args.threads
        )
        generator.run(output_file=args.output)


if __name__ == "__main__":
    main()