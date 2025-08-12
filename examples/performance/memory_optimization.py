#!/usr/bin/env python3
"""
memory_optimization.py - Memory-Efficient Query Generation

This example demonstrates memory optimization techniques for PyRQG:
- Streaming generation without accumulation
- Generator-based processing
- Memory-mapped file operations
- Bloom filter efficiency
- Garbage collection strategies

To run:
    python memory_optimization.py --demo streaming
    python memory_optimization.py --profile
"""

import sys
import gc
import time
import psutil
import weakref
import argparse
from pathlib import Path
from typing import Generator, Iterator, List
import tracemalloc
from dataclasses import dataclass
import array
import mmap
import tempfile

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.api import RQG
from pyrqg.dsl.core import Grammar, choice, template, number


@dataclass
class MemorySnapshot:
    """Memory usage snapshot."""
    timestamp: float
    rss_mb: float
    vms_mb: float
    available_mb: float
    percent: float
    description: str


class MemoryProfiler:
    """Profile memory usage during query generation."""
    
    def __init__(self):
        self.process = psutil.Process()
        self.snapshots: List[MemorySnapshot] = []
        self.start_time = time.time()
    
    def take_snapshot(self, description: str = ""):
        """Take a memory snapshot."""
        mem_info = self.process.memory_info()
        vm = psutil.virtual_memory()
        
        snapshot = MemorySnapshot(
            timestamp=time.time() - self.start_time,
            rss_mb=mem_info.rss / 1024 / 1024,
            vms_mb=mem_info.vms / 1024 / 1024,
            available_mb=vm.available / 1024 / 1024,
            percent=vm.percent,
            description=description
        )
        
        self.snapshots.append(snapshot)
        return snapshot
    
    def print_summary(self):
        """Print memory usage summary."""
        print("\nMemory Usage Summary")
        print("=" * 70)
        print(f"{'Time (s)':<10} {'RSS (MB)':<10} {'VMS (MB)':<10} {'Available':<10} {'Description'}")
        print("-" * 70)
        
        for snap in self.snapshots:
            print(f"{snap.timestamp:<10.1f} "
                  f"{snap.rss_mb:<10.1f} "
                  f"{snap.vms_mb:<10.1f} "
                  f"{snap.available_mb:<10.0f} "
                  f"{snap.description}")
        
        # Calculate peak and growth
        if self.snapshots:
            initial = self.snapshots[0].rss_mb
            peak = max(s.rss_mb for s in self.snapshots)
            final = self.snapshots[-1].rss_mb
            
            print(f"\nInitial RSS: {initial:.1f} MB")
            print(f"Peak RSS:    {peak:.1f} MB")
            print(f"Final RSS:   {final:.1f} MB")
            print(f"Growth:      {final - initial:.1f} MB")


class StreamingGenerator:
    """Memory-efficient streaming query generator."""
    
    def __init__(self, rqg: RQG, grammar: str = "dml_basic"):
        self.rqg = rqg
        self.grammar = grammar
        self.generated_count = 0
    
    def generate_stream(self, count: int) -> Generator[str, None, None]:
        """Generate queries as a stream without accumulation."""
        for i in range(count):
            query = self.rqg.generate_query(self.grammar, seed=i)
            self.generated_count += 1
            yield query
            
            # Periodic garbage collection
            if i % 10000 == 0:
                gc.collect()
    
    def generate_chunked(self, total: int, chunk_size: int = 1000) -> Generator[List[str], None, None]:
        """Generate queries in memory-efficient chunks."""
        for start in range(0, total, chunk_size):
            chunk = []
            end = min(start + chunk_size, total)
            
            for i in range(start, end):
                query = self.rqg.generate_query(self.grammar, seed=i)
                chunk.append(query)
            
            yield chunk
            
            # Clear references
            chunk = None
            gc.collect()
    
    def write_streaming(self, filename: str, count: int):
        """Write queries to file with minimal memory usage."""
        with open(filename, 'w', buffering=8192) as f:
            for query in self.generate_stream(count):
                f.write(query + ';\n')
                
                # Don't keep query in memory
                del query


class MemoryMappedWriter:
    """Use memory-mapped files for large outputs."""
    
    def __init__(self, filename: str, estimated_size: int):
        self.filename = filename
        self.file = open(filename, 'w+b')
        self.position = 0
        
        # Pre-allocate file space
        self.file.seek(estimated_size - 1)
        self.file.write(b'\0')
        self.file.flush()
        
        # Memory map the file
        self.mmap = mmap.mmap(self.file.fileno(), 0)
    
    def write_query(self, query: str):
        """Write query to memory-mapped file."""
        data = (query + ';\n').encode('utf-8')
        data_len = len(data)
        
        # Write to memory map
        self.mmap[self.position:self.position + data_len] = data
        self.position += data_len
    
    def close(self):
        """Close and truncate to actual size."""
        self.mmap.close()
        self.file.truncate(self.position)
        self.file.close()


class EfficientBloomFilter:
    """Memory-efficient Bloom filter using array."""
    
    def __init__(self, expected_items: int, false_positive_rate: float = 0.001):
        # Calculate optimal size and hash functions
        self.size = self._optimal_size(expected_items, false_positive_rate)
        self.hash_count = self._optimal_hash_count(expected_items, self.size)
        
        # Use array for efficient memory usage
        self.bit_array = array.array('B', [0] * (self.size // 8 + 1))
        self.count = 0
    
    def _optimal_size(self, n: int, p: float) -> int:
        """Calculate optimal bit array size."""
        import math
        return int(-n * math.log(p) / (math.log(2) ** 2))
    
    def _optimal_hash_count(self, n: int, m: int) -> int:
        """Calculate optimal number of hash functions."""
        import math
        return int((m / n) * math.log(2))
    
    def _hash(self, item: str, seed: int) -> int:
        """Simple hash function with seed."""
        import hashlib
        h = hashlib.md5(f"{seed}{item}".encode()).hexdigest()
        return int(h, 16) % self.size
    
    def add(self, item: str):
        """Add item to filter."""
        for i in range(self.hash_count):
            pos = self._hash(item, i)
            byte_idx = pos // 8
            bit_idx = pos % 8
            self.bit_array[byte_idx] |= (1 << bit_idx)
        self.count += 1
    
    def contains(self, item: str) -> bool:
        """Check if item might be in filter."""
        for i in range(self.hash_count):
            pos = self._hash(item, i)
            byte_idx = pos // 8
            bit_idx = pos % 8
            if not (self.bit_array[byte_idx] & (1 << bit_idx)):
                return False
        return True
    
    def memory_usage_bytes(self) -> int:
        """Get memory usage in bytes."""
        return len(self.bit_array) * self.bit_array.itemsize


class WeakRefCache:
    """Cache using weak references to allow garbage collection."""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.cache = weakref.WeakValueDictionary()
        self.strong_refs = []  # Keep some strong references
    
    def get(self, key: str) -> Optional[str]:
        """Get value from cache."""
        return self.cache.get(key)
    
    def put(self, key: str, value: str):
        """Put value in cache."""
        self.cache[key] = value
        
        # Keep some strong references for frequently used items
        if len(self.strong_refs) < self.max_size // 10:
            self.strong_refs.append(value)
        elif len(self.strong_refs) >= self.max_size // 10:
            # Replace oldest
            self.strong_refs.pop(0)
            self.strong_refs.append(value)


def demonstrate_streaming():
    """Demonstrate streaming generation."""
    print("Streaming Query Generation Demo")
    print("=" * 50)
    
    rqg = RQG()
    profiler = MemoryProfiler()
    
    # Take initial snapshot
    profiler.take_snapshot("Initial")
    
    # Create streaming generator
    generator = StreamingGenerator(rqg)
    
    print("\n1. Streaming 100,000 queries to file...")
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
        filename = f.name
        
        for i, query in enumerate(generator.generate_stream(100000)):
            f.write(query + ';\n')
            
            # Progress and snapshots
            if i % 20000 == 0:
                profiler.take_snapshot(f"After {i} queries")
                print(f"  Generated {i} queries...")
    
    profiler.take_snapshot("After streaming")
    
    # Clean up
    Path(filename).unlink()
    
    print("\n2. Chunked generation (1000 per chunk)...")
    profiler.take_snapshot("Before chunked")
    
    total_length = 0
    for chunk in generator.generate_chunked(50000, chunk_size=1000):
        total_length += sum(len(q) for q in chunk)
        # Chunk is automatically cleared after iteration
    
    profiler.take_snapshot("After chunked")
    print(f"  Total characters generated: {total_length:,}")
    
    profiler.print_summary()


def demonstrate_memory_mapped():
    """Demonstrate memory-mapped file writing."""
    print("\nMemory-Mapped File Writing Demo")
    print("=" * 50)
    
    rqg = RQG()
    profiler = MemoryProfiler()
    
    profiler.take_snapshot("Initial")
    
    # Estimate 100 bytes per query
    query_count = 50000
    estimated_size = query_count * 100
    
    with tempfile.NamedTemporaryFile(delete=False) as f:
        filename = f.name
    
    print(f"Writing {query_count} queries using memory mapping...")
    
    writer = MemoryMappedWriter(filename, estimated_size)
    
    for i in range(query_count):
        query = rqg.generate_query("dml_basic", seed=i)
        writer.write_query(query)
        
        if i % 10000 == 0:
            profiler.take_snapshot(f"After {i} mmap writes")
    
    writer.close()
    profiler.take_snapshot("After mmap complete")
    
    # Check file size
    file_size = Path(filename).stat().st_size
    print(f"\nFile size: {file_size / 1024 / 1024:.1f} MB")
    
    # Clean up
    Path(filename).unlink()
    
    profiler.print_summary()


def demonstrate_bloom_filter():
    """Demonstrate memory-efficient Bloom filter."""
    print("\nMemory-Efficient Bloom Filter Demo")
    print("=" * 50)
    
    profiler = MemoryProfiler()
    profiler.take_snapshot("Initial")
    
    # Create Bloom filter for 1 million items
    expected_items = 1_000_000
    bloom = EfficientBloomFilter(expected_items, false_positive_rate=0.001)
    
    profiler.take_snapshot("After Bloom creation")
    
    print(f"Bloom filter created for {expected_items:,} items")
    print(f"Size: {bloom.size:,} bits ({bloom.size / 8 / 1024 / 1024:.1f} MB)")
    print(f"Hash functions: {bloom.hash_count}")
    print(f"Actual memory: {bloom.memory_usage_bytes() / 1024 / 1024:.1f} MB")
    
    # Add items
    print("\nAdding items...")
    rqg = RQG()
    
    false_positives = 0
    for i in range(100000):
        query = rqg.generate_query("dml_basic", seed=i)
        
        # Check before adding
        if bloom.contains(query):
            false_positives += 1
        
        bloom.add(query)
        
        if i % 20000 == 0:
            profiler.take_snapshot(f"After {i} items added")
    
    profiler.take_snapshot("After all items added")
    
    print(f"\nFalse positives during insertion: {false_positives}")
    print(f"False positive rate: {false_positives / 100000:.3%}")
    
    # Test lookups
    print("\nTesting lookups...")
    found = 0
    for i in range(1000):
        query = rqg.generate_query("dml_basic", seed=i)
        if bloom.contains(query):
            found += 1
    
    print(f"Found {found}/1000 previously added items")
    
    profiler.print_summary()


def memory_optimization_tips():
    """Print memory optimization tips."""
    print("\nMemory Optimization Tips for PyRQG")
    print("=" * 50)
    
    tips = [
        ("Use Generators", 
         "Always use generators instead of lists for large query sets"),
        
        ("Stream to Files",
         "Write queries directly to files instead of accumulating in memory"),
        
        ("Batch Processing",
         "Process queries in small batches and clear after each batch"),
        
        ("Garbage Collection",
         "Call gc.collect() periodically for long-running generation"),
        
        ("Weak References",
         "Use weakref for caches that can be garbage collected"),
        
        ("Memory Mapping",
         "Use mmap for very large file operations"),
        
        ("Bloom Filters",
         "Use space-efficient probabilistic data structures when appropriate"),
        
        ("Profile Memory",
         "Use tracemalloc and memory_profiler to identify leaks"),
        
        ("Limit Recursion",
         "Avoid deep recursion in grammar rules"),
        
        ("Clear References",
         "Explicitly clear large objects when done: obj = None")
    ]
    
    for title, desc in tips:
        print(f"\n{title}:")
        print(f"  {desc}")
    
    print("\n\nExample: Memory-Efficient Pipeline")
    print("-" * 50)
    print("""
def efficient_pipeline(output_file: str, count: int):
    rqg = RQG()
    
    # Use context manager for automatic cleanup
    with open(output_file, 'w', buffering=65536) as f:
        # Generate in chunks
        for start in range(0, count, 1000):
            for i in range(start, min(start + 1000, count)):
                query = rqg.generate_query('dml_basic', seed=i)
                f.write(query + ';\\n')
                
                # Clear reference immediately
                query = None
            
            # Periodic garbage collection
            if start % 10000 == 0:
                gc.collect()
    """)


def profile_with_tracemalloc():
    """Profile memory allocation with tracemalloc."""
    print("\nDetailed Memory Profiling with tracemalloc")
    print("=" * 50)
    
    tracemalloc.start()
    
    # Take snapshot before
    snapshot1 = tracemalloc.take_snapshot()
    
    # Generate queries
    rqg = RQG()
    queries = []
    for i in range(10000):
        queries.append(rqg.generate_query("dml_basic", seed=i))
    
    # Take snapshot after
    snapshot2 = tracemalloc.take_snapshot()
    
    # Compare snapshots
    top_stats = snapshot2.compare_to(snapshot1, 'lineno')
    
    print("\nTop 10 memory allocations:")
    for stat in top_stats[:10]:
        print(f"{stat}")
    
    # Clear and measure
    queries.clear()
    gc.collect()
    
    current, peak = tracemalloc.get_traced_memory()
    print(f"\nCurrent memory usage: {current / 1024 / 1024:.1f} MB")
    print(f"Peak memory usage:    {peak / 1024 / 1024:.1f} MB")
    
    tracemalloc.stop()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Memory optimization examples for PyRQG"
    )
    
    parser.add_argument('--demo', 
                       choices=['streaming', 'mmap', 'bloom', 'all'],
                       help='Run specific demonstration')
    
    parser.add_argument('--profile', action='store_true',
                       help='Run memory profiling')
    
    parser.add_argument('--tips', action='store_true',
                       help='Show optimization tips')
    
    args = parser.parse_args()
    
    if args.tips:
        memory_optimization_tips()
    
    elif args.profile:
        profile_with_tracemalloc()
    
    elif args.demo:
        if args.demo == 'streaming' or args.demo == 'all':
            demonstrate_streaming()
        
        if args.demo == 'mmap' or args.demo == 'all':
            demonstrate_memory_mapped()
        
        if args.demo == 'bloom' or args.demo == 'all':
            demonstrate_bloom_filter()
    
    else:
        # Run all demos
        demonstrate_streaming()
        print("\n" + "=" * 70 + "\n")
        demonstrate_memory_mapped()
        print("\n" + "=" * 70 + "\n")
        demonstrate_bloom_filter()
        print("\n" + "=" * 70 + "\n")
        memory_optimization_tips()


if __name__ == "__main__":
    main()