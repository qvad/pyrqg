"""
Configurable multithreading system for high-performance query generation.

Provides a producer-consumer architecture with work queues, backpressure
handling, and comprehensive monitoring.
"""

import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Callable, Tuple
from collections import deque
import multiprocessing


@dataclass
class ThreadingConfig:
    """Configuration for thread pool management"""
    num_threads: Optional[int] = None  # None = CPU count
    queue_size: int = 10000  # Max items in queue
    batch_size: int = 1000  # Queries per batch
    backpressure_threshold: float = 0.8  # Slow down at 80% full
    monitor_interval: float = 1.0  # Stats update interval
    worker_timeout: float = 30.0  # Worker thread timeout
    enable_affinity: bool = True  # CPU affinity for threads


@dataclass
class ThreadStats:
    """Statistics for a single thread"""
    thread_id: int
    queries_generated: int = 0
    batches_completed: int = 0
    total_time: float = 0.0
    idle_time: float = 0.0
    generation_time: float = 0.0
    queue_time: float = 0.0
    errors: int = 0
    last_activity: float = field(default_factory=time.time)
    
    @property
    def queries_per_second(self) -> float:
        """Calculate QPS for this thread"""
        if self.total_time > 0:
            return self.queries_generated / self.total_time
        return 0.0
    
    @property
    def efficiency(self) -> float:
        """Calculate thread efficiency (active time / total time)"""
        if self.total_time > 0:
            return (self.total_time - self.idle_time) / self.total_time
        return 0.0


@dataclass
class ThreadPoolStats:
    """Aggregate statistics for thread pool"""
    total_queries: int = 0
    total_batches: int = 0
    total_errors: int = 0
    start_time: float = field(default_factory=time.time)
    queue_size: int = 0
    active_threads: int = 0
    thread_stats: Dict[int, ThreadStats] = field(default_factory=dict)
    
    @property
    def runtime(self) -> float:
        """Total runtime in seconds"""
        return time.time() - self.start_time
    
    @property
    def overall_qps(self) -> float:
        """Overall queries per second"""
        if self.runtime > 0:
            return self.total_queries / self.runtime
        return 0.0
    
    @property
    def average_efficiency(self) -> float:
        """Average thread efficiency"""
        if self.thread_stats:
            efficiencies = [ts.efficiency for ts in self.thread_stats.values()]
            return sum(efficiencies) / len(efficiencies)
        return 0.0


class QueryBatch:
    """Represents a batch of queries to generate"""
    
    def __init__(self, batch_id: int, grammar_name: str, count: int, 
                 params: Optional[Dict[str, Any]] = None):
        self.batch_id = batch_id
        self.grammar_name = grammar_name
        self.count = count
        self.params = params or {}
        self.created_at = time.time()
        self.queries: List[str] = []
        self.completed = False
        self.error: Optional[Exception] = None


class WorkerThread(threading.Thread):
    """Worker thread for query generation"""
    
    def __init__(self, worker_id: int, input_queue: queue.Queue, 
                 output_queue: queue.Queue, generator_factory: Callable,
                 stats: ThreadStats, stop_event: threading.Event):
        super().__init__(name=f"QueryWorker-{worker_id}")
        self.worker_id = worker_id
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.generator_factory = generator_factory
        self.stats = stats
        self.stop_event = stop_event
        self.daemon = True
        
    def run(self):
        """Main worker loop"""
        generator = self.generator_factory(self.worker_id)
        
        while not self.stop_event.is_set():
            try:
                # Get batch with timeout
                start_wait = time.time()
                batch = self.input_queue.get(timeout=1.0)
                queue_time = time.time() - start_wait
                
                if batch is None:  # Poison pill
                    break
                    
                # Update stats
                self.stats.queue_time += queue_time
                self.stats.last_activity = time.time()
                
                # Generate queries
                start_gen = time.time()
                self._process_batch(batch, generator)
                gen_time = time.time() - start_gen
                
                # Update stats
                self.stats.generation_time += gen_time
                self.stats.queries_generated += batch.count
                self.stats.batches_completed += 1
                
                # Put completed batch in output queue
                self.output_queue.put(batch)
                
            except queue.Empty:
                # No work available
                self.stats.idle_time += 1.0
            except Exception as e:
                self.stats.errors += 1
                if batch:
                    batch.error = e
                    batch.completed = True
                    self.output_queue.put(batch)
                    
        self.stats.total_time = time.time() - self.stats.last_activity
    
    def _process_batch(self, batch: QueryBatch, generator):
        """Process a single batch of queries"""
        queries = []
        
        for i in range(batch.count):
            query = generator.generate(batch.grammar_name, **batch.params)
            queries.append(query)
            
        batch.queries = queries
        batch.completed = True


class ThreadPoolManager:
    """
    Manages a pool of worker threads for query generation.
    
    Features:
    - Configurable thread count and queue sizes
    - Backpressure handling
    - Real-time statistics
    - CPU affinity optimization
    - Graceful shutdown
    """
    
    def __init__(self, config: ThreadingConfig, generator_factory: Callable):
        self.config = config
        self.generator_factory = generator_factory
        
        # Determine thread count
        self.num_threads = config.num_threads or multiprocessing.cpu_count()
        
        # Queues
        self.input_queue = queue.Queue(maxsize=config.queue_size)
        self.output_queue = queue.Queue(maxsize=config.queue_size)
        
        # Thread management
        self.workers: List[WorkerThread] = []
        self.stop_event = threading.Event()
        
        # Statistics
        self.stats = ThreadPoolStats()
        self.batch_counter = 0
        self._stats_lock = threading.Lock()
        
        # Monitoring
        self.monitor_thread = None
        self._start_monitoring()
        
        # Start workers
        self._start_workers()
    
    def _start_workers(self):
        """Start worker threads"""
        for i in range(self.num_threads):
            stats = ThreadStats(thread_id=i)
            self.stats.thread_stats[i] = stats
            
            worker = WorkerThread(
                worker_id=i,
                input_queue=self.input_queue,
                output_queue=self.output_queue,
                generator_factory=self.generator_factory,
                stats=stats,
                stop_event=self.stop_event
            )
            
            worker.start()
            self.workers.append(worker)
            
            # Set CPU affinity if enabled and supported
            if self.config.enable_affinity and hasattr(os, 'sched_setaffinity'):
                try:
                    # Pin thread to CPU core
                    cpu = i % multiprocessing.cpu_count()
                    os.sched_setaffinity(worker.ident, {cpu})
                except Exception as e:
                    # Log but don't fail - affinity is optimization only
                    import logging
                    logging.debug(f"Failed to set CPU affinity for worker {i}: {e}")
                    
        self.stats.active_threads = len(self.workers)
    
    def _start_monitoring(self):
        """Start monitoring thread"""
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            name="ThreadPoolMonitor",
            daemon=True
        )
        self.monitor_thread.start()
    
    def _monitor_loop(self):
        """Monitor thread loop"""
        while not self.stop_event.is_set():
            time.sleep(self.config.monitor_interval)
            self._update_stats()
    
    def _update_stats(self):
        """Update aggregate statistics"""
        with self._stats_lock:
            total_queries = 0
            total_batches = 0
            total_errors = 0
            
            for thread_stats in self.stats.thread_stats.values():
                total_queries += thread_stats.queries_generated
                total_batches += thread_stats.batches_completed
                total_errors += thread_stats.errors
                
            self.stats.total_queries = total_queries
            self.stats.total_batches = total_batches
            self.stats.total_errors = total_errors
            self.stats.queue_size = self.input_queue.qsize()
    
    def submit_batch(self, grammar_name: str, count: int, 
                    params: Optional[Dict[str, Any]] = None) -> Future[QueryBatch]:
        """Submit a batch of queries for generation"""
        # Check backpressure
        queue_usage = self.input_queue.qsize() / self.config.queue_size
        if queue_usage > self.config.backpressure_threshold:
            # Apply backpressure - wait a bit
            wait_time = 0.1 * (queue_usage - self.config.backpressure_threshold)
            time.sleep(wait_time)
        
        # Create batch
        batch_id = self.batch_counter
        self.batch_counter += 1
        batch = QueryBatch(batch_id, grammar_name, count, params)
        
        # Submit to queue
        future = Future()
        
        def submit():
            try:
                self.input_queue.put(batch)
                # Wait for completion with exponential backoff
                wait_time = 0.001  # Start with 1ms
                max_wait = 0.1     # Max 100ms
                while not batch.completed:
                    time.sleep(wait_time)
                    wait_time = min(wait_time * 1.5, max_wait)  # Exponential backoff
                    if self.stop_event.is_set():
                        future.cancel()
                        return
                        
                if batch.error:
                    future.set_exception(batch.error)
                else:
                    future.set_result(batch)
            except Exception as e:
                future.set_exception(e)
                
        # Submit in background
        threading.Thread(target=submit, daemon=True).start()
        
        return future
    
    def submit_batches(self, grammar_name: str, total_count: int,
                      params: Optional[Dict[str, Any]] = None) -> List[Future[QueryBatch]]:
        """Submit multiple batches to generate total_count queries"""
        futures = []
        remaining = total_count
        
        while remaining > 0:
            batch_size = min(remaining, self.config.batch_size)
            future = self.submit_batch(grammar_name, batch_size, params)
            futures.append(future)
            remaining -= batch_size
            
        return futures
    
    def get_completed_batches(self, timeout: float = None) -> List[QueryBatch]:
        """Get completed batches from output queue"""
        batches = []
        deadline = time.time() + timeout if timeout else None
        
        while True:
            try:
                remaining = deadline - time.time() if deadline else None
                if remaining is not None and remaining <= 0:
                    break
                    
                batch = self.output_queue.get(timeout=remaining or 0.1)
                batches.append(batch)
            except queue.Empty:
                break
                
        return batches
    
    def get_statistics(self) -> ThreadPoolStats:
        """Get current statistics"""
        with self._stats_lock:
            # Create a copy to avoid race conditions
            import copy
            return copy.deepcopy(self.stats)
    
    def shutdown(self, wait: bool = True, timeout: float = None):
        """Shutdown thread pool"""
        # Signal stop
        self.stop_event.set()
        
        # Send poison pills
        for _ in range(self.num_threads):
            self.input_queue.put(None)
            
        if wait:
            # Wait for workers to finish
            deadline = time.time() + timeout if timeout else None
            for worker in self.workers:
                remaining = deadline - time.time() if deadline else None
                if remaining is not None and remaining <= 0:
                    break
                worker.join(timeout=remaining)
                
        # Stop monitoring
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1.0)
    
    def __enter__(self):
        """Context manager support"""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.shutdown(wait=True, timeout=30.0)