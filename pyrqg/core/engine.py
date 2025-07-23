"""
PyRQG Engine - Main execution engine for random query generation and testing
"""

import time
import logging
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from pathlib import Path
import signal
import sys

from .executor import Executor, create_executor
from .result import Result, Status
from .validator import Validator, ValidatorRegistry
from .reporter import Reporter, ReporterRegistry
from ..dsl.core import Grammar


logger = logging.getLogger(__name__)


@dataclass
class EngineConfig:
    """Engine configuration"""
    duration: int = 300  # seconds
    queries: Optional[int] = None  # number of queries (overrides duration)
    threads: int = 1
    seed: Optional[int] = None
    dsn: Optional[str] = None
    database: str = 'postgres'
    validators: List[str] = field(default_factory=list)
    reporters: List[str] = field(default_factory=list)
    grammar_file: Optional[str] = None
    debug: bool = False


@dataclass
class EngineStats:
    """Engine execution statistics"""
    queries_generated: int = 0
    queries_executed: int = 0
    queries_failed: int = 0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    
    @property
    def duration(self) -> float:
        end = self.end_time or time.time()
        return end - self.start_time
    
    @property
    def queries_per_second(self) -> float:
        if self.duration == 0:
            return 0
        return self.queries_executed / self.duration
    
    @property
    def success_rate(self) -> float:
        if self.queries_executed == 0:
            return 0
        return (self.queries_executed - self.queries_failed) / self.queries_executed * 100


class Engine:
    """Main PyRQG execution engine"""
    
    def __init__(self, config: EngineConfig):
        self.config = config
        self.grammar: Optional[Grammar] = None
        self.executor: Optional[Executor] = None
        self.validators: List[Validator] = []
        self.reporters: List[Reporter] = []
        self.stats = EngineStats()
        self._running = False
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        # Configure logging
        if config.debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, shutting down...")
        self._running = False
    
    def load_grammar(self, grammar_path: str) -> None:
        """Load grammar from file"""
        logger.info(f"Loading grammar from {grammar_path}")
        
        # Import the grammar module
        import importlib.util
        spec = importlib.util.spec_from_file_location("grammar", grammar_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Find the Grammar instance
        for name in dir(module):
            obj = getattr(module, name)
            if isinstance(obj, Grammar):
                self.grammar = obj
                logger.info(f"Loaded grammar: {obj.name}")
                return
        
        raise ValueError(f"No Grammar instance found in {grammar_path}")
    
    def initialize(self) -> None:
        """Initialize engine components"""
        logger.info("Initializing PyRQG engine")
        
        # Load grammar if specified
        if self.config.grammar_file:
            self.load_grammar(self.config.grammar_file)
        
        # Create executor
        if self.config.dsn:
            self.executor = create_executor(self.config.dsn)
            self.executor.connect()
        
        # Load validators
        for validator_name in self.config.validators:
            validator_class = ValidatorRegistry.get(validator_name)
            if validator_class:
                validator = validator_class()
                self.validators.append(validator)
                logger.info(f"Loaded validator: {validator_name}")
        
        # Load reporters
        for reporter_name in self.config.reporters:
            reporter_class = ReporterRegistry.get(reporter_name)
            if reporter_class:
                reporter = reporter_class()
                self.reporters.append(reporter)
                logger.info(f"Loaded reporter: {reporter_name}")
    
    def generate_query(self) -> str:
        """Generate a random query"""
        if not self.grammar:
            raise ValueError("No grammar loaded")
        
        query = self.grammar.generate("query", seed=self.config.seed)
        self.stats.queries_generated += 1
        return query
    
    def execute_query(self, query: str) -> Result:
        """Execute a query and return result"""
        if not self.executor:
            # Dry run mode - just validate syntax
            return Result(
                query=query,
                status=Status.OK,
                affected_rows=0
            )
        
        result = self.executor.execute(query)
        self.stats.queries_executed += 1
        
        if result.status != Status.OK:
            self.stats.queries_failed += 1
        
        return result
    
    def validate_result(self, result: Result) -> List[str]:
        """Run validators on result"""
        issues = []
        
        for validator in self.validators:
            validator_issues = validator.validate(result)
            if validator_issues:
                issues.extend(validator_issues)
                logger.warning(f"Validator {validator.__class__.__name__} found issues: {validator_issues}")
        
        return issues
    
    def report_result(self, result: Result, issues: List[str]) -> None:
        """Report result to all reporters"""
        for reporter in self.reporters:
            reporter.report(result, issues)
    
    def run_single_iteration(self) -> bool:
        """Run a single test iteration"""
        try:
            # Generate query
            query = self.generate_query()
            logger.debug(f"Generated query: {query}")
            
            # Execute query
            result = self.execute_query(query)
            logger.debug(f"Execution result: {result.status}")
            
            # Validate result
            issues = self.validate_result(result)
            
            # Report result
            self.report_result(result, issues)
            
            # Check for critical issues
            if result.status == Status.SERVER_CRASHED:
                logger.error("Server crashed! Stopping execution.")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error in iteration: {e}", exc_info=True)
            return False
    
    def run(self) -> EngineStats:
        """Run the main test loop"""
        logger.info("Starting PyRQG engine")
        self._running = True
        self.stats.start_time = time.time()
        
        try:
            # Initialize components
            self.initialize()
            
            # Notify reporters of start
            for reporter in self.reporters:
                reporter.start(self.config)
            
            # Main loop
            if self.config.queries:
                # Run specific number of queries
                for i in range(self.config.queries):
                    if not self._running:
                        break
                    
                    if not self.run_single_iteration():
                        break
                    
                    if (i + 1) % 100 == 0:
                        logger.info(f"Executed {i + 1} queries, QPS: {self.stats.queries_per_second:.2f}")
            else:
                # Run for duration
                end_time = time.time() + self.config.duration
                
                while self._running and time.time() < end_time:
                    if not self.run_single_iteration():
                        break
                    
                    # Log progress every 10 seconds
                    if int(time.time()) % 10 == 0:
                        logger.info(f"Progress: {self.stats.queries_executed} queries, "
                                  f"QPS: {self.stats.queries_per_second:.2f}")
            
        finally:
            self.stats.end_time = time.time()
            
            # Cleanup
            if self.executor:
                self.executor.close()
            
            # Final report
            for reporter in self.reporters:
                reporter.finish(self.stats)
            
            # Log summary
            logger.info(f"PyRQG execution completed:")
            logger.info(f"  Duration: {self.stats.duration:.2f} seconds")
            logger.info(f"  Queries generated: {self.stats.queries_generated}")
            logger.info(f"  Queries executed: {self.stats.queries_executed}")
            logger.info(f"  Queries failed: {self.stats.queries_failed}")
            logger.info(f"  Success rate: {self.stats.success_rate:.2f}%")
            logger.info(f"  QPS: {self.stats.queries_per_second:.2f}")
        
        return self.stats


def run_engine(config: EngineConfig) -> EngineStats:
    """Convenience function to run engine"""
    engine = Engine(config)
    return engine.run()