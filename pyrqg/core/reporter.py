"""
Reporters - Output test results in various formats
"""

import json
import logging
from typing import List, Dict, Any, Optional, Type, TextIO
from abc import ABC, abstractmethod
from datetime import datetime

from .result import Result, Status


logger = logging.getLogger(__name__)


class Reporter(ABC):
    """Base reporter class"""
    
    @abstractmethod
    def start(self, config: Any) -> None:
        """Called when test starts"""
        pass
    
    @abstractmethod
    def report(self, result: Result, issues: List[str]) -> None:
        """Report a single test result"""
        pass
    
    @abstractmethod
    def finish(self, stats: Any) -> None:
        """Called when test finishes"""
        pass


class ReporterRegistry:
    """Registry for reporters"""
    _reporters: Dict[str, Type[Reporter]] = {}
    
    @classmethod
    def register(cls, name: str, reporter_class: Type[Reporter]) -> None:
        """Register a reporter"""
        cls._reporters[name] = reporter_class
    
    @classmethod
    def get(cls, name: str) -> Optional[Type[Reporter]]:
        """Get reporter class by name"""
        return cls._reporters.get(name)
    
    @classmethod
    def list(cls) -> List[str]:
        """List available reporters"""
        return list(cls._reporters.keys())


# ============================================================================
# Built-in Reporters
# ============================================================================

class ConsoleReporter(Reporter):
    """Reports to console/stdout"""
    
    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.error_count = 0
        self.issue_count = 0
    
    def start(self, config: Any) -> None:
        """Print test start information"""
        print(f"\n{'='*60}")
        print("PyRQG Test Starting")
        print(f"{'='*60}")
        print(f"Grammar: {config.grammar_file}")
        print(f"Duration: {config.duration}s")
        print(f"Database: {config.database}")
        print(f"Validators: {', '.join(config.validators)}")
        print(f"{'='*60}\n")
    
    def report(self, result: Result, issues: List[str]) -> None:
        """Report result to console"""
        if result.status != Status.OK:
            self.error_count += 1
            print(f"\n[ERROR] Query failed with status: {result.status.name}")
            print(f"Query: {result.query[:100]}..." if len(result.query) > 100 else f"Query: {result.query}")
            if result.errstr:
                print(f"Error: {result.errstr}")
        
        if issues:
            self.issue_count += len(issues)
            print(f"\n[ISSUE] Validator found {len(issues)} issue(s):")
            for issue in issues:
                print(f"  - {issue}")
            if self.verbose:
                print(f"Query: {result.query}")
        
        elif self.verbose and result.status == Status.OK:
            print(f"[OK] {result.query[:80]}...")
    
    def finish(self, stats: Any) -> None:
        """Print test summary"""
        print(f"\n{'='*60}")
        print("Test Summary")
        print(f"{'='*60}")
        print(f"Duration: {stats.duration:.2f}s")
        print(f"Queries Generated: {stats.queries_generated}")
        print(f"Queries Executed: {stats.queries_executed}")
        print(f"Queries Failed: {stats.queries_failed}")
        print(f"Validation Issues: {self.issue_count}")
        print(f"Success Rate: {stats.success_rate:.2f}%")
        print(f"Queries/Second: {stats.queries_per_second:.2f}")
        print(f"{'='*60}\n")


class FileReporter(Reporter):
    """Reports to a file"""
    
    def __init__(self, filename: str = "pyrqg_report.txt"):
        self.filename = filename
        self.file: Optional[TextIO] = None
        self.error_count = 0
        self.issue_count = 0
    
    def start(self, config: Any) -> None:
        """Open file and write header"""
        self.file = open(self.filename, 'w')
        self.file.write(f"PyRQG Test Report\n")
        self.file.write(f"Generated: {datetime.now().isoformat()}\n")
        self.file.write(f"Grammar: {config.grammar_file}\n")
        self.file.write(f"Database: {config.database}\n")
        self.file.write(f"{'-'*60}\n\n")
    
    def report(self, result: Result, issues: List[str]) -> None:
        """Write result to file"""
        if not self.file:
            return
        
        if result.status != Status.OK or issues:
            self.file.write(f"Timestamp: {datetime.now().isoformat()}\n")
            self.file.write(f"Query: {result.query}\n")
            self.file.write(f"Status: {result.status.name}\n")
            
            if result.errstr:
                self.file.write(f"Error: {result.errstr}\n")
                self.error_count += 1
            
            if issues:
                self.file.write(f"Issues: {', '.join(issues)}\n")
                self.issue_count += len(issues)
            
            self.file.write(f"{'-'*40}\n\n")
            self.file.flush()
    
    def finish(self, stats: Any) -> None:
        """Write summary and close file"""
        if not self.file:
            return
        
        self.file.write(f"\nTest Summary\n")
        self.file.write(f"{'-'*60}\n")
        self.file.write(f"Duration: {stats.duration:.2f}s\n")
        self.file.write(f"Total Queries: {stats.queries_executed}\n")
        self.file.write(f"Failed Queries: {stats.queries_failed}\n")
        self.file.write(f"Validation Issues: {self.issue_count}\n")
        self.file.write(f"Success Rate: {stats.success_rate:.2f}%\n")
        self.file.write(f"QPS: {stats.queries_per_second:.2f}\n")
        
        self.file.close()
        print(f"Report written to: {self.filename}")


class JSONReporter(Reporter):
    """Reports in JSON format"""
    
    def __init__(self, filename: str = "pyrqg_report.json"):
        self.filename = filename
        self.results = []
        self.config_data = {}
    
    def start(self, config: Any) -> None:
        """Store config for JSON output"""
        self.config_data = {
            'start_time': datetime.now().isoformat(),
            'grammar': config.grammar_file,
            'database': config.database,
            'duration': config.duration,
            'validators': config.validators
        }
    
    def report(self, result: Result, issues: List[str]) -> None:
        """Collect result for JSON output"""
        if result.status != Status.OK or issues:
            self.results.append({
                'timestamp': datetime.now().isoformat(),
                'query': result.query,
                'status': result.status.name,
                'duration': result.duration,
                'error': result.errstr,
                'issues': issues
            })
    
    def finish(self, stats: Any) -> None:
        """Write JSON report"""
        report = {
            'config': self.config_data,
            'summary': {
                'end_time': datetime.now().isoformat(),
                'duration': stats.duration,
                'queries_generated': stats.queries_generated,
                'queries_executed': stats.queries_executed,
                'queries_failed': stats.queries_failed,
                'success_rate': stats.success_rate,
                'qps': stats.queries_per_second
            },
            'results': self.results
        }
        
        with open(self.filename, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"JSON report written to: {self.filename}")


class ErrorOnlyReporter(Reporter):
    """Only reports errors and issues"""
    
    def __init__(self):
        self.errors = []
        self.issues = []
    
    def start(self, config: Any) -> None:
        """Nothing to do on start"""
        pass
    
    def report(self, result: Result, issues: List[str]) -> None:
        """Collect only errors and issues"""
        if result.status != Status.OK:
            self.errors.append({
                'query': result.query,
                'status': result.status.name,
                'error': result.errstr
            })
        
        if issues:
            self.issues.append({
                'query': result.query,
                'issues': issues
            })
    
    def finish(self, stats: Any) -> None:
        """Report only if there were errors"""
        if self.errors or self.issues:
            print(f"\n{'='*60}")
            print(f"ERRORS AND ISSUES FOUND")
            print(f"{'='*60}")
            
            if self.errors:
                print(f"\nQuery Errors ({len(self.errors)}):")
                for i, error in enumerate(self.errors[:10]):  # Show first 10
                    print(f"\n{i+1}. {error['status']}")
                    print(f"   Query: {error['query'][:100]}...")
                    if error['error']:
                        print(f"   Error: {error['error']}")
                
                if len(self.errors) > 10:
                    print(f"\n... and {len(self.errors) - 10} more errors")
            
            if self.issues:
                print(f"\nValidation Issues ({len(self.issues)}):")
                for i, issue in enumerate(self.issues[:10]):  # Show first 10
                    print(f"\n{i+1}. Issues: {', '.join(issue['issues'])}")
                    print(f"   Query: {issue['query'][:100]}...")
                
                if len(self.issues) > 10:
                    print(f"\n... and {len(self.issues) - 10} more issues")
        else:
            print("\n✓ No errors or issues found!")


# ============================================================================
# Register built-in reporters
# ============================================================================

ReporterRegistry.register('console', ConsoleReporter)
ReporterRegistry.register('file', FileReporter)
ReporterRegistry.register('json', JSONReporter)
ReporterRegistry.register('errors', ErrorOnlyReporter)