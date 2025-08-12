#!/usr/bin/env python3
"""
cicd_integration.py - PyRQG Integration with CI/CD Pipelines

This example demonstrates how to integrate PyRQG into CI/CD workflows:
- Database migration testing
- Schema compatibility checks
- Performance regression testing
- Query validation in CI
- Automated database testing

Can be used with GitHub Actions, GitLab CI, Jenkins, etc.
"""

import sys
import os
import json
import argparse
import psycopg2
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.api import RQG
from pyrqg.dsl.core import Grammar, choice, template, number


@dataclass
class TestResult:
    """Result of a test execution."""
    test_name: str
    passed: bool
    duration: float
    message: str
    details: Optional[Dict] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class TestSuite:
    """Collection of test results."""
    suite_name: str
    results: List[TestResult]
    start_time: datetime
    end_time: Optional[datetime] = None
    
    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)
    
    @property
    def total_tests(self) -> int:
        return len(self.results)
    
    @property
    def passed_tests(self) -> int:
        return sum(1 for r in self.results if r.passed)
    
    @property
    def failed_tests(self) -> int:
        return sum(1 for r in self.results if not r.passed)
    
    @property
    def duration(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    def to_junit_xml(self) -> str:
        """Convert to JUnit XML format for CI integration."""
        xml_parts = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            f'<testsuite name="{self.suite_name}" tests="{self.total_tests}" '
            f'failures="{self.failed_tests}" time="{self.duration:.3f}">'
        ]
        
        for result in self.results:
            xml_parts.append(f'  <testcase name="{result.test_name}" time="{result.duration:.3f}">')
            if not result.passed:
                xml_parts.append(f'    <failure message="{result.message}">')
                if result.details:
                    xml_parts.append(f'      {json.dumps(result.details)}')
                xml_parts.append('    </failure>')
            xml_parts.append('  </testcase>')
        
        xml_parts.append('</testsuite>')
        return '\n'.join(xml_parts)


class DatabaseTester:
    """Base class for database testing."""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.rqg = RQG()
    
    def get_connection(self):
        """Get database connection."""
        return psycopg2.connect(self.connection_string)
    
    def run_test(self, test_name: str, test_func) -> TestResult:
        """Run a single test and capture result."""
        start_time = time.time()
        
        try:
            result = test_func()
            duration = time.time() - start_time
            
            if isinstance(result, bool):
                return TestResult(
                    test_name=test_name,
                    passed=result,
                    duration=duration,
                    message="Test passed" if result else "Test failed"
                )
            elif isinstance(result, TestResult):
                result.duration = duration
                return result
            else:
                return TestResult(
                    test_name=test_name,
                    passed=True,
                    duration=duration,
                    message=str(result)
                )
                
        except Exception as e:
            duration = time.time() - start_time
            return TestResult(
                test_name=test_name,
                passed=False,
                duration=duration,
                message=f"Test failed with exception: {e}",
                details={"exception": str(e), "type": type(e).__name__}
            )


class SchemaMigrationTester(DatabaseTester):
    """Test schema migrations with generated queries."""
    
    def test_backward_compatibility(self) -> TestResult:
        """Test if old queries still work with new schema."""
        conn = self.get_connection()
        cur = conn.cursor()
        
        # Define queries that should work across schema versions
        compatibility_queries = [
            "SELECT id, name FROM users LIMIT 1",
            "SELECT COUNT(*) FROM products",
            "SELECT * FROM orders WHERE created_at > CURRENT_DATE - INTERVAL '7 days'"
        ]
        
        failed_queries = []
        
        for query in compatibility_queries:
            try:
                cur.execute(query)
                cur.fetchall()
            except Exception as e:
                failed_queries.append({
                    "query": query,
                    "error": str(e)
                })
        
        conn.close()
        
        if failed_queries:
            return TestResult(
                test_name="backward_compatibility",
                passed=False,
                duration=0,
                message=f"{len(failed_queries)} queries failed",
                details={"failed_queries": failed_queries}
            )
        
        return TestResult(
            test_name="backward_compatibility",
            passed=True,
            duration=0,
            message="All compatibility queries passed"
        )
    
    def test_new_features(self) -> TestResult:
        """Test new schema features work correctly."""
        conn = self.get_connection()
        cur = conn.cursor()
        
        # Test new features (example: new columns, indexes, etc.)
        new_feature_tests = []
        
        # Check if new column exists
        try:
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'users' 
                AND column_name = 'last_login'
            """)
            if cur.fetchone():
                new_feature_tests.append(("New column 'last_login'", True))
            else:
                new_feature_tests.append(("New column 'last_login'", False))
        except:
            new_feature_tests.append(("New column 'last_login'", False))
        
        conn.close()
        
        passed = all(result for _, result in new_feature_tests)
        
        return TestResult(
            test_name="new_features",
            passed=passed,
            duration=0,
            message=f"{sum(1 for _, r in new_feature_tests if r)}/{len(new_feature_tests)} features verified",
            details={"features": new_feature_tests}
        )
    
    def test_data_integrity(self) -> TestResult:
        """Test data integrity after migration."""
        conn = self.get_connection()
        cur = conn.cursor()
        
        integrity_checks = []
        
        # Check foreign key constraints
        cur.execute("""
            SELECT COUNT(*) 
            FROM orders o 
            LEFT JOIN users u ON o.user_id = u.id 
            WHERE u.id IS NULL AND o.user_id IS NOT NULL
        """)
        orphaned_orders = cur.fetchone()[0]
        integrity_checks.append({
            "check": "No orphaned orders",
            "passed": orphaned_orders == 0,
            "details": f"{orphaned_orders} orphaned orders found"
        })
        
        # Check data constraints
        cur.execute("SELECT COUNT(*) FROM products WHERE price < 0")
        negative_prices = cur.fetchone()[0]
        integrity_checks.append({
            "check": "No negative prices",
            "passed": negative_prices == 0,
            "details": f"{negative_prices} products with negative price"
        })
        
        conn.close()
        
        all_passed = all(check["passed"] for check in integrity_checks)
        
        return TestResult(
            test_name="data_integrity",
            passed=all_passed,
            duration=0,
            message=f"{sum(1 for c in integrity_checks if c['passed'])}/{len(integrity_checks)} integrity checks passed",
            details={"checks": integrity_checks}
        )


class PerformanceRegressionTester(DatabaseTester):
    """Test for performance regressions."""
    
    def __init__(self, connection_string: str, baseline_file: Optional[str] = None):
        super().__init__(connection_string)
        self.baseline_file = baseline_file
        self.baseline_data = self._load_baseline()
    
    def _load_baseline(self) -> Dict:
        """Load baseline performance data."""
        if self.baseline_file and os.path.exists(self.baseline_file):
            with open(self.baseline_file, 'r') as f:
                return json.load(f)
        return {}
    
    def test_query_performance(self) -> TestResult:
        """Test query performance against baseline."""
        conn = self.get_connection()
        cur = conn.cursor()
        
        # Define benchmark queries
        benchmark_queries = [
            ("simple_select", "SELECT * FROM users LIMIT 100"),
            ("aggregate", "SELECT COUNT(*) FROM orders"),
            ("join", "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name"),
            ("complex", """
                WITH recent_orders AS (
                    SELECT * FROM orders WHERE created_at > CURRENT_DATE - INTERVAL '30 days'
                )
                SELECT COUNT(*) FROM recent_orders
            """)
        ]
        
        results = {}
        regressions = []
        
        for query_name, query in benchmark_queries:
            # Warm up
            cur.execute(query)
            cur.fetchall()
            
            # Measure
            times = []
            for _ in range(5):
                start = time.time()
                cur.execute(query)
                cur.fetchall()
                times.append(time.time() - start)
            
            avg_time = sum(times) / len(times)
            results[query_name] = avg_time
            
            # Compare with baseline
            if query_name in self.baseline_data:
                baseline_time = self.baseline_data[query_name]
                regression_threshold = 1.2  # 20% slower is regression
                
                if avg_time > baseline_time * regression_threshold:
                    regressions.append({
                        "query": query_name,
                        "baseline": baseline_time,
                        "current": avg_time,
                        "regression": (avg_time / baseline_time - 1) * 100
                    })
        
        conn.close()
        
        # Save current results as new baseline
        if self.baseline_file:
            with open(self.baseline_file, 'w') as f:
                json.dump(results, f, indent=2)
        
        if regressions:
            return TestResult(
                test_name="query_performance",
                passed=False,
                duration=0,
                message=f"{len(regressions)} queries showed performance regression",
                details={"regressions": regressions, "all_results": results}
            )
        
        return TestResult(
            test_name="query_performance",
            passed=True,
            duration=0,
            message="No performance regressions detected",
            details={"results": results}
        )
    
    def test_concurrent_load(self) -> TestResult:
        """Test performance under concurrent load."""
        import threading
        import queue
        
        results_queue = queue.Queue()
        errors = []
        
        def worker(thread_id: int, num_queries: int):
            conn = self.get_connection()
            cur = conn.cursor()
            
            try:
                for i in range(num_queries):
                    query = f"SELECT COUNT(*) FROM users WHERE id > {thread_id * 1000 + i}"
                    start = time.time()
                    cur.execute(query)
                    cur.fetchone()
                    duration = time.time() - start
                    results_queue.put(duration)
            except Exception as e:
                errors.append(str(e))
            finally:
                conn.close()
        
        # Run concurrent test
        threads = []
        num_threads = 5
        queries_per_thread = 20
        
        start_time = time.time()
        
        for i in range(num_threads):
            t = threading.Thread(target=worker, args=(i, queries_per_thread))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        total_time = time.time() - start_time
        
        # Collect results
        query_times = []
        while not results_queue.empty():
            query_times.append(results_queue.get())
        
        if errors:
            return TestResult(
                test_name="concurrent_load",
                passed=False,
                duration=total_time,
                message=f"Concurrent test failed with {len(errors)} errors",
                details={"errors": errors}
            )
        
        avg_query_time = sum(query_times) / len(query_times) if query_times else 0
        queries_per_second = len(query_times) / total_time if total_time > 0 else 0
        
        # Check if performance is acceptable
        if avg_query_time > 0.1:  # 100ms threshold
            return TestResult(
                test_name="concurrent_load",
                passed=False,
                duration=total_time,
                message=f"Average query time {avg_query_time:.3f}s exceeds threshold",
                details={
                    "avg_query_time": avg_query_time,
                    "queries_per_second": queries_per_second,
                    "total_queries": len(query_times)
                }
            )
        
        return TestResult(
            test_name="concurrent_load",
            passed=True,
            duration=total_time,
            message=f"Concurrent load test passed: {queries_per_second:.1f} QPS",
            details={
                "avg_query_time": avg_query_time,
                "queries_per_second": queries_per_second,
                "total_queries": len(query_times)
            }
        )


class QueryValidationTester(DatabaseTester):
    """Validate generated queries work correctly."""
    
    def test_generated_queries(self) -> TestResult:
        """Test that generated queries execute successfully."""
        conn = self.get_connection()
        cur = conn.cursor()
        
        # Generate test queries
        grammar_tests = [
            ("dml_basic", 50),
            ("complex_queries", 20)
        ]
        
        total_queries = 0
        failed_queries = []
        
        for grammar_name, count in grammar_tests:
            for i in range(count):
                query = self.rqg.generate_query(grammar_name, seed=i)
                total_queries += 1
                
                try:
                    cur.execute(query)
                    
                    # Rollback write operations
                    if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                        conn.rollback()
                    else:
                        cur.fetchall()
                        
                except Exception as e:
                    failed_queries.append({
                        "grammar": grammar_name,
                        "query": query[:100] + "..." if len(query) > 100 else query,
                        "error": str(e)
                    })
        
        conn.close()
        
        success_rate = (total_queries - len(failed_queries)) / total_queries if total_queries > 0 else 0
        
        if success_rate < 0.95:  # 95% success threshold
            return TestResult(
                test_name="generated_queries",
                passed=False,
                duration=0,
                message=f"Query success rate {success_rate:.1%} below threshold",
                details={
                    "total_queries": total_queries,
                    "failed_queries": len(failed_queries),
                    "failures": failed_queries[:10]  # First 10 failures
                }
            )
        
        return TestResult(
            test_name="generated_queries",
            passed=True,
            duration=0,
            message=f"Query validation passed: {success_rate:.1%} success rate",
            details={
                "total_queries": total_queries,
                "failed_queries": len(failed_queries)
            }
        )


def run_ci_tests(args) -> int:
    """Run CI test suite."""
    suite = TestSuite(
        suite_name="PyRQG CI Tests",
        results=[],
        start_time=datetime.now()
    )
    
    # Schema migration tests
    if args.test_migrations:
        print("Running schema migration tests...")
        migration_tester = SchemaMigrationTester(args.database)
        
        suite.results.append(
            migration_tester.run_test(
                "backward_compatibility",
                migration_tester.test_backward_compatibility
            )
        )
        suite.results.append(
            migration_tester.run_test(
                "new_features",
                migration_tester.test_new_features
            )
        )
        suite.results.append(
            migration_tester.run_test(
                "data_integrity",
                migration_tester.test_data_integrity
            )
        )
    
    # Performance tests
    if args.test_performance:
        print("Running performance regression tests...")
        perf_tester = PerformanceRegressionTester(
            args.database,
            args.baseline_file
        )
        
        suite.results.append(
            perf_tester.run_test(
                "query_performance",
                perf_tester.test_query_performance
            )
        )
        suite.results.append(
            perf_tester.run_test(
                "concurrent_load",
                perf_tester.test_concurrent_load
            )
        )
    
    # Query validation tests
    if args.test_queries:
        print("Running query validation tests...")
        query_tester = QueryValidationTester(args.database)
        
        suite.results.append(
            query_tester.run_test(
                "generated_queries",
                query_tester.test_generated_queries
            )
        )
    
    suite.end_time = datetime.now()
    
    # Output results
    print("\n" + "=" * 60)
    print(f"Test Suite: {suite.suite_name}")
    print(f"Duration: {suite.duration:.2f}s")
    print(f"Tests: {suite.total_tests} | Passed: {suite.passed_tests} | Failed: {suite.failed_tests}")
    print("=" * 60)
    
    for result in suite.results:
        status = "✓" if result.passed else "✗"
        print(f"{status} {result.test_name}: {result.message}")
        if not result.passed and result.details:
            print(f"  Details: {json.dumps(result.details, indent=2)}")
    
    # Save JUnit XML for CI
    if args.junit_xml:
        with open(args.junit_xml, 'w') as f:
            f.write(suite.to_junit_xml())
        print(f"\nJUnit XML saved to: {args.junit_xml}")
    
    # Save JSON report
    if args.json_report:
        report = {
            "suite_name": suite.suite_name,
            "start_time": suite.start_time.isoformat(),
            "end_time": suite.end_time.isoformat(),
            "duration": suite.duration,
            "total_tests": suite.total_tests,
            "passed_tests": suite.passed_tests,
            "failed_tests": suite.failed_tests,
            "results": [
                {
                    "test_name": r.test_name,
                    "passed": r.passed,
                    "duration": r.duration,
                    "message": r.message,
                    "details": r.details,
                    "timestamp": r.timestamp.isoformat()
                }
                for r in suite.results
            ]
        }
        
        with open(args.json_report, 'w') as f:
            json.dump(report, f, indent=2)
        print(f"JSON report saved to: {args.json_report}")
    
    # Exit code for CI
    return 0 if suite.passed else 1


def main():
    """Main entry point for CI integration."""
    parser = argparse.ArgumentParser(
        description="PyRQG CI/CD Integration Tests"
    )
    
    parser.add_argument(
        '--database',
        required=True,
        help='PostgreSQL connection string'
    )
    
    parser.add_argument(
        '--test-migrations',
        action='store_true',
        help='Run schema migration tests'
    )
    
    parser.add_argument(
        '--test-performance',
        action='store_true',
        help='Run performance regression tests'
    )
    
    parser.add_argument(
        '--test-queries',
        action='store_true',
        help='Run query validation tests'
    )
    
    parser.add_argument(
        '--baseline-file',
        help='Performance baseline file (JSON)'
    )
    
    parser.add_argument(
        '--junit-xml',
        help='Output JUnit XML file for CI'
    )
    
    parser.add_argument(
        '--json-report',
        help='Output detailed JSON report'
    )
    
    args = parser.parse_args()
    
    # Default to all tests if none specified
    if not any([args.test_migrations, args.test_performance, args.test_queries]):
        args.test_migrations = True
        args.test_performance = True
        args.test_queries = True
    
    # Run tests
    exit_code = run_ci_tests(args)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()