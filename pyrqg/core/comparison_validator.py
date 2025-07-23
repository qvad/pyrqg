"""
Result Comparison Validator
Compares query results between multiple database servers
"""

import logging
import hashlib
import json
from typing import List, Dict, Any, Optional, Tuple, Set
from dataclasses import dataclass, field
from abc import ABC, abstractmethod

from .validator import Validator, ValidatorRegistry
from .result import Result, Status
try:
    from .executor_production import create_production_executor as create_executor
    from .executor_production import ProductionExecutor as Executor
except ImportError:
    from .executor import create_executor, Executor

logger = logging.getLogger(__name__)


@dataclass
class ComparisonResult:
    """Result of comparing two query executions"""
    query: str
    matches: bool
    differences: List[str] = field(default_factory=list)
    server1_result: Optional[Result] = None
    server2_result: Optional[Result] = None
    explain1: Optional[str] = None
    explain2: Optional[str] = None


class ResultComparator:
    """Compares results between two database servers"""
    
    def __init__(self, dsn1: str, dsn2: str, 
                 server1_name: str = "Server1",
                 server2_name: str = "Server2",
                 explain_analyze: bool = False,
                 explain_options: Dict[str, Any] = None):
        """
        Initialize comparator with two database connections
        
        Args:
            dsn1: Connection string for first database
            dsn2: Connection string for second database
            server1_name: Display name for first server (e.g., "PostgreSQL")
            server2_name: Display name for second server (e.g., "YugabyteDB")
            explain_analyze: Whether to run EXPLAIN ANALYZE
            explain_options: Additional EXPLAIN options (e.g., {"BUFFERS": True, "VERBOSE": True})
        """
        self.dsn1 = dsn1
        self.dsn2 = dsn2
        self.server1_name = server1_name
        self.server2_name = server2_name
        self.explain_analyze = explain_analyze
        self.explain_options = explain_options or {}
        
        # Create executors
        self.executor1 = create_executor(dsn1)
        self.executor2 = create_executor(dsn2)
        
        # Connect to both databases
        self.executor1.connect()
        self.executor2.connect()
        
        # Statistics
        self.stats = {
            'total_queries': 0,
            'matching_results': 0,
            'different_results': 0,
            'execution_errors': 0,
            'comparison_errors': 0,
            'differences_by_type': {}
        }
    
    def compare_query(self, query: str) -> ComparisonResult:
        """Compare execution of a query on both servers"""
        self.stats['total_queries'] += 1
        
        # Execute on both servers
        result1 = self.executor1.execute(query)
        result2 = self.executor2.execute(query)
        
        # Initialize comparison result
        comp_result = ComparisonResult(
            query=query,
            matches=True,
            server1_result=result1,
            server2_result=result2
        )
        
        # Compare execution status
        if result1.status != result2.status:
            comp_result.matches = False
            comp_result.differences.append(
                f"Status differs: {self.server1_name}={result1.status.name}, "
                f"{self.server2_name}={result2.status.name}"
            )
            self.stats['different_results'] += 1
            self._record_difference('status_mismatch')
            
        # If both failed, compare error types
        if result1.status != Status.OK and result2.status != Status.OK:
            if self._normalize_error(result1.errstr) != self._normalize_error(result2.errstr):
                comp_result.differences.append(
                    f"Error differs: {self.server1_name}='{result1.errstr}', "
                    f"{self.server2_name}='{result2.errstr}'"
                )
                self._record_difference('error_mismatch')
            self.stats['execution_errors'] += 1
            return comp_result
            
        # If only one failed
        if result1.status != Status.OK or result2.status != Status.OK:
            self.stats['execution_errors'] += 1
            return comp_result
            
        # Both succeeded - compare results
        differences = self._compare_result_sets(result1, result2)
        if differences:
            comp_result.matches = False
            comp_result.differences.extend(differences)
            self.stats['different_results'] += 1
        else:
            self.stats['matching_results'] += 1
            
        # Run EXPLAIN ANALYZE if requested and query is SELECT
        if self.explain_analyze and query.strip().upper().startswith('SELECT'):
            comp_result.explain1 = self._get_explain_analyze(self.executor1, query)
            comp_result.explain2 = self._get_explain_analyze(self.executor2, query)
            
        return comp_result
    
    def _compare_result_sets(self, result1: Result, result2: Result) -> List[str]:
        """Compare two result sets for differences"""
        differences = []
        
        # Compare affected rows for DML
        if result1.affected_rows != result2.affected_rows:
            differences.append(
                f"Affected rows differ: {self.server1_name}={result1.affected_rows}, "
                f"{self.server2_name}={result2.affected_rows}"
            )
            self._record_difference('affected_rows_mismatch')
            
        # Compare data for SELECT
        if result1.data is not None and result2.data is not None:
            # Check row count
            if len(result1.data) != len(result2.data):
                differences.append(
                    f"Row count differs: {self.server1_name}={len(result1.data)}, "
                    f"{self.server2_name}={len(result2.data)}"
                )
                self._record_difference('row_count_mismatch')
                
            # Compare actual data
            data_diffs = self._compare_data(result1.data, result2.data)
            if data_diffs:
                differences.extend(data_diffs)
                
        return differences
    
    def _compare_data(self, data1: List[Tuple], data2: List[Tuple]) -> List[str]:
        """Compare two data sets row by row"""
        differences = []
        
        # Convert to comparable format (handle None, different numeric precision, etc.)
        normalized1 = [self._normalize_row(row) for row in data1]
        normalized2 = [self._normalize_row(row) for row in data2]
        
        # Sort both datasets for comparison (assuming ORDER BY is used)
        try:
            sorted1 = sorted(normalized1)
            sorted2 = sorted(normalized2)
        except TypeError:
            # If sorting fails, compare as-is
            sorted1 = normalized1
            sorted2 = normalized2
            
        # Compare sorted data
        if sorted1 != sorted2:
            # Find specific differences
            max_len = max(len(sorted1), len(sorted2))
            for i in range(max_len):
                if i >= len(sorted1):
                    differences.append(f"Row {i+1}: Missing in {self.server1_name}")
                    self._record_difference('missing_row')
                elif i >= len(sorted2):
                    differences.append(f"Row {i+1}: Missing in {self.server2_name}")
                    self._record_difference('missing_row')
                elif sorted1[i] != sorted2[i]:
                    # Find which columns differ
                    col_diffs = []
                    for j, (val1, val2) in enumerate(zip(sorted1[i], sorted2[i])):
                        if val1 != val2:
                            col_diffs.append(f"col{j+1}: {val1} vs {val2}")
                    if col_diffs and len(differences) < 10:  # Limit output
                        differences.append(f"Row {i+1}: {', '.join(col_diffs)}")
                    self._record_difference('value_mismatch')
                    
        return differences
    
    def _normalize_row(self, row: Tuple) -> Tuple:
        """Normalize a row for comparison"""
        normalized = []
        for value in row:
            if value is None:
                normalized.append(None)
            elif isinstance(value, float):
                # Handle floating point precision
                normalized.append(round(value, 6))
            elif isinstance(value, str):
                # Normalize strings (strip whitespace)
                normalized.append(value.strip())
            else:
                normalized.append(value)
        return tuple(normalized)
    
    def _normalize_error(self, error: str) -> str:
        """Normalize error messages for comparison"""
        if not error:
            return ""
            
        # Remove server-specific prefixes
        error = error.lower()
        for prefix in ['error:', 'fatal:', 'warning:', 'notice:']:
            error = error.replace(prefix, '')
            
        # Normalize common variations
        replacements = {
            'table': 'relation',
            'column': 'attribute',
            'does not exist': 'doesn\'t exist',
        }
        for old, new in replacements.items():
            error = error.replace(old, new)
            
        return error.strip()
    
    def _get_explain_analyze(self, executor: Executor, query: str) -> Optional[str]:
        """Get EXPLAIN ANALYZE output for a query"""
        try:
            # Build EXPLAIN query
            explain_parts = ["EXPLAIN"]
            
            # Add options
            options = []
            if self.explain_analyze:
                options.append("ANALYZE")
            for opt, val in self.explain_options.items():
                if val:
                    options.append(opt.upper())
                    
            if options:
                explain_parts.append(f"({', '.join(options)})")
                
            explain_query = f"{' '.join(explain_parts)} {query}"
            
            # Execute EXPLAIN
            result = executor.execute(explain_query)
            if result.status == Status.OK and result.data:
                # Format EXPLAIN output
                return '\n'.join(str(row[0]) if isinstance(row, tuple) else str(row) 
                               for row in result.data)
        except Exception as e:
            logger.warning(f"Failed to get EXPLAIN: {e}")
            
        return None
    
    def _record_difference(self, diff_type: str):
        """Record a type of difference for statistics"""
        if diff_type not in self.stats['differences_by_type']:
            self.stats['differences_by_type'][diff_type] = 0
        self.stats['differences_by_type'][diff_type] += 1
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comparison statistics"""
        return self.stats.copy()
    
    def close(self):
        """Close database connections"""
        self.executor1.close()
        self.executor2.close()


class ComparisonValidator(Validator):
    """Validator that compares results between multiple servers"""
    
    def __init__(self, reference_dsn: str, 
                 reference_name: str = "Reference",
                 explain_analyze: bool = False,
                 explain_options: Dict[str, Any] = None):
        """
        Initialize comparison validator
        
        Args:
            reference_dsn: DSN for reference database (e.g., PostgreSQL)
            reference_name: Display name for reference server
            explain_analyze: Whether to capture EXPLAIN ANALYZE
            explain_options: Additional EXPLAIN options
        """
        self.reference_dsn = reference_dsn
        self.reference_name = reference_name
        self.explain_analyze = explain_analyze
        self.explain_options = explain_options or {}
        
        # Will be initialized when first used
        self.reference_executor = None
        
        # Cache for comparison results
        self.comparison_cache = {}
        
    def validate(self, result: Result) -> List[str]:
        """Validate by comparing with reference database"""
        issues = []
        
        # Skip if query failed on primary
        if result.status != Status.OK:
            return issues
            
        # Skip non-deterministic queries
        if self._is_non_deterministic(result.query):
            return issues
            
        # Initialize reference executor if needed
        if not self.reference_executor:
            try:
                self.reference_executor = create_executor(self.reference_dsn)
                self.reference_executor.connect()
            except Exception as e:
                logger.error(f"Failed to connect to reference database: {e}")
                return [f"Cannot connect to reference database: {e}"]
                
        # Execute on reference
        try:
            ref_result = self.reference_executor.execute(result.query)
            
            # Compare results
            if ref_result.status != result.status:
                issues.append(
                    f"Status mismatch: Current={result.status.name}, "
                    f"{self.reference_name}={ref_result.status.name}"
                )
                
            elif result.status == Status.OK:
                # Compare successful results
                data_issues = self._compare_results(result, ref_result)
                issues.extend(data_issues)
                
                # Get EXPLAIN if requested
                if self.explain_analyze and result.query.upper().startswith('SELECT'):
                    explain = self._get_explain(result.query)
                    if explain:
                        logger.info(f"EXPLAIN ANALYZE:\n{explain}")
                        
        except Exception as e:
            issues.append(f"Error comparing with reference: {e}")
            
        return issues
    
    def _is_non_deterministic(self, query: str) -> bool:
        """Check if query might produce non-deterministic results"""
        query_upper = query.upper()
        
        # Functions that are non-deterministic
        non_det_functions = [
            'RANDOM()', 'NOW()', 'CURRENT_TIMESTAMP', 
            'CURRENT_TIME', 'CURRENT_DATE', 'UUID'
        ]
        
        for func in non_det_functions:
            if func in query_upper:
                return True
                
        # SELECT without ORDER BY might be non-deterministic
        if 'SELECT' in query_upper and 'ORDER BY' not in query_upper:
            # Only if selecting multiple rows
            if 'LIMIT 1' not in query_upper:
                return True
                
        return False
    
    def _compare_results(self, result1: Result, result2: Result) -> List[str]:
        """Compare two successful query results"""
        issues = []
        
        # Compare affected rows
        if result1.affected_rows != result2.affected_rows:
            issues.append(
                f"Affected rows mismatch: Current={result1.affected_rows}, "
                f"{self.reference_name}={result2.affected_rows}"
            )
            
        # Compare data
        if result1.data is not None and result2.data is not None:
            if len(result1.data) != len(result2.data):
                issues.append(
                    f"Row count mismatch: Current={len(result1.data)}, "
                    f"{self.reference_name}={len(result2.data)}"
                )
            else:
                # Compare actual data (normalized)
                data1_normalized = [self._normalize_row(row) for row in result1.data]
                data2_normalized = [self._normalize_row(row) for row in result2.data]
                
                if data1_normalized != data2_normalized:
                    # Show first difference
                    for i, (row1, row2) in enumerate(zip(data1_normalized, data2_normalized)):
                        if row1 != row2:
                            issues.append(
                                f"Data mismatch at row {i+1}: "
                                f"Current={row1}, {self.reference_name}={row2}"
                            )
                            break  # Only show first difference
                            
        return issues
    
    def _normalize_row(self, row: Tuple) -> Tuple:
        """Normalize row data for comparison"""
        if not isinstance(row, (list, tuple)):
            return (row,)
            
        normalized = []
        for val in row:
            if isinstance(val, float):
                # Round floats to avoid precision issues
                normalized.append(round(val, 6))
            elif isinstance(val, str):
                # Normalize strings
                normalized.append(val.strip())
            else:
                normalized.append(val)
        return tuple(normalized)
    
    def _get_explain(self, query: str) -> Optional[str]:
        """Get EXPLAIN output from reference database"""
        options = ["ANALYZE"] + [k for k, v in self.explain_options.items() if v]
        explain_query = f"EXPLAIN ({', '.join(options)}) {query}"
        
        try:
            result = self.reference_executor.execute(explain_query)
            if result.status == Status.OK and result.data:
                return '\n'.join(str(row[0]) for row in result.data)
        except Exception as e:
            logger.warning(f"Failed to get EXPLAIN: {e}")
            
        return None
    
    def close(self):
        """Clean up resources"""
        if self.reference_executor:
            self.reference_executor.close()


# Register validators
ValidatorRegistry.register('comparison', ComparisonValidator)