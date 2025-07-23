"""
Validators - Check query results for correctness
"""

import re
import logging
from typing import List, Dict, Any, Optional, Type
from abc import ABC, abstractmethod

from .result import Result, Status


logger = logging.getLogger(__name__)


class Validator(ABC):
    """Base validator class"""
    
    @abstractmethod
    def validate(self, result: Result) -> List[str]:
        """
        Validate a query result.
        Returns list of issues found (empty list if valid).
        """
        pass


class ValidatorRegistry:
    """Registry for validators"""
    _validators: Dict[str, Type[Validator]] = {}
    
    @classmethod
    def register(cls, name: str, validator_class: Type[Validator]) -> None:
        """Register a validator"""
        cls._validators[name] = validator_class
    
    @classmethod
    def get(cls, name: str) -> Optional[Type[Validator]]:
        """Get validator class by name"""
        return cls._validators.get(name)
    
    @classmethod
    def list(cls) -> List[str]:
        """List available validators"""
        return list(cls._validators.keys())


# ============================================================================
# Built-in Validators
# ============================================================================

class ErrorMessageValidator(Validator):
    """Validates that error messages are expected"""
    
    def __init__(self):
        # Known/expected errors that should not be reported
        self.expected_errors = [
            "duplicate key value",
            "division by zero",
            "value too long",
            "out of range",
            "invalid input syntax"
        ]
    
    def validate(self, result: Result) -> List[str]:
        issues = []
        
        if result.status != Status.OK and result.errstr:
            # Check if this is an unexpected error
            error_lower = result.errstr.lower()
            
            if not any(expected in error_lower for expected in self.expected_errors):
                # Check for crash-like errors
                if any(word in error_lower for word in ['crash', 'core dump', 'segmentation']):
                    issues.append(f"CRITICAL: Possible server crash: {result.errstr}")
                elif 'internal error' in error_lower:
                    issues.append(f"Internal database error: {result.errstr}")
        
        return issues


class ResultSetValidator(Validator):
    """Validates result sets for consistency"""
    
    def validate(self, result: Result) -> List[str]:
        issues = []
        
        if result.status == Status.OK and result.data:
            # Check for NULL handling issues
            for row in result.data:
                if row is None:
                    issues.append("Got None instead of row tuple")
                elif isinstance(row, (list, tuple)):
                    for i, value in enumerate(row):
                        if value is None:
                            continue  # NULL is valid
                        # Add more validation as needed
        
        return issues


class PerformanceValidator(Validator):
    """Validates query performance"""
    
    def __init__(self, slow_query_threshold: float = 5.0):
        self.slow_query_threshold = slow_query_threshold
    
    def validate(self, result: Result) -> List[str]:
        issues = []
        
        if result.duration > self.slow_query_threshold:
            issues.append(f"Slow query: {result.duration:.2f}s > {self.slow_query_threshold}s threshold")
        
        return issues


class TransactionValidator(Validator):
    """Validates transaction behavior"""
    
    def __init__(self):
        self.in_transaction = False
        self.savepoints = []
    
    def validate(self, result: Result) -> List[str]:
        issues = []
        query_upper = result.query.upper()
        
        # Track transaction state
        if 'START TRANSACTION' in query_upper or 'BEGIN' in query_upper:
            if self.in_transaction:
                issues.append("Starting transaction while already in transaction")
            self.in_transaction = True
            self.savepoints = []
            
        elif 'COMMIT' in query_upper:
            if not self.in_transaction:
                issues.append("COMMIT outside of transaction")
            self.in_transaction = False
            self.savepoints = []
            
        elif 'ROLLBACK' in query_upper:
            if 'SAVEPOINT' in query_upper:
                # Rollback to savepoint
                sp_match = re.search(r'ROLLBACK\s+TO\s+(?:SAVEPOINT\s+)?(\w+)', query_upper)
                if sp_match:
                    sp_name = sp_match.group(1)
                    if sp_name not in self.savepoints:
                        issues.append(f"Rolling back to non-existent savepoint: {sp_name}")
            else:
                if not self.in_transaction:
                    issues.append("ROLLBACK outside of transaction")
                self.in_transaction = False
                self.savepoints = []
                
        elif 'SAVEPOINT' in query_upper and 'ROLLBACK' not in query_upper:
            sp_match = re.search(r'SAVEPOINT\s+(\w+)', query_upper)
            if sp_match:
                sp_name = sp_match.group(1)
                self.savepoints.append(sp_name)
        
        return issues


class ReplicationValidator(Validator):
    """Validates replication consistency (for distributed databases)"""
    
    def __init__(self, replicas: List[str]):
        self.replicas = replicas
    
    def validate(self, result: Result) -> List[str]:
        issues = []
        
        # This would check consistency across replicas
        # For now, just a placeholder
        if result.status == Status.OK and 'INSERT' in result.query.upper():
            # In real implementation, would verify data appears on all replicas
            pass
        
        return issues


class ZeroSumValidator(Validator):
    """Validates zero-sum operations (sum of changes should be zero)"""
    
    def __init__(self):
        self.table_sums: Dict[str, int] = {}
    
    def validate(self, result: Result) -> List[str]:
        issues = []
        
        # Look for zero-sum patterns in the query
        query = result.query
        
        # Pattern: UPDATE table SET col1 = col1 - X, col2 = col2 + X
        zero_sum_pattern = r'SET\s+(\w+)\s*=\s*\1\s*([+-])\s*(\d+)\s*,\s*(\w+)\s*=\s*\4\s*([+-])\s*(\d+)'
        match = re.search(zero_sum_pattern, query, re.IGNORECASE)
        
        if match:
            col1, op1, val1, col2, op2, val2 = match.groups()
            val1, val2 = int(val1), int(val2)
            
            # Check if it's truly zero-sum
            delta1 = val1 if op1 == '+' else -val1
            delta2 = val2 if op2 == '+' else -val2
            
            if delta1 + delta2 != 0:
                issues.append(f"Non-zero sum operation: {col1} {op1} {val1}, {col2} {op2} {val2}")
        
        return issues


# ============================================================================
# Register built-in validators
# ============================================================================

ValidatorRegistry.register('error_message', ErrorMessageValidator)
ValidatorRegistry.register('result_set', ResultSetValidator)
ValidatorRegistry.register('performance', PerformanceValidator)
ValidatorRegistry.register('transaction', TransactionValidator)
ValidatorRegistry.register('replication', ReplicationValidator)
ValidatorRegistry.register('zero_sum', ZeroSumValidator)