#!/usr/bin/env python3
"""
YugabyteDB Filter Configuration
Replaces known_issues.ff

This module defines filters for known issues and query patterns to skip.
"""

import re
from typing import List, Callable, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum

class FilterAction(Enum):
    """Action to take when filter matches"""
    SKIP = "skip"          # Skip this query
    IGNORE_ERROR = "ignore_error"  # Ignore errors from this query
    WARN = "warn"          # Log warning but continue
    MODIFY = "modify"      # Modify the query

@dataclass
class Filter:
    """Query filter definition"""
    name: str
    description: str
    pattern: Optional[str] = None  # Regex pattern
    function: Optional[Callable[[str], bool]] = None  # Custom function
    action: FilterAction = FilterAction.SKIP
    enabled: bool = True
    metadata: Dict[str, Any] = None

class FilterRegistry:
    """Registry of all filters"""
    def __init__(self):
        self.filters: List[Filter] = []
    
    def add(self, filter: Filter):
        """Add a filter to the registry"""
        self.filters.append(filter)
    
    def add_regex(self, name: str, pattern: str, description: str, 
                  action: FilterAction = FilterAction.SKIP, enabled: bool = True):
        """Add a regex-based filter"""
        self.add(Filter(
            name=name,
            description=description,
            pattern=pattern,
            action=action,
            enabled=enabled
        ))
    
    def add_function(self, name: str, func: Callable, description: str,
                    action: FilterAction = FilterAction.SKIP, enabled: bool = True):
        """Add a function-based filter"""
        self.add(Filter(
            name=name,
            description=description,
            function=func,
            action=action,
            enabled=enabled
        ))
    
    def apply(self, query: str) -> Optional[FilterAction]:
        """Apply all filters to a query and return action if any match"""
        for filter in self.filters:
            if not filter.enabled:
                continue
                
            # Check regex pattern
            if filter.pattern:
                if re.search(filter.pattern, query, re.IGNORECASE | re.DOTALL):
                    return filter.action
            
            # Check function
            if filter.function:
                if filter.function(query):
                    return filter.action
        
        return None
    
    def get_filter(self, name: str) -> Optional[Filter]:
        """Get filter by name"""
        for filter in self.filters:
            if filter.name == name:
                return filter
        return None
    
    def enable(self, name: str):
        """Enable a filter"""
        filter = self.get_filter(name)
        if filter:
            filter.enabled = True
    
    def disable(self, name: str):
        """Disable a filter"""
        filter = self.get_filter(name)
        if filter:
            filter.enabled = False

# ============================================================================
# Known Issues Filters (from known_issues.ff)
# ============================================================================

# Create global registry
known_issues = FilterRegistry()

# Bug #21012 - enabled by default
known_issues.add_regex(
    name="bug_21012",
    pattern=r"(E|DD|EE|FF|PP|B|BB|GG|HH|I|II|J|JJ|K|KK|L|LL|M|MM|N|NN|O|OO|P)\s*AS",
    description="Bug #21012: Queries referencing certain tables",
    action=FilterAction.SKIP,
    enabled=True
)

# Bitmap scan filters - disabled by default
known_issues.add_regex(
    name="bitmap_scan_ge64",
    pattern=r"col_bigint.*(>|>=)\s*64",
    description="Bitmap scan issues with col_bigint >= 64",
    action=FilterAction.SKIP,
    enabled=False
)

known_issues.add_regex(
    name="bitmap_scan_gt63",
    pattern=r"col_bigint.*>\s*63",
    description="Bitmap scan issues with col_bigint > 63",
    action=FilterAction.SKIP,
    enabled=False
)

known_issues.add_regex(
    name="bitmap_scan_le191",
    pattern=r"col_bigint.*(<=|<)\s*191",
    description="Bitmap scan issues with col_bigint <= 191",
    action=FilterAction.SKIP,
    enabled=False
)

known_issues.add_regex(
    name="bitmap_scan_lt192",
    pattern=r"col_bigint.*<\s*192",
    description="Bitmap scan issues with col_bigint < 192",
    action=FilterAction.SKIP,
    enabled=False
)

# Batched Nested Loop filter - disabled by default
def is_batched_nested_loop(query: str) -> bool:
    """Check if query might trigger batched nested loop"""
    # This is a placeholder - actual implementation would analyze query plan
    return False

known_issues.add_function(
    name="batched_nested_loop",
    func=is_batched_nested_loop,
    description="Skip queries that might use batched nested loop",
    action=FilterAction.SKIP,
    enabled=False
)

# ============================================================================
# Additional YugabyteDB-specific Filters
# ============================================================================

# Skip very large joins
def has_too_many_joins(query: str) -> bool:
    """Check if query has too many joins"""
    join_count = query.upper().count(' JOIN ')
    return join_count > 15

known_issues.add_function(
    name="excessive_joins",
    func=has_too_many_joins,
    description="Skip queries with more than 15 joins",
    action=FilterAction.SKIP,
    enabled=True
)

# Skip queries with known timeout patterns
known_issues.add_regex(
    name="timeout_pattern",
    pattern=r"SELECT.*FROM.*WHERE.*NOT EXISTS.*GROUP BY.*HAVING",
    description="Complex pattern known to cause timeouts",
    action=FilterAction.SKIP,
    enabled=False
)

# Ignore specific errors
known_issues.add_regex(
    name="deadlock_retry",
    pattern=r"ERROR.*deadlock detected",
    description="Deadlock errors are expected in concurrent testing",
    action=FilterAction.IGNORE_ERROR,
    enabled=True
)

# ============================================================================
# Filter Sets for Different Test Scenarios
# ============================================================================

class FilterSet:
    """Predefined filter configurations"""
    
    @staticmethod
    def default():
        """Default filter set - only critical issues"""
        registry = FilterRegistry()
        registry.filters = known_issues.filters.copy()
        # Enable only bug filters by default
        for filter in registry.filters:
            filter.enabled = filter.name.startswith("bug_")
        return registry
    
    @staticmethod
    def strict():
        """Strict filter set - all known issues"""
        registry = FilterRegistry()
        registry.filters = known_issues.filters.copy()
        # Enable all filters
        for filter in registry.filters:
            filter.enabled = True
        return registry
    
    @staticmethod
    def performance():
        """Performance testing filter set"""
        registry = FilterRegistry()
        registry.filters = known_issues.filters.copy()
        # Enable timeout and performance-related filters
        for filter in registry.filters:
            filter.enabled = filter.name in ["excessive_joins", "timeout_pattern"]
        return registry
    
    @staticmethod
    def concurrent():
        """Concurrent testing filter set"""
        registry = FilterRegistry()
        registry.filters = known_issues.filters.copy()
        # Enable concurrency-related filters
        for filter in registry.filters:
            filter.enabled = filter.name in ["deadlock_retry", "bug_21012"]
        return registry

# ============================================================================
# Usage Functions
# ============================================================================

def should_skip_query(query: str, filter_set: FilterRegistry = None) -> bool:
    """Check if a query should be skipped"""
    if filter_set is None:
        filter_set = FilterSet.default()
    
    action = filter_set.apply(query)
    return action == FilterAction.SKIP

def should_ignore_error(query: str, error: str, filter_set: FilterRegistry = None) -> bool:
    """Check if an error should be ignored for a query"""
    if filter_set is None:
        filter_set = FilterSet.default()
    
    # Check error-specific filters
    error_registry = FilterRegistry()
    for filter in filter_set.filters:
        if filter.action == FilterAction.IGNORE_ERROR:
            error_registry.add(filter)
    
    action = error_registry.apply(error)
    return action == FilterAction.IGNORE_ERROR

# ============================================================================
# Test
# ============================================================================

if __name__ == "__main__":
    print("YugabyteDB Filter Configuration")
    print("=" * 60)
    
    # Show all filters
    print("\nRegistered Filters:")
    for filter in known_issues.filters:
        status = "enabled" if filter.enabled else "disabled"
        print(f"  {filter.name}: {filter.description} [{status}]")
    
    # Test queries
    test_queries = [
        "SELECT * FROM E AS table1 JOIN F AS table2",  # Should match bug_21012
        "SELECT * FROM A WHERE col_bigint > 64",  # Bitmap scan (disabled)
        "SELECT * FROM t1 JOIN t2 JOIN t3 JOIN t4 JOIN t5 JOIN t6 JOIN t7 JOIN t8 JOIN t9 JOIN t10 JOIN t11 JOIN t12 JOIN t13 JOIN t14 JOIN t15 JOIN t16",  # Too many joins
        "SELECT * FROM users WHERE id = 1"  # Normal query
    ]
    
    print("\n\nQuery Filter Tests (default filter set):")
    default_filters = FilterSet.default()
    for query in test_queries:
        should_skip = should_skip_query(query, default_filters)
        print(f"\n  Query: {query[:60]}...")
        print(f"  Skip: {should_skip}")
    
    print("\n\nQuery Filter Tests (strict filter set):")
    strict_filters = FilterSet.strict()
    for query in test_queries:
        should_skip = should_skip_query(query, strict_filters)
        print(f"\n  Query: {query[:60]}...")
        print(f"  Skip: {should_skip}")