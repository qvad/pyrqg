#!/usr/bin/env python3
"""
05_adaptive_grammar.py - Self-Improving Query Generation

This example demonstrates adaptive grammar techniques:
- Learning from execution feedback
- Pattern recognition and optimization
- Dynamic rule adjustment
- Performance-based adaptation
- Query complexity management

Key concepts:
- Feedback loops
- Performance tracking
- Rule evolution
- Complexity scoring
- Adaptive strategies
"""

import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
import time
from dataclasses import dataclass
from enum import Enum

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, Element, Context, template, choice, ref, Lambda


class QueryResult(Enum):
    """Query execution results."""
    SUCCESS = "success"
    ERROR = "error"
    TIMEOUT = "timeout"
    SLOW = "slow"


@dataclass
class QueryFeedback:
    """Feedback from query execution."""
    query: str
    result: QueryResult
    execution_time: float
    error_message: Optional[str] = None
    row_count: Optional[int] = None


@dataclass
class PatternStats:
    """Statistics for a query pattern."""
    success_count: int = 0
    error_count: int = 0
    timeout_count: int = 0
    total_time: float = 0.0
    avg_rows: float = 0.0
    
    @property
    def total_count(self) -> int:
        return self.success_count + self.error_count + self.timeout_count
    
    @property
    def success_rate(self) -> float:
        return self.success_count / self.total_count if self.total_count > 0 else 0.0
    
    @property
    def avg_time(self) -> float:
        return self.total_time / self.success_count if self.success_count > 0 else 0.0


class AdaptiveGrammar(Grammar):
    """Grammar that adapts based on execution feedback."""
    
    def __init__(self, name: str = "adaptive"):
        super().__init__(name)
        self.pattern_stats: Dict[str, PatternStats] = {}
        self.disabled_patterns: Set[str] = set()
        self.pattern_weights: Dict[str, float] = {}
        self.learning_rate = 0.1
        self.min_samples = 5
        
        # Complexity scoring
        self.complexity_scores: Dict[str, float] = {}
        self.target_complexity = 5.0
        
        # Initialize base patterns
        self._init_patterns()
    
    def _init_patterns(self):
        """Initialize query patterns with complexity scores."""
        
        patterns = [
            ("simple_select", 1.0),
            ("filtered_select", 2.0),
            ("join_query", 4.0),
            ("subquery", 5.0),
            ("aggregate_query", 3.0),
            ("window_function", 6.0),
            ("cte_query", 7.0),
            ("complex_join", 8.0)
        ]
        
        for pattern, complexity in patterns:
            self.pattern_weights[pattern] = 1.0
            self.complexity_scores[pattern] = complexity
            self.pattern_stats[pattern] = PatternStats()
    
    def add_feedback(self, pattern: str, feedback: QueryFeedback):
        """Add execution feedback for a pattern."""
        
        if pattern not in self.pattern_stats:
            self.pattern_stats[pattern] = PatternStats()
        
        stats = self.pattern_stats[pattern]
        
        # Update statistics
        if feedback.result == QueryResult.SUCCESS:
            stats.success_count += 1
            stats.total_time += feedback.execution_time
            if feedback.row_count is not None:
                stats.avg_rows = (stats.avg_rows * (stats.success_count - 1) + feedback.row_count) / stats.success_count
        elif feedback.result == QueryResult.ERROR:
            stats.error_count += 1
        elif feedback.result == QueryResult.TIMEOUT:
            stats.timeout_count += 1
        elif feedback.result == QueryResult.SLOW:
            stats.success_count += 1
            stats.total_time += feedback.execution_time
        
        # Adapt weights
        self._adapt_weights(pattern, feedback)
    
    def _adapt_weights(self, pattern: str, feedback: QueryFeedback):
        """Adjust pattern weights based on feedback."""
        
        if pattern not in self.pattern_weights:
            return
        
        stats = self.pattern_stats[pattern]
        
        # Wait for minimum samples
        if stats.total_count < self.min_samples:
            return
        
        # Calculate performance score
        if feedback.result == QueryResult.SUCCESS:
            # Good performance increases weight
            if feedback.execution_time < 0.1:  # Fast query
                self.pattern_weights[pattern] *= (1 + self.learning_rate)
            elif feedback.execution_time < 1.0:  # Normal query
                self.pattern_weights[pattern] *= (1 + self.learning_rate * 0.5)
        elif feedback.result == QueryResult.ERROR:
            # Errors decrease weight
            self.pattern_weights[pattern] *= (1 - self.learning_rate)
        elif feedback.result == QueryResult.TIMEOUT:
            # Timeouts significantly decrease weight
            self.pattern_weights[pattern] *= (1 - self.learning_rate * 2)
        elif feedback.result == QueryResult.SLOW:
            # Slow queries slightly decrease weight
            self.pattern_weights[pattern] *= (1 - self.learning_rate * 0.5)
        
        # Disable patterns with very poor performance
        if stats.success_rate < 0.2 and stats.total_count >= 10:
            self.disabled_patterns.add(pattern)
            self.pattern_weights[pattern] = 0.0
        
        # Ensure minimum weight
        self.pattern_weights[pattern] = max(0.01, self.pattern_weights[pattern])
    
    def select_pattern(self, context: Context) -> str:
        """Select a pattern based on current weights and complexity target."""
        
        available_patterns = [p for p in self.pattern_weights.keys() 
                            if p not in self.disabled_patterns]
        
        if not available_patterns:
            return "simple_select"  # Fallback
        
        # Filter by complexity if needed
        complexity_filtered = [p for p in available_patterns 
                             if abs(self.complexity_scores.get(p, 5.0) - self.target_complexity) <= 2.0]
        
        if complexity_filtered:
            available_patterns = complexity_filtered
        
        # Weighted random selection
        patterns = list(available_patterns)
        weights = [self.pattern_weights[p] for p in patterns]
        
        total = sum(weights)
        if total == 0:
            return context.rng.choice(patterns)
        
        normalized = [w/total for w in weights]
        
        r = context.rng.random()
        cumsum = 0
        
        for pattern, weight in zip(patterns, normalized):
            cumsum += weight
            if r <= cumsum:
                return pattern
        
        return patterns[-1]
    
    def adjust_complexity_target(self, performance_ratio: float):
        """Adjust target complexity based on system performance."""
        
        if performance_ratio > 0.9:  # System performing well
            self.target_complexity = min(8.0, self.target_complexity + 0.5)
        elif performance_ratio < 0.7:  # System struggling
            self.target_complexity = max(2.0, self.target_complexity - 0.5)
    
    def get_performance_summary(self) -> str:
        """Get summary of pattern performance."""
        
        lines = ["Pattern Performance Summary:"]
        lines.append("-" * 60)
        
        for pattern, stats in sorted(self.pattern_stats.items()):
            if stats.total_count == 0:
                continue
            
            weight = self.pattern_weights.get(pattern, 0.0)
            complexity = self.complexity_scores.get(pattern, 0.0)
            
            status = "DISABLED" if pattern in self.disabled_patterns else "ACTIVE"
            
            lines.append(
                f"{pattern:20s} | Success: {stats.success_rate:5.1%} | "
                f"Avg Time: {stats.avg_time:6.3f}s | Weight: {weight:5.2f} | "
                f"Complexity: {complexity:3.1f} | {status}"
            )
        
        return "\n".join(lines)


class QueryOptimizer(Element):
    """Element that optimizes queries based on learned patterns."""
    
    def __init__(self, base_element: Element):
        self.base_element = base_element
        self.optimization_rules = []
        self.failed_patterns = set()
        
        # Common optimization patterns
        self._init_optimizations()
    
    def _init_optimizations(self):
        """Initialize optimization rules."""
        
        self.optimization_rules = [
            # Avoid SELECT *
            (r"SELECT \*", self._optimize_select_star),
            # Add LIMIT to unbounded queries
            (r"SELECT .+ FROM .+ WHERE", self._add_limit_if_missing),
            # Use EXISTS instead of IN for subqueries
            (r"WHERE .+ IN \(SELECT", self._optimize_in_subquery),
            # Add indexes hints for known slow queries
            (r"FROM (\w+) WHERE", self._add_index_hint)
        ]
    
    def generate(self, context: Context) -> str:
        """Generate and optimize query."""
        
        # Generate base query
        query = self.base_element.generate(context)
        
        # Apply optimizations
        for pattern, optimizer in self.optimization_rules:
            if pattern in self.failed_patterns:
                continue
            
            try:
                import re
                if re.search(pattern, query):
                    query = optimizer(query, context)
            except Exception:
                # Disable failed optimization
                self.failed_patterns.add(pattern)
        
        return query
    
    def _optimize_select_star(self, query: str, context: Context) -> str:
        """Replace SELECT * with specific columns."""
        if context.rng.random() < 0.7:  # 70% chance to optimize
            columns = "id, name, status"
            return query.replace("SELECT *", f"SELECT {columns}")
        return query
    
    def _add_limit_if_missing(self, query: str, context: Context) -> str:
        """Add LIMIT if query doesn't have one."""
        if "LIMIT" not in query and context.rng.random() < 0.8:
            return f"{query} LIMIT 1000"
        return query
    
    def _optimize_in_subquery(self, query: str, context: Context) -> str:
        """Convert IN subquery to EXISTS."""
        # Simple example - real implementation would be more robust
        if "IN (SELECT" in query:
            # This is simplified - real optimization would parse properly
            return query.replace("IN (SELECT", "EXISTS (SELECT 1 FROM")
        return query
    
    def _add_index_hint(self, query: str, context: Context) -> str:
        """Add index hints for known patterns."""
        # Example: Add index hint for user queries
        if "FROM users WHERE" in query and "email" in query:
            return query.replace("FROM users", "FROM users /*+ INDEX(users_email_idx) */")
        return query


class ComplexityAnalyzer(Element):
    """Analyzes and scores query complexity."""
    
    def __init__(self):
        self.complexity_factors = {
            'SELECT': 1,
            'JOIN': 3,
            'LEFT JOIN': 3,
            'SUBQUERY': 4,
            'GROUP BY': 2,
            'ORDER BY': 1,
            'HAVING': 2,
            'UNION': 3,
            'WITH': 4,
            'WINDOW': 5
        }
    
    def analyze(self, query: str) -> float:
        """Calculate complexity score for a query."""
        
        score = 0.0
        query_upper = query.upper()
        
        # Count occurrences of complexity factors
        for factor, weight in self.complexity_factors.items():
            count = query_upper.count(factor)
            score += count * weight
        
        # Additional factors
        if 'DISTINCT' in query_upper:
            score += 1
        
        # Nested subqueries add exponential complexity
        nested_level = query.count('(SELECT')
        if nested_level > 1:
            score += (nested_level - 1) ** 2
        
        # Long queries are more complex
        score += len(query) / 1000
        
        return score


def demonstrate_adaptive_grammar():
    """Demonstrate adaptive grammar behavior."""
    
    print("Adaptive Grammar Demonstration")
    print("=" * 50)
    
    # Create adaptive grammar
    grammar = AdaptiveGrammar()
    
    # Define query patterns
    grammar.rule("simple_select", template(
        "SELECT * FROM users WHERE id = {id}",
        id=Lambda(lambda ctx: ctx.rng.randint(1, 1000))
    ))
    
    grammar.rule("filtered_select", template(
        "SELECT id, name FROM users WHERE status = '{status}' AND created_at > CURRENT_DATE - INTERVAL '{days} days'",
        status=choice("active", "pending"),
        days=Lambda(lambda ctx: ctx.rng.randint(1, 30))
    ))
    
    grammar.rule("join_query", template(
        "SELECT u.name, COUNT(o.id) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name"
    ))
    
    grammar.rule("subquery", template(
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > {amount})",
        amount=Lambda(lambda ctx: ctx.rng.randint(100, 1000))
    ))
    
    grammar.rule("aggregate_query", template(
        "SELECT status, COUNT(*) as cnt FROM users GROUP BY status HAVING COUNT(*) > {min_count}",
        min_count=Lambda(lambda ctx: ctx.rng.randint(10, 100))
    ))
    
    # Main query rule
    grammar.rule("query", Lambda(lambda ctx: 
        grammar.generate(grammar.select_pattern(ctx), seed=ctx.rng.randint(0, 10000))
    ))
    
    # Simulate query execution with feedback
    print("\nSimulating 30 query executions with feedback...\n")
    
    for i in range(30):
        # Generate query
        pattern = grammar.select_pattern(grammar.context)
        query = grammar.generate(pattern, seed=i)
        
        # Simulate execution (mock feedback)
        if "subquery" in pattern and i < 10:
            # Subqueries fail initially
            feedback = QueryFeedback(
                query=query,
                result=QueryResult.TIMEOUT,
                execution_time=5.0
            )
        elif "join" in pattern and i % 3 == 0:
            # Some joins are slow
            feedback = QueryFeedback(
                query=query,
                result=QueryResult.SLOW,
                execution_time=2.5,
                row_count=1000
            )
        elif "simple" in pattern:
            # Simple queries are fast
            feedback = QueryFeedback(
                query=query,
                result=QueryResult.SUCCESS,
                execution_time=0.05,
                row_count=1
            )
        else:
            # Most queries succeed
            feedback = QueryFeedback(
                query=query,
                result=QueryResult.SUCCESS,
                execution_time=0.2 + (i % 5) * 0.1,
                row_count=50
            )
        
        # Add feedback
        grammar.add_feedback(pattern, feedback)
        
        # Show progress
        if i % 10 == 9:
            print(f"After {i+1} queries:")
            print(grammar.get_performance_summary())
            print()
    
    print("\nFinal pattern selection (based on learned weights):")
    for _ in range(5):
        pattern = grammar.select_pattern(grammar.context)
        weight = grammar.pattern_weights.get(pattern, 0.0)
        print(f"  Selected: {pattern} (weight: {weight:.2f})")


def demonstrate_query_optimization():
    """Show query optimization based on patterns."""
    
    print("\n\nQuery Optimization Demonstration")
    print("=" * 50)
    
    # Base grammar
    grammar = Grammar("base")
    
    grammar.rule("bad_query", choice(
        "SELECT * FROM large_table",
        "SELECT * FROM users WHERE email IN (SELECT email FROM temp_users)",
        "SELECT * FROM orders WHERE status = 'pending'",
        "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id"
    ))
    
    # Wrap with optimizer
    optimizer = QueryOptimizer(ref("bad_query"))
    grammar.rule("optimized_query", optimizer)
    
    print("  Original vs Optimized queries:")
    for i in range(4):
        original = grammar.generate("bad_query", seed=i)
        optimized = grammar.generate("optimized_query", seed=i)
        
        print(f"\n  Original:  {original}")
        print(f"  Optimized: {optimized}")


def create_self_tuning_workload():
    """Create a self-tuning query workload."""
    
    print("\n\nSelf-Tuning Workload")
    print("=" * 50)
    
    class SelfTuningWorkload(Element):
        """Workload that adjusts based on system state."""
        
        def __init__(self):
            self.system_load = 0.5  # 0.0 = idle, 1.0 = overloaded
            self.query_history = []
            self.performance_window = 10
            
            # Query categories by resource usage
            self.query_categories = {
                'lightweight': {
                    'queries': [
                        "SELECT 1",
                        "SELECT * FROM small_table LIMIT 1",
                        "SELECT id, name FROM users WHERE id = 123"
                    ],
                    'resource_usage': 0.1
                },
                'moderate': {
                    'queries': [
                        "SELECT COUNT(*) FROM orders WHERE created_at > CURRENT_DATE - INTERVAL '1 day'",
                        "SELECT u.name, p.name FROM users u JOIN profiles p ON u.id = p.user_id LIMIT 100",
                        "UPDATE stats SET last_updated = CURRENT_TIMESTAMP WHERE id < 100"
                    ],
                    'resource_usage': 0.3
                },
                'heavy': {
                    'queries': [
                        "SELECT category, SUM(amount) FROM transactions GROUP BY category",
                        "WITH recursive_cte AS (...) SELECT * FROM recursive_cte",
                        "SELECT * FROM large_table ORDER BY created_at DESC LIMIT 1000"
                    ],
                    'resource_usage': 0.7
                },
                'maintenance': {
                    'queries': [
                        "VACUUM ANALYZE small_table",
                        "REINDEX INDEX users_email_idx",
                        "ANALYZE large_table"
                    ],
                    'resource_usage': 0.5
                }
            }
        
        def generate(self, context: Context) -> str:
            # Select category based on system load
            if self.system_load > 0.8:
                # High load - only lightweight queries
                category = 'lightweight'
            elif self.system_load > 0.6:
                # Moderate load - avoid heavy queries
                category = context.rng.choice(['lightweight', 'moderate'], p=[0.7, 0.3])
            elif self.system_load > 0.3:
                # Normal load - balanced mix
                category = context.rng.choice(
                    ['lightweight', 'moderate', 'heavy'],
                    p=[0.3, 0.5, 0.2]
                )
            else:
                # Low load - can do maintenance
                category = context.rng.choice(
                    ['lightweight', 'moderate', 'heavy', 'maintenance'],
                    p=[0.2, 0.3, 0.3, 0.2]
                )
            
            # Select query from category
            queries = self.query_categories[category]['queries']
            query = context.rng.choice(queries)
            
            # Track query and update load
            resource_usage = self.query_categories[category]['resource_usage']
            self.query_history.append((query, resource_usage))
            
            # Update system load based on recent queries
            if len(self.query_history) > self.performance_window:
                self.query_history.pop(0)
            
            avg_usage = sum(h[1] for h in self.query_history) / len(self.query_history)
            self.system_load = min(1.0, avg_usage * 1.2)  # Amplify effect
            
            return f"-- Load: {self.system_load:.2f} | Category: {category}\n{query}"
    
    grammar = Grammar("self_tuning")
    workload = SelfTuningWorkload()
    grammar.rule("query", workload)
    
    print("  Self-tuning workload adjusting to system load:")
    print("  (Notice how query complexity changes with load)\n")
    
    for i in range(20):
        query = grammar.generate("query", seed=i)
        lines = query.split('\n')
        load_info = lines[0]
        actual_query = lines[1] if len(lines) > 1 else ""
        
        print(f"  {i+1:2d}. {load_info}")
        print(f"      {actual_query[:60]}...")
        
        # Simulate load spikes
        if i == 8:
            workload.system_load = 0.9
            print("      >>> LOAD SPIKE! <<<")


def create_learning_grammar():
    """Create grammar that learns query patterns from a database."""
    
    print("\n\nLearning Grammar from Patterns")
    print("=" * 50)
    
    class PatternLearner(Element):
        """Learns and reproduces query patterns."""
        
        def __init__(self):
            self.learned_patterns = []
            self.pattern_fragments = {
                'select_clause': set(),
                'from_clause': set(),
                'where_clause': set(),
                'group_clause': set(),
                'order_clause': set()
            }
        
        def learn_from_query(self, query: str):
            """Learn patterns from an example query."""
            
            # Simple pattern extraction (real implementation would use SQL parser)
            import re
            
            # Extract SELECT clause
            select_match = re.search(r'SELECT\s+(.+?)\s+FROM', query, re.IGNORECASE)
            if select_match:
                self.pattern_fragments['select_clause'].add(select_match.group(1))
            
            # Extract FROM clause
            from_match = re.search(r'FROM\s+(\w+(?:\s+\w+)?)', query, re.IGNORECASE)
            if from_match:
                self.pattern_fragments['from_clause'].add(from_match.group(1))
            
            # Extract WHERE clause
            where_match = re.search(r'WHERE\s+(.+?)(?:\s+GROUP\s+BY|\s+ORDER\s+BY|$)', query, re.IGNORECASE)
            if where_match:
                self.pattern_fragments['where_clause'].add(where_match.group(1))
            
            # Store complete pattern
            self.learned_patterns.append(query)
        
        def generate(self, context: Context) -> str:
            if not self.learned_patterns:
                return "SELECT 1"  # Fallback
            
            # Mix learned patterns
            if context.rng.random() < 0.3 and len(self.learned_patterns) > 0:
                # Sometimes reproduce exact learned query
                return context.rng.choice(self.learned_patterns)
            else:
                # Combine fragments from different queries
                parts = []
                
                if self.pattern_fragments['select_clause']:
                    select = context.rng.choice(list(self.pattern_fragments['select_clause']))
                    parts.append(f"SELECT {select}")
                else:
                    parts.append("SELECT *")
                
                if self.pattern_fragments['from_clause']:
                    from_clause = context.rng.choice(list(self.pattern_fragments['from_clause']))
                    parts.append(f"FROM {from_clause}")
                else:
                    parts.append("FROM users")
                
                if self.pattern_fragments['where_clause'] and context.rng.random() < 0.7:
                    where = context.rng.choice(list(self.pattern_fragments['where_clause']))
                    parts.append(f"WHERE {where}")
                
                return " ".join(parts)
    
    # Example training queries
    training_queries = [
        "SELECT id, name, email FROM users WHERE status = 'active'",
        "SELECT COUNT(*) as total FROM orders WHERE created_at > CURRENT_DATE - INTERVAL '7 days'",
        "SELECT u.name, COUNT(o.id) FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name",
        "SELECT product_id, SUM(quantity) FROM order_items GROUP BY product_id HAVING SUM(quantity) > 100"
    ]
    
    learner = PatternLearner()
    
    print("  Training on example queries:")
    for query in training_queries:
        learner.learn_from_query(query)
        print(f"    Learned: {query[:60]}...")
    
    grammar = Grammar("learning")
    grammar.rule("query", learner)
    
    print("\n  Generated queries based on learned patterns:")
    for i in range(8):
        query = grammar.generate("query", seed=i*10)
        print(f"  {i+1}. {query}")


def main():
    """Run all adaptive grammar examples."""
    
    demonstrate_adaptive_grammar()
    demonstrate_query_optimization()
    create_self_tuning_workload()
    create_learning_grammar()
    
    print("\n" + "=" * 50)
    print("Adaptive Grammar Summary:")
    print("- Track performance metrics for each pattern")
    print("- Adjust weights based on execution feedback")
    print("- Optimize queries using learned rules")
    print("- Self-tune based on system load")
    print("- Learn from example query patterns")
    print("- Continuously improve generation quality")


if __name__ == "__main__":
    main()