#!/usr/bin/env python3
"""
04_weighted_distribution.py - Statistical Distributions in Query Generation

This example demonstrates advanced weighted distribution techniques:
- Realistic data distributions
- Time-based patterns
- Correlated distributions
- Dynamic weight adjustment
- Statistical modeling

Key concepts:
- Probability distributions
- Temporal patterns
- Data correlation
- Adaptive weights
- Statistical realism
"""

import sys
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import math
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, Element, Context, choice, template, Lambda, number


class DistributionElement(Element):
    """Base class for distribution-based elements."""
    
    def __init__(self, name: str = "distribution"):
        self.name = name
        self.sample_count = 0
        self.history = []
    
    def generate(self, context: Context) -> str:
        value = self._sample(context)
        self.sample_count += 1
        self.history.append(value)
        return str(value)
    
    def _sample(self, context: Context) -> any:
        """Override in subclasses."""
        raise NotImplementedError
    
    def get_statistics(self) -> Dict:
        """Get distribution statistics."""
        if not self.history:
            return {}
        
        # Count occurrences
        counts = {}
        for value in self.history:
            counts[value] = counts.get(value, 0) + 1
        
        # Calculate percentages
        total = len(self.history)
        stats = {
            'total_samples': total,
            'unique_values': len(counts),
            'distribution': {k: (v/total)*100 for k, v in counts.items()}
        }
        
        return stats


class ParetoDistribution(DistributionElement):
    """80/20 rule distribution - common in real-world data."""
    
    def __init__(self, values: List[str], pareto_ratio: float = 0.8):
        super().__init__("pareto")
        self.values = values
        self.pareto_ratio = pareto_ratio
        
        # Calculate split
        self.split_point = max(1, int(len(values) * (1 - pareto_ratio)))
        self.common_values = values[:self.split_point]
        self.rare_values = values[self.split_point:]
    
    def _sample(self, context: Context) -> str:
        # 80% of queries use 20% of values
        if context.rng.random() < self.pareto_ratio:
            return context.rng.choice(self.common_values)
        else:
            return context.rng.choice(self.rare_values)


class ZipfDistribution(DistributionElement):
    """Zipf's law - frequency inversely proportional to rank."""
    
    def __init__(self, values: List[str], s: float = 1.0):
        super().__init__("zipf")
        self.values = values
        self.s = s
        
        # Precompute weights
        self.weights = []
        for i in range(1, len(values) + 1):
            self.weights.append(1.0 / (i ** s))
        
        # Normalize
        total = sum(self.weights)
        self.weights = [w/total for w in self.weights]
    
    def _sample(self, context: Context) -> str:
        # Sample based on Zipf weights
        r = context.rng.random()
        cumsum = 0
        
        for value, weight in zip(self.values, self.weights):
            cumsum += weight
            if r <= cumsum:
                return value
        
        return self.values[-1]


class NormalDistribution(DistributionElement):
    """Normal (Gaussian) distribution for numeric values."""
    
    def __init__(self, mean: float, std_dev: float, min_val: Optional[float] = None, max_val: Optional[float] = None):
        super().__init__("normal")
        self.mean = mean
        self.std_dev = std_dev
        self.min_val = min_val
        self.max_val = max_val
    
    def _sample(self, context: Context) -> int:
        # Box-Muller transform for normal distribution
        u1 = context.rng.random()
        u2 = context.rng.random()
        
        z0 = math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
        value = self.mean + z0 * self.std_dev
        
        # Apply bounds if specified
        if self.min_val is not None:
            value = max(self.min_val, value)
        if self.max_val is not None:
            value = min(self.max_val, value)
        
        return int(value)


class TimeBasedDistribution(DistributionElement):
    """Distribution that changes based on time patterns."""
    
    def __init__(self, base_values: List[str]):
        super().__init__("time_based")
        self.base_values = base_values
        
        # Define time patterns
        self.hour_weights = self._create_hour_weights()
        self.day_weights = self._create_day_weights()
    
    def _create_hour_weights(self) -> List[float]:
        """Create hourly activity pattern (peaks at 9am, 2pm, 7pm)."""
        weights = []
        for hour in range(24):
            if 9 <= hour <= 10:  # Morning peak
                weight = 1.0
            elif 14 <= hour <= 15:  # Afternoon peak
                weight = 0.8
            elif 19 <= hour <= 20:  # Evening peak
                weight = 0.9
            elif 0 <= hour <= 6:  # Night low
                weight = 0.1
            else:
                weight = 0.4
            weights.append(weight)
        return weights
    
    def _create_day_weights(self) -> List[float]:
        """Create weekly pattern (lower on weekends)."""
        # Mon=0, Sun=6
        return [1.0, 1.0, 1.0, 1.0, 0.8, 0.3, 0.2]
    
    def _sample(self, context: Context) -> str:
        # Get current time (or simulate it)
        now = datetime.now()
        hour = now.hour
        weekday = now.weekday()
        
        # Get time-based weight
        hour_weight = self.hour_weights[hour]
        day_weight = self.day_weights[weekday]
        combined_weight = hour_weight * day_weight
        
        # Adjust selection based on activity level
        if combined_weight > 0.7:
            # High activity - more diverse queries
            return context.rng.choice(self.base_values)
        elif combined_weight > 0.3:
            # Medium activity - common queries
            common_values = self.base_values[:len(self.base_values)//2]
            return context.rng.choice(common_values)
        else:
            # Low activity - maintenance queries
            return context.rng.choice(["SELECT 1", "SELECT version()"])


class CorrelatedDistribution(DistributionElement):
    """Distribution where values are correlated with context."""
    
    def __init__(self, correlations: Dict[str, Dict[str, float]]):
        super().__init__("correlated")
        self.correlations = correlations
    
    def _sample(self, context: Context) -> str:
        # Check context for correlation keys
        context_key = getattr(context, 'correlation_key', 'default')
        
        if context_key in self.correlations:
            weights = self.correlations[context_key]
            values = list(weights.keys())
            probs = list(weights.values())
            
            # Normalize probabilities
            total = sum(probs)
            probs = [p/total for p in probs]
            
            # Weighted random choice
            r = context.rng.random()
            cumsum = 0
            
            for value, prob in zip(values, probs):
                cumsum += prob
                if r <= cumsum:
                    return value
            
            return values[-1]
        
        # Default: uniform distribution
        all_values = set()
        for weights in self.correlations.values():
            all_values.update(weights.keys())
        
        return context.rng.choice(list(all_values))


class AdaptiveDistribution(DistributionElement):
    """Distribution that adapts based on feedback."""
    
    def __init__(self, initial_values: List[Tuple[str, float]]):
        super().__init__("adaptive")
        self.values = [v[0] for v in initial_values]
        self.weights = [v[1] for v in initial_values]
        self.feedback_history = []
        self.adaptation_rate = 0.1
    
    def _sample(self, context: Context) -> str:
        # Normalize weights
        total = sum(self.weights)
        normalized = [w/total for w in self.weights]
        
        # Weighted selection
        r = context.rng.random()
        cumsum = 0
        
        for i, (value, prob) in enumerate(zip(self.values, normalized)):
            cumsum += prob
            if r <= cumsum:
                self.last_index = i
                return value
        
        self.last_index = len(self.values) - 1
        return self.values[-1]
    
    def provide_feedback(self, success: bool):
        """Provide feedback to adapt weights."""
        if hasattr(self, 'last_index'):
            idx = self.last_index
            
            if success:
                # Increase weight for successful value
                self.weights[idx] *= (1 + self.adaptation_rate)
            else:
                # Decrease weight for unsuccessful value
                self.weights[idx] *= (1 - self.adaptation_rate)
            
            # Prevent weights from becoming too small
            self.weights[idx] = max(0.01, self.weights[idx])
            
            self.feedback_history.append((idx, success))


def demonstrate_distributions():
    """Show different distribution types."""
    
    print("Statistical Distribution Examples")
    print("=" * 50)
    
    # 1. Pareto Distribution (80/20 rule)
    print("\n1. Pareto Distribution (80/20 rule):")
    
    products = [f"product_{i}" for i in range(20)]
    pareto = ParetoDistribution(products, pareto_ratio=0.8)
    
    grammar = Grammar("pareto")
    grammar.rule("product", pareto)
    grammar.rule("query", template(
        "SELECT * FROM orders WHERE product = '{product}'"
    ))
    
    # Generate many queries to show distribution
    for _ in range(100):
        grammar.generate("query")
    
    stats = pareto.get_statistics()
    print(f"  Total samples: {stats['total_samples']}")
    print(f"  Top 20% products (should get ~80% of queries):")
    
    sorted_dist = sorted(stats['distribution'].items(), key=lambda x: x[1], reverse=True)
    for product, percentage in sorted_dist[:4]:
        print(f"    {product}: {percentage:.1f}%")
    
    # 2. Zipf Distribution
    print("\n\n2. Zipf Distribution (power law):")
    
    search_terms = ["iphone", "samsung", "laptop", "tablet", "headphones", "camera", "tv", "watch"]
    zipf = ZipfDistribution(search_terms, s=1.5)
    
    grammar = Grammar("zipf")
    grammar.rule("term", zipf)
    grammar.rule("query", template(
        "SELECT * FROM products WHERE name LIKE '%{term}%'"
    ))
    
    for _ in range(100):
        grammar.generate("query")
    
    stats = zipf.get_statistics()
    print("  Search term frequency (follows power law):")
    sorted_dist = sorted(stats['distribution'].items(), key=lambda x: x[1], reverse=True)
    for term, percentage in sorted_dist:
        bar = "█" * int(percentage / 2)
        print(f"    {term:12s}: {percentage:5.1f}% {bar}")
    
    # 3. Normal Distribution
    print("\n\n3. Normal Distribution (response times):")
    
    normal = NormalDistribution(mean=250, std_dev=50, min_val=10, max_val=1000)
    
    grammar = Grammar("normal")
    grammar.rule("response_time", normal)
    grammar.rule("log_entry", template(
        "-- Query completed in {response_time}ms"
    ))
    
    times = []
    for _ in range(50):
        query = grammar.generate("log_entry")
        # Extract time from query
        time = int(query.split()[-1].rstrip('ms'))
        times.append(time)
    
    # Show distribution
    print("  Response time distribution:")
    buckets = {}
    for time in times:
        bucket = (time // 50) * 50
        buckets[bucket] = buckets.get(bucket, 0) + 1
    
    for bucket in sorted(buckets.keys()):
        count = buckets[bucket]
        bar = "█" * count
        print(f"    {bucket:3d}-{bucket+49:3d}ms: {count:2d} {bar}")


def create_realistic_workload():
    """Create realistic query workload with distributions."""
    
    print("\n\nRealistic Query Workload")
    print("=" * 50)
    
    grammar = Grammar("workload")
    
    # Query type distribution (typical OLTP workload)
    query_types = [
        ("SELECT", 0.60),   # 60% reads
        ("INSERT", 0.20),   # 20% inserts
        ("UPDATE", 0.15),   # 15% updates
        ("DELETE", 0.05)    # 5% deletes
    ]
    
    grammar.rule("query_type", choice(
        *[qt[0] for qt in query_types],
        weights=[qt[1] for qt in query_types]
    ))
    
    # Table access pattern (some tables more popular)
    tables = [
        ("users", 0.30),
        ("orders", 0.25),
        ("products", 0.20),
        ("sessions", 0.15),
        ("logs", 0.10)
    ]
    
    grammar.rule("table", choice(
        *[t[0] for t in tables],
        weights=[t[1] for t in tables]
    ))
    
    # User ID distribution (active users)
    active_users = ParetoDistribution([str(i) for i in range(1, 1001)], pareto_ratio=0.9)
    grammar.rule("user_id", active_users)
    
    # Time-based patterns
    time_dist = TimeBasedDistribution(["regular_query", "batch_query", "maintenance_query"])
    
    # Build queries
    grammar.rule("select_query", template(
        "SELECT * FROM {table} WHERE user_id = {user_id}"
    ))
    
    grammar.rule("insert_query", template(
        "INSERT INTO {table} (user_id, data) VALUES ({user_id}, 'data')"
    ))
    
    grammar.rule("update_query", template(
        "UPDATE {table} SET updated_at = CURRENT_TIMESTAMP WHERE user_id = {user_id}"
    ))
    
    grammar.rule("delete_query", template(
        "DELETE FROM {table} WHERE user_id = {user_id} AND created_at < CURRENT_DATE - INTERVAL '30 days'"
    ))
    
    grammar.rule("query", Lambda(lambda ctx:
        grammar.generate(f"{grammar.generate('query_type', seed=ctx.rng.randint(0, 1000)).lower()}_query", seed=ctx.rng.randint(0, 1000))
    ))
    
    print("  Workload queries:")
    query_counts = {}
    
    for i in range(20):
        query = grammar.generate("query", seed=i)
        query_type = query.split()[0]
        query_counts[query_type] = query_counts.get(query_type, 0) + 1
        print(f"  {i+1:2d}. {query[:70]}...")
    
    print("\n  Query type distribution:")
    for qtype, count in sorted(query_counts.items()):
        percentage = (count / 20) * 100
        print(f"    {qtype}: {percentage:.0f}%")


def create_correlated_workload():
    """Create workload with correlated distributions."""
    
    print("\n\nCorrelated Distribution Workload")
    print("=" * 50)
    
    grammar = Grammar("correlated")
    
    # Correlations: user type affects query patterns
    user_query_correlation = CorrelatedDistribution({
        'power_user': {
            'complex_analytics': 0.4,
            'bulk_operations': 0.3,
            'simple_lookup': 0.2,
            'admin_query': 0.1
        },
        'regular_user': {
            'simple_lookup': 0.6,
            'basic_search': 0.3,
            'profile_update': 0.1,
            'complex_analytics': 0.0
        },
        'api_user': {
            'batch_read': 0.5,
            'single_lookup': 0.3,
            'bulk_write': 0.2,
            'admin_query': 0.0
        }
    })
    
    # Time-of-day affects table access
    table_time_correlation = CorrelatedDistribution({
        'business_hours': {
            'orders': 0.4,
            'products': 0.3,
            'customers': 0.2,
            'reports': 0.1
        },
        'after_hours': {
            'reports': 0.4,
            'analytics': 0.3,
            'logs': 0.2,
            'maintenance': 0.1
        },
        'weekend': {
            'maintenance': 0.5,
            'backups': 0.3,
            'analytics': 0.2,
            'orders': 0.0
        }
    })
    
    grammar.rule("query_pattern", user_query_correlation)
    grammar.rule("table", table_time_correlation)
    
    # Define query patterns
    grammar.rule("complex_analytics", template(
        """WITH daily_stats AS (
  SELECT DATE(created_at) as day, COUNT(*) as total
  FROM {table}
  GROUP BY DATE(created_at)
)
SELECT * FROM daily_stats ORDER BY day DESC"""
    ))
    
    grammar.rule("simple_lookup", template(
        "SELECT * FROM {table} WHERE id = {id}",
        id=number(1, 1000)
    ))
    
    grammar.rule("bulk_operations", template(
        "UPDATE {table} SET processed = true WHERE status = 'pending' LIMIT 1000"
    ))
    
    grammar.rule("batch_read", template(
        "SELECT * FROM {table} WHERE created_at > CURRENT_DATE - INTERVAL '1 hour' LIMIT 100"
    ))
    
    # Simulate different contexts
    contexts = [
        ('power_user', 'business_hours'),
        ('regular_user', 'business_hours'),
        ('api_user', 'after_hours'),
        ('power_user', 'weekend')
    ]
    
    print("  Correlated queries based on user type and time:")
    for user_type, time_period in contexts:
        grammar.context.correlation_key = user_type
        pattern = grammar.generate("query_pattern")
        
        grammar.context.correlation_key = time_period
        table = grammar.generate("table")
        
        print(f"\n  [{user_type} during {time_period}]:")
        print(f"    Pattern: {pattern}, Table: {table}")


def create_adaptive_workload():
    """Create workload that adapts based on performance."""
    
    print("\n\nAdaptive Distribution Workload")
    print("=" * 50)
    
    # Initial query weights
    query_patterns = [
        ("full_scan", 0.2),
        ("index_scan", 0.5),
        ("point_lookup", 0.3)
    ]
    
    adaptive = AdaptiveDistribution(query_patterns)
    
    grammar = Grammar("adaptive")
    grammar.rule("pattern", adaptive)
    
    grammar.rule("full_scan", "SELECT * FROM large_table")
    grammar.rule("index_scan", "SELECT * FROM users WHERE status = 'active'")
    grammar.rule("point_lookup", "SELECT * FROM users WHERE id = 123")
    
    grammar.rule("query", Lambda(lambda ctx:
        grammar.generate(adaptive.generate(ctx))
    ))
    
    print("  Initial distribution:")
    for pattern, weight in query_patterns:
        print(f"    {pattern}: {weight:.1%}")
    
    print("\n  Simulating adaptive behavior...")
    print("  (full_scan queries fail, others succeed)")
    
    # Simulate 20 queries with feedback
    success_count = {"full_scan": 0, "index_scan": 0, "point_lookup": 0}
    
    for i in range(20):
        query = grammar.generate("query", seed=i)
        
        # Simulate performance feedback
        if "large_table" in query:
            success = False  # Full scan is slow
            pattern = "full_scan"
        elif "WHERE id" in query:
            success = True   # Point lookup is fast
            pattern = "point_lookup"
        else:
            success = True   # Index scan is good
            pattern = "index_scan"
        
        adaptive.provide_feedback(success)
        if success:
            success_count[pattern] += 1
    
    print("\n  Adapted weights after feedback:")
    total = sum(adaptive.weights)
    for i, pattern in enumerate(["full_scan", "index_scan", "point_lookup"]):
        weight = adaptive.weights[i] / total
        print(f"    {pattern}: {weight:.1%} (success rate: {success_count[pattern]}/20)")


def main():
    """Run all distribution examples."""
    
    demonstrate_distributions()
    create_realistic_workload()
    create_correlated_workload()
    create_adaptive_workload()
    
    print("\n" + "=" * 50)
    print("Weighted Distribution Summary:")
    print("- Use Pareto distribution for 80/20 patterns")
    print("- Apply Zipf's law for search/access patterns")
    print("- Normal distribution for performance metrics")
    print("- Time-based patterns for realistic workloads")
    print("- Correlate distributions for complex behaviors")
    print("- Adaptive distributions learn from feedback")


if __name__ == "__main__":
    main()