#!/usr/bin/env python3
"""
Test query uniqueness - verify if generated queries are truly unique
"""

import sys
from pathlib import Path
from collections import Counter

sys.path.insert(0, str(Path(__file__).parent))

from grammars.yugabyte.transactions_postgres import g as tg
from grammars.yugabyte.optimizer_subquery_portable import g as sg
from grammars.yugabyte.outer_join_portable import g as og
from grammars.dml_yugabyte import g as dml
from grammars.dml_fixed import g as dml_fixed

def test_uniqueness(grammar, rule_name, num_queries=1000, grammar_name="Grammar"):
    """Test if a grammar generates unique queries"""
    print(f"\n{'='*70}")
    print(f"Testing: {grammar_name}")
    print(f"{'='*70}")
    
    queries = []
    query_counter = Counter()
    
    # Generate queries
    for i in range(num_queries):
        try:
            query = grammar.generate(rule_name, seed=i)
            queries.append(query)
            query_counter[query] += 1
        except Exception as e:
            print(f"Error at seed {i}: {e}")
    
    # Analyze results
    total_generated = len(queries)
    unique_queries = len(set(queries))
    uniqueness_rate = (unique_queries / total_generated * 100) if total_generated > 0 else 0
    
    print(f"Total queries generated: {total_generated}")
    print(f"Unique queries: {unique_queries}")
    print(f"Uniqueness rate: {uniqueness_rate:.1f}%")
    
    # Find duplicates
    duplicates = [(query, count) for query, count in query_counter.items() if count > 1]
    
    if duplicates:
        print(f"\n⚠️  Found {len(duplicates)} queries that appear multiple times:")
        # Show top 5 most repeated
        duplicates.sort(key=lambda x: x[1], reverse=True)
        for query, count in duplicates[:5]:
            print(f"\n  Appears {count} times:")
            print(f"  {query[:100]}...")
            
            # Find which seeds produce this query
            seeds = [i for i in range(num_queries) if i < len(queries) and queries[i] == query]
            print(f"  Seeds: {seeds[:10]}{'...' if len(seeds) > 10 else ''}")
    else:
        print(f"\n✅ ALL {total_generated} queries are unique!")
    
    # Analyze patterns
    print(f"\n📊 Pattern Analysis:")
    
    # Check for common prefixes
    prefix_counter = Counter()
    for query in queries:
        prefix = query[:30]
        prefix_counter[prefix] += 1
    
    common_prefixes = [(prefix, count) for prefix, count in prefix_counter.most_common(5)]
    print(f"\nMost common query starts:")
    for prefix, count in common_prefixes:
        print(f"  '{prefix}...' : {count} queries ({count/total_generated*100:.1f}%)")
    
    return uniqueness_rate == 100

def test_with_same_seed():
    """Test what happens with the same seed"""
    print(f"\n{'='*70}")
    print("Testing same seed behavior")
    print(f"{'='*70}")
    
    # Test if same seed produces same query
    query1 = tg.generate('query', seed=42)
    query2 = tg.generate('query', seed=42)
    
    print(f"Query with seed 42 (first call):")
    print(f"  {query1[:80]}...")
    print(f"\nQuery with seed 42 (second call):")
    print(f"  {query2[:80]}...")
    print(f"\nSame query? {query1 == query2}")
    
    # Test consecutive seeds
    print(f"\nTesting consecutive seeds:")
    for i in range(5):
        q = tg.generate('query', seed=i)
        print(f"  Seed {i}: {q[:60]}...")

def main():
    """Run uniqueness tests on all grammars"""
    print("🔍 Query Uniqueness Analysis")
    print("="*70)
    
    grammars_to_test = [
        (tg, 'query', 'Transactions Grammar', 1000),
        (sg, 'query', 'Subquery Grammar', 1000),
        (og, 'query', 'Outer Join Grammar', 1000),
        (dml, 'query', 'YugabyteDB DML Grammar', 1000),
        (dml_fixed, 'query', 'Fixed DML Grammar', 500),
    ]
    
    all_unique = True
    results = []
    
    for grammar, rule, name, num in grammars_to_test:
        is_unique = test_uniqueness(grammar, rule, num, name)
        results.append((name, is_unique))
        if not is_unique:
            all_unique = False
    
    # Test same seed behavior
    test_with_same_seed()
    
    # Summary
    print(f"\n{'='*70}")
    print("📊 UNIQUENESS SUMMARY")
    print(f"{'='*70}")
    
    for name, is_unique in results:
        status = "✅ UNIQUE" if is_unique else "❌ DUPLICATES"
        print(f"{name:.<40} {status}")
    
    if all_unique:
        print(f"\n✅ All grammars generate unique queries with different seeds!")
    else:
        print(f"\n⚠️  Some grammars have duplicate queries")
        print(f"\nReasons for duplicates:")
        print("1. Limited randomness in some rules")
        print("2. Fixed values in grammar definitions")
        print("3. Small choice sets for certain elements")
        print("4. Deterministic patterns with same random choices")
        
        print(f"\n💡 To improve uniqueness:")
        print("1. Add more variety to choice elements")
        print("2. Use Lambda functions for dynamic values")
        print("3. Increase the number of possible combinations")
        print("4. Add more randomized elements to templates")

if __name__ == "__main__":
    main()