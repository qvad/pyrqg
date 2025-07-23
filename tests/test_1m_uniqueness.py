#!/usr/bin/env python3
"""
Test 1 Million Query Uniqueness
Proves that PyRQG generates unique queries
"""

import sys
import time
import hashlib
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent))

from pyrqg.api import create_rqg

def test_million_queries():
    """Generate 1 million queries and test uniqueness"""
    print("PyRQG 1 Million Query Uniqueness Test")
    print("=" * 80)
    
    rqg = create_rqg()
    
    # Test configuration
    total_queries = 1_000_000
    
    # Distribution of query types
    distribution = {
        'workload_select': 400_000,  # 40%
        'workload_insert': 300_000,  # 30%
        'workload_update': 200_000,  # 20%
        'workload_delete': 50_000,   # 5%
        'workload_upsert': 50_000    # 5%
    }
    
    print(f"Generating {total_queries:,} queries...")
    print("\nDistribution:")
    for qtype, count in distribution.items():
        print(f"  {qtype}: {count:,} ({count/total_queries*100:.0f}%)")
    
    # Store query hashes for uniqueness check
    query_hashes = set()
    duplicate_count = 0
    duplicate_examples = []
    
    # Statistics
    stats = {
        'by_type': defaultdict(int),
        'duplicates_by_type': defaultdict(int),
        'generation_times': [],
        'unique_by_type': defaultdict(set)
    }
    
    print("\nGenerating queries...")
    print("-" * 80)
    
    overall_start = time.time()
    queries_generated = 0
    
    # Generate queries for each type
    for grammar_name, count in distribution.items():
        print(f"\n{grammar_name}:")
        type_start = time.time()
        
        # Generate in batches for progress reporting
        batch_size = 10_000
        for batch_start in range(0, count, batch_size):
            batch_end = min(batch_start + batch_size, count)
            batch_queries = batch_end - batch_start
            
            batch_time = time.time()
            
            # Generate batch
            for i in range(batch_start, batch_end):
                try:
                    # Generate query with unique seed
                    query = rqg.generate_from_grammar(
                        grammar_name, 
                        count=1, 
                        seed=i + hash(grammar_name)
                    )[0]
                    
                    # Create hash of query
                    query_hash = hashlib.md5(query.encode()).hexdigest()
                    
                    # Check uniqueness
                    if query_hash in query_hashes:
                        duplicate_count += 1
                        stats['duplicates_by_type'][grammar_name] += 1
                        if len(duplicate_examples) < 10:
                            duplicate_examples.append({
                                'type': grammar_name,
                                'query': query[:100] + '...' if len(query) > 100 else query
                            })
                    else:
                        query_hashes.add(query_hash)
                        stats['unique_by_type'][grammar_name].add(query_hash)
                    
                    stats['by_type'][grammar_name] += 1
                    queries_generated += 1
                    
                except Exception as e:
                    print(f"Error generating query: {e}")
            
            # Progress report
            elapsed = time.time() - batch_time
            qps = batch_queries / elapsed
            progress = queries_generated / total_queries * 100
            
            print(f"  Batch {batch_start//batch_size + 1}: "
                  f"{batch_queries:,} queries in {elapsed:.2f}s "
                  f"({qps:.0f} q/s) - Total: {queries_generated:,} ({progress:.1f}%)")
        
        type_elapsed = time.time() - type_start
        type_qps = count / type_elapsed
        print(f"  Completed {count:,} queries in {type_elapsed:.1f}s ({type_qps:.0f} q/s)")
        print(f"  Unique: {len(stats['unique_by_type'][grammar_name]):,}")
    
    overall_elapsed = time.time() - overall_start
    
    # Final report
    print("\n" + "=" * 80)
    print("FINAL RESULTS")
    print("=" * 80)
    
    print(f"\nTotal queries generated: {queries_generated:,}")
    print(f"Unique queries: {len(query_hashes):,}")
    print(f"Duplicate queries: {duplicate_count:,}")
    print(f"Uniqueness rate: {len(query_hashes)/queries_generated*100:.2f}%")
    print(f"\nTotal time: {overall_elapsed:.2f} seconds")
    print(f"Overall rate: {queries_generated/overall_elapsed:.0f} queries/second")
    
    # Breakdown by type
    print("\nUniqueness by query type:")
    for qtype in distribution.keys():
        total = stats['by_type'][qtype]
        unique = len(stats['unique_by_type'][qtype])
        duplicates = stats['duplicates_by_type'][qtype]
        uniqueness = unique / total * 100 if total > 0 else 0
        
        print(f"\n{qtype}:")
        print(f"  Total: {total:,}")
        print(f"  Unique: {unique:,}")
        print(f"  Duplicates: {duplicates:,}")
        print(f"  Uniqueness: {uniqueness:.2f}%")
    
    # Show duplicate examples if any
    if duplicate_examples:
        print("\nSample duplicate queries:")
        for i, dup in enumerate(duplicate_examples[:5]):
            print(f"{i+1}. [{dup['type']}] {dup['query']}")
    
    # Memory usage
    import sys
    memory_mb = sys.getsizeof(query_hashes) / 1024 / 1024
    print(f"\nMemory used for uniqueness tracking: {memory_mb:.2f} MB")
    
    # Success criteria
    print("\n" + "=" * 80)
    if duplicate_count == 0:
        print("✅ SUCCESS: All 1,000,000 queries are UNIQUE!")
    else:
        uniqueness_rate = len(query_hashes)/queries_generated*100
        if uniqueness_rate >= 99.9:
            print(f"✅ SUCCESS: {uniqueness_rate:.2f}% uniqueness rate (>99.9%)")
        else:
            print(f"⚠️  WARNING: Only {uniqueness_rate:.2f}% unique queries")
    
    return len(query_hashes), duplicate_count

def test_specific_grammar_uniqueness():
    """Test uniqueness of specific grammars"""
    print("\n\nTesting Individual Grammar Uniqueness")
    print("=" * 80)
    
    rqg = create_rqg()
    
    # Test each grammar with smaller sample
    grammars = [
        'workload_select',
        'workload_insert', 
        'workload_update',
        'workload_delete',
        'workload_upsert',
        'dml_unique'  # Our enhanced uniqueness grammar
    ]
    
    for grammar in grammars:
        if grammar not in rqg.grammars:
            print(f"\nSkipping {grammar} (not loaded)")
            continue
            
        print(f"\nTesting {grammar}:")
        
        # Generate 10,000 queries
        queries = set()
        duplicates = 0
        
        for i in range(10_000):
            query = rqg.generate_from_grammar(grammar, seed=i)[0]
            if query in queries:
                duplicates += 1
            else:
                queries.add(query)
        
        uniqueness = len(queries) / 10_000 * 100
        print(f"  Generated: 10,000")
        print(f"  Unique: {len(queries):,}")
        print(f"  Duplicates: {duplicates}")
        print(f"  Uniqueness: {uniqueness:.1f}%")

def main():
    """Run uniqueness tests"""
    # First test individual grammars
    test_specific_grammar_uniqueness()
    
    # Then run the million query test
    print("\n" + "=" * 80)
    print("Starting 1 million query test...")
    
    unique_count, duplicate_count = test_million_queries()
    
    # Show how to execute against PostgreSQL
    print("\n\nTo test against real PostgreSQL:")
    print("=" * 80)
    print("""
# 1. Start PostgreSQL container:
docker run -d --name postgres-test \\
  -e POSTGRES_PASSWORD=postgres \\
  -p 5432:5432 \\
  postgres:15

# 2. Install psycopg2:
pip install psycopg2-binary

# 3. Run this script with database execution:
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres", 
    password="postgres"
)

# Create test schema
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(200),
        email VARCHAR(200),
        status VARCHAR(50)
    );
    -- Add other tables...
''')
conn.commit()

# Execute the million queries
for query in queries:
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        # Log error
""")

if __name__ == "__main__":
    main()