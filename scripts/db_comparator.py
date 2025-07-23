#!/usr/bin/env python3
"""
Database Comparator - Compare query results between PostgreSQL and YugabyteDB
Supports result comparison and EXPLAIN ANALYZE output
"""

import sys
import json
import time
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from dataclasses import dataclass, field
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent))

from pyrqg.core.executor import Executor
from pyrqg.core.generator import Generator

class MockExecutor(Executor):
    """Mock executor for testing without real database"""
    
    def __init__(self, db_type="postgres"):
        super().__init__()
        self.db_type = db_type
        self.tables = {
            "users": [
                {"id": 1, "email": "john@example.com", "name": "John Doe", "status": "active"},
                {"id": 2, "email": "jane@example.com", "name": "Jane Smith", "status": "active"},
                {"id": 3, "email": "bob@example.com", "name": "Bob Wilson", "status": "inactive"}
            ],
            "products": [
                {"id": 1, "product_id": "PROD001", "name": "Laptop Pro", "price": 1299.99, "quantity": 50},
                {"id": 2, "product_id": "PROD002", "name": "Mouse Wireless", "price": 29.99, "quantity": 200}
            ],
            "orders": [
                {"id": 1, "order_id": "ORD001", "user_id": 1, "product_id": 1, "total": 1299.99}
            ]
        }
    
    def execute(self, query: str, params: dict):
        """Simulate query execution"""
        query_upper = query.upper()
        
        # Handle EXPLAIN ANALYZE
        if "EXPLAIN ANALYZE" in query_upper:
            if self.db_type == "postgres":
                return "Seq Scan on users  (cost=0.00..1.03 rows=3 width=32) (actual time=0.001..0.002 rows=3 loops=1)\nPlanning Time: 0.025 ms\nExecution Time: 0.015 ms"
            else:
                return "Seq Scan on users  (cost=0.00..100.00 rows=1000 width=32) (actual time=0.001..0.002 rows=3 loops=1)\nStorage Index Read Requests: 1\nExecution Time: 0.025 ms"
        
        # Handle SELECT
        if query_upper.startswith("SELECT"):
            # Simple mock - return some data
            if "users" in query.lower():
                return self.tables["users"][:2]
            elif "products" in query.lower():
                return self.tables["products"][:2]
            else:
                return [{"result": 1}]
        
        # Handle INSERT/UPDATE/DELETE
        if any(query_upper.startswith(cmd) for cmd in ["INSERT", "UPDATE", "DELETE"]):
            if "RETURNING" in query_upper:
                return [{"id": 999, "status": "created"}]
            return 1
        
        # Handle CREATE TABLE
        if query_upper.startswith("CREATE"):
            return None
        
        # Default
        return []

@dataclass
class ComparisonResult:
    """Result of comparing a query between two databases"""
    query: str
    postgres_result: Any
    yugabyte_result: Any
    postgres_explain: Optional[str] = None
    yugabyte_explain: Optional[str] = None
    results_match: bool = False
    error_postgres: Optional[str] = None
    error_yugabyte: Optional[str] = None
    execution_time_postgres: float = 0.0
    execution_time_yugabyte: float = 0.0
    
    def __post_init__(self):
        """Check if results match"""
        if self.error_postgres or self.error_yugabyte:
            self.results_match = False
        else:
            # Convert results to comparable format
            pg_hash = self._hash_result(self.postgres_result)
            yb_hash = self._hash_result(self.yugabyte_result)
            self.results_match = pg_hash == yb_hash
    
    def _hash_result(self, result):
        """Create hash of result for comparison"""
        if result is None:
            return ""
        # Convert to sorted JSON for consistent comparison
        try:
            sorted_json = json.dumps(result, sort_keys=True)
            return hashlib.md5(sorted_json.encode()).hexdigest()
        except:
            return str(result)

class DatabaseComparator:
    """Compare query execution between PostgreSQL and YugabyteDB"""
    
    def __init__(self, postgres_conn=None, yugabyte_conn=None):
        """Initialize comparator with database connections"""
        self.postgres_conn = postgres_conn or MockExecutor(db_type="postgres")
        self.yugabyte_conn = yugabyte_conn or MockExecutor(db_type="yugabyte")
        self.results: List[ComparisonResult] = []
        self.setup_schema()
    
    def setup_schema(self):
        """Create test schema in both databases"""
        schema_queries = [
            # Users table
            """CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                name VARCHAR(255),
                status VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            
            # Products table
            """CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY,
                product_id VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(255),
                price DECIMAL(10,2),
                quantity INTEGER,
                data JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            
            # Orders table
            """CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                order_id VARCHAR(50) UNIQUE NOT NULL,
                user_id INTEGER REFERENCES users(id),
                product_id INTEGER REFERENCES products(id),
                quantity INTEGER,
                total DECIMAL(10,2),
                status VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            
            # Transactions table
            """CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                user_id INTEGER,
                amount DECIMAL(10,2),
                type VARCHAR(50),
                status VARCHAR(50),
                metadata JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )""",
            
            # Insert test data
            """INSERT INTO users (email, name, status) VALUES
                ('john@example.com', 'John Doe', 'active'),
                ('jane@example.com', 'Jane Smith', 'active'),
                ('bob@example.com', 'Bob Wilson', 'inactive')
                ON CONFLICT (email) DO NOTHING""",
            
            """INSERT INTO products (product_id, name, price, quantity) VALUES
                ('PROD001', 'Laptop Pro', 1299.99, 50),
                ('PROD002', 'Mouse Wireless', 29.99, 200),
                ('PROD003', 'Keyboard Mechanical', 149.99, 75)
                ON CONFLICT (product_id) DO NOTHING""",
            
            """INSERT INTO orders (order_id, user_id, product_id, quantity, total, status) VALUES
                ('ORD001', 1, 1, 1, 1299.99, 'completed'),
                ('ORD002', 2, 2, 2, 59.98, 'completed'),
                ('ORD003', 1, 3, 1, 149.99, 'pending')
                ON CONFLICT (order_id) DO NOTHING"""
        ]
        
        print("Setting up test schema...")
        for query in schema_queries:
            try:
                self.postgres_conn.execute(query, {})
                self.yugabyte_conn.execute(query, {})
            except Exception as e:
                print(f"Schema setup warning: {e}")
    
    def execute_with_explain(self, conn, query: str) -> Tuple[Any, str, float]:
        """Execute query and get EXPLAIN ANALYZE output"""
        try:
            # First get EXPLAIN ANALYZE
            explain_query = f"EXPLAIN ANALYZE {query}"
            explain_result = conn.execute(explain_query, {})
            
            # Then execute actual query
            start_time = time.time()
            result = conn.execute(query, {})
            execution_time = time.time() - start_time
            
            return result, explain_result, execution_time
        except Exception as e:
            return None, None, 0.0
    
    def compare_query(self, query: str) -> ComparisonResult:
        """Compare a single query between databases"""
        print(f"\nComparing: {query[:80]}...")
        
        # Execute on PostgreSQL
        try:
            pg_result, pg_explain, pg_time = self.execute_with_explain(
                self.postgres_conn, query
            )
            pg_error = None
        except Exception as e:
            pg_result, pg_explain, pg_time = None, None, 0.0
            pg_error = str(e)
        
        # Execute on YugabyteDB
        try:
            yb_result, yb_explain, yb_time = self.execute_with_explain(
                self.yugabyte_conn, query
            )
            yb_error = None
        except Exception as e:
            yb_result, yb_explain, yb_time = None, None, 0.0
            yb_error = str(e)
        
        # Create comparison result
        result = ComparisonResult(
            query=query,
            postgres_result=pg_result,
            yugabyte_result=yb_result,
            postgres_explain=pg_explain,
            yugabyte_explain=yb_explain,
            error_postgres=pg_error,
            error_yugabyte=yb_error,
            execution_time_postgres=pg_time,
            execution_time_yugabyte=yb_time
        )
        
        self.results.append(result)
        return result
    
    def compare_grammar_queries(self, grammar, rule="query", num_queries=10):
        """Generate and compare queries from a grammar"""
        generator = Generator(grammar)
        
        for i in range(num_queries):
            query = generator.generate_query(rule, seed=i)
            
            # Add ORDER BY for deterministic results
            if "SELECT" in query.upper() and "ORDER BY" not in query.upper():
                # Simple heuristic - add ORDER BY 1
                query = query.rstrip(";") + " ORDER BY 1;"
            
            self.compare_query(query)
    
    def generate_report(self):
        """Generate comparison report"""
        print("\n" + "="*80)
        print("DATABASE COMPARISON REPORT")
        print("="*80)
        
        total_queries = len(self.results)
        matching_results = sum(1 for r in self.results if r.results_match)
        pg_errors = sum(1 for r in self.results if r.error_postgres)
        yb_errors = sum(1 for r in self.results if r.error_yugabyte)
        
        print(f"\nTotal Queries Tested: {total_queries}")
        print(f"Matching Results: {matching_results} ({matching_results/total_queries*100:.1f}%)")
        print(f"PostgreSQL Errors: {pg_errors}")
        print(f"YugabyteDB Errors: {yb_errors}")
        
        # Performance comparison
        avg_pg_time = sum(r.execution_time_postgres for r in self.results) / total_queries
        avg_yb_time = sum(r.execution_time_yugabyte for r in self.results) / total_queries
        
        print(f"\nAverage Execution Time:")
        print(f"  PostgreSQL: {avg_pg_time*1000:.2f}ms")
        print(f"  YugabyteDB: {avg_yb_time*1000:.2f}ms")
        
        # Show mismatches
        mismatches = [r for r in self.results if not r.results_match and not r.error_postgres and not r.error_yugabyte]
        if mismatches:
            print(f"\n⚠️  Result Mismatches: {len(mismatches)}")
            for i, r in enumerate(mismatches[:5]):
                print(f"\n{i+1}. Query: {r.query[:100]}...")
                print(f"   PostgreSQL: {str(r.postgres_result)[:100]}...")
                print(f"   YugabyteDB: {str(r.yugabyte_result)[:100]}...")
        
        # Show errors
        errors = [r for r in self.results if r.error_postgres or r.error_yugabyte]
        if errors:
            print(f"\n❌ Queries with Errors: {len(errors)}")
            for i, r in enumerate(errors[:5]):
                print(f"\n{i+1}. Query: {r.query[:100]}...")
                if r.error_postgres:
                    print(f"   PostgreSQL Error: {r.error_postgres}")
                if r.error_yugabyte:
                    print(f"   YugabyteDB Error: {r.error_yugabyte}")
        
        # EXPLAIN ANALYZE comparison
        print("\n" + "="*80)
        print("EXPLAIN ANALYZE COMPARISON")
        print("="*80)
        
        for i, r in enumerate(self.results[:3]):
            if r.postgres_explain and r.yugabyte_explain:
                print(f"\nQuery {i+1}: {r.query[:80]}...")
                print("\nPostgreSQL Plan:")
                print(r.postgres_explain[:500] + "..." if len(r.postgres_explain) > 500 else r.postgres_explain)
                print("\nYugabyteDB Plan:")
                print(r.yugabyte_explain[:500] + "..." if len(r.yugabyte_explain) > 500 else r.yugabyte_explain)

def main():
    """Run database comparison tests"""
    print("Database Query Comparison Tool")
    print("="*80)
    
    # Initialize comparator (using mock executors for now)
    comparator = DatabaseComparator()
    
    # Test with DML grammar
    from grammars.dml_unique import g as dml_grammar
    print("\nTesting DML Grammar...")
    comparator.compare_grammar_queries(dml_grammar, num_queries=20)
    
    # Test specific YugabyteDB features
    yugabyte_queries = [
        # INSERT ON CONFLICT
        """INSERT INTO users (email, name, status) 
           VALUES ('test@example.com', 'Test User', 'active')
           ON CONFLICT (email) DO UPDATE 
           SET name = EXCLUDED.name, updated_at = CURRENT_TIMESTAMP
           RETURNING *""",
        
        # CTE with RETURNING
        """WITH new_order AS (
               INSERT INTO orders (order_id, user_id, product_id, quantity, total, status)
               VALUES ('ORD999', 1, 1, 2, 2599.98, 'pending')
               ON CONFLICT (order_id) DO NOTHING
               RETURNING *
           )
           SELECT * FROM new_order""",
        
        # Multi-row UPSERT
        """INSERT INTO products (product_id, name, price, quantity)
           VALUES 
               ('PROD004', 'Monitor 4K', 599.99, 25),
               ('PROD005', 'Webcam HD', 79.99, 100)
           ON CONFLICT (product_id) DO UPDATE
           SET price = EXCLUDED.price,
               quantity = products.quantity + EXCLUDED.quantity""",
        
        # Complex UPDATE with subquery
        """UPDATE users SET status = 'premium'
           WHERE id IN (
               SELECT DISTINCT user_id 
               FROM orders 
               WHERE total > 1000 AND status = 'completed'
           )"""
    ]
    
    print("\n\nTesting YugabyteDB-specific features...")
    for query in yugabyte_queries:
        comparator.compare_query(query)
    
    # Generate final report
    comparator.generate_report()

if __name__ == "__main__":
    main()