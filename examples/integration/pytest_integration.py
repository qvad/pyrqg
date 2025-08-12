#!/usr/bin/env python3
"""
pytest_integration.py - PyRQG Integration with pytest

This example shows how to use PyRQG in pytest for database testing:
- Generating test queries
- Property-based testing with queries
- Database fixture setup
- Query execution testing
- Performance benchmarking

To run:
    pytest pytest_integration.py -v
"""

import pytest
import psycopg2
import time
from typing import List, Optional
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.api import RQG
from pyrqg.dsl.core import Grammar, choice, template, number


# ==================== Fixtures ====================

@pytest.fixture(scope="session")
def database_url():
    """Database connection URL."""
    return os.environ.get("TEST_DATABASE_URL", "postgresql://localhost/test_db")


@pytest.fixture(scope="session")
def db_connection(database_url):
    """Create database connection for tests."""
    conn = psycopg2.connect(database_url)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def setup_test_schema(db_connection):
    """Setup test schema."""
    cur = db_connection.cursor()
    
    # Create test tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) UNIQUE,
            status VARCHAR(50) DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            price DECIMAL(10,2) CHECK (price >= 0),
            stock INTEGER DEFAULT 0,
            category VARCHAR(100)
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES test_users(id),
            total DECIMAL(10,2),
            status VARCHAR(50) DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Insert test data
    cur.execute("""
        INSERT INTO test_users (name, email) VALUES
        ('Test User 1', 'user1@test.com'),
        ('Test User 2', 'user2@test.com'),
        ('Test User 3', 'user3@test.com')
        ON CONFLICT (email) DO NOTHING
    """)
    
    cur.execute("""
        INSERT INTO test_products (name, price, stock, category) VALUES
        ('Product A', 10.99, 100, 'electronics'),
        ('Product B', 25.50, 50, 'books'),
        ('Product C', 5.00, 200, 'electronics')
        ON CONFLICT DO NOTHING
    """)
    
    db_connection.commit()
    
    yield
    
    # Cleanup (optional)
    # cur.execute("DROP TABLE IF EXISTS test_orders, test_products, test_users CASCADE")
    # db_connection.commit()


@pytest.fixture
def query_generator():
    """Create PyRQG query generator."""
    rqg = RQG()
    
    # Add test grammar
    grammar = Grammar("test_queries")
    
    # Define test tables
    grammar.define_tables(
        test_users=100,
        test_products=50,
        test_orders=500
    )
    
    # SELECT queries
    grammar.rule("select_query", choice(
        template("SELECT * FROM {table}"),
        template("SELECT * FROM {table} WHERE {condition}"),
        template("SELECT COUNT(*) FROM {table}"),
        template("SELECT {columns} FROM {table} ORDER BY {order_column} LIMIT {limit}")
    ))
    
    grammar.rule("table", choice("test_users", "test_products", "test_orders"))
    grammar.rule("condition", choice(
        template("id = {id}"),
        template("status = '{status}'"),
        template("price > {price}"),
        template("created_at > CURRENT_DATE - INTERVAL '{days} days'")
    ))
    
    grammar.rule("columns", choice("id, name", "id, email", "id, price"))
    grammar.rule("order_column", choice("id", "created_at", "name"))
    grammar.rule("limit", choice(10, 50, 100))
    
    grammar.rule("id", number(1, 100))
    grammar.rule("status", choice("active", "pending", "completed"))
    grammar.rule("price", number(1, 100))
    grammar.rule("days", number(1, 30))
    
    # INSERT queries
    grammar.rule("insert_query", choice(
        template("INSERT INTO test_users (name, email) VALUES ('{name}', '{email}')"),
        template("INSERT INTO test_products (name, price, stock) VALUES ('{product}', {price}, {stock})"),
        template("INSERT INTO test_orders (user_id, total) VALUES ({user_id}, {total})")
    ))
    
    grammar.rule("name", choice("Test User", "John Doe", "Jane Smith"))
    grammar.rule("email", template("{name}_{id}@test.com"))
    grammar.rule("product", choice("Widget", "Gadget", "Tool"))
    grammar.rule("stock", number(0, 1000))
    grammar.rule("user_id", number(1, 3))
    grammar.rule("total", number(10, 1000))
    
    # Main query rule
    grammar.rule("query", choice(
        ref("select_query"),
        ref("insert_query"),
        weights=[80, 20]
    ))
    
    # Register grammar
    rqg.grammars["test_queries"] = grammar
    
    return rqg


# ==================== Basic Query Tests ====================

class TestBasicQueries:
    """Test basic query generation and execution."""
    
    def test_generate_select_queries(self, query_generator):
        """Test SELECT query generation."""
        queries = []
        for i in range(10):
            query = query_generator.generate_query("test_queries", seed=i)
            queries.append(query)
            
            # Verify it's a valid SQL query
            assert query.strip()
            assert any(keyword in query.upper() for keyword in ["SELECT", "INSERT"])
        
        # Verify diversity
        unique_queries = set(queries)
        assert len(unique_queries) >= 5  # At least 5 different queries
    
    def test_execute_generated_queries(self, query_generator, db_connection, setup_test_schema):
        """Test executing generated queries."""
        cur = db_connection.cursor()
        
        success_count = 0
        error_count = 0
        
        for i in range(20):
            query = query_generator.generate_query("test_queries", seed=i*10)
            
            try:
                cur.execute(query)
                
                # Fetch results for SELECT
                if query.upper().startswith("SELECT"):
                    results = cur.fetchall()
                    assert results is not None
                else:
                    # Rollback INSERTs to keep test data stable
                    db_connection.rollback()
                
                success_count += 1
                
            except Exception as e:
                error_count += 1
                print(f"Query failed: {query}")
                print(f"Error: {e}")
        
        # Most queries should succeed
        assert success_count >= 18  # 90% success rate
    
    def test_query_results_validation(self, query_generator, db_connection, setup_test_schema):
        """Test that query results are valid."""
        cur = db_connection.cursor()
        
        # Generate COUNT queries
        count_queries = []
        for i in range(5):
            query = query_generator.generate_query("test_queries", seed=i*100)
            if "COUNT(*)" in query:
                count_queries.append(query)
        
        for query in count_queries:
            cur.execute(query)
            result = cur.fetchone()
            
            # COUNT should return non-negative integer
            assert result is not None
            assert isinstance(result[0], int)
            assert result[0] >= 0


# ==================== Property-Based Testing ====================

class TestPropertyBased:
    """Property-based tests using generated queries."""
    
    @pytest.mark.parametrize("seed", range(50))
    def test_query_syntax_valid(self, query_generator, seed):
        """Property: All generated queries have valid SQL syntax."""
        query = query_generator.generate_query("test_queries", seed=seed)
        
        # Basic syntax checks
        assert query.strip()
        assert not query.endswith(",")
        assert query.count("(") == query.count(")")
        assert query.count("'") % 2 == 0  # Quotes are paired
    
    @pytest.mark.parametrize("seed", range(20))
    def test_select_queries_return_data(self, query_generator, db_connection, setup_test_schema, seed):
        """Property: SELECT queries always return a result set."""
        query = query_generator.generate_query("test_queries", seed=seed*7)
        
        if query.upper().startswith("SELECT"):
            cur = db_connection.cursor()
            cur.execute(query)
            
            # Should always return a result (even if empty)
            result = cur.fetchall()
            assert result is not None
            assert isinstance(result, list)
    
    def test_idempotent_select_queries(self, query_generator, db_connection, setup_test_schema):
        """Property: SELECT queries are idempotent."""
        cur = db_connection.cursor()
        
        # Find SELECT queries
        for seed in range(100):
            query = query_generator.generate_query("test_queries", seed=seed)
            
            if query.upper().startswith("SELECT") and "COUNT" not in query:
                # Execute twice
                cur.execute(query)
                result1 = cur.fetchall()
                
                cur.execute(query)
                result2 = cur.fetchall()
                
                # Results should be identical
                assert result1 == result2
                break


# ==================== Performance Testing ====================

class TestPerformance:
    """Performance benchmarking with generated queries."""
    
    def test_query_generation_performance(self, query_generator, benchmark):
        """Benchmark query generation speed."""
        def generate_queries():
            queries = []
            for i in range(100):
                query = query_generator.generate_query("test_queries", seed=i)
                queries.append(query)
            return queries
        
        # Use pytest-benchmark if available
        result = benchmark(generate_queries)
        assert len(result) == 100
    
    def test_query_execution_performance(self, query_generator, db_connection, setup_test_schema):
        """Test query execution performance."""
        cur = db_connection.cursor()
        
        execution_times = []
        
        for i in range(50):
            query = query_generator.generate_query("test_queries", seed=i*3)
            
            if query.upper().startswith("SELECT"):
                start_time = time.time()
                cur.execute(query)
                cur.fetchall()
                execution_time = time.time() - start_time
                
                execution_times.append(execution_time)
        
        # Calculate statistics
        avg_time = sum(execution_times) / len(execution_times)
        max_time = max(execution_times)
        
        # Performance assertions
        assert avg_time < 0.1  # Average under 100ms
        assert max_time < 0.5  # No query over 500ms


# ==================== Integration Scenarios ====================

class TestIntegrationScenarios:
    """Complex integration test scenarios."""
    
    def test_transaction_workflow(self, query_generator, db_connection, setup_test_schema):
        """Test transaction workflow with generated queries."""
        cur = db_connection.cursor()
        
        try:
            # Start transaction
            cur.execute("BEGIN")
            
            # Insert user
            insert_user = "INSERT INTO test_users (name, email) VALUES ('TX User', 'tx@test.com') RETURNING id"
            cur.execute(insert_user)
            user_id = cur.fetchone()[0]
            
            # Insert order for user
            insert_order = f"INSERT INTO test_orders (user_id, total) VALUES ({user_id}, 99.99)"
            cur.execute(insert_order)
            
            # Verify with SELECT
            verify_query = f"SELECT COUNT(*) FROM test_orders WHERE user_id = {user_id}"
            cur.execute(verify_query)
            count = cur.fetchone()[0]
            
            assert count == 1
            
            # Rollback to keep test data clean
            cur.execute("ROLLBACK")
            
        except Exception as e:
            cur.execute("ROLLBACK")
            raise e
    
    def test_concurrent_query_safety(self, query_generator, db_connection, setup_test_schema):
        """Test concurrent query execution safety."""
        import threading
        
        errors = []
        
        def execute_queries(thread_id):
            conn = psycopg2.connect(db_connection.dsn)
            cur = conn.cursor()
            
            try:
                for i in range(10):
                    query = query_generator.generate_query("test_queries", seed=thread_id*100+i)
                    
                    if query.upper().startswith("SELECT"):
                        cur.execute(query)
                        cur.fetchall()
                    
            except Exception as e:
                errors.append((thread_id, str(e)))
            
            finally:
                conn.close()
        
        # Run queries in multiple threads
        threads = []
        for i in range(5):
            t = threading.Thread(target=execute_queries, args=(i,))
            threads.append(t)
            t.start()
        
        # Wait for completion
        for t in threads:
            t.join()
        
        # Should have no errors
        assert len(errors) == 0


# ==================== Custom Test Grammar ====================

def test_custom_grammar_integration(db_connection, setup_test_schema):
    """Test integration with custom grammar."""
    
    # Create custom grammar for specific test case
    grammar = Grammar("custom_test")
    
    # Aggregation queries
    grammar.rule("agg_query", template(
        "SELECT {agg_func}({column}) FROM {table} GROUP BY {group_column}"
    ))
    
    grammar.rule("agg_func", choice("COUNT", "SUM", "AVG", "MAX", "MIN"))
    grammar.rule("column", choice("id", "price", "stock"))
    grammar.rule("table", "test_products")
    grammar.rule("group_column", "category")
    
    # Generate and test
    cur = db_connection.cursor()
    
    for i in range(5):
        query = grammar.generate("agg_query", seed=i)
        
        cur.execute(query)
        results = cur.fetchall()
        
        # Should return results grouped by category
        assert len(results) > 0
        for row in results:
            assert len(row) >= 1  # At least the aggregated value


# ==================== Pytest Configuration ====================

if __name__ == "__main__":
    # Can be run directly with pytest
    pytest.main([__file__, "-v"])