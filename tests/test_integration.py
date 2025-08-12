"""
Integration tests for PyRQG
Tests end-to-end functionality and real-world scenarios
"""

import pytest
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, repeat
from pyrqg.production.generator import ProductionQueryGenerator
from pyrqg.production.configs import ProductionConfig, billion_scale_config
from pyrqg.production.monitor import QueryMonitor
from pyrqg.api import app


@pytest.mark.integration
class TestEndToEndScenarios:
    """Test complete end-to-end scenarios"""
    
    def test_create_and_use_grammar(self):
        """Test creating a grammar and generating queries"""
        # Create a realistic e-commerce grammar
        g = Grammar("ecommerce")
        
        # Define schema
        g.define_tables(
            users=10000,
            products=5000,
            orders=50000,
            order_items=200000,
            categories=100,
            reviews=100000
        )
        
        g.define_fields(
            "id", "user_id", "product_id", "order_id",
            "name", "email", "price", "quantity", 
            "status", "created_at", "updated_at",
            "category_id", "rating", "comment"
        )
        
        # Define query patterns
        g.rule("query", choice(
            ref("analytical_query"),
            ref("transactional_query"),
            ref("reporting_query"),
            weights=[30, 50, 20]
        ))
        
        # Analytical queries
        g.rule("analytical_query", template("""
SELECT 
    {group_by} as dimension,
    COUNT(*) as count,
    AVG({metric}) as avg_metric,
    SUM({metric}) as total_metric
FROM {table}
WHERE {date_field} >= CURRENT_DATE - INTERVAL '{days} days'
GROUP BY {group_by}
ORDER BY count DESC
LIMIT {limit}""",
            group_by=choice("category_id", "user_id", "status", "DATE(created_at)"),
            metric=choice("price", "quantity", "rating"),
            table=choice("orders", "order_items", "reviews"),
            date_field=choice("created_at", "updated_at"),
            days=choice("7", "30", "90"),
            limit=choice("10", "20", "50")
        ))
        
        # Transactional queries
        g.rule("transactional_query", choice(
            template("SELECT * FROM {table} WHERE {pk} = {id}",
                table=ref("table_name"),
                pk=choice("id", "user_id", "product_id"),
                id=number(1, 10000)
            ),
            template("UPDATE {table} SET {field} = {value} WHERE {pk} = {id}",
                table=ref("table_name"),
                field=choice("status", "quantity", "price"),
                value=ref("field_value"),
                pk=choice("id", "order_id"),
                id=number(1, 10000)
            ),
            template("INSERT INTO {table} ({fields}) VALUES ({values})",
                table=ref("table_name"),
                fields=repeat(ref("field_name"), min=3, max=5, sep=", "),
                values=repeat(ref("field_value"), min=3, max=5, sep=", ")
            )
        ))
        
        # Helper rules
        g.rule("table_name", choice("users", "products", "orders", "order_items"))
        g.rule("field_name", choice("name", "email", "status", "price", "quantity"))
        g.rule("field_value", choice(
            number(1, 1000),
            "'active'",
            "'pending'",
            "'completed'",
            "CURRENT_TIMESTAMP"
        ))
        
        # Reporting queries
        g.rule("reporting_query", template("""
WITH daily_stats AS (
    SELECT 
        DATE(created_at) as date,
        COUNT(*) as daily_orders,
        SUM(price * quantity) as daily_revenue
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.id
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE(created_at)
)
SELECT 
    date,
    daily_orders,
    daily_revenue,
    AVG(daily_revenue) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as weekly_avg
FROM daily_stats
ORDER BY date DESC"""))
        
        # Generate and validate queries
        queries_generated = []
        for i in range(50):
            query = g.generate("query", seed=i)
            queries_generated.append(query)
            
            # Validate query has expected structure
            assert any(keyword in query.upper() for keyword in ["SELECT", "UPDATE", "INSERT", "WITH"])
            
        # Check variety
        unique_queries = set(queries_generated)
        assert len(unique_queries) >= 30, "Not enough query variety"
        
        # Check distribution (roughly)
        analytical_count = sum(1 for q in queries_generated if "GROUP BY" in q)
        transactional_count = sum(1 for q in queries_generated if "WHERE" in q and "GROUP BY" not in q)
        
        assert analytical_count > 10
        assert transactional_count > 15
        
    def test_production_workload_simulation(self):
        """Test simulating a production workload"""
        # Create configuration for mixed workload
        config = ProductionConfig(
            name="mixed_workload_test",
            grammars=["simple_dml", "subquery_dsl", "ddl_focused"],
            workload_distribution={
                "simple_dml": 0.7,      # 70% simple DML
                "subquery_dsl": 0.25,   # 25% complex queries
                "ddl_focused": 0.05     # 5% DDL
            },
            target_queries=1000,
            batch_size=100,
            parallel_generators=4
        )
        
        # Mock the grammars
        from unittest.mock import patch, MagicMock
        
        mock_grammars = {
            'simple_dml': MagicMock(generate=lambda **k: "SELECT * FROM users"),
            'subquery_dsl': MagicMock(generate=lambda **k: "SELECT * FROM (SELECT * FROM orders) sub"),
            'ddl_focused': MagicMock(generate=lambda **k: "CREATE TABLE test (id INT)")
        }
        
        with patch('pyrqg.production.generator.registered_grammars', mock_grammars):
            generator = ProductionQueryGenerator(config)
            monitor = QueryMonitor()
            
            # Simulate workload
            start_time = time.time()
            query_counts = {"SELECT": 0, "CREATE": 0}
            
            for batch in range(10):  # 10 batches of 100
                queries = generator.generate_batch(100)
                
                for query in queries:
                    # Simulate execution
                    execution_time = 0.001  # 1ms base
                    if "subquery" in query.lower():
                        execution_time = 0.005  # 5ms for complex
                    elif "CREATE" in query:
                        execution_time = 0.01   # 10ms for DDL
                        
                    monitor.record_query(query, execution_time * 1000)
                    
                    # Count query types
                    if "SELECT" in query:
                        query_counts["SELECT"] += 1
                    elif "CREATE" in query:
                        query_counts["CREATE"] += 1
                        
            end_time = time.time()
            duration = end_time - start_time
            
            # Verify results
            stats = monitor.get_stats()
            assert stats['total_queries'] == 1000
            assert stats['queries_per_second'] > 100  # Should be fast
            
            # Check workload distribution
            select_ratio = query_counts["SELECT"] / 1000
            create_ratio = query_counts["CREATE"] / 1000
            
            assert 0.9 < select_ratio < 1.0  # ~95% SELECT
            assert 0.0 < create_ratio < 0.1  # ~5% CREATE
            
    def test_concurrent_api_and_generation(self):
        """Test API serving queries while generating in background"""
        from unittest.mock import patch, MagicMock
        
        # Mock grammars
        mock_grammar = MagicMock()
        counter = 0
        
        def generate_with_counter(**kwargs):
            nonlocal counter
            counter += 1
            return f"QUERY_{counter}"
            
        mock_grammar.generate = generate_with_counter
        
        with patch('pyrqg.api.registered_grammars', {'test': mock_grammar}):
            # Start API client
            app.config['TESTING'] = True
            client = app.test_client()
            
            # Concurrent operations
            api_results = []
            generation_results = []
            
            def api_worker():
                for _ in range(20):
                    response = client.get('/api/generate/test')
                    if response.status_code == 200:
                        api_results.append(response.json['query'])
                        
            def generation_worker():
                config = ProductionConfig(
                    name="test",
                    grammars=["test"]
                )
                with patch('pyrqg.production.generator.registered_grammars', {'test': mock_grammar}):
                    generator = ProductionQueryGenerator(config)
                    for _ in range(20):
                        query = generator.generate_query()
                        generation_results.append(query)
                        
            # Run concurrently
            threads = []
            for _ in range(2):
                t1 = threading.Thread(target=api_worker)
                t2 = threading.Thread(target=generation_worker)
                threads.extend([t1, t2])
                t1.start()
                t2.start()
                
            for t in threads:
                t.join()
                
            # Verify both completed successfully
            assert len(api_results) == 40
            assert len(generation_results) == 40
            
            # All queries should be unique (due to counter)
            all_queries = api_results + generation_results
            assert len(set(all_queries)) == len(all_queries)
            
    def test_grammar_composition(self):
        """Test composing multiple grammars together"""
        # Create base grammars
        g1 = Grammar("base_tables")
        g1.rule("table", choice("users", "products", "orders"))
        g1.rule("field", choice("id", "name", "status"))
        
        g2 = Grammar("conditions")
        g2.rule("condition", choice(
            template("{field} = {value}", field=ref("field"), value=number(1, 100)),
            template("{field} LIKE '%test%'", field=ref("field")),
            template("{field} IN ({values})", 
                field=ref("field"), 
                values=repeat(number(1, 10), min=2, max=5, sep=", "))
        ))
        
        # Compose into complex grammar
        main = Grammar("composed")
        
        # Import rules from other grammars
        main.rules.update(g1.rules)
        main.rules.update(g2.rules)
        
        # Add composed rules
        main.rule("query", choice(
            template("SELECT * FROM {table} WHERE {condition}",
                table=ref("table"),
                condition=ref("condition")
            ),
            template("UPDATE {table} SET status = 'updated' WHERE {condition}",
                table=ref("table"),
                condition=ref("condition")
            )
        ))
        
        # Generate queries
        for i in range(10):
            query = main.generate("query", seed=i)
            
            # Should have elements from both grammars
            assert any(table in query for table in ["users", "products", "orders"])
            assert "WHERE" in query
            assert any(op in query for op in ["=", "LIKE", "IN"])
            
    def test_stress_testing_scenario(self):
        """Test stress testing with high concurrency"""
        from unittest.mock import patch, MagicMock
        
        # Create a grammar that generates varied queries
        mock_grammar = MagicMock()
        
        def generate_varied(**kwargs):
            import random
            seed = kwargs.get('seed', None)
            if seed:
                random.seed(seed)
                
            patterns = [
                "SELECT * FROM users WHERE id = {}",
                "INSERT INTO logs (message) VALUES ('{}')",
                "UPDATE metrics SET value = {} WHERE metric = 'qps'",
                "DELETE FROM temp_data WHERE created < NOW() - INTERVAL '1 hour'"
            ]
            
            pattern = random.choice(patterns)
            if '{}' in pattern:
                return pattern.format(random.randint(1, 1000))
            return pattern
            
        mock_grammar.generate = generate_varied
        
        with patch('pyrqg.production.generator.registered_grammars', {'stress': mock_grammar}):
            config = ProductionConfig(
                name="stress_test",
                grammars=["stress"],
                batch_size=1000,
                parallel_generators=8
            )
            
            generator = ProductionQueryGenerator(config)
            
            # Generate queries in parallel
            total_queries = []
            errors = []
            
            def worker(worker_id):
                try:
                    queries = generator.generate_batch(1000)
                    total_queries.extend(queries)
                except Exception as e:
                    errors.append(e)
                    
            # Run stress test
            start_time = time.time()
            
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(worker, i) for i in range(8)]
                for future in futures:
                    future.result()
                    
            duration = time.time() - start_time
            
            # Verify results
            assert len(errors) == 0, f"Errors during generation: {errors}"
            assert len(total_queries) == 8000
            
            # Performance check
            qps = len(total_queries) / duration
            assert qps > 1000, f"Too slow: {qps} queries/second"
            
            # Verify variety
            unique_patterns = set()
            for query in total_queries[:100]:  # Sample
                # Extract pattern
                for keyword in ["SELECT", "INSERT", "UPDATE", "DELETE"]:
                    if keyword in query:
                        unique_patterns.add(keyword)
                        
            assert len(unique_patterns) >= 3, "Not enough query variety"


@pytest.mark.integration
class TestRealWorldPatterns:
    """Test real-world usage patterns"""
    
    def test_time_series_workload(self):
        """Test time-series database workload"""
        g = Grammar("timeseries")
        
        # Time-series specific patterns
        g.rule("query", choice(
            ref("insert_metric"),
            ref("query_range"),
            ref("aggregate_window"),
            weights=[60, 30, 10]
        ))
        
        g.rule("insert_metric", template(
            "INSERT INTO metrics (timestamp, metric, value, tags) VALUES (NOW(), '{metric}', {value}, '{tags}')",
            metric=choice("cpu_usage", "memory_usage", "disk_io", "network_bytes"),
            value=number(0, 100),
            tags=choice("host=server1", "host=server2", "region=us-east")
        ))
        
        g.rule("query_range", template("""
SELECT 
    time_bucket('5 minutes', timestamp) as bucket,
    metric,
    AVG(value) as avg_value
FROM metrics
WHERE 
    timestamp >= NOW() - INTERVAL '{interval}'
    AND metric = '{metric}'
GROUP BY bucket, metric
ORDER BY bucket DESC""",
            interval=choice("1 hour", "6 hours", "1 day"),
            metric=choice("cpu_usage", "memory_usage")
        ))
        
        g.rule("aggregate_window", template("""
SELECT 
    metric,
    value,
    AVG(value) OVER (ORDER BY timestamp ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as moving_avg
FROM metrics
WHERE timestamp >= NOW() - INTERVAL '10 minutes'
ORDER BY timestamp DESC
LIMIT 100"""))
        
        # Simulate time-series workload
        insert_count = 0
        query_count = 0
        
        for i in range(100):
            query = g.generate("query", seed=i)
            if "INSERT" in query:
                insert_count += 1
            else:
                query_count += 1
                
        # Should be insert-heavy
        assert insert_count > 50
        assert query_count < 50
        
    def test_multi_tenant_workload(self):
        """Test multi-tenant application workload"""
        g = Grammar("multitenant")
        
        # Always include tenant isolation
        g.rule("query", template(
            "{base_query} AND tenant_id = {tenant}",
            base_query=choice(
                "SELECT * FROM customers WHERE status = 'active'",
                "SELECT COUNT(*) FROM orders WHERE created_at >= CURRENT_DATE",
                "UPDATE users SET last_login = NOW() WHERE id = 123"
            ),
            tenant=number(1, 100)
        ))
        
        # Verify tenant isolation
        for i in range(20):
            query = g.generate("query", seed=i)
            assert "tenant_id" in query, "Missing tenant isolation"
            
    def test_audit_log_workload(self):
        """Test audit logging workload"""
        g = Grammar("audit")
        
        g.rule("query", choice(
            ref("insert_audit"),
            ref("query_audit"),
            weights=[80, 20]  # Mostly inserts
        ))
        
        g.rule("insert_audit", template("""
INSERT INTO audit_log (
    timestamp, user_id, action, resource_type, resource_id, details
) VALUES (
    NOW(), {user}, '{action}', '{resource_type}', {resource_id}, '{details}'
)""",
            user=number(1, 1000),
            action=choice("CREATE", "UPDATE", "DELETE", "VIEW"),
            resource_type=choice("order", "product", "user", "payment"),
            resource_id=number(1, 10000),
            details=choice("Success", "Failed: Permission denied", "Failed: Not found")
        ))
        
        g.rule("query_audit", template("""
SELECT * FROM audit_log
WHERE 
    user_id = {user}
    AND timestamp >= NOW() - INTERVAL '{interval}'
    AND action = '{action}'
ORDER BY timestamp DESC
LIMIT 50""",
            user=number(1, 1000),
            interval=choice("1 hour", "1 day", "7 days"),
            action=choice("CREATE", "UPDATE", "DELETE")
        ))
        
        # Generate audit queries
        queries = [g.generate("query", seed=i) for i in range(50)]
        
        # Verify audit fields
        for query in queries:
            if "INSERT" in query:
                assert all(field in query for field in 
                          ["timestamp", "user_id", "action", "resource_type"])


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"])