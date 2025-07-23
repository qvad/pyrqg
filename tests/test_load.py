import pytest
import os
import sys
import time
import psycopg2
from pyrqg.api import create_rqg
from pyrqg.core.execution import WorkloadExecutor
from pyrqg.dsl.core import Context

@pytest.mark.skipif(not os.environ.get("PYRQG_DSN"), reason="PYRQG_DSN not set")
def test_run_1m_queries():
    """
    Performance/Load test: Runs 1,000,000 queries against the database defined in PYRQG_DSN.
    
    Steps:
    1. Connects to DB.
    2. Initializes a fresh schema (drops/recreates tables).
    3. Introspects the schema.
    4. Runs 1M queries using real_workload grammar.
    """
    dsn = os.environ["PYRQG_DSN"]
    count = 1_000_000
    grammar_name = "real_workload"
    
    print(f"\n[Load Test] Target DSN: {dsn}")
    print(f"[Load Test] Target Count: {count:,}")
    
    # 1. Initialize Schema
    rqg = create_rqg()
    
    # We need a connection to execute DDL
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    
    # Generate DDL for default schema (more tables = more query diversity)
    print("[Load Test] Generating schema...")
    ddl_stmts = rqg.generate_complex_ddl(num_tables=15)
    
    # Clean up existing tables (simplistic approach for test)
    # A real robust cleanup might differ, but here we just try to drop tables if they exist
    # or rely on the fact that we are generating new random tables if names are unique.
    # However, generate_complex_ddl creates standard tables (users, products, etc) plus random ones.
    # Let's just run the DDL.
    
    with conn.cursor() as cur:
        # Drop standard tables to ensure fresh start
        standard_tables = ["audit_log", "order_items", "orders", "products", "addresses", "categories", "users"]
        for t in standard_tables:
            cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
            
        print("[Load Test] Applying DDL...")
        for stmt in ddl_stmts:
            try:
                cur.execute(stmt)
            except Exception as e:
                print(f"Warning during DDL: {e}")
    
    conn.close()
    
    # 2. Introspect Schema
    print("[Load Test] Introspecting schema...")
    # Context now handles schema introspection via dsn parameter
    ctx = Context(dsn=dsn, seed=42)
    print(f"[Load Test] Found {len(ctx.tables)} tables.")
    assert len(ctx.tables) > 0, "Schema introspection failed to find tables"
    
    # 3. Execute Workload
    print(f"[Load Test] Executing {count} queries with 10 threads...")
    start_time = time.time()
    
    executor = WorkloadExecutor(dsn=dsn, threads=10)
    stats = executor.run(
        rqg=rqg,
        grammar_name=grammar_name,
        count=count,
        seed=42,
        context=ctx,
        progress_interval=10000  # Update every 10k queries
    )
    
    duration = time.time() - start_time
    print(f"\n[Load Test] Finished in {duration:.2f}s")
    print(f"  Total:   {stats.total}")
    print(f"  Success: {stats.success}")
    print(f"  Failed:  {stats.failed}")
    print(f"  QPS:     {stats.total / duration:.2f}")
    
    # Context is a dataclass, no explicit close needed

    # Assertions
    assert stats.total == count, f"Expected {count} queries, ran {stats.total}"
    assert stats.success > 0, "Zero queries succeeded"
    # We allow some failures in a fuzzer, but maybe we want to cap the error rate?
    # For now, just ensuring it runs to completion is the goal.
