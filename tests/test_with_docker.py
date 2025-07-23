#!/usr/bin/env python3
"""
Test PyRQG with Docker containers for PostgreSQL and YugabyteDB
This script provides instructions and code for running comparison tests
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

print("""
PostgreSQL and YugabyteDB Comparison Test Setup
===============================================

To run comparison tests between PostgreSQL and YugabyteDB, follow these steps:

1. Start PostgreSQL container:
   docker run -d --name postgres-test \\
     -e POSTGRES_PASSWORD=postgres \\
     -e POSTGRES_DB=testdb \\
     -p 5432:5432 \\
     postgres:15

2. Start YugabyteDB container:
   docker run -d --name yugabyte-test \\
     -p 7000:7000 -p 9000:9000 -p 5433:5433 \\
     -p 9042:9042 \\
     yugabytedb/yugabyte:latest \\
     bin/yugabyted start --daemon=false

3. Wait for containers to be ready (about 30 seconds)

4. Install psycopg2 or psycopg:
   pip install psycopg2-binary
   # or
   pip install psycopg[binary]

5. Run the actual comparison test:
   python test_db_comparison_live.py

The comparison will:
- Generate queries using PyRQG grammars
- Execute them on both databases
- Compare results for consistency
- Show EXPLAIN ANALYZE output
- Report any differences or errors
""")

# Create the actual test script
test_script = '''#!/usr/bin/env python3
"""
Live database comparison test using PostgreSQL and YugabyteDB
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False
    print("psycopg2 not available. Install with: pip install psycopg2-binary")

from pyrqg.executor import Executor
from db_comparator import DatabaseComparator

class PostgreSQLExecutor(Executor):
    """PostgreSQL executor using psycopg2"""
    
    def __init__(self, host="localhost", port=5432, database="testdb", 
                 user="postgres", password="postgres"):
        super().__init__()
        self.connection_params = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password
        }
        self.conn = None
        self.connect()
    
    def connect(self):
        """Establish database connection"""
        if HAS_PSYCOPG2:
            self.conn = psycopg2.connect(**self.connection_params)
            self.conn.autocommit = True
    
    def execute(self, query: str, params: dict):
        """Execute a query"""
        if not self.conn:
            raise Exception("No database connection")
        
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            
            # Handle different query types
            if query.strip().upper().startswith(("SELECT", "WITH", "EXPLAIN")):
                return cursor.fetchall()
            elif query.strip().upper().startswith(("INSERT", "UPDATE", "DELETE")):
                if "RETURNING" in query.upper():
                    return cursor.fetchall()
                else:
                    return cursor.rowcount
            else:
                return None
        finally:
            cursor.close()

class YugabyteDBExecutor(PostgreSQLExecutor):
    """YugabyteDB executor (uses PostgreSQL protocol)"""
    
    def __init__(self):
        super().__init__(port=5433)  # YugabyteDB PostgreSQL port

def wait_for_db(executor, name, max_attempts=30):
    """Wait for database to be ready"""
    print(f"Waiting for {name} to be ready...")
    for i in range(max_attempts):
        try:
            executor.execute("SELECT 1", {})
            print(f"{name} is ready!")
            return True
        except Exception as e:
            print(f"  Attempt {i+1}/{max_attempts}: {str(e)[:50]}...")
            time.sleep(2)
    return False

def main():
    """Run live database comparison"""
    if not HAS_PSYCOPG2:
        print("Cannot run live tests without psycopg2")
        return
    
    print("Starting PostgreSQL and YugabyteDB Comparison")
    print("=" * 80)
    
    try:
        # Create executors
        pg_executor = PostgreSQLExecutor()
        yb_executor = YugabyteDBExecutor()
        
        # Wait for databases
        if not wait_for_db(pg_executor, "PostgreSQL"):
            print("PostgreSQL not available. Is the container running?")
            return
        
        if not wait_for_db(yb_executor, "YugabyteDB"):
            print("YugabyteDB not available. Is the container running?")
            return
        
        # Run comparison
        comparator = DatabaseComparator(pg_executor, yb_executor)
        
        # Test different grammars
        from grammars.dml_unique import g as dml_grammar
        from grammars.yugabyte.transactions_postgres import g as txn_grammar
        
        print("\\nTesting DML Grammar...")
        comparator.compare_grammar_queries(dml_grammar, num_queries=25)
        
        print("\\nTesting Transaction Grammar...")
        comparator.compare_grammar_queries(txn_grammar, num_queries=15)
        
        # Generate report
        comparator.generate_report()
        
    except Exception as e:
        print(f"Error during comparison: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
'''

# Write the live test script
with open("test_db_comparison_live.py", "w") as f:
    f.write(test_script)

print("\nCreated test_db_comparison_live.py for running live database comparisons.")
print("\nWithout Docker, you can still run the mock comparison test:")
print("  python3 db_comparator.py")