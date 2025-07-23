#!/usr/bin/env python3
"""
Integrated DDL and DML Test
Creates tables with PyRQG's DDL generator, then generates and executes DML
"""

import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from pyrqg.api import create_rqg
from pyrqg.ddl_generator import DDLGenerator, TableDefinition, ColumnDefinition, TableConstraint

def test_integrated_ddl_dml():
    """Test integrated DDL and DML generation"""
    print("=" * 80)
    print("PyRQG Integrated DDL + DML Test")
    print("=" * 80)
    
    # Use psycopg2 if available, otherwise sqlite
    try:
        import psycopg2
        use_postgres = True
        print("Using PostgreSQL for testing")
    except ImportError:
        import sqlite3
        use_postgres = False
        print("Using SQLite for testing")
    
    # Create RQG instance
    rqg = create_rqg()
    ddl_gen = DDLGenerator()
    
    # Step 1: Generate DDL using the DDL generator
    print("\n1. GENERATING DDL WITH COMPLEX CONSTRAINTS")
    print("-" * 60)
    
    # Get sample tables from DDL generator
    tables = ddl_gen.generate_sample_tables()[:3]  # users, products, orders
    
    # Generate DDL statements
    ddl_statements = []
    for table in tables:
        create_sql = ddl_gen.generate_create_table(table)
        ddl_statements.append(create_sql)
        
        print(f"\n-- {table.name} table:")
        print(create_sql[:200] + "..." if len(create_sql) > 200 else create_sql)
        
        # Add indexes
        for index in table.indexes[:2]:  # Just first 2 indexes
            index_sql = ddl_gen.generate_create_index(table.name, index)
            ddl_statements.append(index_sql)
    
    # Step 2: Create database and execute DDL
    print("\n\n2. CREATING DATABASE SCHEMA")
    print("-" * 60)
    
    if use_postgres:
        # PostgreSQL connection
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="postgres",
                user="qvad",
                password="qvad"
            )
            print("✓ Connected to PostgreSQL")
        except:
            print("✗ Failed to connect to PostgreSQL, falling back to SQLite")
            use_postgres = False
    
    if not use_postgres:
        import sqlite3
        conn = sqlite3.connect(':memory:')
        print("✓ Using SQLite in-memory database")
    
    cursor = conn.cursor()
    
    # Drop existing tables if any
    for table in reversed(tables):  # Reverse order for foreign keys
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table.name} CASCADE")
        except:
            pass
    
    # Create required referenced tables first
    print("\nCreating referenced tables...")
    
    # Create categories table (referenced by products)
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100)
            )
        """ if use_postgres else """
            CREATE TABLE IF NOT EXISTS categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(100)
            )
        """)
        print("✓ Created categories table")
    except Exception as e:
        print(f"✗ Failed to create categories: {e}")
    
    # Create addresses table (referenced by orders)
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS addresses (
                id SERIAL PRIMARY KEY,
                street VARCHAR(200),
                city VARCHAR(100),
                country VARCHAR(100)
            )
        """ if use_postgres else """
            CREATE TABLE IF NOT EXISTS addresses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                street VARCHAR(200),
                city VARCHAR(100),
                country VARCHAR(100)
            )
        """)
        print("✓ Created addresses table")
    except Exception as e:
        print(f"✗ Failed to create addresses: {e}")
    
    conn.commit()
    
    # Now create our main tables
    print("\nCreating main tables...")
    
    for i, ddl in enumerate(ddl_statements):
        if 'CREATE INDEX' in ddl:
            continue  # Skip indexes for now
        
        try:
            # Adapt DDL if using SQLite
            if not use_postgres:
                # Basic adaptations for SQLite
                ddl = ddl.replace('SERIAL', 'INTEGER PRIMARY KEY AUTOINCREMENT')
                ddl = ddl.replace('TIMESTAMP', 'DATETIME')
                ddl = ddl.replace('BOOLEAN', 'INTEGER')
                ddl = ddl.replace(' DEFERRABLE', '')
                # Remove ON DELETE/UPDATE clauses
                import re
                ddl = re.sub(r'ON DELETE \w+', '', ddl)
                ddl = re.sub(r'ON UPDATE \w+', '', ddl)
            
            cursor.execute(ddl)
            table_name = tables[i].name if i < len(tables) else f"table_{i}"
            print(f"✓ Created {table_name} table")
        except Exception as e:
            print(f"✗ Failed to create table: {e}")
            print(f"  DDL: {ddl[:100]}...")
    
    conn.commit()
    
    # Step 3: Generate and execute DML
    print("\n\n3. GENERATING AND EXECUTING DML")
    print("-" * 60)
    
    # First, insert some data into referenced tables
    print("\nInserting reference data...")
    try:
        cursor.execute("INSERT INTO categories (name) VALUES ('Electronics'), ('Books'), ('Clothing')")
        cursor.execute("INSERT INTO addresses (street, city, country) VALUES ('123 Main St', 'NYC', 'USA')")
        conn.commit()
        print("✓ Inserted reference data")
    except Exception as e:
        print(f"✗ Failed to insert reference data: {e}")
    
    # Generate DML using the dml_unique grammar
    print("\nGenerating DML queries...")
    
    # We'll generate specific queries for our tables
    dml_templates = [
        # Users table
        "INSERT INTO users (username, email, first_name, last_name, age, status) VALUES ('user_{i}', 'user_{i}@test.com', 'First_{i}', 'Last_{i}', {age}, 'active')",
        
        # Products table  
        "INSERT INTO products (sku, name, category_id, price, quantity, is_active, created_by) VALUES ('SKU_{i}', 'Product_{i}', 1, {price}, {qty}, true, 1)",
        
        # Orders table
        "INSERT INTO orders (order_date, order_number, customer_id, total_amount, status) VALUES (CURRENT_DATE, {i}, 1, {total}, 'pending')",
        
        # Select queries
        "SELECT * FROM users WHERE age > 18 AND status = 'active'",
        "SELECT p.name, p.price, c.name as category FROM products p JOIN categories c ON p.category_id = c.id WHERE p.is_active = true",
        "SELECT COUNT(*) as order_count, SUM(total_amount) as revenue FROM orders WHERE status != 'cancelled'",
        
        # Updates
        "UPDATE users SET status = 'inactive' WHERE age < 18",
        "UPDATE products SET price = price * 1.1 WHERE category_id = 1",
        "UPDATE orders SET status = 'completed' WHERE order_date < CURRENT_DATE - INTERVAL '7 days'"
    ]
    
    # Execute DML
    print("\nExecuting DML queries...")
    
    # First, ensure we have at least one user for foreign key constraints
    try:
        if use_postgres:
            cursor.execute("INSERT INTO users (username, email, first_name, last_name, status) VALUES ('admin', 'admin@test.com', 'Admin', 'User', 'active')")
        else:
            cursor.execute("INSERT INTO users (username, email, first_name, last_name, status) VALUES ('admin', 'admin@test.com', 'Admin', 'User', 'active')")
        conn.commit()
        print("✓ Created admin user")
    except Exception as e:
        print(f"✗ Failed to create admin user: {e}")
    
    # Insert some test data
    for i in range(5):
        # Users
        try:
            age = 20 + i * 5
            query = f"INSERT INTO users (username, email, first_name, last_name, age, status) VALUES ('user_{i}', 'user_{i}@test.com', 'First_{i}', 'Last_{i}', {age}, 'active')"
            cursor.execute(query)
            print(f"✓ Inserted user_{i}")
        except Exception as e:
            print(f"✗ Failed to insert user_{i}: {e}")
        
        # Products
        try:
            price = 10.99 + i * 5
            qty = 100 - i * 10
            query = f"INSERT INTO products (sku, name, category_id, price, quantity, is_active, created_by) VALUES ('SKU_{i}', 'Product_{i}', 1, {price}, {qty}, {'true' if use_postgres else '1'}, 1)"
            cursor.execute(query)
            print(f"✓ Inserted product_{i}")
        except Exception as e:
            print(f"✗ Failed to insert product_{i}: {e}")
    
    conn.commit()
    
    # Step 4: Generate complex DML using grammar
    print("\n\n4. GRAMMAR-BASED DML GENERATION")
    print("-" * 60)
    
    # Generate queries using different grammars
    grammars = ['workload_insert', 'workload_select', 'workload_update']
    
    for grammar in grammars:
        print(f"\n{grammar.upper()} queries:")
        queries = rqg.generate_from_grammar(grammar, count=3, seed=int(time.time()))
        
        for i, query in enumerate(queries):
            # Show query (truncated if long)
            print(f"\n{i+1}. {query[:100]}..." if len(query) > 100 else f"\n{i+1}. {query}")
            
            # Try to execute if it matches our schema
            if any(table in query.lower() for table in ['users', 'products', 'orders']):
                try:
                    # Basic adaptation for our schema
                    adapted_query = query
                    
                    # Skip if it references non-existent columns
                    if 'data' in query or 'metadata' in query or 'rating' in query:
                        print("   ⚠️  Skipped: references non-existent columns")
                        continue
                    
                    cursor.execute(adapted_query)
                    if 'select' in query.lower():
                        results = cursor.fetchall()
                        print(f"   ✓ Returned {len(results)} rows")
                    else:
                        conn.commit()
                        print(f"   ✓ Executed successfully")
                except Exception as e:
                    print(f"   ✗ Failed: {e}")
    
    # Step 5: Show final state
    print("\n\n5. FINAL DATABASE STATE")
    print("-" * 60)
    
    # Get all tables
    if use_postgres:
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    else:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        if table_name.startswith('sqlite_'):
            continue
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"\nTable {table_name}: {count} rows")
            
            # Show sample data
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
            rows = cursor.fetchall()
            for row in rows:
                print(f"  {row}")
        except Exception as e:
            print(f"\nTable {table_name}: Error - {e}")
    
    # Cleanup
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ Integrated DDL + DML Test Complete!")
    print("=" * 80)

def test_with_ddl_focused_grammar():
    """Test using DDL-focused grammar"""
    print("\n\n6. DDL-FOCUSED GRAMMAR TEST")
    print("=" * 80)
    
    rqg = create_rqg()
    
    # Generate various DDL statements
    ddl_types = [
        ('create_table', 'CREATE TABLE'),
        ('create_index', 'CREATE INDEX'),
        ('alter_table', 'ALTER TABLE'),
        ('create_view', 'CREATE VIEW')
    ]
    
    print("Generating DDL statements with grammar:")
    
    for rule, desc in ddl_types:
        print(f"\n{desc}:")
        sqls = rqg.generate_from_grammar('ddl_focused', rule=rule, count=2, seed=int(time.time()))
        
        for i, sql in enumerate(sqls):
            print(f"{i+1}. {sql}")

if __name__ == "__main__":
    test_integrated_ddl_dml()
    test_with_ddl_focused_grammar()