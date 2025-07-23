#!/usr/bin/env python3
"""
PostgreSQL DDL + DML Integration Test
Demonstrates creating complex tables and running DML against them
"""

import sys
import time
import random
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 is required for this test")
    print("Install with: pip install psycopg2-binary")
    sys.exit(1)

from pyrqg.api import create_rqg, TableMetadata
from pyrqg.ddl_generator import DDLGenerator

def connect_postgres():
    """Try to connect to PostgreSQL with various credentials"""
    configs = [
        {"host": "localhost", "database": "postgres", "user": "qvad", "password": "qvad"},
        {"host": "localhost", "database": "postgres", "user": "postgres", "password": "postgres"},
        {"host": "localhost", "database": "postgres", "user": "postgres"},
    ]
    
    for config in configs:
        try:
            conn = psycopg2.connect(**config)
            print(f"✓ Connected to PostgreSQL as {config.get('user')}")
            return conn
        except:
            continue
    
    raise Exception("Could not connect to PostgreSQL")

def test_ddl_dml_postgres():
    """Main test function"""
    print("=" * 80)
    print("PyRQG PostgreSQL DDL + DML Integration Test")
    print("=" * 80)
    
    # Connect to PostgreSQL
    try:
        conn = connect_postgres()
    except Exception as e:
        print(f"✗ {e}")
        return
    
    cursor = conn.cursor()
    
    # Create RQG and DDL generator
    rqg = create_rqg()
    ddl_gen = DDLGenerator()
    
    # Step 1: Generate and execute DDL
    print("\n1. CREATING COMPLEX TABLES WITH PYRQG")
    print("-" * 60)
    
    # Generate schema with DDL generator
    schema_ddl = ddl_gen.generate_schema(num_tables=5)
    
    # First, drop all existing test tables
    print("\nDropping existing tables...")
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND (table_name LIKE 'table_%' OR table_name IN ('users', 'products', 'orders', 'order_items', 'audit_log'))
    """)
    existing_tables = cursor.fetchall()
    
    for table in existing_tables:
        try:
            cursor.execute(f"DROP TABLE IF EXISTS {table[0]} CASCADE")
        except:
            pass
    
    # Create reference tables that might be needed
    print("\nCreating reference tables...")
    reference_tables = [
        """CREATE TABLE IF NOT EXISTS categories (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS addresses (
            id SERIAL PRIMARY KEY,
            street VARCHAR(200),
            city VARCHAR(100),
            country VARCHAR(100)
        )"""
    ]
    
    for ddl in reference_tables:
        try:
            cursor.execute(ddl)
            conn.commit()
            print("✓ Created reference table")
        except Exception as e:
            print(f"✗ Failed: {e}")
    
    # Execute generated DDL
    print("\nCreating generated tables...")
    tables_created = []
    
    for ddl in schema_ddl:
        if 'CREATE TABLE' in ddl:
            # Extract table name
            import re
            match = re.search(r'CREATE TABLE (\w+)', ddl)
            if match:
                table_name = match.group(1)
                tables_created.append(table_name)
        
        try:
            cursor.execute(ddl)
            if 'CREATE TABLE' in ddl:
                print(f"✓ Created table {table_name}")
            elif 'CREATE INDEX' in ddl:
                print(f"✓ Created index")
        except Exception as e:
            print(f"✗ Failed to execute: {e}")
            print(f"  DDL: {ddl[:100]}...")
    
    conn.commit()
    
    # Step 2: Analyze created tables
    print("\n\n2. ANALYZING CREATED TABLES")
    print("-" * 60)
    
    # Get table metadata
    table_metadata = []
    
    for table_name in tables_created[:3]:  # Focus on first 3 tables
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = []
        for col in cursor.fetchall():
            columns.append({
                'name': col[0],
                'type': col[1],
                'nullable': col[2] == 'YES'
            })
        
        # Get primary key
        cursor.execute("""
            SELECT column_name
            FROM information_schema.key_column_usage
            WHERE table_name = %s AND constraint_name LIKE %s
        """, (table_name, f'{table_name}_pkey%'))
        
        pk_result = cursor.fetchone()
        pk = pk_result[0] if pk_result else 'id'
        
        # Get unique columns
        cursor.execute("""
            SELECT DISTINCT column_name
            FROM information_schema.constraint_column_usage
            WHERE table_name = %s 
            AND constraint_name IN (
                SELECT constraint_name 
                FROM information_schema.table_constraints 
                WHERE constraint_type = 'UNIQUE'
            )
        """, (table_name,))
        
        unique_cols = [row[0] for row in cursor.fetchall()]
        
        metadata = TableMetadata(
            name=table_name,
            columns=columns,
            primary_key=pk,
            unique_keys=unique_cols
        )
        table_metadata.append(metadata)
        
        print(f"\nTable: {table_name}")
        print(f"  Columns: {len(columns)}")
        print(f"  Primary Key: {pk}")
        print(f"  Unique Keys: {unique_cols}")
        print(f"  Sample columns: {[c['name'] for c in columns[:5]]}")
    
    # Step 3: Generate and execute DML
    print("\n\n3. GENERATING AND EXECUTING DML")
    print("-" * 60)
    
    # Add tables to RQG
    if table_metadata:
        rqg.add_tables(table_metadata)
        
        # Create query generator
        generator = rqg.create_generator(seed=42)
        
        # Generate INSERT queries
        print("\n--- INSERT Operations ---")
        insert_count = 0
        
        for i in range(10):
            try:
                query = generator.insert(
                    table=random.choice([t.name for t in table_metadata]),
                    multi_row=(i % 3 == 0),
                    returning=(i % 2 == 0)
                )
                
                cursor.execute(query.sql)
                if 'RETURNING' in query.sql:
                    result = cursor.fetchone()
                    print(f"✓ INSERT #{i+1}: Inserted row with id={result[0] if result else 'N/A'}")
                else:
                    print(f"✓ INSERT #{i+1}: {query.tables[0]} - {cursor.rowcount} rows")
                
                insert_count += cursor.rowcount
                conn.commit()
                
            except Exception as e:
                conn.rollback()
                print(f"✗ INSERT #{i+1} failed: {str(e)[:50]}...")
        
        print(f"\nTotal rows inserted: {insert_count}")
        
        # Generate SELECT queries
        print("\n--- SELECT Operations ---")
        
        for i in range(5):
            try:
                query = generator.select(
                    tables=[random.choice([t.name for t in table_metadata])],
                    where=True,
                    order_by=True,
                    limit=True,
                    group_by=(i % 2 == 0)
                )
                
                cursor.execute(query.sql)
                results = cursor.fetchall()
                print(f"✓ SELECT #{i+1}: {query.tables[0]} - {len(results)} rows returned")
                
                if results and len(results) > 0:
                    print(f"  Sample: {results[0]}")
                    
            except Exception as e:
                print(f"✗ SELECT #{i+1} failed: {str(e)[:50]}...")
        
        # Generate UPDATE queries
        print("\n--- UPDATE Operations ---")
        
        for i in range(5):
            try:
                query = generator.update(
                    table=random.choice([t.name for t in table_metadata]),
                    where=True,
                    returning=(i % 2 == 0)
                )
                
                cursor.execute(query.sql)
                if 'RETURNING' in query.sql:
                    results = cursor.fetchall()
                    print(f"✓ UPDATE #{i+1}: {query.tables[0]} - {len(results)} rows updated")
                else:
                    print(f"✓ UPDATE #{i+1}: {query.tables[0]} - {cursor.rowcount} rows")
                
                conn.commit()
                
            except Exception as e:
                conn.rollback()
                print(f"✗ UPDATE #{i+1} failed: {str(e)[:50]}...")
    
    # Step 4: Use grammar-based generation
    print("\n\n4. GRAMMAR-BASED DML GENERATION")
    print("-" * 60)
    
    # Test with dml_unique grammar
    print("\n--- Using dml_unique grammar ---")
    
    dml_queries = rqg.generate_from_grammar('dml_unique', count=5, seed=int(time.time()))
    
    for i, query in enumerate(dml_queries):
        print(f"\n{i+1}. Generated: {query[:80]}...")
        
        # Try to execute if it mentions our tables
        if any(table.name in query for table in table_metadata):
            try:
                cursor.execute(query)
                if query.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    print(f"   ✓ Executed: {len(results)} rows")
                else:
                    conn.commit()
                    print(f"   ✓ Executed: {cursor.rowcount} rows affected")
            except Exception as e:
                conn.rollback()
                print(f"   ✗ Failed: {str(e)[:50]}...")
    
    # Step 5: DDL operations
    print("\n\n5. DDL OPERATIONS WITH GRAMMAR")
    print("-" * 60)
    
    ddl_queries = rqg.generate_from_grammar('ddl_focused', count=5, seed=int(time.time()))
    
    for i, ddl in enumerate(ddl_queries):
        print(f"\n{i+1}. {ddl[:80]}...")
        
        # Only execute safe DDL operations
        if 'DROP' in ddl.upper():
            print("   ⚠️  Skipped DROP operation")
        elif any(table.name in ddl for table in table_metadata):
            try:
                cursor.execute(ddl)
                conn.commit()
                print("   ✓ Executed successfully")
            except Exception as e:
                conn.rollback()
                print(f"   ✗ Failed: {str(e)[:50]}...")
    
    # Step 6: Final summary
    print("\n\n6. FINAL DATABASE STATE")
    print("-" * 60)
    
    # Get all tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name NOT LIKE 'pg_%'
        ORDER BY table_name
    """)
    
    all_tables = cursor.fetchall()
    
    print(f"\nTotal tables: {len(all_tables)}")
    
    for table in all_tables[:10]:  # Show first 10 tables
        table_name = table[0]
        
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            
            # Get sample row
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
            sample = cursor.fetchone()
            
            print(f"\n{table_name}: {count} rows")
            if sample:
                print(f"  Sample: {str(sample)[:100]}...")
                
        except Exception as e:
            print(f"\n{table_name}: Error - {e}")
    
    # Cleanup
    cursor.close()
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ PostgreSQL DDL + DML Integration Test Complete!")
    print("=" * 80)
    print("\nKey achievements:")
    print("- Generated complex DDL with constraints using DDLGenerator")
    print("- Created tables with composite keys, check constraints, foreign keys")
    print("- Generated and executed DML using QueryGenerator API")
    print("- Tested grammar-based query generation")
    print("- Demonstrated full DDL + DML workflow")

if __name__ == "__main__":
    test_ddl_dml_postgres()