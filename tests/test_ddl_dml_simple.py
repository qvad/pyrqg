#!/usr/bin/env python3
"""
Simple DDL + DML Test
Works with SQLite - demonstrates basic workflow
"""

import sys
import sqlite3
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from pyrqg.api import create_rqg, TableMetadata

def test_simple_ddl_dml():
    """Simple test that works with SQLite"""
    print("=" * 80)
    print("PyRQG Simple DDL + DML Test (SQLite)")
    print("=" * 80)
    
    # Create RQG instance
    rqg = create_rqg()
    
    # Create in-memory SQLite database
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    
    # Step 1: Use DDL grammar to create tables
    print("\n1. GENERATING DDL WITH GRAMMAR")
    print("-" * 60)
    
    # Generate simple CREATE TABLE statements
    create_tables = []
    for i in range(3):
        sql = rqg.generate_from_grammar('ddl_focused', rule='create_table', seed=i)[0]
        create_tables.append(sql)
        print(f"\nTable {i+1}:")
        print(sql)
    
    # Step 2: Execute DDL
    print("\n\n2. CREATING TABLES IN DATABASE")
    print("-" * 60)
    
    table_names = []
    
    for i, ddl in enumerate(create_tables):
        try:
            # Simple adaptations for SQLite
            ddl = ddl.replace('SERIAL PRIMARY KEY', 'INTEGER PRIMARY KEY AUTOINCREMENT')
            ddl = ddl.replace('SERIAL', 'INTEGER')
            ddl = ddl.replace('JSONB', 'TEXT')
            ddl = ddl.replace('UUID', 'TEXT')
            ddl = ddl.replace('BOOLEAN', 'INTEGER')
            ddl = ddl.replace('TIMESTAMP', 'DATETIME')
            
            cursor.execute(ddl)
            
            # Extract table name
            import re
            match = re.search(r'CREATE TABLE (\w+)', ddl)
            if match:
                table_name = match.group(1)
                table_names.append(table_name)
                print(f"✓ Created table: {table_name}")
                
        except Exception as e:
            print(f"✗ Failed to create table {i+1}: {e}")
    
    # Step 3: Analyze tables and create metadata
    print("\n\n3. ANALYZING TABLE STRUCTURE")
    print("-" * 60)
    
    table_metadata = []
    
    for table_name in table_names:
        # Get table info
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        
        columns = []
        pk_col = None
        
        for col in columns_info:
            col_name = col[1]
            col_type = col[2]
            is_pk = col[5] == 1
            
            columns.append({
                'name': col_name,
                'type': col_type.lower()
            })
            
            if is_pk:
                pk_col = col_name
        
        metadata = TableMetadata(
            name=table_name,
            columns=columns,
            primary_key=pk_col or 'id'
        )
        table_metadata.append(metadata)
        
        print(f"\nTable: {table_name}")
        print(f"  Columns: {[c['name'] for c in columns]}")
        print(f"  Primary Key: {metadata.primary_key}")
    
    # Step 4: Generate and execute DML
    print("\n\n4. GENERATING AND EXECUTING DML")
    print("-" * 60)
    
    if table_metadata:
        # Add tables to RQG
        rqg.add_tables(table_metadata)
        
        # Create query generator
        generator = rqg.create_generator(seed=42)
        
        print("\n--- INSERT Operations ---")
        
        for i in range(5):
            table = table_metadata[i % len(table_metadata)]
            
            try:
                query = generator.insert(table=table.name)
                
                # Remove RETURNING clause for SQLite
                sql = query.sql.replace(' RETURNING *', '')
                
                cursor.execute(sql)
                conn.commit()
                print(f"✓ INSERT into {table.name}: 1 row")
                
            except Exception as e:
                print(f"✗ INSERT failed: {e}")
        
        print("\n--- SELECT Operations ---")
        
        for i in range(5):
            table = table_metadata[i % len(table_metadata)]
            
            try:
                query = generator.select(
                    tables=[table.name],
                    where=True,
                    order_by=True,
                    limit=True
                )
                
                cursor.execute(query.sql)
                results = cursor.fetchall()
                print(f"✓ SELECT from {table.name}: {len(results)} rows")
                
                if results:
                    print(f"  Sample: {results[0]}")
                    
            except Exception as e:
                print(f"✗ SELECT failed: {e}")
        
        print("\n--- UPDATE Operations ---")
        
        for i in range(3):
            table = table_metadata[i % len(table_metadata)]
            
            try:
                query = generator.update(table=table.name, where=True)
                
                # Remove RETURNING clause
                sql = query.sql.replace(' RETURNING *', '')
                
                cursor.execute(sql)
                conn.commit()
                print(f"✓ UPDATE {table.name}: {cursor.rowcount} rows affected")
                
            except Exception as e:
                print(f"✗ UPDATE failed: {e}")
    
    # Step 5: Complex operations with grammar
    print("\n\n5. GRAMMAR-BASED QUERY GENERATION")
    print("-" * 60)
    
    # Generate queries for specific workloads
    workloads = ['workload_insert', 'workload_select', 'workload_update']
    
    for workload in workloads:
        print(f"\n--- {workload} ---")
        
        queries = rqg.generate_from_grammar(workload, count=2, seed=100+workloads.index(workload))
        
        for i, query in enumerate(queries):
            print(f"{i+1}. {query[:80]}..." if len(query) > 80 else f"{i+1}. {query}")
            
            # Try to execute if it matches our tables
            if any(table.name in query for table in table_metadata):
                try:
                    # Basic adaptation
                    adapted = query.replace(' RETURNING *', '')
                    adapted = adapted.replace(' RETURNING', '')
                    
                    cursor.execute(adapted)
                    
                    if 'SELECT' in adapted.upper():
                        results = cursor.fetchall()
                        print(f"   ✓ Executed: {len(results)} rows")
                    else:
                        conn.commit()
                        print(f"   ✓ Executed: {cursor.rowcount} rows affected")
                        
                except Exception as e:
                    print(f"   ✗ Failed: {str(e)[:50]}...")
    
    # Step 6: DDL modifications
    print("\n\n6. DDL MODIFICATIONS")
    print("-" * 60)
    
    # Generate ALTER TABLE statements
    alter_statements = rqg.generate_from_grammar('ddl_focused', rule='alter_table', count=3, seed=200)
    
    for i, alter in enumerate(alter_statements):
        print(f"\n{i+1}. {alter}")
        
        # Try to execute if it's for our tables
        if any(table.name in alter for table in table_metadata):
            try:
                # SQLite has limited ALTER TABLE support
                if 'ADD COLUMN' in alter:
                    # Simplify constraints for SQLite
                    alter = alter.replace(' UNIQUE', '')
                    alter = alter.replace(' CHECK', '')
                    
                    cursor.execute(alter)
                    conn.commit()
                    print("   ✓ Executed successfully")
                else:
                    print("   ⚠️  Skipped: SQLite doesn't support this ALTER type")
                    
            except Exception as e:
                print(f"   ✗ Failed: {e}")
    
    # Step 7: Final summary
    print("\n\n7. FINAL DATABASE STATE")
    print("-" * 60)
    
    # Show all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = cursor.fetchall()
    
    print(f"\nTotal tables: {len(all_tables)}")
    
    for table in all_tables:
        table_name = table[0]
        
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        sample = cursor.fetchone()
        
        print(f"\n{table_name}: {count} rows")
        if sample:
            print(f"  Sample: {sample}")
    
    # Close connection
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ Simple DDL + DML Test Complete!")
    print("=" * 80)
    print("\nDemonstrated:")
    print("- DDL generation with ddl_focused grammar")
    print("- Table creation and analysis")
    print("- DML generation with QueryGenerator API")
    print("- Workload-specific query generation")
    print("- DDL modifications (ALTER TABLE)")

if __name__ == "__main__":
    test_simple_ddl_dml()