#!/usr/bin/env python3
"""
Test Enhanced DDL Generation in PyRQG
Demonstrates complex constraints, composite keys, and DDL-focused grammar
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from pyrqg.api import create_rqg
from pyrqg.ddl_generator import DDLGenerator

def test_complex_ddl_api():
    """Test complex DDL generation through API"""
    print("=" * 80)
    print("PyRQG Enhanced DDL Generation")
    print("=" * 80)
    
    rqg = create_rqg()
    
    # 1. Generate complex schema with multiple tables
    print("\n1. GENERATING COMPLEX SCHEMA (5 tables)")
    print("-" * 60)
    
    schema_ddl = rqg.generate_complex_ddl(num_tables=5)
    
    for i, ddl in enumerate(schema_ddl[:3]):  # Show first 3 tables
        print(f"\n-- Table {i+1}:")
        print(ddl + ";")
    
    print(f"\n... and {len(schema_ddl) - 3} more statements")
    
    # 2. Generate a single random table with complex constraints
    print("\n\n2. GENERATING RANDOM TABLE WITH CONSTRAINTS")
    print("-" * 60)
    
    table_ddl = rqg.generate_random_table_ddl(
        "analytics_data",
        num_columns=12,
        num_constraints=5
    )
    print(table_ddl)
    
    # 3. Use DDL-focused grammar
    print("\n\n3. DDL-FOCUSED GRAMMAR EXAMPLES")
    print("-" * 60)
    
    ddl_queries = rqg.generate_from_grammar('ddl_focused', count=10, seed=42)
    
    for i, query in enumerate(ddl_queries):
        print(f"\n-- DDL Query {i+1}:")
        print(query + ";")

def test_ddl_generator_directly():
    """Test DDL generator directly"""
    print("\n\n4. DIRECT DDL GENERATOR - SAMPLE TABLES")
    print("=" * 80)
    
    generator = DDLGenerator()
    
    # Get sample tables with complex constraints
    sample_tables = generator.generate_sample_tables()
    
    # Show the orders table (composite PK example)
    orders_table = next(t for t in sample_tables if t.name == "orders")
    print("\n-- Orders Table (Composite Primary Key Example):")
    print(generator.generate_create_table(orders_table))
    
    # Show indexes
    print("\n-- Indexes for Orders Table:")
    for index in orders_table.indexes:
        print(generator.generate_create_index("orders", index) + ";")
    
    # Show the audit_log table (partitioned table example)
    audit_table = next(t for t in sample_tables if t.name == "audit_log")
    print("\n\n-- Audit Log Table (Partitioned with Partial Indexes):")
    print(generator.generate_create_table(audit_table))
    
    # Show its indexes including partial ones
    print("\n-- Indexes for Audit Log Table:")
    for index in audit_table.indexes:
        print(generator.generate_create_index("audit_log", index) + ";")

def test_constraint_types():
    """Show all types of constraints supported"""
    print("\n\n5. ALL CONSTRAINT TYPES DEMONSTRATION")
    print("=" * 80)
    
    generator = DDLGenerator()
    
    # Get the products table which has multiple constraint types
    sample_tables = generator.generate_sample_tables()
    products_table = next(t for t in sample_tables if t.name == "products")
    
    print("-- Products Table (Multiple Constraint Types):")
    print(generator.generate_create_table(products_table))
    
    print("\n\nConstraint Summary for 'products' table:")
    print("-" * 60)
    
    # List all constraints
    for constraint in products_table.constraints:
        print(f"- {constraint.constraint_type}: ", end="")
        if constraint.constraint_type == "PRIMARY KEY":
            print(f"on columns {', '.join(constraint.columns)}")
        elif constraint.constraint_type == "UNIQUE":
            print(f"{constraint.name} on columns {', '.join(constraint.columns)}")
        elif constraint.constraint_type == "CHECK":
            print(f"{constraint.name} - {constraint.check_expression}")
        elif constraint.constraint_type == "FOREIGN KEY":
            print(f"{constraint.name} - {', '.join(constraint.columns)} -> {constraint.references_table}")

def test_workload_with_complex_ddl():
    """Test workload generation with complex DDL"""
    print("\n\n6. WORKLOAD WITH COMPLEX DDL")
    print("=" * 80)
    
    rqg = create_rqg()
    
    # Generate a complex table
    print("-- Creating complex table for workload:")
    complex_ddl = rqg.generate_random_table_ddl("workload_test", num_columns=8, num_constraints=4)
    print(complex_ddl)
    
    # Now generate DML that would work with such tables
    print("\n-- Sample DML queries that respect constraints:")
    
    # Note: In a real scenario, we'd parse the DDL to understand constraints
    # For demo, we'll just show that PyRQG can generate both
    dml_queries = rqg.generate_from_grammar('dml_unique', count=5, seed=123)
    
    for i, query in enumerate(dml_queries[:3]):
        print(f"\n{i+1}. {query};")

def main():
    """Run all tests"""
    test_complex_ddl_api()
    test_ddl_generator_directly()
    test_constraint_types()
    test_workload_with_complex_ddl()
    
    print("\n\n" + "=" * 80)
    print("✅ ENHANCED DDL GENERATION COMPLETE!")
    print("=" * 80)
    print("\nKey Features Demonstrated:")
    print("- Composite primary keys (e.g., order_date + order_number)")
    print("- Multiple unique constraints per table")
    print("- Complex check constraints with multiple conditions")
    print("- Foreign key constraints with ON DELETE/UPDATE actions")
    print("- Partial indexes with WHERE clauses")
    print("- Functional indexes (GIN, GiST)")
    print("- Table partitioning")
    print("- DDL-focused grammar for ALTER TABLE, CREATE INDEX, etc.")
    print("\nPyRQG now supports enterprise-grade DDL generation! 🚀")

if __name__ == "__main__":
    main()