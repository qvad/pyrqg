#!/usr/bin/env python3
"""
01_simple_queries.py - Basic Query Generation Example

This example demonstrates the fundamentals of PyRQG:
- Creating a simple grammar
- Defining basic rules
- Generating different types of SQL queries

Key concepts covered:
- Grammar creation
- Rule definition  
- Choice elements
- Template usage
- Query generation
"""

import sys
from pathlib import Path

# Add pyrqg to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, number


def create_simple_grammar():
    """Create a basic SQL grammar with common query patterns."""
    
    # Initialize grammar
    grammar = Grammar("simple_sql")
    
    # Define available tables
    grammar.define_tables(
        users=1000,      # Table with 1000 rows
        products=500,    # Table with 500 rows
        orders=5000      # Table with 5000 rows
    )
    
    # Define available columns
    grammar.define_fields(
        "id", "name", "email", "price", "quantity", 
        "user_id", "product_id", "created_at", "status"
    )
    
    # Main rule - generates different query types
    grammar.rule("query", choice(
        ref("select_query"),
        ref("insert_query"),
        ref("update_query"),
        ref("delete_query")
    ))
    
    # SELECT query patterns
    grammar.rule("select_query", choice(
        template("SELECT * FROM {table}"),
        template("SELECT {columns} FROM {table}"),
        template("SELECT * FROM {table} WHERE {condition}"),
        template("SELECT COUNT(*) FROM {table}")
    ))
    
    # INSERT query patterns
    grammar.rule("insert_query", choice(
        template("INSERT INTO users (name, email) VALUES ('{name}', '{email}')"),
        template("INSERT INTO products (name, price) VALUES ('{name}', {price})"),
        template("INSERT INTO orders (user_id, product_id, quantity) VALUES ({user_id}, {product_id}, {quantity})")
    ))
    
    # UPDATE query patterns
    grammar.rule("update_query", choice(
        template("UPDATE {table} SET {column} = {value}"),
        template("UPDATE {table} SET {column} = {value} WHERE {condition}"),
        template("UPDATE {table} SET status = '{status}' WHERE id = {id}")
    ))
    
    # DELETE query patterns
    grammar.rule("delete_query", choice(
        template("DELETE FROM {table} WHERE {condition}"),
        template("DELETE FROM {table} WHERE id = {id}")
    ))
    
    # Define component rules
    grammar.rule("table", choice("users", "products", "orders"))
    
    grammar.rule("columns", choice(
        "id, name",
        "id, email", 
        "name, price",
        "*"
    ))
    
    grammar.rule("column", choice("name", "email", "price", "status", "quantity"))
    
    grammar.rule("condition", choice(
        template("id = {id}"),
        template("status = '{status}'"),
        template("price > {price}"),
        template("created_at > '2024-01-01'")
    ))
    
    # Value generators
    grammar.rule("id", number(1, 1000))
    grammar.rule("user_id", number(1, 1000))
    grammar.rule("product_id", number(1, 500))
    grammar.rule("quantity", number(1, 10))
    grammar.rule("price", number(10, 1000))
    
    grammar.rule("name", choice("'John Doe'", "'Jane Smith'", "'Product A'", "'Product B'"))
    grammar.rule("email", choice("'john@example.com'", "'jane@example.com'", "'test@example.com'"))
    grammar.rule("status", choice("active", "pending", "completed", "cancelled"))
    
    grammar.rule("value", choice(
        "'new value'",
        "123",
        "true",
        "NULL"
    ))
    
    return grammar


def main():
    """Generate and display example queries."""
    
    print("PyRQG Basic Query Generation Example")
    print("=" * 50)
    print()
    
    # Create grammar
    grammar = create_simple_grammar()
    
    # Generate queries with different seeds for variety
    print("Generated Queries:")
    print("-" * 50)
    
    for i in range(10):
        query = grammar.generate("query", seed=i)
        print(f"{i+1}. {query}")
    
    print()
    print("Specific Query Types:")
    print("-" * 50)
    
    # Generate specific query types
    query_types = ["select_query", "insert_query", "update_query", "delete_query"]
    
    for query_type in query_types:
        print(f"\n{query_type.replace('_', ' ').title()}:")
        for i in range(3):
            query = grammar.generate(query_type, seed=i+100)
            print(f"  - {query}")
    
    print()
    print("Key Takeaways:")
    print("-" * 50)
    print("1. Grammars define the structure of generated queries")
    print("2. Rules can reference other rules using ref()")
    print("3. Templates allow flexible query patterns")
    print("4. Choice elements provide variation")
    print("5. Seeds ensure reproducible output")
    

if __name__ == "__main__":
    main()

# Fix missing import
from pyrqg.dsl.core import ref