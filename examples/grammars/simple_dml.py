#!/usr/bin/env python3
"""
simple_dml.py - Simple DML Grammar Example

This grammar generates basic Data Manipulation Language (DML) queries:
- SELECT statements with various clauses
- INSERT statements with different patterns
- UPDATE statements with conditions
- DELETE statements with safety checks

This example shows how to create a complete, production-ready grammar.
"""

from pyrqg.dsl.core import Grammar, choice, template, number, maybe, repeat, ref

# Create the grammar
grammar = Grammar("simple_dml")

# Define available tables with metadata
grammar.define_tables(
    users=10000,
    products=500,
    orders=50000,
    customers=8000,
    inventory=1000
)

# Define common fields
grammar.define_fields(
    "id", "name", "email", "status", "created_at", "updated_at",
    "price", "quantity", "description", "category", "user_id",
    "product_id", "order_id", "customer_id", "total", "discount"
)

# ==================== SELECT Queries ====================

grammar.rule("select_query", template(
    "{select_clause} {from_clause} {where_clause} {group_by_clause} {having_clause} {order_by_clause} {limit_clause}",
    select_clause=ref("select_clause"),
    from_clause=ref("from_clause"),
    where_clause=ref("where_clause"),
    group_by_clause=ref("group_by_clause"),
    having_clause=ref("having_clause"),
    order_by_clause=ref("order_by_clause"),
    limit_clause=ref("limit_clause")
))

# SELECT clause variations
grammar.rule("select_clause", choice(
    "SELECT *",
    "SELECT DISTINCT *",
    template("SELECT {column_list}"),
    template("SELECT {aggregate_list}"),
    template("SELECT {column_list}, {aggregate_list}"),
    weights=[30, 5, 40, 15, 10]
))

grammar.rule("column_list", choice(
    ref("single_column"),
    ref("multiple_columns")
))

grammar.rule("single_column", choice(
    "id", "name", "email", "status", "price", "quantity"
))

grammar.rule("multiple_columns", repeat(
    ref("single_column"),
    min=2,
    max=5,
    separator=", "
))

grammar.rule("aggregate_list", repeat(
    ref("aggregate_function"),
    min=1,
    max=3,
    separator=", "
))

grammar.rule("aggregate_function", choice(
    "COUNT(*)",
    template("COUNT(DISTINCT {column})"),
    template("SUM({numeric_column})"),
    template("AVG({numeric_column})"),
    template("MAX({column})"),
    template("MIN({column})")
))

grammar.rule("column", ref("single_column"))
grammar.rule("numeric_column", choice("price", "quantity", "total", "discount"))

# FROM clause
grammar.rule("from_clause", choice(
    template("FROM {table}"),
    template("FROM {table} {alias}"),
    template("FROM {table} {alias} {joins}")
))

grammar.rule("table", choice(
    "users", "products", "orders", "customers", "inventory"
))

grammar.rule("alias", choice("u", "p", "o", "c", "i"))

grammar.rule("joins", repeat(
    ref("single_join"),
    min=1,
    max=2,
    separator=" "
))

grammar.rule("single_join", template(
    "{join_type} {joined_table} {joined_alias} ON {join_condition}"
))

grammar.rule("join_type", choice(
    "INNER JOIN",
    "LEFT JOIN",
    "RIGHT JOIN",
    weights=[60, 35, 5]
))

grammar.rule("joined_table", ref("table"))
grammar.rule("joined_alias", choice("j1", "j2", "t2"))

grammar.rule("join_condition", choice(
    template("{alias}.id = {joined_alias}.{fk_column}"),
    template("{alias}.{pk_column} = {joined_alias}.{fk_column}")
))

grammar.rule("pk_column", choice("id", "user_id", "product_id"))
grammar.rule("fk_column", choice("user_id", "product_id", "order_id", "customer_id"))

# WHERE clause
grammar.rule("where_clause", maybe(
    template("WHERE {conditions}"),
    probability=0.7
))

grammar.rule("conditions", choice(
    ref("simple_condition"),
    ref("compound_condition"),
    weights=[60, 40]
))

grammar.rule("simple_condition", choice(
    template("{column} = {value}"),
    template("{column} != {value}"),
    template("{column} > {value}"),
    template("{column} < {value}"),
    template("{column} >= {value}"),
    template("{column} <= {value}"),
    template("{column} BETWEEN {value} AND {value2}"),
    template("{column} IN ({value_list})"),
    template("{column} LIKE {pattern}"),
    template("{column} IS NULL"),
    template("{column} IS NOT NULL")
))

grammar.rule("compound_condition", choice(
    template("{simple_condition} AND {simple_condition}"),
    template("{simple_condition} OR {simple_condition}"),
    template("({simple_condition} AND {simple_condition}) OR {simple_condition}")
))

grammar.rule("value", choice(
    ref("numeric_value"),
    ref("string_value"),
    ref("date_value"),
    ref("boolean_value")
))

grammar.rule("numeric_value", number(1, 1000))
grammar.rule("string_value", choice(
    "'active'", "'inactive'", "'pending'", "'completed'",
    "'shipped'", "'delivered'", "'cancelled'"
))
grammar.rule("date_value", choice(
    "CURRENT_DATE",
    "CURRENT_TIMESTAMP",
    "'2024-01-01'",
    "CURRENT_DATE - INTERVAL '7 days'",
    "CURRENT_DATE - INTERVAL '30 days'"
))
grammar.rule("boolean_value", choice("true", "false"))

grammar.rule("value2", number(1001, 2000))

grammar.rule("value_list", repeat(
    ref("value"),
    min=2,
    max=5,
    separator=", "
))

grammar.rule("pattern", choice(
    "'%test%'", "'user_%'", "'%@example.com'", "'product_%'"
))

# GROUP BY clause
grammar.rule("group_by_clause", maybe(
    template("GROUP BY {grouping_list}"),
    probability=0.3
))

grammar.rule("grouping_list", choice(
    ref("single_column"),
    ref("multiple_columns")
))

# HAVING clause (only with GROUP BY)
grammar.rule("having_clause", maybe(
    template("HAVING {having_condition}"),
    probability=0.5  # When GROUP BY is present
))

grammar.rule("having_condition", choice(
    template("COUNT(*) > {small_number}"),
    template("SUM({numeric_column}) > {large_number}"),
    template("AVG({numeric_column}) < {medium_number}")
))

grammar.rule("small_number", number(5, 50))
grammar.rule("medium_number", number(50, 500))
grammar.rule("large_number", number(500, 10000))

# ORDER BY clause
grammar.rule("order_by_clause", maybe(
    template("ORDER BY {ordering_list}"),
    probability=0.5
))

grammar.rule("ordering_list", repeat(
    ref("ordering_item"),
    min=1,
    max=3,
    separator=", "
))

grammar.rule("ordering_item", template(
    "{column} {direction}"
))

grammar.rule("direction", choice("ASC", "DESC", weights=[40, 60]))

# LIMIT clause
grammar.rule("limit_clause", maybe(
    template("LIMIT {limit_value}"),
    probability=0.4
))

grammar.rule("limit_value", choice(
    10, 20, 50, 100, 500, 1000,
    weights=[30, 25, 20, 15, 7, 3]
))

# ==================== INSERT Queries ====================

grammar.rule("insert_query", choice(
    ref("simple_insert"),
    ref("multi_row_insert"),
    ref("insert_select")
))

grammar.rule("simple_insert", template(
    "INSERT INTO {table} ({insert_columns}) VALUES ({insert_values})"
))

grammar.rule("insert_columns", choice(
    "name, email, status",
    "user_id, product_id, quantity, price",
    "customer_id, total, status",
    "name, description, price, category"
))

grammar.rule("insert_values", choice(
    "'John Doe', 'john@example.com', 'active'",
    "123, 456, 5, 99.99",
    "789, 299.99, 'pending'",
    "'New Product', 'Description here', 49.99, 'electronics'"
))

grammar.rule("multi_row_insert", template(
    "INSERT INTO {table} ({insert_columns}) VALUES {multiple_value_sets}"
))

grammar.rule("multiple_value_sets", repeat(
    template("({insert_values})"),
    min=2,
    max=5,
    separator=", "
))

grammar.rule("insert_select", template(
    "INSERT INTO {table} ({insert_columns}) SELECT {insert_columns} FROM {source_table} WHERE {simple_condition}"
))

grammar.rule("source_table", ref("table"))

# ==================== UPDATE Queries ====================

grammar.rule("update_query", template(
    "UPDATE {table} SET {update_assignments} {where_clause}"
))

grammar.rule("update_assignments", repeat(
    ref("assignment"),
    min=1,
    max=3,
    separator=", "
))

grammar.rule("assignment", choice(
    template("{column} = {value}"),
    template("{column} = {column} + {numeric_value}"),
    template("{column} = {column} * {multiplier}"),
    template("{column} = CASE WHEN {simple_condition} THEN {value} ELSE {column} END")
))

grammar.rule("multiplier", choice("0.9", "1.1", "1.5", "2.0"))

# ==================== DELETE Queries ====================

grammar.rule("delete_query", template(
    "DELETE FROM {table} WHERE {delete_condition}"
))

# DELETE always requires WHERE for safety
grammar.rule("delete_condition", choice(
    ref("simple_condition"),
    template("{simple_condition} AND {simple_condition}"),
    # Common safe delete patterns
    template("created_at < CURRENT_DATE - INTERVAL '90 days'"),
    template("status = 'archived'"),
    template("id IN (SELECT id FROM {table} WHERE {simple_condition} LIMIT 100)")
))

# ==================== Main Query Rule ====================

grammar.rule("query", choice(
    ref("select_query"),
    ref("insert_query"),
    ref("update_query"),
    ref("delete_query"),
    weights=[60, 20, 15, 5]  # SELECT most common, DELETE least common
))

# ==================== Entry Point ====================

if __name__ == "__main__":
    """Test the grammar by generating sample queries."""
    
    print("Simple DML Grammar - Sample Queries")
    print("=" * 50)
    
    # Generate various query types
    query_types = ["select_query", "insert_query", "update_query", "delete_query"]
    
    for query_type in query_types:
        print(f"\n{query_type.replace('_', ' ').title()}:")
        print("-" * 50)
        
        for i in range(3):
            query = grammar.generate(query_type, seed=i * 10)
            # Clean up extra spaces
            query = " ".join(query.split())
            print(f"\n{query};")
    
    print("\n\nMixed queries:")
    print("-" * 50)
    
    for i in range(10):
        query = grammar.generate("query", seed=i * 5)
        query = " ".join(query.split())
        print(f"{i+1}. {query};")