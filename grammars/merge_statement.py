"""
MERGE Statement Grammar for PostgreSQL 15+
Tests the new MERGE command with various edge cases and conditions
"""

import sys
from pathlib import Path

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat
try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None
import random

# Connect to database and get real schema information
def get_database_schema():
    """Fetch actual tables and columns from the database"""
    try:
        conn = psycopg2.connect("dbname=postgres")
        cur = conn.cursor()
        
        # Get all tables
        cur.execute("""
            SELECT DISTINCT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            AND table_name NOT LIKE 'table_%'
            ORDER BY table_name
            LIMIT 20
        """)
        tables = [row[0] for row in cur.fetchall()]
        
        # Get columns for each table
        columns_by_table = {}
        for table in tables:
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                ORDER BY ordinal_position
            """, (table,))
            columns_by_table[table] = [(col, dtype) for col, dtype in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return tables, columns_by_table
    except:
        # Fallback to some default tables if connection fails
        return (
            ["users", "products", "orders", "inventory"],
            {
                "users": [("id", "integer"), ("name", "varchar"), ("email", "varchar")],
                "products": [("id", "integer"), ("name", "varchar"), ("price", "numeric")],
                "orders": [("id", "integer"), ("user_id", "integer"), ("product_id", "integer")],
                "inventory": [("id", "integer"), ("product_id", "integer"), ("quantity", "integer")]
            }
        )

# Get actual schema
tables, columns_by_table = get_database_schema()

# Filter for tables with useful columns
valid_tables = [t for t in tables if len(columns_by_table.get(t, [])) > 2]
id_columns = []
text_columns = []
numeric_columns = []
timestamp_columns = []

for table, cols in columns_by_table.items():
    for col_name, col_type in cols:
        if 'id' in col_name or col_name == 'id':
            id_columns.append(col_name)
        elif col_type in ('character varying', 'text', 'varchar'):
            text_columns.append(col_name)
        elif col_type in ('integer', 'numeric', 'bigint', 'smallint'):
            numeric_columns.append(col_name)
        elif col_type in ('timestamp', 'date', 'timestamp without time zone'):
            timestamp_columns.append(col_name)

# Deduplicate
id_columns = list(set(id_columns))
text_columns = list(set(text_columns))
numeric_columns = list(set(numeric_columns))
timestamp_columns = list(set(timestamp_columns))

# Ensure we have some columns
if not id_columns:
    id_columns = ["id"]
if not text_columns:
    text_columns = ["name", "email", "status"]
if not numeric_columns:
    numeric_columns = ["quantity", "price", "amount"]
if not timestamp_columns:
    timestamp_columns = ["created_at", "updated_at"]

g = Grammar("merge_statement")

# Main MERGE statement variations
g.rule("query",
    choice(
        ref("simple_merge"),
        ref("complex_merge"),
        ref("merge_with_cte"),
        ref("merge_multi_action"),
        ref("merge_edge_cases"),
        weights=[20, 25, 15, 25, 15]
    )
)

# Simple MERGE - basic INSERT or UPDATE
g.rule("simple_merge",
    template("""MERGE INTO {target_table} AS t
USING {source_table} AS s
ON t.{id_column} = s.{id_column}
WHEN MATCHED THEN
    UPDATE SET {update_columns}
WHEN NOT MATCHED THEN
    INSERT ({insert_columns}) VALUES ({insert_values})""")
)

# Complex MERGE with additional conditions
g.rule("complex_merge",
    template("""MERGE INTO {target_table} AS t
USING (
    SELECT {source_columns}
    FROM {source_table}
    WHERE {source_condition}
    {maybe_order_limit}
) AS s
ON t.{id_column} = s.{id_column} {additional_join_condition}
WHEN MATCHED AND {matched_condition} THEN
    UPDATE SET {update_columns}
WHEN MATCHED THEN
    DELETE
WHEN NOT MATCHED THEN
    INSERT ({insert_columns}) VALUES ({insert_values})""")
)

# MERGE with CTE
g.rule("merge_with_cte",
    template("""WITH {cte_name} AS (
    SELECT {cte_columns}
    FROM {cte_table}
    {maybe_where}
    {maybe_group_by}
)
MERGE INTO {target_table} AS t
USING {cte_name} AS s
ON t.{id_column} = s.{id_column}
WHEN MATCHED THEN
    UPDATE SET {update_columns}
WHEN NOT MATCHED THEN
    INSERT ({insert_columns}) VALUES ({insert_values})""")
)

# MERGE with multiple actions per condition
g.rule("merge_multi_action",
    template("""MERGE INTO {target_table} AS t
USING {source_table} AS s
ON t.{id_column} = s.{id_column}
WHEN MATCHED AND t.{numeric_column} < s.{numeric_column} THEN
    UPDATE SET {numeric_column} = s.{numeric_column}
WHEN MATCHED AND t.{numeric_column} > 1000 THEN
    DELETE
WHEN NOT MATCHED AND s.{numeric_column} > 0 THEN
    INSERT ({insert_columns}) VALUES ({insert_values})""")
)

# Edge cases and stress testing
g.rule("merge_edge_cases",
    choice(
        # Self-merge
        template("""MERGE INTO {table} AS t1
USING {table} AS t2
ON t1.{id_column} = t2.{id_column} + 1
WHEN MATCHED THEN
    UPDATE SET {numeric_column} = t2.{numeric_column}"""),
        
        # MERGE with subquery in UPDATE
        template("""MERGE INTO {target_table} AS t
USING {source_table} AS s
ON t.{id_column} = s.{id_column}
WHEN MATCHED THEN
    UPDATE SET 
        {numeric_column} = (SELECT MAX({numeric_column}) FROM {other_table}),
        {id_column} = t.{id_column}"""),
        
        # Simple MERGE with constants
        template("""MERGE INTO {target_table} AS t
USING (SELECT 1 as {id_column}, 100 as {numeric_column}) AS s
ON t.{id_column} = s.{id_column}
WHEN MATCHED THEN
    UPDATE SET {numeric_column} = s.{numeric_column}
WHEN NOT MATCHED THEN
    INSERT ({id_column}, {numeric_column}) VALUES (s.{id_column}, s.{numeric_column})""")
    )
)

# Dynamic rules based on actual schema
g.rule("target_table", choice(*valid_tables) if valid_tables else choice("users", "products"))
g.rule("source_table", choice(*valid_tables) if valid_tables else choice("users", "products"))
g.rule("table", choice(*valid_tables[:5]) if valid_tables else choice("users"))
g.rule("other_table", choice(*valid_tables[1:6]) if len(valid_tables) > 1 else choice("products"))
g.rule("cte_table", choice(*valid_tables[:5]) if valid_tables else choice("orders"))

# Column references from actual schema
g.rule("id_column", choice(*id_columns))
g.rule("text_column", choice(*text_columns))
g.rule("numeric_column", choice(*numeric_columns))
g.rule("timestamp_column", choice(*timestamp_columns) if timestamp_columns else choice("created_at"))

# Dynamic column lists based on tables
def get_table_columns(table_name):
    """Get actual columns for a table"""
    if table_name in columns_by_table:
        return [col[0] for col in columns_by_table[table_name]]
    return ["id", "name", "value"]

# Update columns - use actual column names
g.rule("update_columns", 
    choice(
        template("{numeric_column} = s.{numeric_column}"),
        template("{text_column} = s.{text_column}"),
        template("{numeric_column} = s.{numeric_column}, {text_column} = s.{text_column}"),
        template("{numeric_column} = s.{numeric_column} + 1")
    )
)

# Insert columns - minimal set that should work
g.rule("insert_columns", choice(
    template("{id_column}"),
    template("{id_column}, {numeric_column}"),
    template("{id_column}, {text_column}")
))

g.rule("insert_values", choice(
    template("s.{id_column}"),
    template("s.{id_column}, s.{numeric_column}"),
    template("s.{id_column}, s.{text_column}")
))

# Source columns
g.rule("source_columns", choice(
    "*",
    template("{id_column}, {numeric_column}"),
    template("{id_column}, {text_column}, {numeric_column}")
))

# CTE columns
g.rule("cte_columns", choice(
    template("{id_column}, COUNT(*) as count"),
    template("{id_column}, SUM({numeric_column}) as total"),
    template("{id_column}, MAX({numeric_column}) as max_val")
))

# Conditions
g.rule("source_condition", choice(
    template("{numeric_column} > 0"),
    template("{numeric_column} < 1000"),
    template("{id_column} > 0"),
    template("{text_column} IS NOT NULL")
))

g.rule("matched_condition", choice(
    template("s.{numeric_column} > t.{numeric_column}"),
    template("s.{id_column} != t.{id_column}"),
    template("t.{numeric_column} > 0")
))

g.rule("additional_join_condition", 
    maybe(choice(
        template(" AND t.{numeric_column} = s.{numeric_column}"),
        template(" AND t.{id_column} > 0"),
        ""
    ))
)

g.rule("maybe_order_limit", 
    maybe(choice(
        "ORDER BY 1 DESC LIMIT 100",
        "ORDER BY 1",
        ""
    ))
)

g.rule("maybe_where",
    maybe(choice(
        template("WHERE {numeric_column} > 0"),
        template("WHERE {id_column} IS NOT NULL"),
        ""
    ))
)

g.rule("maybe_group_by",
    maybe(choice(
        template("GROUP BY {id_column}"),
        ""
    ))
)

# Utility rules
g.rule("cte_name", choice("cte_data", "temp_data", "agg_data"))

# Export grammar
grammar = g
