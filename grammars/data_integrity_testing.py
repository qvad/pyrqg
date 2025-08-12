"""
Data Integrity Testing Grammar for PostgreSQL
Tests constraints, triggers, foreign keys, check constraints, and data validation edge cases
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat
import psycopg2

# Get real database schema
def get_schema_info():
    """Fetch tables, columns, and constraints from database"""
    try:
        conn = psycopg2.connect("dbname=postgres")
        cur = conn.cursor()
        
        # Get tables with their columns
        cur.execute("""
            SELECT 
                t.table_name,
                array_agg(DISTINCT c.column_name) as columns,
                array_agg(DISTINCT c.data_type) as types
            FROM information_schema.tables t
            JOIN information_schema.columns c ON t.table_name = c.table_name
            WHERE t.table_schema = 'public' 
            AND t.table_type = 'BASE TABLE'
            AND t.table_name NOT LIKE 'table_%'
            GROUP BY t.table_name
            LIMIT 10
        """)
        
        table_info = {}
        for table, columns, types in cur.fetchall():
            table_info[table] = list(zip(columns, types))
        
        # Get primary key columns
        cur.execute("""
            SELECT 
                tc.table_name,
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = 'public'
        """)
        
        pk_columns = {}
        for table, col in cur.fetchall():
            if table not in pk_columns:
                pk_columns[table] = []
            pk_columns[table].append(col)
        
        cur.close()
        conn.close()
        
        return table_info, pk_columns
    except:
        # Fallback
        return (
            {
                "users": [("id", "integer"), ("name", "varchar"), ("email", "varchar")],
                "products": [("id", "integer"), ("name", "varchar"), ("price", "numeric")],
                "orders": [("id", "integer"), ("user_id", "integer"), ("quantity", "integer")]
            },
            {"users": ["id"], "products": ["id"], "orders": ["id"]}
        )

table_info, pk_columns = get_schema_info()

# Extract column types
numeric_columns = []
text_columns = []
date_columns = []
tables = list(table_info.keys())

for table, cols in table_info.items():
    for col_name, col_type in cols:
        if col_type in ('integer', 'bigint', 'smallint', 'numeric', 'real', 'double precision'):
            numeric_columns.append((table, col_name))
        elif col_type in ('character varying', 'text', 'varchar', 'char'):
            text_columns.append((table, col_name))
        elif col_type in ('date', 'timestamp', 'timestamp without time zone'):
            date_columns.append((table, col_name))

# Ensure we have some columns
if not numeric_columns:
    numeric_columns = [("users", "id"), ("products", "price")]
if not text_columns:
    text_columns = [("users", "name"), ("users", "email")]
if not tables:
    tables = ["users", "products", "orders"]

g = Grammar("data_integrity_testing")

# Main integrity test categories
g.rule("query",
    choice(
        ref("constraint_violations"),
        ref("null_tests"),
        ref("unique_violations"),
        ref("check_constraint_tests"),
        ref("foreign_key_tests"),
        ref("type_boundary_tests"),
        ref("transaction_integrity"),
        weights=[20, 15, 15, 15, 15, 10, 10]
    )
)

# Constraint violation attempts
g.rule("constraint_violations",
    choice(
        # Insert with NULL in NOT NULL column
        template("""-- NOT NULL constraint violation
INSERT INTO {table} ({pk_column})
VALUES (NULL)"""),
        
        # Update to violate NOT NULL
        template("""-- Update to NULL violation
UPDATE {table}
SET {column} = NULL
WHERE {pk_column} = 1"""),
        
        # Insert duplicate primary key
        template("""-- Primary key violation
INSERT INTO {table} ({pk_column})
SELECT {pk_column} FROM {table} LIMIT 1"""),
        
        # Violate positive number constraint
        template("""-- Negative value constraint
UPDATE {table}
SET {numeric_column} = -1
WHERE {pk_column} > 0
RETURNING *""")
    )
)

# NULL handling tests
g.rule("null_tests",
    choice(
        # NULL comparisons
        template("""-- NULL comparison test
SELECT COUNT(*)
FROM {table}
WHERE {column} = NULL"""),  # This is wrong on purpose
        
        # NULL arithmetic
        template("""-- NULL arithmetic
SELECT {numeric_column} + NULL as result
FROM {table}
LIMIT 1"""),
        
        # COALESCE tests
        template("""-- COALESCE chain
SELECT COALESCE({column}, {column}, 'default') as result
FROM {table}
WHERE {pk_column} > 0"""),
        
        # NULL in aggregates
        template("""-- NULL in aggregates
SELECT 
    COUNT(*) as total,
    COUNT({column}) as non_null,
    COUNT(*) - COUNT({column}) as null_count
FROM {table}""")
    )
)

# Unique constraint violations
g.rule("unique_violations",
    choice(
        # Insert duplicate value
        template("""-- Unique constraint violation
INSERT INTO {table} ({text_column})
SELECT {text_column} 
FROM {table} 
WHERE {text_column} IS NOT NULL
LIMIT 1"""),
        
        # Update to create duplicate
        template("""-- Update to duplicate
UPDATE {table} t1
SET {text_column} = (
    SELECT {text_column} 
    FROM {table} t2 
    WHERE t2.{pk_column} != t1.{pk_column}
    LIMIT 1
)
WHERE {pk_column} = (SELECT MIN({pk_column}) FROM {table})"""),
        
        # Multi-column unique violation
        template("""-- Compound unique violation
INSERT INTO {table} ({column1}, {column2})
SELECT {column1}, {column2}
FROM {table}
WHERE {column1} IS NOT NULL
LIMIT 1""")
    )
)

# Check constraint tests
g.rule("check_constraint_tests",
    choice(
        # Email format check
        template("""-- Email format check
INSERT INTO {table} ({text_column})
VALUES ('invalid-email-format')"""),
        
        # Range checks
        template("""-- Range constraint
UPDATE {table}
SET {numeric_column} = 999999999
WHERE {pk_column} = 1"""),
        
        # Length constraint
        template("""-- Length constraint
UPDATE {table}
SET {text_column} = REPEAT('x', 1000)
WHERE {pk_column} = 1"""),
        
        # Custom validation
        template("""-- Custom check constraint
INSERT INTO {table} ({numeric_column})
VALUES (-999)""")
    )
)

# Foreign key tests
g.rule("foreign_key_tests",
    choice(
        # Insert with non-existent FK
        template("""-- Foreign key violation
INSERT INTO {table} ({numeric_column})
VALUES (999999999)"""),
        
        # Delete parent with children
        template("""-- Cascade delete test
DELETE FROM {table}
WHERE {pk_column} IN (
    SELECT {pk_column} 
    FROM {table} 
    LIMIT 1
)"""),
        
        # Update FK to invalid value
        template("""-- Update foreign key
UPDATE {table}
SET {numeric_column} = 999999999
WHERE {pk_column} = 1"""),
        
        # Self-referential FK
        template("""-- Self-referential test
UPDATE {table}
SET {pk_column} = {pk_column} + 1
WHERE {pk_column} = (SELECT MAX({pk_column}) FROM {table})""")
    )
)

# Type boundary tests
g.rule("type_boundary_tests",
    choice(
        # Integer overflow
        template("""-- Integer boundary test
INSERT INTO {table} ({numeric_column})
VALUES (2147483647)"""),
        
        # Numeric precision
        template("""-- Numeric precision test
UPDATE {table}
SET {numeric_column} = 0.123456789012345678901234567890
WHERE {pk_column} = 1"""),
        
        # String truncation
        template("""-- String truncation test
INSERT INTO {table} ({text_column})
VALUES (REPEAT('a', 255))"""),
        
        # Special characters
        template("""-- Special character test
INSERT INTO {table} ({text_column})
VALUES (E'\\x00\\x01\\x02\\x03')""")
    )
)

# Transaction integrity tests
g.rule("transaction_integrity",
    choice(
        # Concurrent update test
        template("""-- Concurrent update simulation
UPDATE {table}
SET {numeric_column} = {numeric_column} + 1
WHERE {pk_column} = 1
RETURNING {numeric_column}"""),
        
        # Read-modify-write
        template("""-- Read-modify-write pattern
WITH current_val AS (
    SELECT {numeric_column} as val
    FROM {table}
    WHERE {pk_column} = 1
    FOR UPDATE
)
UPDATE {table}
SET {numeric_column} = (SELECT val + 1 FROM current_val)
WHERE {pk_column} = 1"""),
        
        # Upsert conflict
        template("""-- UPSERT with conflict
INSERT INTO {table} ({pk_column}, {numeric_column})
VALUES (1, 100)
ON CONFLICT ({pk_column}) 
DO UPDATE SET {numeric_column} = EXCLUDED.{numeric_column}"""),
        
        # Deadlock prone pattern
        template("""-- Potential deadlock pattern
UPDATE {table}
SET {numeric_column} = {numeric_column} + 1
WHERE {pk_column} IN (1, 2)
ORDER BY {pk_column} DESC""")
    )
)

# Table selection helpers
def get_table_column(col_list):
    """Get a table and column from list of (table, column) tuples"""
    if col_list:
        table, column = col_list[0]  # Simple selection for now
        return table, column
    return "users", "id"

# Rules for dynamic values
g.rule("table", choice(*tables))

# Primary key columns
g.rule("pk_column", 
    choice(*[col for table in tables for col in pk_columns.get(table, ["id"])])
)

# Column types
g.rule("column", 
    choice(*[col for _, col in text_columns + numeric_columns])
)

g.rule("numeric_column",
    choice(*[col for _, col in numeric_columns])
)

g.rule("text_column",
    choice(*[col for _, col in text_columns])
)

# For multi-column operations
g.rule("column1", choice(*[col for _, col in text_columns + numeric_columns]))
g.rule("column2", choice(*[col for _, col in text_columns + numeric_columns]))

# Export grammar
grammar = g