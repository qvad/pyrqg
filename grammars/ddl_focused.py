"""
DDL-focused Grammar for PyRQG
Generates complex CREATE TABLE, ALTER TABLE, and INDEX statements
"""

from pyrqg.dsl.core import Grammar, Choice, Template, Lambda, Optional
import time

# Create the grammar
g = Grammar()

# Define the root rule - generates various DDL statements
g.rule("query", Choice(
    "create_table",
    "create_complex_table",
    "alter_table",
    "create_index",
    "create_unique_index",
    "drop_table",
    "create_view"
))

# Table names with uniqueness
g.rule("table_name", Lambda(lambda ctx: f"table_{ctx.rng.randint(1, 100)}_{int(time.time() * 1000) % 100000}"))
g.rule("existing_table", Choice("users", "products", "orders", "inventory", "customers"))

# Column types
g.rule("data_type", Choice(
    "INTEGER", "BIGINT", "SMALLINT",
    "VARCHAR(50)", "VARCHAR(100)", "VARCHAR(200)", "TEXT",
    "DECIMAL(10,2)", "NUMERIC(12,4)", "REAL", "DOUBLE PRECISION",
    "BOOLEAN", "DATE", "TIMESTAMP", "TIME",
    "UUID", "JSON", "JSONB", "INET", "MACADDR"
))

# Column names
g.rule("column_name", Choice(
    "id", "name", "email", "username", "password_hash",
    "status", "created_at", "updated_at", "deleted_at",
    "price", "quantity", "amount", "total", "discount",
    "description", "notes", "metadata", "tags",
    "user_id", "product_id", "order_id", "customer_id",
    "is_active", "is_deleted", "is_verified", "is_premium"
))

# Unique column names for complex constraints
g.rule("unique_column", Lambda(lambda ctx: f"col_{ctx.rng.choice(['data', 'value', 'info', 'attr'])}_{ctx.rng.randint(1, 100)}"))

# Simple CREATE TABLE
g.rule("create_table", Template("""CREATE TABLE {table_name} (
    id SERIAL PRIMARY KEY,
    {column_name} {data_type} NOT NULL,
    {column_name2:column_name} {data_type2:data_type},
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)"""))

# Complex CREATE TABLE with multiple constraints
g.rule("create_complex_table", Template("""CREATE TABLE {table_name} (
    {pk_column:column_name} SERIAL,
    {col1:unique_column} {type1:data_type} NOT NULL,
    {col2:unique_column} {type2:data_type} UNIQUE,
    {col3:unique_column} VARCHAR(100) NOT NULL,
    {col4:unique_column} INTEGER CHECK ({col4} >= 0),
    {col5:unique_column} DECIMAL(10,2) DEFAULT 0.00,
    status VARCHAR(20) CHECK (status IN ('active', 'inactive', 'pending')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY ({pk_column}, {col1}),
    UNIQUE ({col3}, status),
    CHECK ({col4} <= {col5} OR {col5} IS NULL)
)"""))

# ALTER TABLE statements
g.rule("alter_table", Choice(
    "add_column",
    "add_constraint",
    "add_foreign_key",
    "drop_column",
    "alter_column"
))

g.rule("add_column", Template(
    "ALTER TABLE {existing_table} ADD COLUMN {unique_column} {data_type} {column_constraint}"
))

g.rule("column_constraint", Choice(
    "NOT NULL",
    "DEFAULT 0",
    "DEFAULT ''",
    "DEFAULT CURRENT_TIMESTAMP",
    "CHECK ({unique_column} > 0)",
    "UNIQUE"
))

g.rule("add_constraint", Choice(
    Template("ALTER TABLE {existing_table} ADD CONSTRAINT {constraint_name} UNIQUE ({column_name}, {column_name2:column_name})"),
    Template("ALTER TABLE {existing_table} ADD CONSTRAINT {constraint_name} CHECK ({column_name} != {column_name2:column_name})"),
    Template("ALTER TABLE {existing_table} ADD CONSTRAINT {constraint_name} CHECK (price > cost)"),
    Template("ALTER TABLE {existing_table} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({column_name}, {column_name2:column_name})")
))

g.rule("constraint_name", Lambda(lambda ctx: f"constraint_{ctx.rng.randint(1000, 9999)}_{int(time.time() * 1000) % 100000}"))

g.rule("add_foreign_key", Template(
    "ALTER TABLE {existing_table} ADD CONSTRAINT {constraint_name} " +
    "FOREIGN KEY ({column_name}) REFERENCES {ref_table:existing_table}(id) {fk_action}"
))

g.rule("fk_action", Choice(
    "ON DELETE CASCADE",
    "ON DELETE SET NULL",
    "ON DELETE RESTRICT",
    "ON UPDATE CASCADE",
    "ON DELETE CASCADE ON UPDATE CASCADE"
))

g.rule("drop_column", Template("ALTER TABLE {existing_table} DROP COLUMN IF EXISTS {column_name}"))

g.rule("alter_column", Choice(
    Template("ALTER TABLE {existing_table} ALTER COLUMN {column_name} SET NOT NULL"),
    Template("ALTER TABLE {existing_table} ALTER COLUMN {column_name} DROP NOT NULL"),
    Template("ALTER TABLE {existing_table} ALTER COLUMN {column_name} SET DEFAULT 0"),
    Template("ALTER TABLE {existing_table} ALTER COLUMN {column_name} TYPE {data_type}")
))

# CREATE INDEX statements
g.rule("create_index", Template(
    "CREATE INDEX {index_name} ON {existing_table} ({index_columns}) {index_options}"
))

g.rule("create_unique_index", Template(
    "CREATE UNIQUE INDEX {index_name} ON {existing_table} ({index_columns}) {index_options}"
))

g.rule("index_name", Lambda(lambda ctx: f"idx_{ctx.rng.randint(1000, 9999)}_{int(time.time() * 1000) % 100000}"))

g.rule("index_columns", Choice(
    Template("{column_name}"),
    Template("{column_name}, {column_name2:column_name}"),
    Template("{column_name} DESC, {column_name2:column_name} ASC"),
    Template("{column_name}, {column_name2:column_name}, {column_name3:column_name}")
))

g.rule("index_options", Choice(
    "",
    "WHERE status = 'active'",
    "WHERE {column_name} IS NOT NULL",
    "WHERE created_at > CURRENT_DATE - INTERVAL '30 days'",
    "INCLUDE ({column_name})",
    "USING btree",
    "USING hash",
    "USING gin"
))

# DROP TABLE
g.rule("drop_table", Template("DROP TABLE IF EXISTS {table_name} CASCADE"))

# CREATE VIEW
g.rule("create_view", Template("""CREATE VIEW view_{view_suffix} AS
SELECT 
    {column_name},
    {column_name2:column_name},
    COUNT(*) as count
FROM {existing_table}
WHERE {column_name} IS NOT NULL
GROUP BY {column_name}, {column_name2}"""))

g.rule("view_suffix", Lambda(lambda ctx: f"{ctx.rng.randint(100, 999)}_{int(time.time() * 1000) % 10000}"))

# Table with partitioning
g.rule("create_partitioned_table", Template("""CREATE TABLE {table_name} (
    id SERIAL,
    {column_name} {data_type} NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    data JSONB,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at)"""))

# Create schema
g.rule("create_schema", Template("CREATE SCHEMA IF NOT EXISTS schema_{schema_suffix}"))
g.rule("schema_suffix", Lambda(lambda ctx: f"{ctx.rng.randint(100, 999)}"))

# Advanced constraints
g.rule("create_table_advanced", Template("""CREATE TABLE {table_name} (
    -- Columns with various constraints
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    code VARCHAR(50) NOT NULL UNIQUE,
    parent_id UUID REFERENCES {table_name2:table_name}(id) ON DELETE CASCADE,
    
    -- Numeric columns with checks
    price DECIMAL(10,2) NOT NULL CHECK (price > 0),
    discount_percent INTEGER CHECK (discount_percent BETWEEN 0 AND 100),
    quantity INTEGER NOT NULL DEFAULT 0 CHECK (quantity >= 0),
    
    -- Status with enumeration
    status VARCHAR(20) NOT NULL DEFAULT 'draft' 
        CHECK (status IN ('draft', 'pending', 'approved', 'rejected', 'archived')),
    
    -- JSON columns
    metadata JSONB DEFAULT '{{}}',
    tags TEXT[] DEFAULT ARRAY[]::TEXT[],
    
    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    
    -- Composite unique constraint
    UNIQUE (code, status),
    
    -- Check constraint referencing multiple columns
    CHECK (
        (status = 'approved' AND price > 0) OR
        (status IN ('draft', 'pending') AND price >= 0)
    ),
    
    -- Exclusion constraint (PostgreSQL specific)
    EXCLUDE USING gist (code WITH =, tstzrange(created_at, deleted_at) WITH &&)
        WHERE (deleted_at IS NOT NULL)
)"""))

# Materialized view
g.rule("create_materialized_view", Template("""CREATE MATERIALIZED VIEW mv_{view_suffix} AS
SELECT 
    DATE_TRUNC('day', created_at) as day,
    status,
    COUNT(*) as count,
    SUM(price) as total_price,
    AVG(quantity) as avg_quantity
FROM {existing_table}
WHERE created_at >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY DATE_TRUNC('day', created_at), status
WITH DATA"""))

# Function-based index
g.rule("create_functional_index", Template(
    "CREATE INDEX {index_name} ON {existing_table} (LOWER({column_name}), DATE_TRUNC('month', created_at))"
))

# GiST index for full-text search
g.rule("create_text_search_index", Template(
    "CREATE INDEX {index_name} ON {existing_table} USING gin(to_tsvector('english', {column_name}))"
))

if __name__ == "__main__":
    # Test the grammar
    for i in range(10):
        print(f"\n-- Query {i+1}:")
        print(g.generate("query", seed=i))