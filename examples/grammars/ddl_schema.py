#!/usr/bin/env python3
"""
ddl_schema.py - DDL Schema Definition Grammar

This grammar generates Data Definition Language (DDL) statements:
- CREATE TABLE with various column types and constraints
- ALTER TABLE operations
- CREATE INDEX with different types
- CREATE VIEW definitions
- DROP statements with CASCADE options

Shows advanced grammar techniques for schema management.
"""

from pyrqg.dsl.core import Grammar, choice, template, maybe, repeat, ref, Lambda

# Create the grammar
grammar = Grammar("ddl_schema")

# ==================== Data Types ====================

grammar.rule("data_type", choice(
    # Numeric types
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "DECIMAL(10,2)",
    "NUMERIC(18,4)",
    "REAL",
    "DOUBLE PRECISION",
    
    # String types
    "VARCHAR(255)",
    "VARCHAR(50)",
    "VARCHAR(1000)",
    "CHAR(10)",
    "TEXT",
    
    # Date/Time types
    "DATE",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
    "INTERVAL",
    
    # Boolean
    "BOOLEAN",
    
    # Binary
    "BYTEA",
    
    # JSON
    "JSON",
    "JSONB",
    
    # Arrays
    "INTEGER[]",
    "VARCHAR(255)[]",
    
    # Special types
    "UUID",
    "INET",
    "CIDR"
))

# ==================== Constraints ====================

grammar.rule("column_constraint", choice(
    "NOT NULL",
    "NULL",
    "UNIQUE",
    "PRIMARY KEY",
    template("DEFAULT {default_value}"),
    template("CHECK ({check_condition})"),
    template("REFERENCES {ref_table}({ref_column})"),
    weights=[30, 10, 15, 5, 20, 10, 10]
))

grammar.rule("default_value", choice(
    "0",
    "''",
    "false",
    "true",
    "CURRENT_TIMESTAMP",
    "CURRENT_DATE",
    "gen_random_uuid()",
    "nextval('seq_name')"
))

grammar.rule("check_condition", choice(
    template("{column_name} > 0"),
    template("{column_name} >= 0"),
    template("{column_name} IN ('A', 'B', 'C')"),
    template("LENGTH({column_name}) > 0"),
    template("{column_name} ~ '^[A-Z]+$'")
))

grammar.rule("ref_table", choice(
    "users", "products", "orders", "customers", "categories"
))

grammar.rule("ref_column", choice("id", "code", "uuid"))

# ==================== CREATE TABLE ====================

grammar.rule("create_table", template(
    """CREATE TABLE {if_not_exists} {table_name} (
{columns}
{table_constraints}
){table_options}""",
    if_not_exists=maybe("IF NOT EXISTS", 0.3),
    table_name=ref("new_table_name"),
    columns=ref("column_definitions"),
    table_constraints=ref("table_constraints"),
    table_options=ref("table_options")
))

grammar.rule("new_table_name", choice(
    "users", "products", "orders", "customers", "inventory",
    "categories", "suppliers", "employees", "departments",
    "transactions", "audit_log", "settings", "permissions"
))

grammar.rule("column_definitions", repeat(
    ref("column_definition"),
    min=3,
    max=8,
    separator=",\n"
))

grammar.rule("column_definition", template(
    "  {column_name} {data_type} {constraints}",
    column_name=ref("column_name"),
    data_type=ref("data_type"),
    constraints=repeat(ref("column_constraint"), min=0, max=2, separator=" ")
))

grammar.rule("column_name", choice(
    "id", "uuid", "name", "email", "password_hash",
    "status", "type", "category", "description",
    "created_at", "updated_at", "deleted_at",
    "price", "quantity", "total", "discount",
    "user_id", "product_id", "order_id", "customer_id",
    "is_active", "is_verified", "is_admin",
    "metadata", "settings", "attributes",
    "start_date", "end_date", "expires_at"
))

grammar.rule("table_constraints", maybe(
    template(",\n{constraint_list}"),
    probability=0.5
))

grammar.rule("constraint_list", repeat(
    ref("table_constraint"),
    min=1,
    max=3,
    separator=",\n"
))

grammar.rule("table_constraint", choice(
    template("  CONSTRAINT {constraint_name} PRIMARY KEY ({column_list})"),
    template("  CONSTRAINT {constraint_name} UNIQUE ({column_list})"),
    template("  CONSTRAINT {constraint_name} FOREIGN KEY ({column_name}) REFERENCES {ref_table}({ref_column})"),
    template("  CONSTRAINT {constraint_name} CHECK ({table_check_condition})")
))

grammar.rule("constraint_name", Lambda(lambda ctx: 
    f"{ctx.rng.choice(['pk', 'uk', 'fk', 'ck'])}_{ctx.rng.choice(['users', 'products', 'orders'])}_{ctx.rng.randint(1, 999)}"
))

grammar.rule("column_list", repeat(
    ref("column_name"),
    min=1,
    max=3,
    separator=", "
))

grammar.rule("table_check_condition", choice(
    "price > 0",
    "quantity >= 0",
    "start_date < end_date",
    "status IN ('active', 'inactive', 'pending')"
))

grammar.rule("table_options", maybe(
    choice(
        " WITH (fillfactor=70)",
        " TABLESPACE fast_ssd",
        " PARTITION BY RANGE (created_at)"
    ),
    probability=0.2
))

# ==================== ALTER TABLE ====================

grammar.rule("alter_table", choice(
    ref("add_column"),
    ref("drop_column"),
    ref("alter_column"),
    ref("add_constraint"),
    ref("drop_constraint"),
    ref("rename_table"),
    ref("rename_column")
))

grammar.rule("add_column", template(
    "ALTER TABLE {existing_table} ADD COLUMN {column_definition}"
))

grammar.rule("drop_column", template(
    "ALTER TABLE {existing_table} DROP COLUMN {if_exists} {column_name} {cascade}",
    if_exists=maybe("IF EXISTS", 0.3),
    cascade=maybe("CASCADE", 0.2)
))

grammar.rule("alter_column", choice(
    template("ALTER TABLE {existing_table} ALTER COLUMN {column_name} TYPE {data_type}"),
    template("ALTER TABLE {existing_table} ALTER COLUMN {column_name} SET DEFAULT {default_value}"),
    template("ALTER TABLE {existing_table} ALTER COLUMN {column_name} DROP DEFAULT"),
    template("ALTER TABLE {existing_table} ALTER COLUMN {column_name} SET NOT NULL"),
    template("ALTER TABLE {existing_table} ALTER COLUMN {column_name} DROP NOT NULL")
))

grammar.rule("add_constraint", template(
    "ALTER TABLE {existing_table} ADD CONSTRAINT {constraint_name} {constraint_type}"
))

grammar.rule("constraint_type", choice(
    template("PRIMARY KEY ({column_list})"),
    template("UNIQUE ({column_list})"),
    template("FOREIGN KEY ({column_name}) REFERENCES {ref_table}({ref_column})"),
    template("CHECK ({table_check_condition})")
))

grammar.rule("drop_constraint", template(
    "ALTER TABLE {existing_table} DROP CONSTRAINT {if_exists} {constraint_name} {cascade}",
    if_exists=maybe("IF EXISTS", 0.3),
    cascade=maybe("CASCADE", 0.2)
))

grammar.rule("rename_table", template(
    "ALTER TABLE {existing_table} RENAME TO {new_table_name}"
))

grammar.rule("rename_column", template(
    "ALTER TABLE {existing_table} RENAME COLUMN {column_name} TO {new_column_name}"
))

grammar.rule("existing_table", choice(
    "users", "products", "orders", "customers"
))

grammar.rule("new_column_name", Lambda(lambda ctx:
    f"new_{ctx.rng.choice(['field', 'column', 'attr'])}_{ctx.rng.randint(1, 99)}"
))

# ==================== CREATE INDEX ====================

grammar.rule("create_index", template(
    "CREATE {unique} INDEX {if_not_exists} {index_name} ON {table_name} {index_method} ({index_columns}) {index_options} {where_clause}",
    unique=maybe("UNIQUE", 0.2),
    if_not_exists=maybe("IF NOT EXISTS", 0.3),
    index_name=ref("index_name"),
    table_name=ref("existing_table"),
    index_method=ref("index_method"),
    index_columns=ref("index_column_list"),
    index_options=ref("index_options"),
    where_clause=maybe(template("WHERE {index_condition}"), 0.1)
))

grammar.rule("index_name", Lambda(lambda ctx:
    f"idx_{ctx.rng.choice(['users', 'products', 'orders'])}_{ctx.rng.choice(['email', 'status', 'created', 'name'])}_{ctx.rng.randint(1, 99)}"
))

grammar.rule("index_method", choice(
    "USING btree",
    "USING hash",
    "USING gin",
    "USING gist",
    "",  # Default B-tree
    weights=[40, 10, 15, 5, 30]
))

grammar.rule("index_column_list", choice(
    ref("column_name"),
    template("{column_name}, {column_name}"),
    template("{column_name} DESC"),
    template("LOWER({column_name})"),
    template("({column_name}, {column_name}) INCLUDE ({column_name})")
))

grammar.rule("index_options", maybe(
    choice(
        "WITH (fillfactor=70)",
        "TABLESPACE indexes",
        "WITH (fillfactor=80, parallel_workers=4)"
    ),
    probability=0.2
))

grammar.rule("index_condition", choice(
    "status = 'active'",
    "deleted_at IS NULL",
    "is_active = true"
))

# ==================== CREATE VIEW ====================

grammar.rule("create_view", template(
    "CREATE {or_replace} VIEW {view_name} AS\n{select_statement}",
    or_replace=maybe("OR REPLACE", 0.5),
    view_name=ref("view_name"),
    select_statement=ref("view_select")
))

grammar.rule("view_name", Lambda(lambda ctx:
    f"v_{ctx.rng.choice(['active', 'summary', 'report', 'latest'])}_{ctx.rng.choice(['users', 'orders', 'products'])}"
))

grammar.rule("view_select", choice(
    # Simple view
    template("""SELECT {column_list}
FROM {existing_table}
WHERE {view_condition}"""),
    
    # Join view
    template("""SELECT 
  u.name as user_name,
  COUNT(o.id) as order_count,
  SUM(o.total) as total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
GROUP BY u.id, u.name"""),
    
    # Complex view with CTE
    template("""WITH recent_orders AS (
  SELECT * FROM orders
  WHERE created_at > CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
  p.name as product_name,
  COUNT(ro.id) as recent_order_count
FROM products p
JOIN recent_orders ro ON p.id = ro.product_id
GROUP BY p.id, p.name""")
))

grammar.rule("view_condition", choice(
    "status = 'active'",
    "created_at > CURRENT_DATE - INTERVAL '7 days'",
    "is_verified = true"
))

# ==================== DROP Statements ====================

grammar.rule("drop_statement", choice(
    ref("drop_table"),
    ref("drop_index"),
    ref("drop_view"),
    ref("drop_constraint")
))

grammar.rule("drop_table", template(
    "DROP TABLE {if_exists} {existing_table} {cascade}",
    if_exists=maybe("IF EXISTS", 0.7),
    cascade=maybe("CASCADE", 0.3)
))

grammar.rule("drop_index", template(
    "DROP INDEX {if_exists} {index_name} {cascade}",
    if_exists=maybe("IF EXISTS", 0.7),
    cascade=maybe("CASCADE", 0.1)
))

grammar.rule("drop_view", template(
    "DROP VIEW {if_exists} {view_name} {cascade}",
    if_exists=maybe("IF EXISTS", 0.7),
    cascade=maybe("CASCADE", 0.2)
))

# ==================== Schema Management ====================

grammar.rule("create_schema", template(
    "CREATE SCHEMA {if_not_exists} {schema_name} {authorization}",
    if_not_exists=maybe("IF NOT EXISTS", 0.5),
    schema_name=ref("schema_name"),
    authorization=maybe(template("AUTHORIZATION {role_name}"), 0.3)
))

grammar.rule("schema_name", choice(
    "app", "analytics", "audit", "staging", "archive"
))

grammar.rule("role_name", choice(
    "app_user", "admin", "readonly", "analyst"
))

# ==================== Main DDL Rule ====================

grammar.rule("ddl", choice(
    ref("create_table"),
    ref("alter_table"),
    ref("create_index"),
    ref("create_view"),
    ref("drop_statement"),
    ref("create_schema"),
    weights=[30, 25, 20, 15, 8, 2]
))

# ==================== Entry Point ====================

if __name__ == "__main__":
    """Test the grammar by generating sample DDL statements."""
    
    print("DDL Schema Grammar - Sample Statements")
    print("=" * 50)
    
    # Generate different DDL types
    ddl_types = [
        ("CREATE TABLE", "create_table"),
        ("ALTER TABLE", "alter_table"),
        ("CREATE INDEX", "create_index"),
        ("CREATE VIEW", "create_view"),
        ("DROP", "drop_statement")
    ]
    
    for ddl_name, ddl_rule in ddl_types:
        print(f"\n{ddl_name} Examples:")
        print("-" * 50)
        
        for i in range(2):
            statement = grammar.generate(ddl_rule, seed=i * 20)
            print(f"\n{statement};\n")
    
    print("\nMixed DDL statements:")
    print("-" * 50)
    
    for i in range(5):
        statement = grammar.generate("ddl", seed=i * 15)
        print(f"\n{statement};\n")