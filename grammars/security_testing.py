"""
Security Testing Grammar for PostgreSQL
Tests GRANT, REVOKE, ROLE management, RLS (Row Level Security), and security edge cases
"""

import sys
from pathlib import Path

from pyrqg.dsl.core import Grammar, choice, template, ref, maybe, repeat
try:
    import psycopg2  # type: ignore
except Exception:
    psycopg2 = None

# Get real database objects
def get_database_objects():
    """Fetch actual tables, columns, and roles from database"""
    try:
        conn = psycopg2.connect("dbname=postgres")
        cur = conn.cursor()
        
        # Get tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            LIMIT 10
        """)
        tables = [row[0] for row in cur.fetchall()]
        
        # Get columns with their tables
        table_columns = {}
        for table in tables:
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s
                LIMIT 5
            """, (table,))
            table_columns[table] = [row[0] for row in cur.fetchall()]
        
        # Get current user and database
        cur.execute("SELECT current_user, current_database()")
        row = cur.fetchone()
        current_user = row[0]
        current_db = row[1]
        
        # Get schemas
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
            LIMIT 5
        """)
        schemas = [row[0] for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return tables, table_columns, current_user, current_db, schemas
    except:
        # Fallback values
        return (
            ["users", "products", "orders"],
            {"users": ["id", "name", "email"], "products": ["id", "name", "price"]},
            "postgres",
            "postgres",
            ["public"]
        )

tables, table_columns, current_user, current_db, schemas = get_database_objects()

# Ensure we have some values
if not tables:
    tables = ["users", "products"]
if not schemas:
    schemas = ["public"]

# Get all columns
all_columns = []
for cols in table_columns.values():
    all_columns.extend(cols)
all_columns = list(set(all_columns)) or ["id", "name"]

g = Grammar("security_testing")

# Main security operations
g.rule("query",
    choice(
        ref("grant_operations"),
        ref("revoke_operations"),
        ref("alter_default_privileges"),
        ref("column_security"),
        ref("row_level_security"),
        ref("security_views"),
        weights=[25, 20, 15, 15, 15, 10]
    )
)

# GRANT operations on real objects
g.rule("grant_operations",
    choice(
        # Table privileges
        template("GRANT {table_privilege} ON TABLE {table} TO {grantee}"),
        template("GRANT {table_privilege} ON ALL TABLES IN SCHEMA {schema} TO {grantee}"),
        
        # Column privileges
        template("GRANT {column_privilege} ({column}) ON TABLE {table} TO {grantee}"),
        
        # Schema privileges
        template("GRANT {schema_privilege} ON SCHEMA {schema} TO {grantee}"),
        
        # Database privileges
        template("GRANT {database_privilege} ON DATABASE {current_database} TO {grantee}"),
        
        # Sequence privileges
        template("GRANT {sequence_privilege} ON ALL SEQUENCES IN SCHEMA {schema} TO {grantee}"),
        
        # Function privileges
        template("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {grantee}")
    )
)

# REVOKE operations
g.rule("revoke_operations",
    choice(
        # Basic revoke
        template("REVOKE {table_privilege} ON TABLE {table} FROM {grantee}"),
        template("REVOKE {column_privilege} ({column}) ON TABLE {table} FROM {grantee}"),
        
        # Revoke with cascade
        template("REVOKE {table_privilege} ON TABLE {table} FROM {grantee} CASCADE"),
        
        # Revoke from all
        template("REVOKE {schema_privilege} ON SCHEMA {schema} FROM PUBLIC"),
        
        # Revoke grant option
        template("REVOKE GRANT OPTION FOR {table_privilege} ON TABLE {table} FROM {grantee}")
    )
)

# ALTER DEFAULT PRIVILEGES
g.rule("alter_default_privileges",
    choice(
        template("ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT {table_privilege} ON TABLES TO {grantee}"),
        template("ALTER DEFAULT PRIVILEGES FOR ROLE {current_user} IN SCHEMA {schema} GRANT SELECT ON TABLES TO {grantee}"),
        template("ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC")
    )
)

# Column-level security
g.rule("column_security",
    choice(
        # Grant column privileges
        template("GRANT SELECT ({column}) ON TABLE {table} TO {grantee}"),
        template("GRANT UPDATE ({column}) ON TABLE {table} TO {grantee}"),
        
        # Revoke column privileges
        template("REVOKE SELECT ({column}) ON TABLE {table} FROM {grantee}"),
        template("REVOKE ALL PRIVILEGES ({column}) ON TABLE {table} FROM PUBLIC")
    )
)

# Row Level Security (RLS)
g.rule("row_level_security",
    choice(
        # Enable RLS
        template("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"),
        
        # Create policies
        template("""CREATE POLICY {policy_name} ON {table}
    FOR {policy_command}
    TO {grantee}
    USING ({rls_condition})"""),
        
        # Create policy with check
        template("""CREATE POLICY {policy_name} ON {table}
    AS {policy_type}
    FOR ALL
    TO {grantee}
    USING ({rls_condition})
    WITH CHECK ({rls_check_condition})"""),
        
        # Drop policy
        template("DROP POLICY IF EXISTS {policy_name} ON {table}"),
        
        # Alter policy
        template("ALTER POLICY {policy_name} ON {table} TO {grantee}")
    )
)

# Security views
g.rule("security_views",
    choice(
        # Create view with security
        template("""CREATE OR REPLACE VIEW {view_name} AS
SELECT {columns}
FROM {table}
WHERE {view_condition}
WITH CHECK OPTION"""),
        
        # Create security barrier view
        template("""CREATE OR REPLACE VIEW {view_name} WITH (security_barrier) AS
SELECT {columns}
FROM {table}
WHERE {view_condition}"""),
        
        # Grant on view
        template("GRANT SELECT ON {view_name} TO {grantee}")
    )
)

# Dynamic values based on actual database
g.rule("table", choice(*tables))
g.rule("schema", choice(*schemas))
g.rule("column", choice(*all_columns))
g.rule("current_database", current_db)
g.rule("current_user", current_user)

# Grantees - use safe values
g.rule("grantee", choice("PUBLIC", "CURRENT_USER", current_user))

# Privileges
g.rule("table_privilege", choice("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "ALL PRIVILEGES"))
g.rule("column_privilege", choice("SELECT", "INSERT", "UPDATE", "REFERENCES"))
g.rule("schema_privilege", choice("USAGE", "CREATE", "ALL PRIVILEGES"))
g.rule("database_privilege", choice("CONNECT", "CREATE"))
g.rule("sequence_privilege", choice("USAGE", "SELECT", "UPDATE", "ALL PRIVILEGES"))

# Policy related
g.rule("policy_name", choice("user_policy", "tenant_policy", "read_policy", "write_policy"))
g.rule("policy_command", choice("SELECT", "INSERT", "UPDATE", "DELETE", "ALL"))
g.rule("policy_type", choice("PERMISSIVE", "RESTRICTIVE"))

# RLS conditions - use actual columns
g.rule("rls_condition", choice(
    template("{column} = current_user"),
    template("{column} IS NOT NULL"),
    template("{column} = CURRENT_USER::text"),
    "true"  # Allow all for testing
))

g.rule("rls_check_condition", choice(
    template("{column} = current_user"),
    template("{column} IS NOT NULL"),
    "true"
))

# View related
g.rule("view_name", choice("user_view", "public_view", "restricted_view", "audit_view"))
g.rule("view_condition", choice(
    template("{column} IS NOT NULL"),
    template("{column} = CURRENT_USER"),
    "1=1"  # All rows
))

# Dynamic column selection
g.rule("columns", choice(
    "*",
    template("{column}"),
    template("{column}, {column}")
))

# Export grammar
grammar = g
