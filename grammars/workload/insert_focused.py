"""
Schema-aware INSERT-focused Grammar for Workload Testing
This version actually knows which columns belong to which tables!
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda
from pyrqg.perfect_schema_registry import get_perfect_registry

g = Grammar("insert_workload_v2")

# ============================================================================
# Main rule - different INSERT patterns
# ============================================================================

g.rule("query",
    choice(
        ref("simple_insert"),          # 40% - Basic single row
        ref("multi_row_insert"),       # 30% - Multiple rows
        ref("insert_select"),          # 20% - INSERT ... SELECT
        ref("insert_default"),         # 10% - With DEFAULT values
        weights=[40, 30, 20, 10]
    )
)

# ============================================================================
# Schema-aware INSERT patterns
# ============================================================================

g.rule("simple_insert",
    Lambda(lambda ctx: generate_insert(ctx, multi_row=False))
)

g.rule("multi_row_insert",
    Lambda(lambda ctx: generate_insert(ctx, multi_row=True))
)

g.rule("insert_select",
    Lambda(lambda ctx: generate_insert_select(ctx))
)

g.rule("insert_default",
    Lambda(lambda ctx: generate_insert_with_defaults(ctx))
)

# ============================================================================
# Helper rules for values
# ============================================================================

g.rule("number", Lambda(lambda ctx: str(ctx.rng.randint(1, 1000))))
g.rule("decimal", Lambda(lambda ctx: f"{ctx.rng.randint(1, 10000)}.{ctx.rng.randint(0, 99):02d}"))
g.rule("email", choice("'user@example.com'", "'admin@example.com'", "'test@example.com'"))
g.rule("name", choice("'Alice'", "'Bob'", "'Charlie'", "'David'", "'Eve'"))
g.rule("status", choice("'active'", "'inactive'", "'pending'", "'completed'", "'cancelled'"))
g.rule("timestamp", "CURRENT_TIMESTAMP")
g.rule("json", "'{}'::jsonb")
g.rule("boolean", choice("true", "false"))
g.rule("string", choice("'Product X'", "'Item Y'", "'Service Z'", "'Test Data'"))

# ============================================================================
# Schema-aware generation functions
# ============================================================================

def generate_insert(ctx, multi_row=False):
    """Generate a schema-aware INSERT statement"""
    registry = get_perfect_registry()
    
    # Pick a random table
    table = ctx.rng.choice(registry.get_tables())
    
    # Get insertable columns for this table
    all_columns = registry.get_insertable_columns(table)
    if not all_columns:
        return f"INSERT INTO {table} DEFAULT VALUES"
    
    # Pick 1-6 random columns (allow small tables)
    upper = min(6, len(all_columns))
    lower = 1 if upper < 3 else 3
    num_columns = ctx.rng.randint(lower, upper)
    columns = ctx.rng.sample(all_columns, num_columns)
    
    # Generate values for each column
    if multi_row:
        # Generate 2-5 rows
        num_rows = ctx.rng.randint(2, 5)
        rows = []
        for _ in range(num_rows):
            values = [generate_value_for_column(ctx, col, table) for col in columns]
            rows.append(f"({', '.join(values)})")
        values_clause = ', '.join(rows)
    else:
        # Single row
        values = [generate_value_for_column(ctx, col, table) for col in columns]
        values_clause = f"({', '.join(values)})"
    
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES {values_clause}"

def generate_insert_select(ctx):
    """Generate INSERT ... SELECT statement"""
    registry = get_perfect_registry()
    
    # Pick target and source tables
    tables = registry.get_tables()
    target_table = ctx.rng.choice(tables)
    source_table = ctx.rng.choice(tables)
    
    # Get common column types
    target_columns = registry.get_insertable_columns(target_table)
    if not target_columns:
        return f"INSERT INTO {target_table} DEFAULT VALUES"
    
    # Pick 1-5 columns (allow small tables)
    upper = min(5, len(target_columns))
    lower = 1 if upper < 3 else 3
    num_columns = ctx.rng.randint(lower, upper)
    columns = ctx.rng.sample(target_columns, num_columns)
    
    # Generate SELECT columns - only use columns that exist in source table
    select_columns = []
    for col in columns:
        if registry.column_exists(source_table, col):
            # Column exists in source table
            select_columns.append(col)
        else:
            # Generate a literal value for missing column
            select_columns.append(f"{generate_value_for_column(ctx, col, target_table)} AS {col}")
    
    # Generate WHERE condition - only use columns that exist in source
    where_candidates = []
    if registry.column_exists(source_table, 'id'):
        where_candidates.append('id')
    if registry.column_exists(source_table, 'status'):
        where_candidates.append('status')
    if registry.column_exists(source_table, 'created_at'):
        where_candidates.append('created_at')
    
    if where_candidates:
        where_column = ctx.rng.choice(where_candidates)
        if where_column == 'id':
            where_clause = f"{where_column} > {ctx.rng.randint(1, 100)}"
        elif where_column == 'status':
            where_clause = f"{where_column} = 'active'"
        else:
            where_clause = f"{where_column} > CURRENT_DATE - INTERVAL '{ctx.rng.randint(1, 30)} days'"
    else:
        where_clause = "1=1"  # Always true if no suitable columns
    
    return (f"INSERT INTO {target_table} ({', '.join(columns)}) "
            f"SELECT {', '.join(select_columns)} FROM {source_table} "
            f"WHERE {where_clause}")

def generate_insert_with_defaults(ctx):
    """Generate INSERT with DEFAULT values"""
    registry = get_perfect_registry()
    
    table = ctx.rng.choice(registry.get_tables())
    all_columns = registry.get_insertable_columns(table)
    
    if not all_columns:
        return f"INSERT INTO {table} DEFAULT VALUES"
    
    # Pick 1-4 columns (allow small tables)
    upper = min(4, len(all_columns))
    lower = 1 if upper < 2 else 2
    num_columns = ctx.rng.randint(lower, upper)
    columns = ctx.rng.sample(all_columns, num_columns)
    
    # Mix real values and DEFAULT
    values = []
    for col in columns:
        if ctx.rng.random() < 0.3:  # 30% chance of DEFAULT
            values.append("DEFAULT")
        else:
            values.append(generate_value_for_column(ctx, col, table))
    
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"

def generate_value_for_column(ctx, column, table=None):
    """Generate appropriate value for a column based on its type"""
    registry = get_perfect_registry()
    return registry.get_column_value(column, ctx.rng, table)

# Export grammar
grammar = g
