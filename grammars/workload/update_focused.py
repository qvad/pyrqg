"""
Schema-aware UPDATE-focused Grammar for Workload Testing
This version actually knows which columns belong to which tables!
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda
from pyrqg.perfect_schema_registry import get_perfect_registry

g = Grammar("update_workload_v2")

# ============================================================================
# Main rule - different UPDATE patterns
# ============================================================================

g.rule("query",
    choice(
        ref("simple_update"),          # 30% - Basic update
        ref("conditional_update"),     # 25% - With WHERE clause
        ref("multi_column_update"),    # 20% - Multiple columns
        ref("calculated_update"),      # 15% - With calculations
        ref("case_update"),           # 10% - With CASE
        weights=[30, 25, 20, 15, 10]
    )
)

# ============================================================================
# Schema-aware UPDATE patterns
# ============================================================================

g.rule("simple_update",
    Lambda(lambda ctx: generate_update(ctx, simple=True))
)

g.rule("conditional_update",
    Lambda(lambda ctx: generate_update(ctx, with_where=True))
)

g.rule("multi_column_update",
    Lambda(lambda ctx: generate_update(ctx, multi_column=True))
)

g.rule("calculated_update",
    Lambda(lambda ctx: generate_update(ctx, with_calculation=True))
)

g.rule("case_update",
    Lambda(lambda ctx: generate_update(ctx, with_case=True))
)

# ============================================================================
# Schema-aware generation functions
# ============================================================================

def generate_update(ctx, simple=False, with_where=False, multi_column=False, 
                   with_calculation=False, with_case=False):
    """Generate a schema-aware UPDATE statement"""
    registry = get_perfect_registry()
    
    # Pick a random table
    table = ctx.rng.choice(registry.get_tables())
    
    # Get updateable columns (exclude id)
    columns = [c for c in registry.tables[table] if c != 'id']
    if not columns:
        return f"-- No updateable columns in {table}"
    
    # Generate assignments
    if simple:
        col = ctx.rng.choice(columns)
        assignment = generate_assignment(ctx, table, col)
        return f"UPDATE {table} SET {assignment}"
    
    elif with_where:
        col = ctx.rng.choice(columns)
        assignment = generate_assignment(ctx, table, col)
        where = generate_where_clause(ctx, table)
        return f"UPDATE {table} SET {assignment} WHERE {where}"
    
    elif multi_column:
        num_cols = min(ctx.rng.randint(2, 4), len(columns))
        selected_cols = ctx.rng.sample(columns, num_cols)
        assignments = [generate_assignment(ctx, table, col) for col in selected_cols]
        where = generate_where_clause(ctx, table)
        return f"UPDATE {table} SET {', '.join(assignments)} WHERE {where}"
    
    elif with_calculation:
        # Find numeric columns
        numeric_cols = []
        for col in columns:
            data_type = registry.column_types.get(f"{table}.{col}")
            if data_type in ['integer', 'bigint', 'numeric', 'decimal']:
                numeric_cols.append(col)
        
        if numeric_cols:
            col = ctx.rng.choice(numeric_cols)
            op = ctx.rng.choice(['+', '-', '*'])
            value = ctx.rng.randint(1, 10)
            assignment = f"{col} = {col} {op} {value}"
        else:
            # Fallback to simple assignment
            col = ctx.rng.choice(columns)
            assignment = generate_assignment(ctx, table, col)
        
        where = generate_where_clause(ctx, table)
        return f"UPDATE {table} SET {assignment} WHERE {where}"
    
    elif with_case:
        col = ctx.rng.choice(columns)
        condition_col = get_condition_column(ctx, table)
        val1 = registry.get_column_value(col, ctx.rng, table)
        val2 = registry.get_column_value(col, ctx.rng, table)
        
        assignment = f"{col} = CASE WHEN {condition_col} THEN {val1} ELSE {val2} END"
        where = generate_where_clause(ctx, table)
        return f"UPDATE {table} SET {assignment} WHERE {where}"

def generate_assignment(ctx, table, column):
    """Generate assignment for a column"""
    registry = get_perfect_registry()
    value = registry.get_column_value(column, ctx.rng, table)
    return f"{column} = {value}"

def generate_where_clause(ctx, table):
    """Generate WHERE clause using actual table columns"""
    registry = get_perfect_registry()
    columns = registry.tables[table]
    
    # Common patterns
    if 'id' in columns and ctx.rng.random() < 0.3:
        return f"id = {ctx.rng.randint(1, 1000)}"
    
    if 'status' in columns and ctx.rng.random() < 0.3:
        return f"status = '{ctx.rng.choice(['active', 'inactive', 'pending'])}'"
    
    # Find numeric columns for range conditions
    numeric_cols = []
    for col in columns:
        data_type = registry.column_types.get(f"{table}.{col}")
        if data_type in ['integer', 'bigint', 'numeric', 'decimal']:
            numeric_cols.append(col)
    
    if numeric_cols and ctx.rng.random() < 0.3:
        col = ctx.rng.choice(numeric_cols)
        return f"{col} > {ctx.rng.randint(0, 100)}"
    
    # Find date columns
    date_cols = [c for c in columns if c.endswith('_at') or 'date' in c]
    if date_cols and ctx.rng.random() < 0.3:
        col = ctx.rng.choice(date_cols)
        return f"{col} < CURRENT_DATE - INTERVAL '{ctx.rng.randint(1, 30)} days'"
    
    # Default to id if available
    if 'id' in columns:
        return f"id IN ({', '.join(str(ctx.rng.randint(1, 100)) for _ in range(ctx.rng.randint(2, 5)))})"
    
    # Last resort - always true
    return "1=1"

def get_condition_column(ctx, table):
    """Get a column suitable for CASE conditions"""
    registry = get_perfect_registry()
    columns = registry.tables[table]
    
    # Prefer status columns
    if 'status' in columns:
        return f"status = '{ctx.rng.choice(['active', 'pending'])}'"
    
    # Numeric comparison
    numeric_cols = []
    for col in columns:
        data_type = registry.column_types.get(f"{table}.{col}")
        if data_type in ['integer', 'bigint']:
            numeric_cols.append(col)
    
    if numeric_cols:
        col = ctx.rng.choice(numeric_cols)
        return f"{col} > {ctx.rng.randint(50, 100)}"
    
    # Boolean columns
    bool_cols = [c for c in columns if c.startswith('is_') or c in ['active', 'deleted']]
    if bool_cols:
        col = ctx.rng.choice(bool_cols)
        return f"{col} = true"
    
    # Default
    return "1=1"

# Export grammar
grammar = g