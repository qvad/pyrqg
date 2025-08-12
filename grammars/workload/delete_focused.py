"""
Schema-aware DELETE-focused Grammar for Workload Testing
This version actually knows which columns belong to which tables!
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda
from pyrqg.perfect_schema_registry import get_perfect_registry

g = Grammar("delete_workload_v2")

# ============================================================================
# Main rule - different DELETE patterns
# ============================================================================

g.rule("query",
    choice(
        ref("simple_delete"),          # 40% - Basic delete with WHERE
        ref("delete_subquery"),        # 30% - With subquery
        ref("delete_using"),           # 20% - DELETE USING (PostgreSQL)
        ref("delete_cascade"),         # 10% - Cascading deletes
        weights=[40, 30, 20, 10]
    )
)

# ============================================================================
# Schema-aware DELETE patterns
# ============================================================================

g.rule("simple_delete",
    Lambda(lambda ctx: generate_delete(ctx, simple=True))
)

g.rule("delete_subquery",
    Lambda(lambda ctx: generate_delete(ctx, with_subquery=True))
)

g.rule("delete_using",
    Lambda(lambda ctx: generate_delete(ctx, with_using=True))
)

g.rule("delete_cascade",
    Lambda(lambda ctx: generate_delete(ctx, cascade=True))
)

# ============================================================================
# Schema-aware generation functions
# ============================================================================

def generate_delete(ctx, simple=False, with_subquery=False, with_using=False, cascade=False):
    """Generate a schema-aware DELETE statement"""
    registry = get_perfect_registry()
    
    # Pick a random table
    table = ctx.rng.choice(registry.get_tables())
    columns = registry.tables[table]
    
    if simple:
        # Simple DELETE with WHERE
        where = generate_where_clause(ctx, table)
        return f"DELETE FROM {table} WHERE {where}"
    
    elif with_subquery:
        # DELETE with subquery
        # Find a column that could be used in subquery
        id_cols = []
        for col in ['id', 'user_id', 'customer_id', 'product_id', 'order_id']:
            if col in columns:
                id_cols.append(col)
        
        if id_cols:
            col = ctx.rng.choice(id_cols)
            
            # Find another table that has the same column
            other_tables = []
            for t in registry.get_tables():
                if t != table and col in registry.tables[t]:
                    other_tables.append(t)
            
            if other_tables:
                sub_table = ctx.rng.choice(other_tables)
                sub_where = generate_where_clause(ctx, sub_table)
                
                return (f"DELETE FROM {table} WHERE {col} IN "
                       f"(SELECT {col} FROM {sub_table} WHERE {sub_where})")
        
        # Fallback to simple delete
        where = generate_where_clause(ctx, table)
        return f"DELETE FROM {table} WHERE {where}"
    
    elif with_using:
        # DELETE USING - PostgreSQL specific
        # Find another table to join with
        table2 = ctx.rng.choice([t for t in registry.get_tables() if t != table])
        
        # Find common columns for join
        common_cols = []
        for col in ['user_id', 'customer_id', 'product_id', 'order_id']:
            if col in columns and col in registry.tables[table2]:
                common_cols.append(col)
        
        if common_cols:
            join_col = ctx.rng.choice(common_cols)
            delete_condition = generate_delete_condition(ctx, table)
            
            return (f"DELETE FROM {table} USING {table2} "
                   f"WHERE {table}.{join_col} = {table2}.{join_col} "
                   f"AND {delete_condition}")
        
        # Fallback to simple delete
        where = generate_where_clause(ctx, table)
        return f"DELETE FROM {table} WHERE {where}"
    
    elif cascade:
        # Cascading delete pattern - delete old/expired records
        date_cols = [c for c in columns if c.endswith('_at') or 'date' in c]
        
        if date_cols:
            date_col = ctx.rng.choice(date_cols)
            days_ago = ctx.rng.randint(30, 365)
            
            # Add additional conditions
            extra_cond = ""
            if 'status' in columns:
                extra_cond = f" AND status IN ('deleted', 'expired', 'cancelled')"
            elif 'active' in columns:
                extra_cond = " AND active = false"
            elif 'is_active' in columns:
                extra_cond = " AND is_active = false"
            
            return f"DELETE FROM {table} WHERE {date_col} < CURRENT_DATE - INTERVAL '{days_ago} days'{extra_cond}"
        
        # Fallback to status-based delete
        if 'status' in columns:
            return f"DELETE FROM {table} WHERE status IN ('deleted', 'cancelled', 'expired')"
        
        # Last resort
        where = generate_where_clause(ctx, table)
        return f"DELETE FROM {table} WHERE {where}"

def generate_where_clause(ctx, table):
    """Generate WHERE clause using actual table columns"""
    registry = get_perfect_registry()
    columns = registry.tables[table]
    
    conditions = []
    
    # ID-based conditions
    if 'id' in columns:
        conditions.append(f"id = {ctx.rng.randint(1, 1000)}")
        conditions.append(f"id IN ({', '.join(str(ctx.rng.randint(1, 100)) for _ in range(ctx.rng.randint(2, 5)))})")
    
    # Status-based conditions (common for deletes)
    if 'status' in columns:
        conditions.append("status = 'deleted'")
        conditions.append("status = 'cancelled'")
        conditions.append("status IN ('deleted', 'cancelled', 'expired')")
    
    # Date-based conditions (delete old records)
    date_cols = [c for c in columns if c.endswith('_at') or 'date' in c]
    for col in date_cols:
        conditions.append(f"{col} < CURRENT_DATE - INTERVAL '{ctx.rng.randint(30, 180)} days'")
    
    # Numeric conditions (zero quantity, negative balance, etc.)
    numeric_cols = []
    for col in columns:
        data_type = registry.column_types.get(f"{table}.{col}")
        if data_type in ['integer', 'bigint', 'numeric', 'decimal']:
            numeric_cols.append(col)
    
    for col in numeric_cols:
        if 'quantity' in col or 'count' in col:
            conditions.append(f"{col} = 0")
            conditions.append(f"{col} < 0")
        elif 'balance' in col:
            conditions.append(f"{col} < 0")
        elif 'retry' in col or 'attempt' in col:
            conditions.append(f"{col} > {ctx.rng.randint(3, 10)}")
    
    # Boolean conditions
    bool_cols = []
    for col in columns:
        data_type = registry.column_types.get(f"{table}.{col}")
        if data_type == 'boolean':
            bool_cols.append(col)
    
    for col in bool_cols:
        if 'deleted' in col:
            conditions.append(f"{col} = true")
        elif 'active' in col:
            conditions.append(f"{col} = false")
        elif 'expired' in col:
            conditions.append(f"{col} = true")
    
    # Specific user/customer conditions
    for col in ['user_id', 'customer_id']:
        if col in columns:
            conditions.append(f"{col} IN ({', '.join(str(ctx.rng.randint(1, 100)) for _ in range(3))})")
    
    if conditions:
        return ctx.rng.choice(conditions)
    else:
        # Fallback - delete specific ID
        if 'id' in columns:
            return f"id = {ctx.rng.randint(1, 1000)}"
        else:
            return "1=0"  # Safe fallback that deletes nothing

def generate_delete_condition(ctx, table):
    """Generate specific delete conditions for DELETE USING"""
    registry = get_perfect_registry()
    columns = registry.tables[table]
    
    conditions = []
    
    if 'status' in columns:
        conditions.append(f"{table}.status = 'inactive'")
        conditions.append(f"{table}.status = 'deleted'")
    
    if 'deleted' in columns:
        conditions.append(f"{table}.deleted = true")
    
    if 'is_deleted' in columns:
        conditions.append(f"{table}.is_deleted = true")
    
    if 'active' in columns:
        conditions.append(f"{table}.active = false")
    
    if 'is_active' in columns:
        conditions.append(f"{table}.is_active = false")
    
    # Date conditions
    date_cols = [c for c in columns if c.endswith('_at') or 'date' in c]
    for col in date_cols:
        if 'expiry' in col or 'expire' in col:
            conditions.append(f"{table}.{col} < CURRENT_DATE")
        elif 'last' in col:
            conditions.append(f"{table}.{col} < CURRENT_DATE - INTERVAL '90 days'")
    
    if conditions:
        return ctx.rng.choice(conditions)
    else:
        return f"{table}.id > 0"  # Safe condition

# Export grammar
grammar = g