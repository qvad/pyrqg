"""
Schema-aware SELECT-focused Grammar for Workload Testing
This version actually knows which columns belong to which tables!
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda
from pyrqg.perfect_schema_registry import get_perfect_registry

g = Grammar("select_workload_v2")

# ============================================================================
# Main rule - different SELECT patterns
# ============================================================================

g.rule("query",
    choice(
        ref("simple_select"),          # 25% - Basic SELECT
        ref("select_where"),           # 20% - With WHERE
        ref("select_join"),            # 15% - With JOIN
        ref("select_aggregate"),       # 15% - With aggregation
        ref("select_order_limit"),     # 15% - With ORDER BY and LIMIT
        ref("select_distinct"),        # 10% - With DISTINCT
        weights=[25, 20, 15, 15, 15, 10]
    )
)

# ============================================================================
# Schema-aware SELECT patterns
# ============================================================================

g.rule("simple_select",
    Lambda(lambda ctx: generate_select(ctx, simple=True))
)

g.rule("select_where",
    Lambda(lambda ctx: generate_select(ctx, with_where=True))
)

g.rule("select_join",
    Lambda(lambda ctx: generate_select(ctx, with_join=True))
)

g.rule("select_aggregate",
    Lambda(lambda ctx: generate_select(ctx, with_aggregate=True))
)

g.rule("select_order_limit",
    Lambda(lambda ctx: generate_select(ctx, with_order_limit=True))
)

g.rule("select_distinct",
    Lambda(lambda ctx: generate_select(ctx, with_distinct=True))
)

# ============================================================================
# Schema-aware generation functions
# ============================================================================

def generate_select(ctx, simple=False, with_where=False, with_join=False, 
                   with_aggregate=False, with_order_limit=False, with_distinct=False):
    """Generate a schema-aware SELECT statement"""
    registry = get_perfect_registry()
    
    # Pick a random table
    table = ctx.rng.choice(registry.get_tables())
    columns = registry.get_insertable_columns(table)
    
    if simple:
        # Simple SELECT
        cols = generate_column_list(ctx, table, columns)
        return f"SELECT {cols} FROM {table}"
    
    elif with_where:
        # SELECT with WHERE
        cols = generate_column_list(ctx, table, columns)
        where = generate_where_clause(ctx, table)
        return f"SELECT {cols} FROM {table} WHERE {where}"
    
    elif with_join:
        # SELECT with JOIN
        table2 = ctx.rng.choice([t for t in registry.get_tables() if t != table])
        
        # Find common columns for join
        common_id_cols = []
        for col in ['id', 'user_id', 'customer_id', 'product_id', 'order_id']:
            if col in columns and registry.column_exists(table2, col):
                common_id_cols.append(col)
        
        if common_id_cols:
            join_col = ctx.rng.choice(common_id_cols)
            cols1 = generate_table_columns(ctx, table, columns, 't1')
            cols2 = generate_table_columns(ctx, table2, registry.get_insertable_columns(table2), 't2')
            
            join_type = ctx.rng.choice(['INNER', 'LEFT', 'RIGHT'])
            where = generate_where_clause(ctx, table, 't1')
            
            return (f"SELECT {cols1}, {cols2} FROM {table} t1 "
                   f"{join_type} JOIN {table2} t2 ON t1.{join_col} = t2.{join_col} "
                   f"WHERE {where}")
        else:
            # No common columns, fall back to simple select
            cols = generate_column_list(ctx, table, columns)
            where = generate_where_clause(ctx, table)
            return f"SELECT {cols} FROM {table} WHERE {where}"
    
    elif with_aggregate:
        # SELECT with aggregation
        numeric_cols = get_numeric_columns(ctx, table, columns, registry)
        
        if numeric_cols:
            agg_col = ctx.rng.choice(numeric_cols)
            group_col = get_groupable_column(ctx, table, columns)
            
            agg_func = ctx.rng.choice(['COUNT', 'SUM', 'AVG', 'MAX', 'MIN'])
            
            if group_col and group_col != agg_col:
                having = ""
                if ctx.rng.random() < 0.3:
                    having = f" HAVING {agg_func}({agg_col}) > {ctx.rng.randint(10, 100)}"
                
                return (f"SELECT {group_col}, {agg_func}({agg_col}) as agg_value "
                       f"FROM {table} GROUP BY {group_col}{having}")
            else:
                # No grouping
                return f"SELECT {agg_func}({agg_col}) as agg_value FROM {table}"
        else:
            # No numeric columns, use COUNT
            group_col = get_groupable_column(ctx, table, columns)
            if group_col:
                return f"SELECT {group_col}, COUNT(*) as cnt FROM {table} GROUP BY {group_col}"
            else:
                return f"SELECT COUNT(*) as cnt FROM {table}"
    
    elif with_order_limit:
        # SELECT with ORDER BY and LIMIT
        cols = generate_column_list(ctx, table, columns)
        order_col = ctx.rng.choice([c for c in columns if c != 'data'])
        order_dir = ctx.rng.choice(['ASC', 'DESC'])
        limit = ctx.rng.randint(5, 50)
        
        where = ""
        if ctx.rng.random() < 0.5:
            where = f" WHERE {generate_where_clause(ctx, table)}"
        
        return f"SELECT {cols} FROM {table}{where} ORDER BY {order_col} {order_dir} LIMIT {limit}"
    
    elif with_distinct:
        # SELECT DISTINCT
        # Pick columns that are likely to have duplicates
        distinct_candidates = []
        for col in columns:
            if col in ['status', 'type', 'category', 'role', 'user_id', 'customer_id']:
                distinct_candidates.append(col)
        
        if distinct_candidates:
            cols = ctx.rng.sample(distinct_candidates, min(3, len(distinct_candidates)))
            return f"SELECT DISTINCT {', '.join(cols)} FROM {table}"
        else:
            # Fall back to regular select
            cols = generate_column_list(ctx, table, columns)
            return f"SELECT DISTINCT {cols} FROM {table}"

def generate_column_list(ctx, table, columns, max_cols=5):
    """Generate a list of columns for SELECT"""
    registry = get_perfect_registry()
    
    if ctx.rng.random() < 0.1:
        return "*"
    
    # Filter out complex types
    simple_cols = [c for c in columns if c not in ['data', 'metadata', 'properties', 'tags']]
    
    if not simple_cols:
        return "*"
    
    num_cols = min(ctx.rng.randint(1, max_cols), len(simple_cols))
    selected = ctx.rng.sample(simple_cols, num_cols)
    
    return ", ".join(selected)

def generate_table_columns(ctx, table, columns, alias):
    """Generate columns with table alias"""
    simple_cols = [c for c in columns if c not in ['data', 'metadata', 'properties', 'tags']]
    
    if not simple_cols:
        return f"{alias}.*"
    
    num_cols = min(ctx.rng.randint(1, 3), len(simple_cols))
    selected = ctx.rng.sample(simple_cols, num_cols)
    
    return ", ".join([f"{alias}.{col}" for col in selected])

def generate_where_clause(ctx, table, alias=None):
    """Generate WHERE clause using actual table columns"""
    registry = get_perfect_registry()
    columns = registry.get_insertable_columns(table)
    
    prefix = f"{alias}." if alias else ""
    
    # Common patterns
    conditions = []
    
    if 'id' in columns:
        conditions.append(f"{prefix}id = {ctx.rng.randint(1, 1000)}")
        conditions.append(f"{prefix}id > {ctx.rng.randint(1, 100)}")
        conditions.append(f"{prefix}id IN ({', '.join(str(ctx.rng.randint(1, 100)) for _ in range(3))})")
    
    if 'status' in columns:
        conditions.append(f"{prefix}status = '{ctx.rng.choice(['active', 'inactive', 'pending'])}'")
    
    # Find numeric columns
    numeric_cols = get_numeric_columns(ctx, table, columns, registry)
    for col in numeric_cols[:3]:  # Limit to avoid too many options
        conditions.append(f"{prefix}{col} > {ctx.rng.randint(0, 100)}")
        conditions.append(f"{prefix}{col} BETWEEN {ctx.rng.randint(1, 50)} AND {ctx.rng.randint(51, 100)}")
    
    # Find date columns
    date_cols = [c for c in columns if c.endswith('_at') or 'date' in c]
    for col in date_cols[:2]:
        conditions.append(f"{prefix}{col} > CURRENT_DATE - INTERVAL '{ctx.rng.randint(1, 30)} days'")
    
    # String columns with LIKE
    string_cols = []
    for col in columns:
        if col in ['name', 'email', 'username', 'first_name', 'last_name']:
            string_cols.append(col)
    
    for col in string_cols[:2]:
        conditions.append(f"{prefix}{col} LIKE '{ctx.rng.choice(['A', 'B', 'C', 'D'])}%'")
    
    # IS NOT NULL checks
    if columns and ctx.rng.random() < 0.2:
        col = ctx.rng.choice(columns)
        conditions.append(f"{prefix}{col} IS NOT NULL")
    
    # Return a random condition
    if conditions:
        return ctx.rng.choice(conditions)
    else:
        return "1=1"

def get_numeric_columns(ctx, table, columns, registry):
    """Get numeric columns from a table"""
    numeric_cols = []
    tname = table.split('.')[-1] if '.' in table else table
    for col in columns:
        data_type = registry.column_types.get(f"{tname}.{col}")
        if data_type in ['integer', 'bigint', 'numeric', 'decimal', 'real', 'double precision']:
            numeric_cols.append(col)
    return numeric_cols

def get_groupable_column(ctx, table, columns):
    """Get a column suitable for GROUP BY"""
    candidates = []
    
    # Prefer these columns for grouping
    preferred = ['status', 'type', 'category', 'role', 'user_id', 'customer_id', 'product_id']
    
    for col in preferred:
        if col in columns:
            candidates.append(col)
    
    # Add other non-unique columns
    for col in columns:
        if col not in ['id', 'created_at', 'updated_at'] and col not in candidates:
            if not col.endswith('_at') and col not in ['data', 'metadata', 'properties']:
                candidates.append(col)
    
    if candidates:
        return ctx.rng.choice(candidates)
    return None

# Export grammar
grammar = g
