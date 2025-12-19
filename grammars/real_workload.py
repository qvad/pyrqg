"""
Real Workload Grammar v5 (Tuned)
A comprehensive, state-aware SQL fuzzer for PostgreSQL/YugabyteDB.
Tuned to reduce common errors (NotNull, DivByZero, TypeMismatch).

Features:
- Recursive Expression Generation (AST-like)
- Strict Type Safety & Nullability Tracking
- Advanced DDL (Arrays, JSONB, Constraints)
- Complex Topologies (CTEs, Set Ops, Subqueries)
- Transactional Logic
"""

import uuid
import random
from pyrqg.dsl.core import Grammar, Choice, Lambda

g = Grammar("real_workload_v5")

# =============================================================================
# 1. Type System & State Management
# =============================================================================

# Compatible types for binary operations
NUMERIC_TYPES = {'INTEGER', 'BIGINT', 'DECIMAL(10,2)', 'REAL', 'DOUBLE PRECISION'}
STRING_TYPES = {'TEXT', 'VARCHAR(255)', 'CHAR(10)'}
DATETIME_TYPES = {'TIMESTAMP', 'DATE', 'TIMESTAMPTZ'}
# Reduce complex types frequency to stabilize base workload
COMPLEX_TYPES = {'BOOLEAN'} # JSONB, UUID, Arrays handled specifically
ALL_TYPES = list(NUMERIC_TYPES | STRING_TYPES | DATETIME_TYPES | COMPLEX_TYPES)

def _get_state(ctx):
    if not hasattr(ctx, 'db_state'):
        ctx.db_state = {'tables': {}, 'views': []}
    return ctx.db_state

def _map_postgres_type(dtype):
    dtype = dtype.upper()
    if 'INT' in dtype: return 'INTEGER'
    if 'CHAR' in dtype or 'TEXT' in dtype: return 'TEXT'
    if 'BOOL' in dtype: return 'BOOLEAN'
    if 'TIME' in dtype or 'DATE' in dtype: return 'TIMESTAMP'
    if 'NUM' in dtype or 'DEC' in dtype or 'FLOAT' in dtype or 'REAL' in dtype or 'DOUBLE' in dtype: return 'DECIMAL(10,2)'
    return 'TEXT' # Default fallback

def _get_tables(ctx):
    # Schema Awareness: Use catalog from SchemaAwareContext
    if hasattr(ctx, 'tables') and ctx.tables:
        # Convert SchemaAwareContext tables to local format on the fly
        # We use a cached version in state if available to avoid re-processing
        state = _get_state(ctx)
        if 'tables_cache' not in state:
            state['tables_cache'] = {}
            for t_name, t_meta in ctx.tables.items():
                cols = {}
                # Handle TableMetadata object
                for c_name, c_meta in t_meta.columns.items():
                    cols[c_name] = {
                        'type': _map_postgres_type(c_meta.data_type),
                        'nullable': c_meta.is_nullable
                    }
                state['tables_cache'][t_name] = {
                    'columns': cols,
                    'pk': [t_meta.primary_key] if t_meta.primary_key else []
                }
        return state['tables_cache']
    return {}

def _random_id():
    return str(uuid.uuid4()).replace('-', '')[:8]

def _pick_table(ctx):
    tables = _get_tables(ctx)
    if not tables: return None
    return ctx.rng.choice(list(tables.keys()))

def _gen_expr(ctx, available_cols, depth=0, desired_type=None):
    """
    Recursively generates a valid SQL expression.
    available_cols: dict of {col_name: type} available in current scope
    """
    rng = ctx.rng
    
    # Base case: deeper than limit or random stop
    if depth > 3 or (depth > 0 and rng.random() < 0.3):
        # Try to find a column matching desired type
        if available_cols and rng.random() < 0.7:
            if desired_type:
                candidates = []
                for c, t in available_cols.items():
                    if t == desired_type: candidates.append(c)
                    elif desired_type == 'NUM' and t in NUMERIC_TYPES: candidates.append(c)
                    elif desired_type == 'STR' and t in STRING_TYPES: candidates.append(c)
                    elif desired_type == 'BOOL' and t == 'BOOLEAN': candidates.append(c)
                
                if candidates: return rng.choice(candidates)
            else:
                return rng.choice(list(available_cols.keys()))
        
        # Fallback to Literal (Safer values)
        if not desired_type or desired_type in NUMERIC_TYPES or desired_type == 'NUM': 
            return str(rng.randint(1, 100)) # Avoid 0 to help with division
        if desired_type in STRING_TYPES or desired_type == 'STR': 
            return f"'{_random_id()}'"
        if desired_type == 'BOOLEAN' or desired_type == 'BOOL': 
            return rng.choice(['TRUE', 'FALSE'])
        if desired_type in DATETIME_TYPES:
            return f"(NOW() - interval '{rng.randint(0, 365)} days')"
        if '[]' in str(desired_type):
             return "ARRAY[1, 2]" # Simple default for arrays
        return "NULL"

    # Determine type of expression to generate
    # If no desired type, pick one concrete type to enforce consistency
    op_type = desired_type if desired_type else rng.choice(ALL_TYPES)
    
    # --- Advanced Control Flow ---
    # CASE WHEN (10% chance)
    if rng.random() < 0.1:
        cond = _gen_expr(ctx, available_cols, depth+1, 'BOOLEAN')
        val1 = _gen_expr(ctx, available_cols, depth+1, op_type)
        val2 = _gen_expr(ctx, available_cols, depth+1, op_type)
        return f"(CASE WHEN {cond} THEN {val1} ELSE {val2} END)"

    # --- Casting (5% chance) ---
    if rng.random() < 0.05 and depth < 2:
        # Cast FROM something TO op_type
        if op_type in STRING_TYPES or op_type == 'STR':
            # Cast Numeric to String
            inner = _gen_expr(ctx, available_cols, depth+1, 'NUM')
            return f"CAST({inner} AS {op_type if op_type != 'STR' else 'TEXT'})"
        if op_type == 'INTEGER':
             inner = _gen_expr(ctx, available_cols, depth+1, 'DECIMAL(10,2)')
             return f"CAST({inner} AS INTEGER)"

    # --- Numeric Logic ---
    if op_type in NUMERIC_TYPES or op_type == 'NUM':
        dice = rng.random()
        
        # 1. Standard Arithmetic (50%)
        if dice < 0.5:
            op = rng.choice(['+', '-', '*', '/', '%'])
            
            # Strict Integer Arithmetic for Modulo
            if op == '%' or op_type == 'INTEGER' or op_type == 'BIGINT':
                left = _gen_expr(ctx, available_cols, depth+1, 'INTEGER')
                right = _gen_expr(ctx, available_cols, depth+1, 'INTEGER')
                # Safe Division/Modulo
                if op in ('%', '/'):
                    return f"({left} {op} NULLIF({right}, 0))"
                return f"({left} {op} {right})"
                
            # General Numeric
            left = _gen_expr(ctx, available_cols, depth+1, 'NUM')
            right = _gen_expr(ctx, available_cols, depth+1, 'NUM')
            
            if op in ('/', '%'):
                 return f"({left} {op} NULLIF({right}, 0))"
            return f"({left} {op} {right})"

        # 2. Math Functions (30%)
        elif dice < 0.8:
            func = rng.choice(['ABS', 'CEIL', 'FLOOR', 'ROUND', 'TRUNC', 'SIGN', 'SQRT'])
            arg = _gen_expr(ctx, available_cols, depth+1, 'NUM')
            if func == 'SQRT':
                return f"SQRT(ABS({arg}))" # Safety
            return f"{func}({arg})"
            
        # 3. String-to-Numeric Functions (20%)
        else:
            func = rng.choice(['LENGTH', 'POSITION'])
            if func == 'LENGTH':
                arg = _gen_expr(ctx, available_cols, depth+1, 'STR')
                return f"LENGTH({arg})"
            elif func == 'POSITION':
                # POSITION('a' IN str)
                sub = f"'{rng.choice(['a','e','i','o','u'])}'"
                target = _gen_expr(ctx, available_cols, depth+1, 'STR')
                return f"POSITION({sub} IN {target})"
    
    # --- String Logic ---
    if op_type in STRING_TYPES or op_type == 'STR':
        dice = rng.random()
        
        # 1. Concatenation (40%)
        if dice < 0.4:
            left = _gen_expr(ctx, available_cols, depth+1, 'STR')
            right = _gen_expr(ctx, available_cols, depth+1, 'STR')
            return f"({left} || {right})"
        
        # 2. Basic Functions (30%)
        elif dice < 0.7:
            func = rng.choice(['LOWER', 'UPPER', 'TRIM', 'MD5', 'REVERSE', 'INITCAP'])
            arg = _gen_expr(ctx, available_cols, depth+1, 'STR')
            return f"{func}({arg})"

        # 3. Advanced Functions (30%)
        else:
            choice_ = rng.choice(['SUBSTRING', 'REPLACE', 'OVERLAY', 'LEFT', 'RIGHT'])
            target = _gen_expr(ctx, available_cols, depth+1, 'STR')
            
            if choice_ == 'SUBSTRING':
                return f"SUBSTRING({target} FROM 1 FOR {rng.randint(1, 5)})"
            elif choice_ == 'REPLACE':
                return f"REPLACE({target}, 'a', 'b')"
            elif choice_ == 'OVERLAY':
                return f"OVERLAY({target} PLACING 'XYZ' FROM {rng.randint(1, 3)})"
            elif choice_ in ('LEFT', 'RIGHT'):
                return f"{choice_}({target}, {rng.randint(1, 5)})"

    # --- Boolean Logic ---
    if op_type == 'BOOLEAN' or op_type == 'BOOL':
        subtype = rng.choice(['NUM', 'STR', 'BOOL', 'NULL_CHECK'])
        
        if subtype == 'NULL_CHECK':
            target_type = rng.choice(['NUM', 'STR', 'BOOL'])
            target = _gen_expr(ctx, available_cols, depth+1, target_type)
            op = rng.choice(['IS NULL', 'IS NOT NULL'])
            return f"({target} {op})"

        if subtype == 'NUM':
            op = rng.choice(['=', '<>', '>', '<', '>=', '<='])
            left = _gen_expr(ctx, available_cols, depth+1, 'NUM')
            right = _gen_expr(ctx, available_cols, depth+1, 'NUM')
            return f"({left} {op} {right})"

        if subtype == 'STR':
            # Simplified pattern matching to avoid regexp errors
            left = _gen_expr(ctx, available_cols, depth+1, 'STR')
            dice_str = rng.random()
            if dice_str < 0.3:
                return f"({left} LIKE '%{rng.choice(['a','e','i'])}%')"
            elif dice_str < 0.6:
                return f"({left} ILIKE '%{rng.choice(['a','e','i'])}%')"
            else:
                right = _gen_expr(ctx, available_cols, depth+1, 'STR')
                return f"({left} = {right})"

        if subtype == 'BOOL':
            op = rng.choice(['AND', 'OR'])
            left = _gen_expr(ctx, available_cols, depth+1, 'BOOL')
            right = _gen_expr(ctx, available_cols, depth+1, 'BOOL')
            if rng.random() < 0.2:
                return f"(NOT ({left} {op} {right}))"
            return f"({left} {op} {right})"

    # Fallback: strictly typed Coalesce
    return f"COALESCE({_gen_expr(ctx, available_cols, depth+1, op_type)}, {_gen_expr(ctx, available_cols, depth+1, op_type)})"

# =============================================================================
# 3. Complex DDL Generator
# =============================================================================

def _gen_ddl(ctx):
    # DDL generation is kept as part of workload churn, but not as fallback
    name = f"t_{_random_id()}"
    
    num_cols = ctx.rng.randint(3, 8)
    col_defs = [f"id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"]
    
    for i in range(num_cols):
        cname = f"c_{i}_{_random_id()}"
        ctype = ctx.rng.choice(ALL_TYPES)
        
        # Determine constraints
        is_not_null = ctx.rng.random() < 0.3
        is_unique = ctx.rng.random() < 0.1 and '[]' not in ctype and ctype != 'JSONB'
        
        constraints = []
        if is_not_null: constraints.append("NOT NULL")
        if is_unique: constraints.append("UNIQUE")
        
        if ctype in NUMERIC_TYPES and ctx.rng.random() < 0.2:
            constraints.append(f"CHECK ({cname} > 0)")
            
        col_defs.append(f"{cname} {ctype} {' '.join(constraints)}")
        
    return f"CREATE TABLE {name} (\n  " + ",\n  ".join(col_defs) + "\n)"

def _gen_index(ctx):
    t = _pick_table(ctx)
    if not t: return "SELECT 1"
    
    table = _get_tables(ctx)[t]
    # Filter for indexable columns (skip arrays/jsonb for now to be safe)
    valid_cols = [c for c, m in table['columns'].items()] 
    
    if not valid_cols: return "SELECT 1"
    
    target = ctx.rng.choice(valid_cols)
    idx_name = f"idx_{_random_id()}"
    return f"CREATE INDEX {idx_name} ON {t} ({target})"

# =============================================================================
# 4. Deep Query Generator (SELECT)
# =============================================================================

def _gen_select_block(ctx, tables_scope, depth=0, column_types=None):
    primary = tables_scope[0]
    joins = []
    
    # Build available columns map: just types
    available_cols = {}
    primary_meta = _get_tables(ctx)[primary]
    for c, m in primary_meta['columns'].items():
        available_cols[f"{primary}.{c}"] = m['type']
        
    for other in tables_scope[1:]:
        jtype = ctx.rng.choice(['JOIN', 'LEFT JOIN']) # Simplified joins
        left_col = ctx.rng.choice(list(primary_meta['columns'].keys()))
        right_meta = _get_tables(ctx)[other]
        right_col = ctx.rng.choice(list(right_meta['columns'].keys()))
        
        joins.append(f"{jtype} {other} ON {primary}.{left_col} = {other}.{right_col}")
        for c, m in right_meta['columns'].items():
            available_cols[f"{other}.{c}"] = m['type']

    select_exprs = []
    
    if column_types:
        for target_type in column_types:
            expr = _gen_expr(ctx, available_cols, depth=0, desired_type=target_type)
            alias = f"col_{_random_id()}"
            select_exprs.append(f"{expr} AS {alias}")
    else:
        target_count = ctx.rng.randint(1, 3)
        for _ in range(target_count):
            expr = _gen_expr(ctx, available_cols, depth=0)
            alias = f"col_{_random_id()}"
            select_exprs.append(f"{expr} AS {alias}")
        
        if ctx.rng.random() < 0.2:
            part = ctx.rng.choice(list(available_cols.keys()))
            select_exprs.append(f"row_number() OVER (PARTITION BY {part} ORDER BY {part}) as rn")

    query = f"SELECT {', '.join(select_exprs)} FROM {primary} {' '.join(joins)}"
    
    # WHERE
    if ctx.rng.random() < 0.5:
        predicate = _gen_expr(ctx, available_cols, depth=0, desired_type='BOOLEAN')
        query += f"\nWHERE {predicate}"

    # GROUP BY (Reduced freq)
    if not column_types and ctx.rng.random() < 0.2:
        gb_col = ctx.rng.choice(list(available_cols.keys()))
        query = f"SELECT {gb_col}, COUNT(*) FROM {primary} GROUP BY {gb_col}"
        if ctx.rng.random() < 0.5:
            query += f" HAVING COUNT(*) > {ctx.rng.randint(0, 5)}"

    return query

def _gen_complex_select(ctx):
    tables = _get_tables(ctx)
    if not tables: return "SELECT 1"
    
    count = min(len(tables), ctx.rng.randint(1, 2)) # Reduce join complexity
    candidates = ctx.rng.sample(list(tables.keys()), count)
    
    # CTEs (10%)
    cte = ""
    if ctx.rng.random() < 0.1:
        cte_name = f"cte_{_random_id()}"
        cte_body = _gen_select_block(ctx, [candidates[0]])
        cte = f"WITH {cte_name} AS ({cte_body}) "
        return f"{cte} SELECT * FROM {cte_name}"

    # Set Ops (10%)
    if ctx.rng.random() < 0.1:
        ncols = ctx.rng.randint(1, 2)
        target_types = [ctx.rng.choice(ALL_TYPES) for _ in range(ncols)]
        q1 = _gen_select_block(ctx, [candidates[0]], column_types=target_types)
        q2 = _gen_select_block(ctx, [candidates[0]], column_types=target_types) 
        op = ctx.rng.choice(['UNION ALL', 'UNION'])
        return f"{q1} {op} {q2}"

    return _gen_select_block(ctx, candidates)

# =============================================================================
# 5. DML (Data Manipulation)
# =============================================================================

def _gen_dml(ctx):
    t = _pick_table(ctx)
    if not t: return "SELECT 1"
    table = _get_tables(ctx)[t]
    
    op = ctx.rng.choice(['INSERT', 'UPDATE', 'DELETE'])
    
    if op == 'INSERT':
        # Pick columns that are NOT ID
        # Must include NOT NULL columns!
        cols_to_insert = []
        for c, m in table['columns'].items():
            if c == 'id': continue
            # If random chance OR column is NOT NULL, include it
            if not m['nullable'] or ctx.rng.random() < 0.7:
                cols_to_insert.append(c)
        
        if not cols_to_insert: return "SELECT 1"
        
        vals = []
        for c in cols_to_insert:
            ctype = table['columns'][c]['type']
            # Generate a Literal (depth 5 forces literal)
            # Ensure it's not NULL if column is NOT NULL
            val = _gen_expr(ctx, {}, 5, ctype)
            if not table['columns'][c]['nullable'] and val == "NULL":
                # Force non-null fallback
                if ctype in NUMERIC_TYPES: val = "1"
                elif ctype in STRING_TYPES: val = "'forced'"
                elif ctype == 'BOOLEAN': val = "TRUE"
                elif ctype in DATETIME_TYPES: val = "NOW()"
                else: val = "'0'" # desperation
            vals.append(val)
            
        return f"INSERT INTO {t} ({', '.join(cols_to_insert)}) VALUES ({', '.join(vals)})"

    elif op == 'UPDATE':
        target_col = ctx.rng.choice([c for c in table['columns'] if c != 'id'])
        ctype = table['columns'][target_col]['type']
        
        # Build strict available map
        avail = {c: m['type'] for c, m in table['columns'].items()}
        expr = _gen_expr(ctx, avail, depth=0, desired_type=ctype)
        
        # Prevent setting NULL to NOT NULL column
        if not table['columns'][target_col]['nullable']:
             expr = f"COALESCE({expr}, 1)" # Simplistic safety wrapper
             
        return f"UPDATE {t} SET {target_col} = {expr} WHERE id IN (SELECT id FROM {t} ORDER BY random() LIMIT 1)"
        
    elif op == 'DELETE':
        return f"DELETE FROM {t} WHERE id IN (SELECT id FROM {t} ORDER BY random() LIMIT 1)"

# =============================================================================
# Root Rules
# =============================================================================

g.rule("query", Choice(
    Lambda(_gen_complex_select),
    Lambda(_gen_dml),
    Lambda(_gen_ddl),
    Lambda(_gen_index),
    weights=[80, 19, 0.5, 0.5]
))

grammar = g