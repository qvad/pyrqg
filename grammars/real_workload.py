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

from pyrqg.dsl.core import Grammar, Choice, Lambda
from pyrqg.core.types import (
    NUMERIC_TYPES, STRING_TYPES, DATETIME_TYPES, BOOLEAN_TYPES
)
from pyrqg.dsl.utils import random_id, pick_table

# Combine all types for DDL generation (sorted for deterministic order)
ALL_TYPES = sorted(NUMERIC_TYPES | STRING_TYPES | DATETIME_TYPES | BOOLEAN_TYPES)

g = Grammar("real_workload_v5")

# =============================================================================
# 0. Generation Constants
# =============================================================================

# Recursion Control
MAX_EXPR_DEPTH = 5
MAX_CAST_DEPTH = 3
EARLY_STOP_PROBABILITY = 0.15
PREFER_COLUMN_PROBABILITY = 0.5

# Expression Type Probabilities
CASE_WHEN_PROBABILITY = 0.15
TYPE_CAST_PROBABILITY = 0.1

# Numeric Expression Distribution
ARITHMETIC_PROBABILITY = 0.5
MATH_FUNC_THRESHOLD = 0.8  # 0.5-0.8 = 30% math functions

# String Expression Distribution
STRING_CONCAT_PROBABILITY = 0.4
STRING_BASIC_FUNC_THRESHOLD = 0.7  # 0.4-0.7 = 30% basic functions

# Boolean Expression Probabilities
STRING_LIKE_PROBABILITY = 0.3
STRING_ILIKE_THRESHOLD = 0.6
BOOLEAN_NOT_PROBABILITY = 0.2

# Value Ranges
NUMERIC_LITERAL_MIN = 1
NUMERIC_LITERAL_MAX = 100
DATE_INTERVAL_MAX_DAYS = 365
SUBSTRING_MAX_LENGTH = 5
OVERLAY_MAX_START = 3
LEFTRIGHT_MAX_CHARS = 5

# Query Generation
WHERE_CLAUSE_PROBABILITY = 0.6
GROUP_BY_PROBABILITY = 0.25
WINDOW_FUNC_PROBABILITY = 0.25
CTE_PROBABILITY = 0.15
SET_OP_PROBABILITY = 0.15

# DDL Constraints
NOT_NULL_PROBABILITY = 0.3
UNIQUE_PROBABILITY = 0.1
CHECK_CONSTRAINT_PROBABILITY = 0.2

# DML
OPTIONAL_COLUMN_PROBABILITY = 0.7

# Root Query Weights
QUERY_SELECT_WEIGHT = 80    # 80% - Primary workload (SELECTs)
QUERY_DML_WEIGHT = 19       # 19% - Data modification (INSERT/UPDATE/DELETE)
QUERY_DDL_WEIGHT = 0.5      # 0.5% - Schema changes (CREATE TABLE)
QUERY_INDEX_WEIGHT = 0.5    # 0.5% - Index operations (CREATE INDEX)

# =============================================================================
# 1. Type System & State Management
# =============================================================================

def _map_postgres_type(dtype):
    """
    Map raw PostgreSQL data types to simplified fuzzer types.

    Normalizes various PostgreSQL type names to a consistent set:
    - INT/BIGINT/SMALLINT -> INTEGER
    - CHAR/VARCHAR/TEXT -> TEXT
    - BOOL -> BOOLEAN
    - TIME/DATE/TIMESTAMP -> TIMESTAMP
    - NUMERIC/DECIMAL/FLOAT/REAL/DOUBLE/MONEY -> DECIMAL(10,2)

    Args:
        dtype: PostgreSQL data type string (e.g., 'character varying').

    Returns:
        Simplified type name from the fuzzer's type system.
    """
    dtype_upper = dtype.upper()
    if 'INT' in dtype_upper:
        return 'INTEGER'
    if 'CHAR' in dtype_upper or 'TEXT' in dtype_upper:
        return 'TEXT'
    if 'BOOL' in dtype_upper:
        return 'BOOLEAN'
    if 'TIME' in dtype_upper or 'DATE' in dtype_upper:
        return 'TIMESTAMP'
    if any(t in dtype_upper for t in ['NUM', 'DEC', 'FLOAT', 'REAL', 'DOUB', 'MONEY']):
        return 'DECIMAL(10,2)'
    return 'TEXT'


# =============================================================================
# 2. Expression Generation (Decomposed)
# =============================================================================

def _gen_base_literal(ctx, available_cols, desired_type):
    """
    Generate a terminal value (column reference or literal).

    Called when recursion limit is reached or random early termination.
    Prefers column references over literals when available.
    """
    rng = ctx.rng

    # Try to find a column matching desired type
    if available_cols and rng.random() < PREFER_COLUMN_PROBABILITY:
        if desired_type:
            candidates = []
            for c, t in available_cols.items():
                if t == desired_type:
                    candidates.append(c)
                elif desired_type == 'NUM' and t in NUMERIC_TYPES:
                    candidates.append(c)
                elif desired_type == 'STR' and t in STRING_TYPES:
                    candidates.append(c)
                elif desired_type == 'BOOL' and t == 'BOOLEAN':
                    candidates.append(c)
            if candidates:
                return rng.choice(candidates)
        else:
            return rng.choice(list(available_cols.keys()))

    # Fallback to literal (use safe values)
    if not desired_type or desired_type in NUMERIC_TYPES or desired_type == 'NUM':
        return str(rng.randint(NUMERIC_LITERAL_MIN, NUMERIC_LITERAL_MAX))
    if desired_type in STRING_TYPES or desired_type == 'STR':
        return f"'{random_id()}'"
    if desired_type == 'BOOLEAN' or desired_type == 'BOOL':
        return rng.choice(['TRUE', 'FALSE'])
    if desired_type in DATETIME_TYPES:
        return f"(NOW() - interval '{rng.randint(0, DATE_INTERVAL_MAX_DAYS)} days')"
    if '[]' in str(desired_type):
        return "ARRAY[1, 2]"
    return "NULL"


def _gen_case_when_expr(ctx, available_cols, depth, op_type):
    """Generate a CASE WHEN expression."""
    cond = _gen_expr(ctx, available_cols, depth + 1, 'BOOLEAN')
    val1 = _gen_expr(ctx, available_cols, depth + 1, op_type)
    val2 = _gen_expr(ctx, available_cols, depth + 1, op_type)
    return f"(CASE WHEN {cond} THEN {val1} ELSE {val2} END)"


def _gen_type_cast_expr(ctx, available_cols, depth, op_type):
    """Generate a type casting expression."""
    if op_type in STRING_TYPES or op_type == 'STR':
        inner = _gen_expr(ctx, available_cols, depth + 1, 'NUM')
        target_type = op_type if op_type != 'STR' else 'TEXT'
        return f"CAST({inner} AS {target_type})"
    if op_type == 'INTEGER':
        inner = _gen_expr(ctx, available_cols, depth + 1, 'DECIMAL(10,2)')
        return f"CAST({inner} AS INTEGER)"
    return None


def _gen_numeric_expr(ctx, available_cols, depth, op_type):
    """
    Generate numeric expressions: arithmetic, math functions, or string-to-numeric.

    Distribution:
    - 50% arithmetic operations (+, -, *, /, %)
    - 30% math functions (ABS, CEIL, FLOOR, etc.)
    - 20% string-to-numeric (LENGTH, POSITION)
    """
    rng = ctx.rng
    dice = rng.random()

    # Arithmetic operations (50%)
    if dice < ARITHMETIC_PROBABILITY:
        op = rng.choice(['+', '-', '*', '/', '%'])

        # Use INTEGER for modulo or integer types
        if op == '%' or op_type in ('INTEGER', 'BIGINT'):
            left = _gen_expr(ctx, available_cols, depth + 1, 'INTEGER')
            right = _gen_expr(ctx, available_cols, depth + 1, 'INTEGER')
            if op in ('%', '/'):
                return f"({left} {op} NULLIF({right}, 0))"
            return f"({left} {op} {right})"

        # General numeric
        left = _gen_expr(ctx, available_cols, depth + 1, 'NUM')
        right = _gen_expr(ctx, available_cols, depth + 1, 'NUM')
        if op in ('/', '%'):
            return f"({left} {op} NULLIF({right}, 0))"
        return f"({left} {op} {right})"

    # Math functions (30%)
    elif dice < MATH_FUNC_THRESHOLD:
        func = rng.choice(['ABS', 'CEIL', 'FLOOR', 'ROUND', 'TRUNC', 'SIGN', 'SQRT'])
        arg = _gen_expr(ctx, available_cols, depth + 1, 'NUM')
        if func == 'SQRT':
            return f"SQRT(ABS({arg}))"  # Safety: avoid negative sqrt
        return f"{func}({arg})"

    # String-to-numeric functions (20%)
    else:
        func = rng.choice(['LENGTH', 'POSITION'])
        if func == 'LENGTH':
            arg = _gen_expr(ctx, available_cols, depth + 1, 'STR')
            return f"LENGTH({arg})"
        else:  # POSITION
            sub = f"'{rng.choice(['a', 'e', 'i', 'o', 'u'])}'"
            target = _gen_expr(ctx, available_cols, depth + 1, 'STR')
            return f"POSITION({sub} IN {target})"


def _gen_string_expr(ctx, available_cols, depth):
    """
    Generate string expressions: concatenation or functions.

    Distribution:
    - 40% concatenation (||)
    - 30% basic functions (LOWER, UPPER, TRIM, etc.)
    - 30% advanced functions (SUBSTRING, REPLACE, etc.)
    """
    rng = ctx.rng
    dice = rng.random()

    # Concatenation (40%)
    if dice < STRING_CONCAT_PROBABILITY:
        left = _gen_expr(ctx, available_cols, depth + 1, 'STR')
        right = _gen_expr(ctx, available_cols, depth + 1, 'STR')
        return f"({left} || {right})"

    # Basic functions (30%)
    elif dice < STRING_BASIC_FUNC_THRESHOLD:
        func = rng.choice(['LOWER', 'UPPER', 'TRIM', 'INITCAP', 'BTRIM', 'LTRIM', 'RTRIM'])
        arg = _gen_expr(ctx, available_cols, depth + 1, 'STR')
        return f"{func}({arg})"

    # Advanced functions (30%)
    else:
        func = rng.choice(['SUBSTRING', 'REPLACE', 'LEFT', 'RIGHT', 'LPAD', 'RPAD'])
        target = _gen_expr(ctx, available_cols, depth + 1, 'STR')

        if func == 'SUBSTRING':
            return f"SUBSTRING({target} FROM 1 FOR {rng.randint(1, SUBSTRING_MAX_LENGTH)})"
        elif func == 'REPLACE':
            return f"REPLACE({target}, 'a', 'b')"
        elif func in ('LPAD', 'RPAD'):
            return f"{func}({target}, {rng.randint(5, 15)}, 'x')"
        else:  # LEFT or RIGHT
            return f"{func}({target}, {rng.randint(1, LEFTRIGHT_MAX_CHARS)})"


def _gen_boolean_expr(ctx, available_cols, depth):
    """
    Generate boolean expressions: comparisons, pattern matching, or logic.

    Subtypes:
    - NULL_CHECK: IS NULL / IS NOT NULL
    - NUM: numeric comparisons (=, <>, >, <, etc.)
    - STR: pattern matching (LIKE, ILIKE) or equality
    - BOOL: boolean logic (AND, OR, NOT)
    """
    rng = ctx.rng
    subtype = rng.choice(['NUM', 'STR', 'BOOL', 'NULL_CHECK'])

    if subtype == 'NULL_CHECK':
        target_type = rng.choice(['NUM', 'STR', 'BOOL'])
        target = _gen_expr(ctx, available_cols, depth + 1, target_type)
        op = rng.choice(['IS NULL', 'IS NOT NULL'])
        return f"({target} {op})"

    if subtype == 'NUM':
        op = rng.choice(['=', '<>', '>', '<', '>=', '<='])
        left = _gen_expr(ctx, available_cols, depth + 1, 'NUM')
        right = _gen_expr(ctx, available_cols, depth + 1, 'NUM')
        return f"({left} {op} {right})"

    if subtype == 'STR':
        left = _gen_expr(ctx, available_cols, depth + 1, 'STR')
        dice = rng.random()
        if dice < STRING_LIKE_PROBABILITY:
            return f"({left} LIKE '%{rng.choice(['a', 'e', 'i'])}%')"
        elif dice < STRING_ILIKE_THRESHOLD:
            return f"({left} ILIKE '%{rng.choice(['a', 'e', 'i'])}%')"
        else:
            right = _gen_expr(ctx, available_cols, depth + 1, 'STR')
            return f"({left} = {right})"

    # BOOL subtype
    op = rng.choice(['AND', 'OR'])
    left = _gen_expr(ctx, available_cols, depth + 1, 'BOOL')
    right = _gen_expr(ctx, available_cols, depth + 1, 'BOOL')
    if rng.random() < BOOLEAN_NOT_PROBABILITY:
        return f"(NOT ({left} {op} {right}))"
    return f"({left} {op} {right})"


def _gen_expr(ctx, available_cols, depth=0, desired_type=None):
    """
    Generate a valid SQL expression with strict type safety.

    This is the main dispatcher that routes to type-specific generators.

    Args:
        ctx: Generation context with RNG.
        available_cols: Dict of {col_name: type} in current scope.
        depth: Current recursion depth.
        desired_type: Target SQL type for the expression.

    Returns:
        A SQL expression string.
    """
    rng = ctx.rng

    # Base case: depth limit or random early termination
    if depth > MAX_EXPR_DEPTH or (depth > 0 and rng.random() < EARLY_STOP_PROBABILITY):
        return _gen_base_literal(ctx, available_cols, desired_type)

    # Determine expression type
    op_type = desired_type if desired_type else rng.choice(ALL_TYPES)

    # CASE WHEN expression (10% chance)
    if rng.random() < CASE_WHEN_PROBABILITY:
        return _gen_case_when_expr(ctx, available_cols, depth, op_type)

    # Type casting (5% chance, only at shallow depth)
    if rng.random() < TYPE_CAST_PROBABILITY and depth < MAX_CAST_DEPTH:
        result = _gen_type_cast_expr(ctx, available_cols, depth, op_type)
        if result:
            return result

    # Route to type-specific generator
    if op_type in NUMERIC_TYPES or op_type == 'NUM':
        return _gen_numeric_expr(ctx, available_cols, depth, op_type)

    if op_type in STRING_TYPES or op_type == 'STR':
        return _gen_string_expr(ctx, available_cols, depth)

    if op_type == 'BOOLEAN' or op_type == 'BOOL':
        return _gen_boolean_expr(ctx, available_cols, depth)

    # Fallback: COALESCE wrapper
    return f"COALESCE({_gen_expr(ctx, available_cols, depth + 1, op_type)}, {_gen_expr(ctx, available_cols, depth + 1, op_type)})"

# =============================================================================
# 3. Complex DDL Generator
# =============================================================================

def _gen_ddl(ctx):
    """
    Generate random CREATE TABLE statements.

    Creates tables with:
    - Auto-generated IDENTITY primary key
    - 3-8 random columns with various types
    - Constraints: NOT NULL (30%), UNIQUE (10%), CHECK (20% for numerics)

    Returns:
        SQL CREATE TABLE statement.
    """
    name = f"t_{random_id()}"

    num_cols = ctx.rng.randint(3, 8)
    col_defs = ["id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"]

    for i in range(num_cols):
        cname = f"c_{i}_{random_id()}"
        ctype = ctx.rng.choice(ALL_TYPES)

        # Determine constraints using constants
        is_not_null = ctx.rng.random() < NOT_NULL_PROBABILITY
        is_unique = ctx.rng.random() < UNIQUE_PROBABILITY and '[]' not in ctype and ctype != 'JSONB'

        constraints = []
        if is_not_null:
            constraints.append("NOT NULL")
        if is_unique:
            constraints.append("UNIQUE")

        if ctype in NUMERIC_TYPES and ctx.rng.random() < CHECK_CONSTRAINT_PROBABILITY:
            constraints.append(f"CHECK ({cname} > 0)")

        col_defs.append(f"{cname} {ctype} {' '.join(constraints)}")

    return f"CREATE TABLE {name} (\n  " + ",\n  ".join(col_defs) + "\n)"


def _gen_index(ctx):
    """
    Generate random CREATE INDEX statements on existing tables.

    Selects a random table and column to create an index on.
    Returns a no-op SELECT if no tables exist.

    Returns:
        SQL CREATE INDEX statement or SELECT 1 fallback.
    """
    t = pick_table(ctx)
    if not t:
        return "SELECT 1"

    table = ctx.tables[t]
    valid_cols = list(table.columns.keys())

    if not valid_cols:
        return "SELECT 1"

    target = ctx.rng.choice(valid_cols)
    idx_name = f"idx_{random_id()}"
    return f"CREATE INDEX {idx_name} ON {t} ({target})"

# =============================================================================
# 4. Deep Query Generator (SELECT)
# =============================================================================

def _gen_select_block(ctx, tables_scope, column_types=None):
    """
    Generate a basic SELECT-FROM-JOIN-WHERE block.

    Args:
        ctx: Generation context with RNG and schema.
        tables_scope: List of table names to include in query.
        column_types: Optional list of types for SELECT columns (for UNION compatibility).

    Returns:
        SQL SELECT statement string.
    """
    primary = tables_scope[0]
    joins = []

    # Build available columns map: {qualified_name: type}
    available_cols = {}
    primary_meta = ctx.tables[primary]
    for c_name, c_meta in primary_meta.columns.items():
        available_cols[f"{primary}.{c_name}"] = _map_postgres_type(c_meta.data_type)

    for other in tables_scope[1:]:
        jtype = ctx.rng.choice(['JOIN', 'LEFT JOIN'])
        primary_meta = ctx.tables[primary]
        left_col = ctx.rng.choice(list(primary_meta.columns.keys()))

        right_meta = ctx.tables[other]
        right_col = ctx.rng.choice(list(right_meta.columns.keys()))

        joins.append(f"{jtype} {other} ON {primary}.{left_col} = {other}.{right_col}")
        for c_name, c_meta in right_meta.columns.items():
            available_cols[f"{other}.{c_name}"] = _map_postgres_type(c_meta.data_type)

    select_exprs = []

    if column_types:
        for target_type in column_types:
            expr = _gen_expr(ctx, available_cols, depth=0, desired_type=target_type)
            alias = f"col_{random_id()}"
            select_exprs.append(f"{expr} AS {alias}")
    else:
        target_count = ctx.rng.randint(1, 5)
        for _ in range(target_count):
            expr = _gen_expr(ctx, available_cols, depth=0)
            alias = f"col_{random_id()}"
            select_exprs.append(f"{expr} AS {alias}")

        # Window function (20% chance)
        if ctx.rng.random() < WINDOW_FUNC_PROBABILITY:
            part = ctx.rng.choice(list(available_cols.keys()))
            select_exprs.append(f"row_number() OVER (PARTITION BY {part} ORDER BY {part}) as rn")

    query = f"SELECT {', '.join(select_exprs)} FROM {primary} {' '.join(joins)}"

    # WHERE clause (50% chance)
    if ctx.rng.random() < WHERE_CLAUSE_PROBABILITY:
        predicate = _gen_expr(ctx, available_cols, depth=0, desired_type='BOOLEAN')
        query += f"\nWHERE {predicate}"

    # GROUP BY (20% chance, not when column_types specified for UNION)
    if not column_types and ctx.rng.random() < GROUP_BY_PROBABILITY:
        gb_col = ctx.rng.choice(list(available_cols.keys()))
        query = f"SELECT {gb_col}, COUNT(*) FROM {primary} GROUP BY {gb_col}"
        if ctx.rng.random() < WHERE_CLAUSE_PROBABILITY:
            query += f" HAVING COUNT(*) > {ctx.rng.randint(0, 5)}"

    return query


def _gen_complex_select(ctx):
    """
    Generate a SELECT statement with complex topologies.

    Can produce:
    - CTEs (WITH clauses): 10% chance
    - Set operations (UNION/UNION ALL): 10% chance
    - Standard joins: remaining 80%

    Returns:
        SQL SELECT statement with potential CTE or set operation.
    """
    if not ctx.tables:
        return "SELECT 1"

    count = min(len(ctx.tables), ctx.rng.randint(1, 4))
    candidates = ctx.rng.sample(list(ctx.tables.keys()), count)

    # CTEs (10%)
    if ctx.rng.random() < CTE_PROBABILITY:
        cte_name = f"cte_{random_id()}"
        cte_body = _gen_select_block(ctx, [candidates[0]])
        return f"WITH {cte_name} AS ({cte_body}) SELECT * FROM {cte_name}"

    # Set operations (10%)
    if ctx.rng.random() < SET_OP_PROBABILITY:
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
    """
    Generate valid INSERT/UPDATE/DELETE statements.

    Respects NOT NULL constraints by:
    - Always including NOT NULL columns in INSERT
    - Wrapping UPDATE expressions in COALESCE for NOT NULL columns
    - Using safe random row selection for UPDATE/DELETE

    Returns:
        SQL DML statement (INSERT, UPDATE, or DELETE).
    """
    t = pick_table(ctx)
    if not t:
        return "SELECT 1"

    table = ctx.tables[t]
    op = ctx.rng.choice(['INSERT', 'UPDATE', 'DELETE'])

    if op == 'INSERT':
        # Select columns: always include NOT NULL, randomly include nullable
        cols_to_insert = []
        for c_name, m in table.columns.items():
            if c_name == 'id':
                continue
            # Include if NOT NULL or with 70% probability
            if not m.is_nullable or ctx.rng.random() < OPTIONAL_COLUMN_PROBABILITY:
                cols_to_insert.append(c_name)

        if not cols_to_insert:
            return "SELECT 1"

        vals = []
        for c_name in cols_to_insert:
            c_meta = table.columns[c_name]
            ctype = _map_postgres_type(c_meta.data_type)
            # Force literal generation (depth > MAX_EXPR_DEPTH)
            val = _gen_expr(ctx, {}, MAX_EXPR_DEPTH + 2, ctype)

            # Ensure NOT NULL columns don't get NULL
            if not c_meta.is_nullable and val == "NULL":
                if ctype in NUMERIC_TYPES:
                    val = "1"
                elif ctype in STRING_TYPES:
                    val = "'forced'"
                elif ctype == 'BOOLEAN':
                    val = "TRUE"
                elif ctype in DATETIME_TYPES:
                    val = "NOW()"
                else:
                    val = "'0'"
            vals.append(val)

        return f"INSERT INTO {t} ({', '.join(cols_to_insert)}) VALUES ({', '.join(vals)})"

    elif op == 'UPDATE':
        valid_targets = [c for c in table.columns.keys() if c != 'id']
        if not valid_targets:
            return "SELECT 1"

        target_col = ctx.rng.choice(valid_targets)
        c_meta = table.columns[target_col]
        ctype = _map_postgres_type(c_meta.data_type)

        avail = {c: _map_postgres_type(m.data_type) for c, m in table.columns.items()}
        expr = _gen_expr(ctx, avail, depth=0, desired_type=ctype)

        # Wrap in COALESCE for NOT NULL columns
        if not c_meta.is_nullable:
            expr = f"COALESCE({expr}, 1)"

        return f"UPDATE {t} SET {target_col} = {expr} WHERE id IN (SELECT id FROM {t} ORDER BY random() LIMIT 1)"

    else:  # DELETE
        return f"DELETE FROM {t} WHERE id IN (SELECT id FROM {t} ORDER BY random() LIMIT 1)"


# =============================================================================
# Root Rules
# =============================================================================

g.rule("query", Choice(
    Lambda(_gen_complex_select),
    Lambda(_gen_dml),
    Lambda(_gen_ddl),
    Lambda(_gen_index),
    weights=[QUERY_SELECT_WEIGHT, QUERY_DML_WEIGHT, QUERY_DDL_WEIGHT, QUERY_INDEX_WEIGHT]
))

grammar = g