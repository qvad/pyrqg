"""
YugabyteDB YCQL Grammar - Based on SQLancer Implementation.

This grammar generates YugabyteDB YCQL (Cassandra Query Language) queries.
YCQL is Cassandra-compatible and has different syntax from SQL.

Key differences from SQL:
- Requires PRIMARY KEY in CREATE TABLE
- No JOINs (denormalized data model)
- Different data types (TINYINT, BIGINT, TIMESTAMP, etc.)
- CQL-specific functions (TIMEUUID, TTL, WRITETIME, etc.)
- Simpler WHERE clause (primary key required for non-SELECT)

Reference: https://github.com/sqlancer/sqlancer (yugabyte/ycql module)
"""

from pyrqg.dsl.core import Grammar, Lambda, choice, template, maybe, repeat, Literal

g = Grammar("sqlancer_ycql")

# =============================================================================
# YCQL Data Types (from YCQLSchema.YCQLDataType)
# =============================================================================

YCQL_DATA_TYPES = ['INT', 'VARCHAR', 'BOOLEAN', 'FLOAT', 'DATE', 'TIMESTAMP']

# Composite type sizes for INT and FLOAT
INT_SIZES = {1: 'TINYINT', 2: 'SMALLINT', 4: 'INT', 8: 'BIGINT'}
FLOAT_SIZES = {4: 'FLOAT', 8: 'DOUBLE'}

# =============================================================================
# Helper Functions
# =============================================================================

def _pick_table(ctx):
    """Pick a random table and store it in context state."""
    if ctx.tables:
        t = ctx.rng.choice(list(ctx.tables.keys()))
        ctx.state['table'] = t
        return t
    return "t0"

def _pick_column(ctx, is_pk=None):
    """Pick a column, optionally filtering by primary key status."""
    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        return "c0"
    table = ctx.tables[t_name]
    cols = list(table.columns.values())
    if is_pk is not None:
        cols = [c for c in cols if c.is_primary_key == is_pk]
    if not cols:
        cols = list(table.columns.values())
    if not cols:
        return "c0"
    return ctx.rng.choice([c.name for c in cols])

def _pick_pk_column(ctx):
    """Pick a primary key column."""
    return _pick_column(ctx, is_pk=True)

def _random_int(ctx):
    """Generate a random integer constant."""
    return str(ctx.rng.randint(-1000000, 1000000))

def _random_boolean(ctx):
    """Generate a random boolean constant."""
    return ctx.rng.choice(['TRUE', 'FALSE'])

def _random_varchar(ctx):
    """Generate a random string constant."""
    length = ctx.rng.randint(0, 20)
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 '
    text = ''.join(ctx.rng.choice(chars) for _ in range(length))
    text = text.replace("'", "''")
    return f"'{text}'"

def _random_float(ctx):
    """Generate a random float constant."""
    return str(ctx.rng.uniform(-1000000, 1000000))

def _random_date(ctx):
    """Generate a random date constant."""
    year = ctx.rng.randint(1970, 2030)
    month = ctx.rng.randint(1, 12)
    day = ctx.rng.randint(1, 28)
    return f"'{year:04d}-{month:02d}-{day:02d}'"

def _random_timestamp(ctx):
    """Generate a random timestamp constant."""
    year = ctx.rng.randint(1970, 2030)
    month = ctx.rng.randint(1, 12)
    day = ctx.rng.randint(1, 28)
    hour = ctx.rng.randint(0, 23)
    minute = ctx.rng.randint(0, 59)
    second = ctx.rng.randint(0, 59)
    return f"'{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}'"

def _gen_constant(ctx, data_type=None):
    """Generate a constant for a given data type."""
    # Small probability of NULL
    if ctx.rng.random() < 0.05:
        return 'NULL'

    if data_type is None:
        data_type = ctx.rng.choice(YCQL_DATA_TYPES)

    generators = {
        'INT': _random_int,
        'VARCHAR': _random_varchar,
        'BOOLEAN': _random_boolean,
        'FLOAT': _random_float,
        'DATE': _random_date,
        'TIMESTAMP': _random_timestamp,
    }

    gen = generators.get(data_type.upper(), _random_varchar)
    return gen(ctx)

# =============================================================================
# YCQL Operators (from YCQLExpressionGenerator)
# =============================================================================

# Comparison operators
YCQL_COMPARISON_OPS = ['=', '>', '>=', '<', '<=', '!=']

# Logical operators
YCQL_LOGICAL_OPS = ['AND', 'OR']

# Arithmetic operators
YCQL_ARITHMETIC_OPS = ['+', '-', '*', '/']

def _gen_comparison_op(ctx):
    return ctx.rng.choice(YCQL_COMPARISON_OPS)

def _gen_logical_op(ctx):
    return ctx.rng.choice(YCQL_LOGICAL_OPS)

def _gen_arithmetic_op(ctx):
    return ctx.rng.choice(YCQL_ARITHMETIC_OPS)

# =============================================================================
# YCQL Functions (from YCQLExpressionGenerator.DBFunction)
# =============================================================================

# Built-in YCQL functions
YCQL_FUNCTIONS = {
    'TIMEUUID': 1,    # toTimestamp(now())
    'BIGINT': 1,      # Cast to bigint
    'BLOB': 1,        # Cast to blob
    'UUID': 0,        # Generate UUID
    'DATE': 0,        # Current date
    'TIME': 0,        # Current time
    'TIMESTAMP': 0,   # Current timestamp
}

# Aggregate functions
YCQL_AGGREGATES = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']

def _gen_function(ctx):
    """Generate a YCQL function call."""
    func = ctx.rng.choice(list(YCQL_FUNCTIONS.keys()))
    num_args = YCQL_FUNCTIONS[func]

    if num_args == 0:
        return f"{func.lower()}()"
    else:
        args = ', '.join(_gen_expression(ctx) for _ in range(num_args))
        return f"{func.lower()}({args})"

def _gen_aggregate(ctx):
    """Generate an aggregate function call."""
    agg = ctx.rng.choice(YCQL_AGGREGATES)
    if agg == 'COUNT' and ctx.rng.random() < 0.3:
        return 'COUNT(*)'
    col = _pick_column(ctx)
    return f"{agg}({col})"

# =============================================================================
# Expression Depth Control
# =============================================================================

MAX_DEPTH = 3

def _get_depth(ctx):
    return ctx.state.get('depth', 0)

def _inc_depth(ctx):
    depth = ctx.state.get('depth', 0) + 1
    ctx.state['depth'] = depth
    return depth

def _dec_depth(ctx):
    ctx.state['depth'] = max(0, ctx.state.get('depth', 0) - 1)

# =============================================================================
# Expression Generator
# =============================================================================

def _gen_expression(ctx, data_type=None):
    """Generate an expression."""
    depth = _inc_depth(ctx)
    try:
        if depth > MAX_DEPTH or ctx.rng.random() < 0.4:
            # Return a leaf node
            if ctx.rng.random() < 0.6 and ctx.tables:
                return _pick_column(ctx)
            return _gen_constant(ctx, data_type)

        choice_val = ctx.rng.random()

        if choice_val < 0.3:
            # Binary comparison
            left = _gen_expression(ctx)
            right = _gen_expression(ctx)
            op = _gen_comparison_op(ctx)
            return f"({left}) {op} ({right})"
        elif choice_val < 0.5:
            # Binary logical
            left = _gen_expression(ctx)
            right = _gen_expression(ctx)
            op = _gen_logical_op(ctx)
            return f"({left}) {op} ({right})"
        elif choice_val < 0.7:
            # Binary arithmetic
            left = _gen_expression(ctx)
            right = _gen_expression(ctx)
            op = _gen_arithmetic_op(ctx)
            return f"({left}) {op} ({right})"
        elif choice_val < 0.8:
            # Function call
            return _gen_function(ctx)
        elif choice_val < 0.9:
            # BETWEEN
            expr = _gen_expression(ctx)
            lower = _gen_expression(ctx)
            upper = _gen_expression(ctx)
            not_between = 'NOT ' if ctx.rng.random() < 0.3 else ''
            return f"({expr}) {not_between}BETWEEN ({lower}) AND ({upper})"
        else:
            # IN
            expr = _gen_expression(ctx)
            num_values = ctx.rng.randint(1, 5)
            values = ', '.join(_gen_expression(ctx) for _ in range(num_values))
            not_in = 'NOT ' if ctx.rng.random() < 0.3 else ''
            return f"({expr}) {not_in}IN ({values})"
    finally:
        _dec_depth(ctx)

def _gen_boolean_expression(ctx):
    """Generate a boolean expression for WHERE clause."""
    return _gen_expression(ctx, 'BOOLEAN')

# =============================================================================
# CREATE TABLE Generator (YCQL requires PRIMARY KEY)
# =============================================================================

def _gen_ycql_column_type(ctx):
    """Generate a YCQL column type."""
    data_type = ctx.rng.choice(YCQL_DATA_TYPES)

    if data_type == 'INT':
        size = ctx.rng.choice([1, 2, 4, 8])
        return INT_SIZES[size]
    elif data_type == 'FLOAT':
        size = ctx.rng.choice([4, 8])
        return FLOAT_SIZES[size]
    else:
        return data_type

def _gen_ycql_create_table(ctx):
    """Generate a YCQL CREATE TABLE statement."""
    ctx.state['col_idx'] = 0
    ctx.state['created_cols'] = []

    if_not_exists = 'IF NOT EXISTS ' if ctx.rng.random() < 0.3 else ''
    table_name = f"t{ctx.rng.randint(0, 100)}"

    # Generate columns (at least 1 for PK)
    num_cols = ctx.rng.randint(1, 5)
    columns = []

    for i in range(num_cols):
        col_name = f"c{i}"
        col_type = _gen_ycql_column_type(ctx)
        columns.append(f"{col_name} {col_type}")
        ctx.state['created_cols'].append(col_name)

    column_defs = ', '.join(columns)

    # PRIMARY KEY is required in YCQL
    pk_cols = ctx.state['created_cols']
    num_pk = ctx.rng.randint(1, min(3, len(pk_cols)))
    pk_columns = ctx.rng.sample(pk_cols, num_pk)

    # Partition key vs clustering columns
    if len(pk_columns) > 1 and ctx.rng.random() < 0.5:
        # Composite primary key with partition key
        partition_key = pk_columns[0]
        clustering = pk_columns[1:]
        pk_def = f"PRIMARY KEY(({partition_key}), {', '.join(clustering)})"
    else:
        pk_def = f"PRIMARY KEY({', '.join(pk_columns)})"

    return f"CREATE TABLE {if_not_exists}{table_name} ({column_defs}, {pk_def})"

# =============================================================================
# CREATE INDEX Generator
# =============================================================================

def _gen_ycql_create_index(ctx):
    """Generate a YCQL CREATE INDEX statement."""
    if_not_exists = 'IF NOT EXISTS ' if ctx.rng.random() < 0.3 else ''
    t_name = _pick_table(ctx)
    index_name = f"idx_{t_name}_{ctx.rng.randint(1, 100)}"
    col = _pick_column(ctx, is_pk=False)  # Index on non-PK column

    return f"CREATE INDEX {if_not_exists}{index_name} ON {t_name} ({col})"

# =============================================================================
# SELECT Generator (no JOINs in YCQL)
# =============================================================================

def _gen_ycql_select_columns(ctx):
    """Generate SELECT column list."""
    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        return "*"

    if ctx.rng.random() < 0.2:
        return "*"

    table = ctx.tables[t_name]
    cols = [c.name for c in table.columns.values()]
    if not cols:
        return "*"

    num_cols = ctx.rng.randint(1, min(5, len(cols)))
    selected = ctx.rng.sample(cols, num_cols)

    result = []
    for col in selected:
        if ctx.rng.random() < 0.2:
            # Add aggregate
            agg = ctx.rng.choice(YCQL_AGGREGATES)
            result.append(f"{agg}({col})")
        else:
            result.append(col)

    return ', '.join(result)

def _gen_ycql_where_clause(ctx):
    """Generate WHERE clause (simpler than SQL)."""
    if ctx.rng.random() < 0.3:
        return ""

    # YCQL WHERE typically operates on primary key columns
    pk_col = _pick_pk_column(ctx)
    op = ctx.rng.choice(['=', '>', '>=', '<', '<='])
    value = _gen_constant(ctx, 'INT')

    conditions = [f"{pk_col} {op} {value}"]

    # Maybe add more conditions
    if ctx.rng.random() < 0.3:
        col = _pick_column(ctx)
        op = ctx.rng.choice(YCQL_COMPARISON_OPS)
        value = _gen_constant(ctx)
        logic = ctx.rng.choice(['AND', 'OR'])
        conditions.append(f"{logic} {col} {op} {value}")

    return "WHERE " + ' '.join(conditions)

def _gen_ycql_order_by(ctx):
    """Generate ORDER BY clause."""
    if ctx.rng.random() < 0.7:
        return ""

    col = _pick_column(ctx)
    direction = ctx.rng.choice(['ASC', 'DESC', ''])
    return f"ORDER BY {col} {direction}".strip()

def _gen_ycql_limit(ctx):
    """Generate LIMIT clause."""
    if ctx.rng.random() < 0.6:
        return ""

    limit = ctx.rng.randint(1, 100)
    return f"LIMIT {limit}"

def _gen_ycql_select(ctx):
    """Generate a YCQL SELECT statement."""
    ctx.state['depth'] = 0

    t_name = _pick_table(ctx)
    columns = _gen_ycql_select_columns(ctx)
    where = _gen_ycql_where_clause(ctx)
    order_by = _gen_ycql_order_by(ctx)
    limit = _gen_ycql_limit(ctx)

    query = f"SELECT {columns} FROM {t_name}"
    if where:
        query += f" {where}"
    if order_by:
        query += f" {order_by}"
    if limit:
        query += f" {limit}"

    return query

# =============================================================================
# INSERT Generator
# =============================================================================

def _gen_ycql_insert(ctx):
    """Generate a YCQL INSERT statement."""
    t_name = _pick_table(ctx)

    if t_name not in ctx.tables:
        return f"INSERT INTO {t_name} (c0) VALUES (1)"

    table = ctx.tables[t_name]
    cols = [c.name for c in table.columns.values()]

    if not cols:
        return f"INSERT INTO {t_name} (c0) VALUES (1)"

    # Select columns
    num_cols = ctx.rng.randint(1, len(cols))
    selected_cols = ctx.rng.sample(cols, num_cols)

    # Generate values
    values = []
    for col in selected_cols:
        val = ctx.get_column_value(t_name, col) if hasattr(ctx, 'get_column_value') else _gen_constant(ctx)
        values.append(val)

    cols_str = ', '.join(selected_cols)
    values_str = ', '.join(values)

    # IF NOT EXISTS is common in YCQL
    if_not_exists = ' IF NOT EXISTS' if ctx.rng.random() < 0.3 else ''

    return f"INSERT INTO {t_name} ({cols_str}) VALUES ({values_str}){if_not_exists}"

# =============================================================================
# UPDATE Generator
# =============================================================================

def _gen_ycql_update(ctx):
    """Generate a YCQL UPDATE statement."""
    t_name = _pick_table(ctx)

    if t_name not in ctx.tables:
        return f"UPDATE {t_name} SET c0 = 1 WHERE c0 = 0"

    table = ctx.tables[t_name]
    cols = [c.name for c in table.columns.values() if not c.is_primary_key]

    if not cols:
        cols = [c.name for c in table.columns.values()]

    if not cols:
        return f"UPDATE {t_name} SET c0 = 1 WHERE c0 = 0"

    # SET clause
    num_cols = ctx.rng.randint(1, min(3, len(cols)))
    selected_cols = ctx.rng.sample(cols, num_cols)

    set_parts = []
    for col in selected_cols:
        val = ctx.get_column_value(t_name, col) if hasattr(ctx, 'get_column_value') else _gen_constant(ctx)
        set_parts.append(f"{col} = {val}")

    set_clause = ', '.join(set_parts)

    # WHERE is required in YCQL UPDATE (must specify PK)
    pk_col = _pick_pk_column(ctx)
    pk_val = _gen_constant(ctx, 'INT')

    # IF EXISTS / IF condition
    if_clause = ''
    if ctx.rng.random() < 0.2:
        if ctx.rng.random() < 0.5:
            if_clause = ' IF EXISTS'
        else:
            col = _pick_column(ctx)
            op = _gen_comparison_op(ctx)
            val = _gen_constant(ctx)
            if_clause = f" IF {col} {op} {val}"

    return f"UPDATE {t_name} SET {set_clause} WHERE {pk_col} = {pk_val}{if_clause}"

# =============================================================================
# DELETE Generator
# =============================================================================

def _gen_ycql_delete(ctx):
    """Generate a YCQL DELETE statement."""
    t_name = _pick_table(ctx)

    # WHERE is required in YCQL DELETE (must specify PK)
    pk_col = _pick_pk_column(ctx)
    pk_val = _gen_constant(ctx, 'INT')

    # IF EXISTS / IF condition
    if_clause = ''
    if ctx.rng.random() < 0.2:
        if ctx.rng.random() < 0.5:
            if_clause = ' IF EXISTS'
        else:
            col = _pick_column(ctx)
            op = _gen_comparison_op(ctx)
            val = _gen_constant(ctx)
            if_clause = f" IF {col} {op} {val}"

    return f"DELETE FROM {t_name} WHERE {pk_col} = {pk_val}{if_clause}"

# =============================================================================
# Grammar Rules
# =============================================================================

# Data Type Rules
g.rule("data_type", Lambda(lambda ctx: ctx.rng.choice(YCQL_DATA_TYPES)))
g.rule("column_type", Lambda(_gen_ycql_column_type))

# Constant Rules
g.rule("constant", Lambda(lambda ctx: _gen_constant(ctx)))
g.rule("int_constant", Lambda(lambda ctx: _gen_constant(ctx, 'INT')))
g.rule("varchar_constant", Lambda(lambda ctx: _gen_constant(ctx, 'VARCHAR')))
g.rule("boolean_constant", Lambda(lambda ctx: _gen_constant(ctx, 'BOOLEAN')))

# Expression Rules
g.rule("expression", Lambda(lambda ctx: _gen_expression(ctx)))
g.rule("boolean_expression", Lambda(_gen_boolean_expression))

# Operator Rules
g.rule("comparison_op", Lambda(_gen_comparison_op))
g.rule("arithmetic_op", Lambda(_gen_arithmetic_op))
g.rule("logical_op", Lambda(_gen_logical_op))

# Function Rules
g.rule("function_call", Lambda(_gen_function))
g.rule("aggregate", Lambda(_gen_aggregate))

# Table/Column Rules
g.rule("table_name", Lambda(_pick_table))
g.rule("column_name", Lambda(lambda ctx: _pick_column(ctx)))
g.rule("pk_column", Lambda(_pick_pk_column))

# Clause Rules
g.rule("where_clause", Lambda(_gen_ycql_where_clause))
g.rule("order_by", Lambda(_gen_ycql_order_by))
g.rule("limit", Lambda(_gen_ycql_limit))

# Statement Rules
g.rule("select", Lambda(_gen_ycql_select))
g.rule("insert", Lambda(_gen_ycql_insert))
g.rule("update", Lambda(_gen_ycql_update))
g.rule("delete", Lambda(_gen_ycql_delete))
g.rule("create_table", Lambda(_gen_ycql_create_table))
g.rule("create_index", Lambda(_gen_ycql_create_index))

# Main Entry Points
g.rule("query", choice(
    Lambda(_gen_ycql_select),
    Lambda(_gen_ycql_select),
    Lambda(_gen_ycql_select),  # Weight SELECT higher
    Lambda(_gen_ycql_insert),
    Lambda(_gen_ycql_update),
    Lambda(_gen_ycql_delete),
))

g.rule("ddl", choice(
    Lambda(_gen_ycql_create_table),
    Lambda(_gen_ycql_create_index),
))

g.rule("dml", choice(
    Lambda(_gen_ycql_insert),
    Lambda(_gen_ycql_update),
    Lambda(_gen_ycql_delete),
))

# Export the grammar
grammar = g
