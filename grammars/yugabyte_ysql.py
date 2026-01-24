"""
PostgreSQL/YugabyteDB Grammar - Based on SQLancer Implementation.

This grammar replicates the PostgreSQL/YugabyteDB query generation logic from SQLancer,
a database testing tool. It generates random but syntactically valid queries
for testing purposes.

Supports:
- PostgreSQL (standard)
- YugabyteDB YSQL (PostgreSQL-compatible SQL API)
- YugabyteDB YCQL (Cassandra Query Language compatible API)

Reference: https://github.com/sqlancer/sqlancer (postgres, yugabyte modules)
"""

from pyrqg.dsl.core import Grammar, Lambda, choice, template, maybe, repeat, Literal

g = Grammar("sqlancer_ysql")

# =============================================================================
# PostgreSQL/YSQL Data Types (from PostgresSchema.PostgresDataType / YSQLSchema.YSQLDataType)
# =============================================================================

# Full PostgreSQL types
POSTGRES_DATA_TYPES = ['INT', 'BOOLEAN', 'TEXT', 'DECIMAL', 'FLOAT', 'REAL', 'RANGE', 'MONEY', 'BIT', 'INET']

# YSQL adds BYTEA type
YSQL_DATA_TYPES = POSTGRES_DATA_TYPES + ['BYTEA']

# Types that are commonly used (when generateOnlyKnown is true in SQLancer)
COMMON_DATA_TYPES = ['INT', 'BOOLEAN', 'TEXT']

# SQL type mappings for CREATE TABLE
TYPE_SQL_MAPPING = {
    'INT': ['smallint', 'integer', 'bigint', 'int', 'int4', 'int8'],
    'BOOLEAN': ['boolean', 'bool'],
    'TEXT': ['text', 'varchar', 'character varying', 'char', 'character', 'name'],
    'DECIMAL': ['decimal', 'numeric'],
    'FLOAT': ['real', 'float4'],
    'REAL': ['double precision', 'float8'],
    'RANGE': ['int4range'],
    'MONEY': ['money'],
    'BIT': ['bit', 'bit varying', 'varbit'],
    'INET': ['inet', 'cidr'],
    'BYTEA': ['bytea'],
}

# =============================================================================
# YCQL Data Types (from YCQLSchema.YCQLDataType)
# =============================================================================

YCQL_DATA_TYPES = ['INT', 'VARCHAR', 'BOOLEAN', 'FLOAT', 'DATE', 'TIMESTAMP']

# YCQL type mappings
YCQL_TYPE_SQL_MAPPING = {
    'INT': {1: 'TINYINT', 2: 'SMALLINT', 4: ['INTEGER', 'INT'], 8: 'BIGINT'},
    'VARCHAR': ['VARCHAR', 'TEXT'],
    'BOOLEAN': ['BOOLEAN'],
    'FLOAT': {4: 'FLOAT', 8: 'DOUBLE'},
    'DATE': ['DATE'],
    'TIMESTAMP': ['TIMESTAMP'],
}

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

def _pick_column(ctx, data_type=None):
    """Pick a column, optionally filtering by data type."""
    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        return "c0"
    table = ctx.tables[t_name]
    cols = list(table.columns.values())
    if data_type:
        cols = [c for c in cols if _matches_type(c.data_type, data_type)]
    if not cols:
        cols = list(table.columns.values())
    if not cols:
        return "c0"
    return ctx.rng.choice([c.name for c in cols])

def _matches_type(col_type: str, target_type: str) -> bool:
    """Check if column type matches target PostgreSQL type."""
    col_type_lower = col_type.lower().split('(')[0].strip()
    target_lower = target_type.lower()

    type_mapping = {
        'int': ['smallint', 'integer', 'bigint', 'int', 'int4', 'int8', 'serial', 'bigserial', 'tinyint'],
        'boolean': ['boolean', 'bool'],
        'text': ['text', 'varchar', 'character varying', 'char', 'character', 'name', 'bpchar'],
        'decimal': ['decimal', 'numeric'],
        'float': ['real', 'float4', 'float', 'double'],
        'real': ['double precision', 'float8'],
        'range': ['int4range', 'int8range', 'numrange', 'tsrange', 'daterange'],
        'money': ['money'],
        'bit': ['bit', 'bit varying', 'varbit'],
        'inet': ['inet', 'cidr'],
        'bytea': ['bytea'],
        'date': ['date'],
        'timestamp': ['timestamp', 'timestamptz'],
    }

    return col_type_lower in type_mapping.get(target_lower, [])

def _random_int(ctx, min_val=-1000000, max_val=1000000):
    """Generate a random integer constant."""
    return str(ctx.rng.randint(min_val, max_val))

def _random_boolean(ctx):
    """Generate a random boolean constant."""
    if ctx.rng.random() < 0.1:
        # Sometimes use text representations (from SQLancer)
        return ctx.rng.choice(["'TRUE'", "'FALSE'", "'0'", "'1'", "'ON'", "'off'"])
    return ctx.rng.choice(['TRUE', 'FALSE'])

def _random_text(ctx):
    """Generate a random text constant."""
    length = ctx.rng.randint(0, 10)
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 '
    text = ''.join(ctx.rng.choice(chars) for _ in range(length))
    # Escape single quotes
    text = text.replace("'", "''")
    return f"'{text}'"

def _random_decimal(ctx):
    """Generate a random decimal constant."""
    whole = ctx.rng.randint(-1000, 1000)
    frac = ctx.rng.randint(0, 999999)
    return f"{whole}.{frac}"

def _random_float(ctx):
    """Generate a random float constant."""
    return str(ctx.rng.uniform(-1000000, 1000000))

def _random_range(ctx):
    """Generate a random int4range constant."""
    lower = ctx.rng.randint(-1000, 1000)
    upper = ctx.rng.randint(lower, lower + 1000)
    lower_inc = ctx.rng.choice(['[', '('])
    upper_inc = ctx.rng.choice([']', ')'])
    return f"'{lower_inc}{lower},{upper}{upper_inc}'::int4range"

def _random_inet(ctx):
    """Generate a random inet constant."""
    octets = [str(ctx.rng.randint(0, 255)) for _ in range(4)]
    return f"'{'.'.join(octets)}'::inet"

def _random_bit(ctx):
    """Generate a random bit constant."""
    length = ctx.rng.randint(1, 32)
    bits = ''.join(ctx.rng.choice(['0', '1']) for _ in range(length))
    return f"B'{bits}'"

def _random_money(ctx):
    """Generate a random money constant."""
    amount = ctx.rng.randint(-10000, 10000)
    cents = ctx.rng.randint(0, 99)
    return f"'{amount}.{cents:02d}'::money"

def _random_bytea(ctx):
    """Generate a random bytea constant (YSQL-specific)."""
    # Generate random hex bytes
    length = ctx.rng.randint(1, 20)
    hex_chars = '0123456789abcdef'
    hex_str = ''.join(ctx.rng.choice(hex_chars) for _ in range(length * 2))
    return f"'\\x{hex_str}'::bytea"

def _random_date(ctx):
    """Generate a random date constant (YCQL)."""
    year = ctx.rng.randint(1970, 2030)
    month = ctx.rng.randint(1, 12)
    day = ctx.rng.randint(1, 28)
    return f"'{year:04d}-{month:02d}-{day:02d}'"

def _random_timestamp(ctx):
    """Generate a random timestamp constant (YCQL)."""
    year = ctx.rng.randint(1970, 2030)
    month = ctx.rng.randint(1, 12)
    day = ctx.rng.randint(1, 28)
    hour = ctx.rng.randint(0, 23)
    minute = ctx.rng.randint(0, 59)
    second = ctx.rng.randint(0, 59)
    return f"'{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}'"

def _gen_constant(ctx, data_type=None):
    """Generate a constant for a given data type."""
    # Small probability of NULL for any type
    if ctx.rng.random() < 0.05:
        return 'NULL'

    if data_type is None:
        data_type = ctx.rng.choice(COMMON_DATA_TYPES)

    generators = {
        'INT': _random_int,
        'BOOLEAN': _random_boolean,
        'TEXT': _random_text,
        'DECIMAL': _random_decimal,
        'FLOAT': _random_float,
        'REAL': _random_float,
        'RANGE': _random_range,
        'MONEY': _random_money,
        'BIT': _random_bit,
        'INET': _random_inet,
        'BYTEA': _random_bytea,      # YSQL
        'VARCHAR': _random_text,     # YCQL
        'DATE': _random_date,        # YCQL
        'TIMESTAMP': _random_timestamp,  # YCQL
    }

    gen = generators.get(data_type.upper(), _random_int)
    return gen(ctx)

# =============================================================================
# Expression Depth Control
# =============================================================================

MAX_DEPTH = 3

def _get_depth(ctx):
    """Get current expression depth."""
    return ctx.state.get('depth', 0)

def _inc_depth(ctx):
    """Increment and return expression depth."""
    depth = ctx.state.get('depth', 0) + 1
    ctx.state['depth'] = depth
    return depth

def _dec_depth(ctx):
    """Decrement expression depth."""
    ctx.state['depth'] = max(0, ctx.state.get('depth', 0) - 1)

# =============================================================================
# Comparison Operators (from PostgresBinaryComparisonOperation)
# =============================================================================

COMPARISON_OPS = ['=', '!=', '<>', '<', '<=', '>', '>=', 'IS DISTINCT FROM', 'IS NOT DISTINCT FROM']

def _gen_comparison_op(ctx):
    """Generate a comparison operator."""
    return ctx.rng.choice(COMPARISON_OPS)

# =============================================================================
# Arithmetic Operators (from PostgresBinaryArithmeticOperation)
# =============================================================================

ARITHMETIC_OPS = ['+', '-', '*', '/', '%', '^']

def _gen_arithmetic_op(ctx):
    """Generate an arithmetic operator."""
    return ctx.rng.choice(ARITHMETIC_OPS)

# =============================================================================
# Logical Operators (from PostgresBinaryLogicalOperation)
# =============================================================================

LOGICAL_OPS = ['AND', 'OR']

def _gen_logical_op(ctx):
    """Generate a logical operator."""
    return ctx.rng.choice(LOGICAL_OPS)

# =============================================================================
# Postfix Operators (from PostgresPostfixOperation)
# =============================================================================

POSTFIX_OPS = ['IS NULL', 'IS NOT NULL', 'IS TRUE', 'IS NOT TRUE', 'IS FALSE', 'IS NOT FALSE', 'IS UNKNOWN', 'IS NOT UNKNOWN']

def _gen_postfix_op(ctx):
    """Generate a postfix operator."""
    return ctx.rng.choice(POSTFIX_OPS)

# =============================================================================
# Prefix Operators (from PostgresPrefixOperation)
# =============================================================================

PREFIX_OPS = ['NOT', '+', '-']

def _gen_prefix_op(ctx):
    """Generate a prefix operator."""
    return ctx.rng.choice(PREFIX_OPS)

# =============================================================================
# Bit Operators (from PostgresBinaryBitOperation)
# =============================================================================

BIT_OPS = ['&', '|', '#', '<<', '>>']

def _gen_bit_op(ctx):
    """Generate a bit operator."""
    return ctx.rng.choice(BIT_OPS)

# =============================================================================
# Range Operators (from PostgresBinaryRangeOperation)
# =============================================================================

RANGE_OPS = ['&&', '@>', '<@', '<<', '>>', '&<', '&>', '-|-', '+', '*', '-']
RANGE_COMPARISON_OPS = ['=', '<>', '<', '<=', '>', '>=', '@>', '<@', '&&', '<<', '>>', '&<', '&>', '-|-']

# =============================================================================
# Functions (from PostgresFunction and PostgresFunctionWithUnknownResult)
# =============================================================================

# Functions with known result types (from PostgresFunctionWithResult)
FUNCTIONS_INT = ['abs', 'length', 'num_nonnulls', 'num_nulls', 'ascii', 'strpos']
FUNCTIONS_TEXT = ['lower', 'upper', 'initcap', 'btrim', 'ltrim', 'rtrim', 'reverse', 'md5', 'quote_literal', 'quote_ident', 'replace', 'translate', 'substr', 'left', 'right', 'lpad', 'rpad', 'chr', 'repeat', 'split_part', 'to_hex', 'to_ascii']
FUNCTIONS_BOOLEAN = ['isempty', 'lower_inc', 'upper_inc', 'lower_inf', 'upper_inf', 'inet_same_family']
FUNCTIONS_REAL = ['abs', 'cbrt', 'ceiling', 'ceil', 'degrees', 'exp', 'floor', 'ln', 'log', 'log10', 'radians', 'round', 'sign', 'sqrt', 'trunc', 'acos', 'acosd', 'asin', 'asind', 'atan', 'atand', 'atan2', 'atan2d', 'cos', 'cosd', 'cot', 'cotd', 'sin', 'sind', 'tan', 'tand', 'sinh', 'cosh', 'tanh', 'asinh', 'acosh', 'atanh', 'pi', 'random']

def _gen_function_call(ctx, return_type='INT'):
    """Generate a function call for a given return type."""
    depth = _inc_depth(ctx)
    try:
        if depth > MAX_DEPTH:
            return _gen_constant(ctx, return_type)

        funcs = {
            'INT': FUNCTIONS_INT,
            'TEXT': FUNCTIONS_TEXT,
            'BOOLEAN': FUNCTIONS_BOOLEAN,
            'REAL': FUNCTIONS_REAL,
            'FLOAT': FUNCTIONS_REAL,
            'DECIMAL': FUNCTIONS_REAL,
        }

        func_list = funcs.get(return_type.upper(), FUNCTIONS_INT)
        func = ctx.rng.choice(func_list)

        # Generate appropriate arguments based on function
        if func in ['abs', 'ceiling', 'ceil', 'floor', 'round', 'sign', 'sqrt', 'trunc', 'cbrt', 'degrees', 'exp', 'ln', 'log', 'log10', 'radians']:
            arg = _gen_expression(ctx, 'INT')
            return f"{func}({arg})"
        elif func in ['lower', 'upper', 'initcap', 'btrim', 'ltrim', 'rtrim', 'reverse', 'md5', 'quote_literal', 'quote_ident', 'to_ascii']:
            arg = _gen_expression(ctx, 'TEXT')
            return f"{func}({arg})"
        elif func == 'length':
            arg = _gen_expression(ctx, 'TEXT')
            return f"length({arg})"
        elif func in ['substr', 'substring']:
            text = _gen_expression(ctx, 'TEXT')
            start = _gen_expression(ctx, 'INT')
            length = _gen_expression(ctx, 'INT')
            return f"substr({text}, {start}, {length})"
        elif func in ['left', 'right']:
            text = _gen_expression(ctx, 'TEXT')
            n = _gen_expression(ctx, 'INT')
            return f"{func}({text}, {n})"
        elif func in ['lpad', 'rpad']:
            text = _gen_expression(ctx, 'TEXT')
            length = _gen_expression(ctx, 'INT')
            fill = _gen_expression(ctx, 'TEXT')
            return f"{func}({text}, {length}, {fill})"
        elif func == 'replace':
            text = _gen_expression(ctx, 'TEXT')
            from_str = _gen_expression(ctx, 'TEXT')
            to_str = _gen_expression(ctx, 'TEXT')
            return f"replace({text}, {from_str}, {to_str})"
        elif func in ['chr', 'to_hex']:
            arg = _gen_expression(ctx, 'INT')
            return f"{func}({arg})"
        elif func in ['ascii', 'strpos']:
            arg = _gen_expression(ctx, 'TEXT')
            if func == 'strpos':
                arg2 = _gen_expression(ctx, 'TEXT')
                return f"strpos({arg}, {arg2})"
            return f"ascii({arg})"
        elif func in ['num_nonnulls', 'num_nulls']:
            args = ', '.join(_gen_expression(ctx) for _ in range(ctx.rng.randint(1, 4)))
            return f"{func}({args})"
        elif func in ['acos', 'acosd', 'asin', 'asind', 'atan', 'atand', 'cos', 'cosd', 'cot', 'cotd', 'sin', 'sind', 'tan', 'tand', 'sinh', 'cosh', 'tanh', 'asinh', 'acosh', 'atanh']:
            arg = _gen_expression(ctx, 'REAL')
            return f"{func}({arg})"
        elif func in ['atan2', 'atan2d', 'power']:
            arg1 = _gen_expression(ctx, 'REAL')
            arg2 = _gen_expression(ctx, 'REAL')
            return f"{func}({arg1}, {arg2})"
        elif func == 'pi':
            return 'pi()'
        elif func == 'random':
            return 'random()'
        else:
            # Default: single argument function
            arg = _gen_expression(ctx, return_type)
            return f"{func}({arg})"
    finally:
        _dec_depth(ctx)

# =============================================================================
# Aggregate Functions (from PostgresAggregate)
# =============================================================================

AGGREGATE_FUNCTIONS = {
    'COUNT': ['INT'],
    'SUM': ['INT', 'DECIMAL', 'FLOAT', 'REAL'],
    'AVG': ['INT', 'DECIMAL', 'FLOAT', 'REAL'],
    'MIN': None,  # Supports all types
    'MAX': None,  # Supports all types
    'BIT_AND': ['INT'],
    'BIT_OR': ['INT'],
    'BOOL_AND': ['BOOLEAN'],
    'BOOL_OR': ['BOOLEAN'],
    'EVERY': ['BOOLEAN'],
    'STRING_AGG': ['TEXT'],
    'ARRAY_AGG': None,  # Supports all types
}

def _gen_aggregate(ctx, return_type=None):
    """Generate an aggregate function call."""
    depth = _inc_depth(ctx)
    try:
        if return_type:
            eligible = [name for name, types in AGGREGATE_FUNCTIONS.items() if types is None or return_type in types]
        else:
            eligible = list(AGGREGATE_FUNCTIONS.keys())

        if not eligible:
            eligible = ['COUNT']

        agg = ctx.rng.choice(eligible)

        if agg == 'COUNT':
            if ctx.rng.random() < 0.3:
                return 'COUNT(*)'
            distinct = 'DISTINCT ' if ctx.rng.random() < 0.2 else ''
            arg = _gen_expression(ctx)
            return f"COUNT({distinct}{arg})"
        elif agg == 'STRING_AGG':
            expr = _gen_expression(ctx, 'TEXT')
            sep = _random_text(ctx)
            return f"STRING_AGG({expr}, {sep})"
        else:
            distinct = 'DISTINCT ' if ctx.rng.random() < 0.1 else ''
            arg = _gen_expression(ctx, return_type or 'INT')
            return f"{agg}({distinct}{arg})"
    finally:
        _dec_depth(ctx)

# =============================================================================
# Window Functions (from PostgresWindowFunction)
# =============================================================================

WINDOW_FUNCTIONS = ['row_number', 'rank', 'dense_rank', 'percent_rank', 'cume_dist', 'ntile', 'lag', 'lead', 'first_value', 'last_value', 'nth_value']

def _gen_window_function(ctx):
    """Generate a window function call."""
    func = ctx.rng.choice(WINDOW_FUNCTIONS)

    # Generate arguments based on function
    if func in ['row_number', 'rank', 'dense_rank', 'percent_rank', 'cume_dist']:
        args = ''
    elif func == 'ntile':
        args = str(ctx.rng.randint(1, 10))
    elif func in ['lag', 'lead']:
        expr = _gen_expression(ctx)
        if ctx.rng.random() < 0.5:
            offset = ctx.rng.randint(1, 5)
            args = f"{expr}, {offset}"
        else:
            args = expr
    elif func in ['first_value', 'last_value']:
        args = _gen_expression(ctx)
    elif func == 'nth_value':
        expr = _gen_expression(ctx)
        n = ctx.rng.randint(1, 10)
        args = f"{expr}, {n}"
    else:
        args = _gen_expression(ctx)

    # Generate OVER clause
    over_parts = []

    # PARTITION BY
    if ctx.rng.random() < 0.5:
        partition_cols = [_pick_column(ctx) for _ in range(ctx.rng.randint(1, 2))]
        over_parts.append(f"PARTITION BY {', '.join(partition_cols)}")

    # ORDER BY
    if ctx.rng.random() < 0.7 or func in ['row_number', 'rank', 'dense_rank', 'lag', 'lead', 'first_value', 'last_value', 'nth_value']:
        order_col = _pick_column(ctx)
        order_dir = ctx.rng.choice(['ASC', 'DESC'])
        nulls = ctx.rng.choice(['', ' NULLS FIRST', ' NULLS LAST'])
        over_parts.append(f"ORDER BY {order_col} {order_dir}{nulls}")

    # Frame clause (for ranking functions that support it)
    if func in ['first_value', 'last_value', 'nth_value'] and ctx.rng.random() < 0.3:
        frame_type = ctx.rng.choice(['ROWS', 'RANGE', 'GROUPS'])
        frame_start = ctx.rng.choice(['UNBOUNDED PRECEDING', 'CURRENT ROW', f'{ctx.rng.randint(1, 5)} PRECEDING'])
        frame_end = ctx.rng.choice(['CURRENT ROW', 'UNBOUNDED FOLLOWING', f'{ctx.rng.randint(1, 5)} FOLLOWING'])
        over_parts.append(f"{frame_type} BETWEEN {frame_start} AND {frame_end}")

    over_clause = ' '.join(over_parts)
    return f"{func}({args}) OVER ({over_clause})"

# =============================================================================
# Expression Generator (from PostgresExpressionGenerator)
# =============================================================================

def _gen_expression(ctx, data_type=None):
    """Generate an expression of a given type."""
    depth = _inc_depth(ctx)
    try:
        if depth > MAX_DEPTH or ctx.rng.random() < 0.3:
            # Return a leaf node (constant or column)
            if ctx.rng.random() < 0.6 and ctx.tables:
                return _pick_column(ctx, data_type)
            return _gen_constant(ctx, data_type)

        if data_type is None:
            data_type = ctx.rng.choice(COMMON_DATA_TYPES)

        data_type = data_type.upper()

        if data_type == 'BOOLEAN':
            return _gen_boolean_expression(ctx)
        elif data_type == 'INT':
            return _gen_int_expression(ctx)
        elif data_type == 'TEXT':
            return _gen_text_expression(ctx)
        elif data_type in ('DECIMAL', 'FLOAT', 'REAL'):
            return _gen_numeric_expression(ctx, data_type)
        elif data_type == 'RANGE':
            return _gen_range_expression(ctx)
        elif data_type == 'BIT':
            return _gen_bit_expression(ctx)
        elif data_type == 'INET':
            return _gen_inet_expression(ctx)
        else:
            return _gen_constant(ctx, data_type)
    finally:
        _dec_depth(ctx)

def _gen_boolean_expression(ctx):
    """Generate a boolean expression."""
    choice_val = ctx.rng.random()

    if choice_val < 0.15:
        # Postfix operation (IS NULL, IS TRUE, etc.)
        expr = _gen_expression(ctx)
        return f"({expr}) {_gen_postfix_op(ctx)}"
    elif choice_val < 0.25:
        # NOT prefix
        expr = _gen_expression(ctx, 'BOOLEAN')
        return f"NOT ({expr})"
    elif choice_val < 0.45:
        # Binary logical operation (AND/OR)
        left = _gen_expression(ctx, 'BOOLEAN')
        right = _gen_expression(ctx, 'BOOLEAN')
        op = _gen_logical_op(ctx)
        return f"({left}) {op} ({right})"
    elif choice_val < 0.65:
        # Binary comparison
        data_type = ctx.rng.choice(COMMON_DATA_TYPES)
        left = _gen_expression(ctx, data_type)
        right = _gen_expression(ctx, data_type)
        op = _gen_comparison_op(ctx)
        return f"({left}) {op} ({right})"
    elif choice_val < 0.75:
        # LIKE operation
        left = _gen_expression(ctx, 'TEXT')
        right = _gen_expression(ctx, 'TEXT')
        return f"({left}) LIKE ({right})"
    elif choice_val < 0.85:
        # BETWEEN operation
        data_type = ctx.rng.choice(['INT', 'TEXT'])
        expr = _gen_expression(ctx, data_type)
        lower = _gen_expression(ctx, data_type)
        upper = _gen_expression(ctx, data_type)
        symmetric = 'SYMMETRIC ' if ctx.rng.random() < 0.2 else ''
        return f"({expr}) BETWEEN {symmetric}({lower}) AND ({upper})"
    elif choice_val < 0.95:
        # IN operation
        data_type = ctx.rng.choice(COMMON_DATA_TYPES)
        expr = _gen_expression(ctx, data_type)
        num_values = ctx.rng.randint(1, 5)
        values = ', '.join(_gen_expression(ctx, data_type) for _ in range(num_values))
        not_in = 'NOT ' if ctx.rng.random() < 0.3 else ''
        return f"({expr}) {not_in}IN ({values})"
    else:
        # Function call
        return _gen_function_call(ctx, 'BOOLEAN')

def _gen_int_expression(ctx):
    """Generate an integer expression."""
    choice_val = ctx.rng.random()

    if choice_val < 0.3:
        # Binary arithmetic
        left = _gen_expression(ctx, 'INT')
        right = _gen_expression(ctx, 'INT')
        op = ctx.rng.choice(['+', '-', '*', '/', '%'])
        return f"({left}) {op} ({right})"
    elif choice_val < 0.5:
        # Unary operation
        expr = _gen_expression(ctx, 'INT')
        op = ctx.rng.choice(['+', '-'])
        return f"{op}({expr})"
    elif choice_val < 0.7:
        # Cast
        source_type = ctx.rng.choice(['TEXT', 'BOOLEAN', 'DECIMAL'])
        expr = _gen_expression(ctx, source_type)
        return f"CAST(({expr}) AS INT)"
    else:
        # Function call
        return _gen_function_call(ctx, 'INT')

def _gen_text_expression(ctx):
    """Generate a text expression."""
    choice_val = ctx.rng.random()

    if choice_val < 0.3:
        # Concatenation
        left = _gen_expression(ctx, 'TEXT')
        right = _gen_expression(ctx, 'TEXT')
        return f"({left}) || ({right})"
    elif choice_val < 0.5:
        # Cast
        source_type = ctx.rng.choice(['INT', 'BOOLEAN', 'DECIMAL'])
        expr = _gen_expression(ctx, source_type)
        return f"CAST(({expr}) AS VARCHAR)"
    elif choice_val < 0.7:
        # COLLATE
        expr = _gen_expression(ctx, 'TEXT')
        collation = ctx.rng.choice(['"C"', '"POSIX"', '"en_US.utf8"'])
        return f"({expr}) COLLATE {collation}"
    else:
        # Function call
        return _gen_function_call(ctx, 'TEXT')

def _gen_numeric_expression(ctx, data_type='REAL'):
    """Generate a numeric expression (DECIMAL, FLOAT, REAL)."""
    # For simplicity, generate constant or column
    if ctx.rng.random() < 0.5:
        return _gen_constant(ctx, data_type)
    return _gen_function_call(ctx, data_type)

def _gen_range_expression(ctx):
    """Generate a range expression."""
    if ctx.rng.random() < 0.7:
        return _random_range(ctx)
    # Binary range operation
    left = _gen_range_expression(ctx)
    right = _gen_range_expression(ctx)
    op = ctx.rng.choice(RANGE_OPS)
    return f"({left}) {op} ({right})"

def _gen_bit_expression(ctx):
    """Generate a bit expression."""
    if ctx.rng.random() < 0.7:
        return _random_bit(ctx)
    # Binary bit operation
    left = _gen_bit_expression(ctx)
    right = _gen_bit_expression(ctx)
    op = ctx.rng.choice(BIT_OPS)
    return f"({left}) {op} ({right})"

def _gen_inet_expression(ctx):
    """Generate an inet expression."""
    return _random_inet(ctx)

# =============================================================================
# JOIN Types (from PostgresJoin.PostgresJoinType)
# =============================================================================

JOIN_TYPES = ['INNER JOIN', 'LEFT OUTER JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN']

def _gen_join_clause(ctx):
    """Generate a JOIN clause."""
    if not ctx.tables or len(ctx.tables) < 2:
        return ""

    tables_list = list(ctx.tables.keys())
    current_table = ctx.state.get('table', tables_list[0])
    other_tables = [t for t in tables_list if t != current_table]

    if not other_tables:
        return ""

    join_table = ctx.rng.choice(other_tables)
    join_type = ctx.rng.choice(JOIN_TYPES)

    if join_type == 'CROSS JOIN':
        return f" {join_type} {join_table}"

    # Generate ON condition
    on_condition = _gen_expression(ctx, 'BOOLEAN')
    return f" {join_type} {join_table} ON ({on_condition})"

# =============================================================================
# SELECT Query Generator
# =============================================================================

def _gen_select_columns(ctx):
    """Generate SELECT column list."""
    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        return "*"

    table = ctx.tables[t_name]
    cols = list(table.columns.values())

    if ctx.rng.random() < 0.1:
        return "*"

    # Select some columns, possibly with expressions
    num_cols = ctx.rng.randint(1, min(5, len(cols)))
    selected = ctx.rng.sample([c.name for c in cols], num_cols)

    result = []
    for col in selected:
        if ctx.rng.random() < 0.3:
            # Wrap in expression
            expr = _gen_expression(ctx, _guess_type(ctx, col))
            alias = f"col_{ctx.rng.randint(1, 100)}"
            result.append(f"({expr}) AS {alias}")
        else:
            result.append(col)

    # Sometimes add aggregate
    if ctx.rng.random() < 0.2:
        agg = _gen_aggregate(ctx)
        result.append(f"{agg} AS agg_result")

    # Sometimes add window function
    if ctx.rng.random() < 0.1:
        window = _gen_window_function(ctx)
        result.append(f"{window} AS window_result")

    return ', '.join(result)

def _guess_type(ctx, col_name):
    """Guess the type of a column."""
    t_name = ctx.state.get('table')
    if t_name and t_name in ctx.tables:
        table = ctx.tables[t_name]
        if col_name in table.columns:
            col_type = table.columns[col_name].data_type.lower()
            if 'int' in col_type or col_type in ['smallint', 'bigint', 'serial']:
                return 'INT'
            elif col_type in ['boolean', 'bool']:
                return 'BOOLEAN'
            elif 'char' in col_type or col_type == 'text':
                return 'TEXT'
    return ctx.rng.choice(COMMON_DATA_TYPES)

def _gen_where_clause(ctx):
    """Generate WHERE clause."""
    if ctx.rng.random() < 0.3:
        return ""
    return f"WHERE {_gen_expression(ctx, 'BOOLEAN')}"

def _gen_group_by(ctx):
    """Generate GROUP BY clause."""
    if ctx.rng.random() < 0.7:
        return ""

    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        return ""

    table = ctx.tables[t_name]
    cols = [c.name for c in table.columns.values()]
    if not cols:
        return ""

    num_cols = ctx.rng.randint(1, min(3, len(cols)))
    group_cols = ctx.rng.sample(cols, num_cols)
    return f"GROUP BY {', '.join(group_cols)}"

def _gen_having(ctx):
    """Generate HAVING clause."""
    if ctx.rng.random() < 0.8:
        return ""
    return f"HAVING {_gen_expression(ctx, 'BOOLEAN')}"

def _gen_order_by(ctx):
    """Generate ORDER BY clause."""
    if ctx.rng.random() < 0.5:
        return ""

    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        col = "1"
    else:
        table = ctx.tables[t_name]
        cols = [c.name for c in table.columns.values()]
        if cols:
            col = ctx.rng.choice(cols)
        else:
            col = "1"

    direction = ctx.rng.choice(['ASC', 'DESC', ''])
    nulls = ctx.rng.choice(['', ' NULLS FIRST', ' NULLS LAST'])
    return f"ORDER BY {col} {direction}{nulls}"

def _gen_limit(ctx):
    """Generate LIMIT clause."""
    if ctx.rng.random() < 0.6:
        return ""

    limit = ctx.rng.randint(1, 100)
    offset = ""
    if ctx.rng.random() < 0.3:
        offset = f" OFFSET {ctx.rng.randint(0, 50)}"
    return f"LIMIT {limit}{offset}"

def _gen_for_clause(ctx):
    """Generate FOR clause (locking)."""
    if ctx.rng.random() < 0.9:
        return ""

    clause = ctx.rng.choice(['FOR UPDATE', 'FOR NO KEY UPDATE', 'FOR SHARE', 'FOR KEY SHARE'])
    return clause

def _gen_select_type(ctx):
    """Generate SELECT type (ALL/DISTINCT)."""
    if ctx.rng.random() < 0.9:
        return "SELECT"

    if ctx.rng.random() < 0.7:
        return "SELECT DISTINCT"

    # DISTINCT ON
    col = _pick_column(ctx)
    return f"SELECT DISTINCT ON ({col})"

# =============================================================================
# CREATE TABLE Generator (from PostgresTableGenerator)
# =============================================================================

def _gen_table_name(ctx):
    """Generate a table name."""
    return f"t{ctx.rng.randint(0, 100)}"

def _gen_column_definition(ctx):
    """Generate a column definition for CREATE TABLE."""
    col_name = f"c{ctx.state.get('col_idx', 0)}"
    ctx.state['col_idx'] = ctx.state.get('col_idx', 0) + 1

    # Choose data type
    data_type = ctx.rng.choice(COMMON_DATA_TYPES + ['DECIMAL', 'REAL'])
    sql_type = ctx.rng.choice(TYPE_SQL_MAPPING.get(data_type, ['integer']))

    # Add size for certain types
    if sql_type in ['varchar', 'character varying', 'char', 'character']:
        size = ctx.rng.randint(1, 255)
        sql_type = f"{sql_type}({size})"
    elif sql_type in ['decimal', 'numeric']:
        precision = ctx.rng.randint(1, 38)
        scale = ctx.rng.randint(0, min(precision, 10))
        sql_type = f"{sql_type}({precision},{scale})"
    elif sql_type in ['bit', 'bit varying', 'varbit']:
        length = ctx.rng.randint(1, 64)
        sql_type = f"{sql_type}({length})"

    # Column constraints
    constraints = []

    # NULL/NOT NULL
    if ctx.rng.random() < 0.3:
        constraints.append(ctx.rng.choice(['NULL', 'NOT NULL']))

    # UNIQUE
    if ctx.rng.random() < 0.1:
        constraints.append('UNIQUE')

    # PRIMARY KEY (only one per table)
    if ctx.rng.random() < 0.1 and not ctx.state.get('has_pk'):
        constraints.append('PRIMARY KEY')
        ctx.state['has_pk'] = True

    # DEFAULT
    if ctx.rng.random() < 0.2:
        default_val = _gen_constant(ctx, data_type)
        constraints.append(f"DEFAULT ({default_val})")

    # CHECK constraint
    if ctx.rng.random() < 0.1:
        check_expr = _gen_expression(ctx, 'BOOLEAN')
        constraints.append(f"CHECK ({check_expr})")

    # GENERATED (for INT columns)
    if data_type == 'INT' and ctx.rng.random() < 0.1 and not ctx.state.get('has_pk'):
        gen_type = ctx.rng.choice(['ALWAYS', 'BY DEFAULT'])
        constraints.append(f"GENERATED {gen_type} AS IDENTITY")

    constraint_str = ' '.join(constraints)
    return f"{col_name} {sql_type} {constraint_str}".strip()

def _gen_table_constraints(ctx):
    """Generate table-level constraints."""
    constraints = []

    # UNIQUE constraint
    if ctx.rng.random() < 0.2:
        cols = ctx.state.get('created_cols', ['c0'])
        if cols:
            unique_cols = ctx.rng.sample(cols, ctx.rng.randint(1, min(2, len(cols))))
            constraints.append(f"UNIQUE ({', '.join(unique_cols)})")

    # CHECK constraint
    if ctx.rng.random() < 0.1:
        check_expr = _gen_expression(ctx, 'BOOLEAN')
        constraints.append(f"CHECK ({check_expr})")

    return constraints

def _gen_create_table(ctx):
    """Generate a CREATE TABLE statement (PostgreSQL-compatible)."""
    ctx.state['col_idx'] = 0
    ctx.state['has_pk'] = False
    ctx.state['created_cols'] = []

    # Table modifiers
    temp = ctx.rng.choice(['', 'TEMPORARY ', 'TEMP ', 'UNLOGGED ']) if ctx.rng.random() < 0.2 else ''
    if_not_exists = 'IF NOT EXISTS ' if ctx.rng.random() < 0.3 else ''

    table_name = _gen_table_name(ctx)

    # Generate columns
    num_cols = ctx.rng.randint(1, 5)
    columns = []
    for _ in range(num_cols):
        col_def = _gen_column_definition(ctx)
        columns.append(col_def)
        ctx.state['created_cols'].append(f"c{len(ctx.state['created_cols'])}")

    # Table constraints
    table_constraints = _gen_table_constraints(ctx)

    all_defs = columns + table_constraints
    column_defs = ',\n    '.join(all_defs)

    # Storage parameters
    with_clause = ''
    if ctx.rng.random() < 0.1:
        params = []
        if ctx.rng.random() < 0.5:
            params.append(f"fillfactor = {ctx.rng.randint(10, 100)}")
        if ctx.rng.random() < 0.3:
            params.append(f"autovacuum_enabled = {ctx.rng.choice(['true', 'false'])}")
        if params:
            with_clause = f"\nWITH ({', '.join(params)})"

    return f"CREATE {temp}TABLE {if_not_exists}{table_name} (\n    {column_defs}\n){with_clause}"


# =============================================================================
# YSQL-Specific CREATE TABLE (YugabyteDB SQL API)
# =============================================================================

def _gen_ysql_split_clause(ctx, temp=''):
    """Generate YSQL SPLIT clause for table partitioning."""
    if temp or ctx.rng.random() < 0.7:
        return ''

    if ctx.rng.random() < 0.6:
        # SPLIT INTO N TABLETS
        num_tablets = ctx.rng.randint(1, 10)
        return f" SPLIT INTO {num_tablets} TABLETS"
    else:
        # SPLIT AT VALUES - simplified version
        num_splits = ctx.rng.randint(1, 3)
        values = ', '.join(f"({ctx.rng.randint(1, 1000)})" for _ in range(num_splits))
        return f" SPLIT AT VALUES ({values})"

def _gen_ysql_tablegroup_clause(ctx, temp=''):
    """Generate YSQL TABLEGROUP clause."""
    if temp or ctx.rng.random() < 0.9:
        return ''

    tg_id = ctx.rng.randint(1, 10)
    return f" TABLEGROUP tg{tg_id}"

def _gen_ysql_create_table(ctx):
    """Generate a CREATE TABLE statement with YugabyteDB YSQL extensions."""
    ctx.state['col_idx'] = 0
    ctx.state['has_pk'] = False
    ctx.state['created_cols'] = []

    # Table modifiers (no UNLOGGED for YSQL - it's distributed)
    temp = 'TEMPORARY ' if ctx.rng.random() < 0.1 else ''
    if_not_exists = 'IF NOT EXISTS ' if ctx.rng.random() < 0.3 else ''

    table_name = _gen_table_name(ctx)

    # Generate columns - YSQL supports BYTEA
    num_cols = ctx.rng.randint(1, 5)
    columns = []
    for _ in range(num_cols):
        col_def = _gen_column_definition(ctx)
        columns.append(col_def)
        ctx.state['created_cols'].append(f"c{len(ctx.state['created_cols'])}")

    # Table constraints
    table_constraints = _gen_table_constraints(ctx)

    all_defs = columns + table_constraints
    column_defs = ',\n    '.join(all_defs)

    # YSQL-specific clauses
    split_clause = _gen_ysql_split_clause(ctx, temp)
    tablegroup_clause = _gen_ysql_tablegroup_clause(ctx, temp)

    # Basic storage parameter
    with_clause = ''
    if ctx.rng.random() < 0.1 and not temp:
        with_clause = " WITHOUT OIDS"

    return f"CREATE {temp}TABLE {if_not_exists}{table_name} (\n    {column_defs}\n){with_clause}{split_clause}{tablegroup_clause}"


# =============================================================================
# YSQL-Specific Index Types
# =============================================================================

YSQL_INDEX_TYPES = ['BTREE', 'HASH', 'GIN', 'GIST']

def _gen_ysql_create_index(ctx):
    """Generate a CREATE INDEX statement with YugabyteDB YSQL extensions."""
    unique = 'UNIQUE ' if ctx.rng.random() < 0.2 else ''

    t_name = _pick_table(ctx)
    index_name = f"idx_{t_name}_{ctx.rng.randint(1, 100)}"

    # Index method
    method_clause = ''
    if ctx.rng.random() < 0.5:
        method = ctx.rng.choice(YSQL_INDEX_TYPES)
        method_clause = f" USING {method}"

    # Columns
    col = _pick_column(ctx)

    # Order and nulls
    order = ctx.rng.choice(['', ' ASC', ' DESC'])
    nulls = ctx.rng.choice(['', ' NULLS FIRST', ' NULLS LAST'])

    # Optional WHERE for partial index
    where_clause = ''
    if ctx.rng.random() < 0.2:
        where_clause = f" WHERE {_gen_expression(ctx, 'BOOLEAN')}"

    return f"CREATE {unique}INDEX {index_name} ON {t_name}{method_clause} ({col}{order}{nulls}){where_clause}"

# =============================================================================
# INSERT Generator
# =============================================================================

def _gen_insert(ctx):
    """Generate an INSERT statement."""
    t_name = _pick_table(ctx)
    if t_name not in ctx.tables:
        # Fallback for when no tables exist
        return f"INSERT INTO {t_name} (c0) VALUES (1)"

    table = ctx.tables[t_name]
    cols = [c.name for c in table.columns.values() if not (c.is_primary_key and 'generated' in c.data_type.lower())]

    if not cols:
        cols = [c.name for c in table.columns.values()]

    if not cols:
        return f"INSERT INTO {t_name} DEFAULT VALUES"

    # Select columns for insert
    num_cols = ctx.rng.randint(1, len(cols))
    selected_cols = ctx.rng.sample(cols, num_cols)

    # Generate values
    num_rows = ctx.rng.randint(1, 3)
    all_values = []
    for _ in range(num_rows):
        row_values = []
        for col in selected_cols:
            val = ctx.get_column_value(t_name, col)
            row_values.append(val)
        all_values.append(f"({', '.join(row_values)})")

    values_str = ', '.join(all_values)
    cols_str = ', '.join(selected_cols)

    # ON CONFLICT clause
    on_conflict = ''
    if ctx.rng.random() < 0.2:
        conflict_col = ctx.rng.choice(selected_cols)
        on_conflict = f" ON CONFLICT ({conflict_col}) DO NOTHING"

    return f"INSERT INTO {t_name} ({cols_str}) VALUES {values_str}{on_conflict}"

# =============================================================================
# UPDATE Generator
# =============================================================================

def _gen_update(ctx):
    """Generate an UPDATE statement."""
    t_name = _pick_table(ctx)
    if t_name not in ctx.tables:
        return f"UPDATE {t_name} SET c0 = 1"

    table = ctx.tables[t_name]
    cols = [c.name for c in table.columns.values() if not c.is_primary_key]

    if not cols:
        cols = [c.name for c in table.columns.values()]

    if not cols:
        return f"UPDATE {t_name} SET c0 = 1"

    # Generate SET clause
    num_cols = ctx.rng.randint(1, min(3, len(cols)))
    selected_cols = ctx.rng.sample(cols, num_cols)

    set_parts = []
    for col in selected_cols:
        val = ctx.get_column_value(t_name, col)
        set_parts.append(f"{col} = {val}")

    set_clause = ', '.join(set_parts)
    where_clause = _gen_where_clause(ctx)

    return f"UPDATE {t_name} SET {set_clause} {where_clause}".strip()

# =============================================================================
# DELETE Generator
# =============================================================================

def _gen_delete(ctx):
    """Generate a DELETE statement."""
    t_name = _pick_table(ctx)
    where_clause = _gen_where_clause(ctx)

    # Sometimes use USING for join-delete
    using_clause = ''
    if ctx.rng.random() < 0.1 and len(ctx.tables) > 1:
        other_tables = [t for t in ctx.tables.keys() if t != t_name]
        if other_tables:
            using_table = ctx.rng.choice(other_tables)
            using_clause = f" USING {using_table}"

    return f"DELETE FROM {t_name}{using_clause} {where_clause}".strip()

# =============================================================================
# Full SELECT Query
# =============================================================================

def _gen_full_select(ctx):
    """Generate a complete SELECT query."""
    ctx.state['depth'] = 0

    t_name = _pick_table(ctx)

    select_type = _gen_select_type(ctx)
    columns = _gen_select_columns(ctx)
    from_clause = t_name
    join_clause = _gen_join_clause(ctx) if ctx.rng.random() < 0.3 else ''
    where = _gen_where_clause(ctx)
    group_by = _gen_group_by(ctx)
    having = _gen_having(ctx) if group_by else ''
    order_by = _gen_order_by(ctx)
    limit = _gen_limit(ctx)
    for_clause = _gen_for_clause(ctx)

    query = f"{select_type} {columns} FROM {from_clause}{join_clause}"
    if where:
        query += f" {where}"
    if group_by:
        query += f" {group_by}"
    if having:
        query += f" {having}"
    if order_by:
        query += f" {order_by}"
    if limit:
        query += f" {limit}"
    if for_clause:
        query += f" {for_clause}"

    return query

# =============================================================================
# Grammar Rules
# =============================================================================

# Data Type Rules
g.rule("data_type", Lambda(lambda ctx: ctx.rng.choice(POSTGRES_DATA_TYPES)))
g.rule("common_data_type", Lambda(lambda ctx: ctx.rng.choice(COMMON_DATA_TYPES)))

# Constant Rules
g.rule("constant", Lambda(lambda ctx: _gen_constant(ctx)))
g.rule("int_constant", Lambda(lambda ctx: _gen_constant(ctx, 'INT')))
g.rule("text_constant", Lambda(lambda ctx: _gen_constant(ctx, 'TEXT')))
g.rule("boolean_constant", Lambda(lambda ctx: _gen_constant(ctx, 'BOOLEAN')))

# Expression Rules
g.rule("expression", Lambda(lambda ctx: _gen_expression(ctx)))
g.rule("boolean_expression", Lambda(lambda ctx: _gen_expression(ctx, 'BOOLEAN')))
g.rule("int_expression", Lambda(lambda ctx: _gen_expression(ctx, 'INT')))
g.rule("text_expression", Lambda(lambda ctx: _gen_expression(ctx, 'TEXT')))

# Operator Rules
g.rule("comparison_op", Lambda(_gen_comparison_op))
g.rule("arithmetic_op", Lambda(_gen_arithmetic_op))
g.rule("logical_op", Lambda(_gen_logical_op))

# Function Rules
g.rule("function_call", Lambda(lambda ctx: _gen_function_call(ctx)))
g.rule("aggregate", Lambda(lambda ctx: _gen_aggregate(ctx)))
g.rule("window_function", Lambda(_gen_window_function))

# Table/Column Rules
g.rule("table_name", Lambda(_pick_table))
g.rule("column_name", Lambda(lambda ctx: _pick_column(ctx)))

# Clause Rules
g.rule("where_clause", Lambda(_gen_where_clause))
g.rule("group_by", Lambda(_gen_group_by))
g.rule("having", Lambda(_gen_having))
g.rule("order_by", Lambda(_gen_order_by))
g.rule("limit", Lambda(_gen_limit))
g.rule("join_clause", Lambda(_gen_join_clause))

# Statement Rules
g.rule("select", Lambda(_gen_full_select))
g.rule("insert", Lambda(_gen_insert))
g.rule("update", Lambda(_gen_update))
g.rule("delete", Lambda(_gen_delete))
g.rule("create_table", Lambda(_gen_create_table))

# Main Entry Points
g.rule("query", choice(
    Lambda(_gen_full_select),
    Lambda(_gen_full_select),
    Lambda(_gen_full_select),  # Weight SELECT higher
    Lambda(_gen_insert),
    Lambda(_gen_update),
    Lambda(_gen_delete),
))

g.rule("ddl", Lambda(_gen_create_table))

g.rule("dml", choice(
    Lambda(_gen_insert),
    Lambda(_gen_update),
    Lambda(_gen_delete),
))

# YSQL-specific rules
g.rule("ysql_create_table", Lambda(_gen_ysql_create_table))
g.rule("ysql_create_index", Lambda(_gen_ysql_create_index))
g.rule("ysql_ddl", choice(
    Lambda(_gen_ysql_create_table),
    Lambda(_gen_ysql_create_index),
))

# Export the grammar
grammar = g
