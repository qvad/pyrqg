"""
YugabyteDB YSQL Grammar - Based on SQLsmith Implementation.

SQLsmith is a random SQL query generator focused on finding bugs through
complex, nested queries. It emphasizes:
- Complex SELECT with subqueries and CTEs
- Various JOIN types (INNER, LEFT, RIGHT, LATERAL)
- TABLESAMPLE
- Window functions
- MERGE statements
- DML with RETURNING clauses
- Type-aware expression generation

Reference: https://github.com/anse1/sqlsmith
"""

from pyrqg.dsl.core import Grammar, Lambda, choice, template, maybe, repeat, Literal

g = Grammar("sqlsmith_ysql")

# =============================================================================
# Data Types (PostgreSQL/YSQL compatible)
# =============================================================================

DATA_TYPES = ['INT', 'BIGINT', 'SMALLINT', 'BOOLEAN', 'TEXT', 'VARCHAR', 'NUMERIC', 'REAL', 'DOUBLE PRECISION', 'DATE', 'TIMESTAMP', 'TIMESTAMPTZ']

NUMERIC_TYPES = ['INT', 'BIGINT', 'SMALLINT', 'NUMERIC', 'REAL', 'DOUBLE PRECISION']
TEXT_TYPES = ['TEXT', 'VARCHAR', 'CHAR']
TEMPORAL_TYPES = ['DATE', 'TIMESTAMP', 'TIMESTAMPTZ']

# =============================================================================
# Helper Functions
# =============================================================================

def _pick_table(ctx):
    """Pick a random table and store it in context state."""
    if ctx.tables:
        t = ctx.rng.choice(list(ctx.tables.keys()))
        ctx.state['table'] = t
        ctx.state.setdefault('available_tables', []).append(t)
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
    """Check if column type matches target type category."""
    col_lower = col_type.lower().split('(')[0].strip()
    target_lower = target_type.lower()

    type_groups = {
        'numeric': ['int', 'integer', 'bigint', 'smallint', 'numeric', 'decimal', 'real', 'float', 'double precision', 'float4', 'float8', 'serial', 'bigserial'],
        'text': ['text', 'varchar', 'character varying', 'char', 'character', 'name', 'bpchar'],
        'boolean': ['boolean', 'bool'],
        'temporal': ['date', 'timestamp', 'timestamptz', 'time', 'timetz', 'interval'],
    }

    for group, types in type_groups.items():
        if target_lower in types or target_lower == group:
            return col_lower in types
    return col_lower == target_lower

# =============================================================================
# Constants Generation
# =============================================================================

def _random_int(ctx, min_val=-100000, max_val=100000):
    return str(ctx.rng.randint(min_val, max_val))

def _random_bigint(ctx):
    return str(ctx.rng.randint(-9223372036854775808 // 1000000, 9223372036854775807 // 1000000))

def _random_boolean(ctx):
    return ctx.rng.choice(['TRUE', 'FALSE'])

def _random_text(ctx):
    length = ctx.rng.randint(0, 20)
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 '
    text = ''.join(ctx.rng.choice(chars) for _ in range(length))
    text = text.replace("'", "''")
    return f"'{text}'"

def _random_numeric(ctx):
    whole = ctx.rng.randint(-10000, 10000)
    frac = ctx.rng.randint(0, 999999)
    return f"{whole}.{frac}"

def _random_date(ctx):
    year = ctx.rng.randint(1970, 2030)
    month = ctx.rng.randint(1, 12)
    day = ctx.rng.randint(1, 28)
    return f"'{year:04d}-{month:02d}-{day:02d}'"

def _random_timestamp(ctx):
    year = ctx.rng.randint(1970, 2030)
    month = ctx.rng.randint(1, 12)
    day = ctx.rng.randint(1, 28)
    hour = ctx.rng.randint(0, 23)
    minute = ctx.rng.randint(0, 59)
    second = ctx.rng.randint(0, 59)
    return f"'{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}'"

def _gen_constant(ctx, data_type=None):
    """Generate a type-appropriate constant."""
    if ctx.rng.random() < 0.05:
        return 'NULL'

    if data_type is None:
        data_type = ctx.rng.choice(['INT', 'TEXT', 'BOOLEAN'])

    data_type_upper = data_type.upper()

    generators = {
        'INT': _random_int,
        'INTEGER': _random_int,
        'BIGINT': _random_bigint,
        'SMALLINT': lambda c: _random_int(c, -32768, 32767),
        'BOOLEAN': _random_boolean,
        'TEXT': _random_text,
        'VARCHAR': _random_text,
        'NUMERIC': _random_numeric,
        'DECIMAL': _random_numeric,
        'REAL': _random_numeric,
        'DOUBLE PRECISION': _random_numeric,
        'DATE': _random_date,
        'TIMESTAMP': _random_timestamp,
        'TIMESTAMPTZ': _random_timestamp,
    }

    gen = generators.get(data_type_upper, _random_text)
    return gen(ctx)

# =============================================================================
# Expression Depth Control
# =============================================================================

MAX_DEPTH = 4

def _get_depth(ctx):
    return ctx.state.get('depth', 0)

def _inc_depth(ctx):
    depth = ctx.state.get('depth', 0) + 1
    ctx.state['depth'] = depth
    return depth

def _dec_depth(ctx):
    ctx.state['depth'] = max(0, ctx.state.get('depth', 0) - 1)

# =============================================================================
# Operators
# =============================================================================

COMPARISON_OPS = ['=', '!=', '<>', '<', '<=', '>', '>=', 'IS DISTINCT FROM', 'IS NOT DISTINCT FROM']
ARITHMETIC_OPS = ['+', '-', '*', '/', '%']
LOGICAL_OPS = ['AND', 'OR']
POSTFIX_OPS = ['IS NULL', 'IS NOT NULL', 'IS TRUE', 'IS NOT TRUE', 'IS FALSE', 'IS NOT FALSE']

# =============================================================================
# Functions (SQLsmith style - type-aware)
# =============================================================================

NUMERIC_FUNCTIONS = ['abs', 'ceil', 'floor', 'round', 'trunc', 'sign', 'sqrt', 'cbrt', 'exp', 'ln', 'log', 'power', 'mod']
TEXT_FUNCTIONS = ['lower', 'upper', 'initcap', 'length', 'trim', 'ltrim', 'rtrim', 'replace', 'substring', 'left', 'right', 'reverse', 'concat', 'repeat']
AGGREGATE_FUNCTIONS = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE', 'BOOL_AND', 'BOOL_OR', 'STRING_AGG', 'ARRAY_AGG']
WINDOW_FUNCTIONS = ['row_number', 'rank', 'dense_rank', 'percent_rank', 'cume_dist', 'ntile', 'lag', 'lead', 'first_value', 'last_value', 'nth_value']

def _gen_function_call(ctx, return_type='INT'):
    """Generate a function call for a given return type."""
    depth = _inc_depth(ctx)
    try:
        if depth > MAX_DEPTH:
            return _gen_constant(ctx, return_type)

        return_type_upper = return_type.upper()

        if return_type_upper in ['INT', 'INTEGER', 'BIGINT', 'NUMERIC', 'REAL']:
            func = ctx.rng.choice(NUMERIC_FUNCTIONS)
            arg = _gen_expression(ctx, 'NUMERIC')
            if func == 'power':
                arg2 = _gen_expression(ctx, 'NUMERIC')
                return f"{func}({arg}, {arg2})"
            elif func == 'mod':
                arg2 = _gen_expression(ctx, 'INT')
                return f"{func}({arg}, {arg2})"
            return f"{func}({arg})"
        elif return_type_upper in ['TEXT', 'VARCHAR']:
            func = ctx.rng.choice(TEXT_FUNCTIONS)
            arg = _gen_expression(ctx, 'TEXT')
            if func == 'replace':
                return f"replace({arg}, {_random_text(ctx)}, {_random_text(ctx)})"
            elif func == 'substring':
                return f"substring({arg} from {ctx.rng.randint(1, 10)} for {ctx.rng.randint(1, 10)})"
            elif func in ['left', 'right']:
                return f"{func}({arg}, {ctx.rng.randint(1, 20)})"
            elif func == 'repeat':
                return f"repeat({arg}, {ctx.rng.randint(1, 5)})"
            elif func == 'concat':
                arg2 = _gen_expression(ctx, 'TEXT')
                return f"concat({arg}, {arg2})"
            return f"{func}({arg})"
        else:
            return _gen_constant(ctx, return_type)
    finally:
        _dec_depth(ctx)

def _gen_aggregate(ctx, return_type=None):
    """Generate an aggregate function call."""
    depth = _inc_depth(ctx)
    try:
        agg = ctx.rng.choice(AGGREGATE_FUNCTIONS)

        if agg == 'COUNT':
            if ctx.rng.random() < 0.3:
                return 'COUNT(*)'
            distinct = 'DISTINCT ' if ctx.rng.random() < 0.2 else ''
            arg = _gen_expression(ctx)
            return f"COUNT({distinct}{arg})"
        elif agg == 'STRING_AGG':
            expr = _gen_expression(ctx, 'TEXT')
            return f"STRING_AGG({expr}, ',')"
        elif agg == 'ARRAY_AGG':
            distinct = 'DISTINCT ' if ctx.rng.random() < 0.2 else ''
            expr = _gen_expression(ctx)
            return f"ARRAY_AGG({distinct}{expr})"
        elif agg in ['BOOL_AND', 'BOOL_OR']:
            expr = _gen_expression(ctx, 'BOOLEAN')
            return f"{agg}({expr})"
        else:
            distinct = 'DISTINCT ' if ctx.rng.random() < 0.1 else ''
            arg = _gen_expression(ctx, 'NUMERIC')
            return f"{agg}({distinct}{arg})"
    finally:
        _dec_depth(ctx)

def _gen_window_function(ctx):
    """Generate a window function call (SQLsmith style)."""
    func = ctx.rng.choice(WINDOW_FUNCTIONS)

    # Generate function arguments
    if func in ['row_number', 'rank', 'dense_rank', 'percent_rank', 'cume_dist']:
        args = ''
    elif func == 'ntile':
        args = str(ctx.rng.randint(1, 10))
    elif func in ['lag', 'lead']:
        expr = _gen_expression(ctx)
        offset = ctx.rng.randint(1, 5)
        default = _gen_constant(ctx)
        args = f"{expr}, {offset}, {default}"
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
    if ctx.rng.random() < 0.6:
        num_cols = ctx.rng.randint(1, 3)
        partition_cols = [_pick_column(ctx) for _ in range(num_cols)]
        over_parts.append(f"PARTITION BY {', '.join(partition_cols)}")

    # ORDER BY
    if ctx.rng.random() < 0.8 or func in ['row_number', 'rank', 'dense_rank', 'lag', 'lead', 'first_value', 'last_value', 'nth_value']:
        col = _pick_column(ctx)
        direction = ctx.rng.choice(['ASC', 'DESC'])
        nulls = ctx.rng.choice(['', ' NULLS FIRST', ' NULLS LAST'])
        over_parts.append(f"ORDER BY {col} {direction}{nulls}")

    # Frame clause
    if func in ['first_value', 'last_value', 'nth_value'] and ctx.rng.random() < 0.4:
        frame_type = ctx.rng.choice(['ROWS', 'RANGE', 'GROUPS'])
        frame_start = ctx.rng.choice(['UNBOUNDED PRECEDING', 'CURRENT ROW', f'{ctx.rng.randint(1, 5)} PRECEDING'])
        frame_end = ctx.rng.choice(['CURRENT ROW', 'UNBOUNDED FOLLOWING', f'{ctx.rng.randint(1, 5)} FOLLOWING'])
        over_parts.append(f"{frame_type} BETWEEN {frame_start} AND {frame_end}")

    over_clause = ' '.join(over_parts)
    return f"{func}({args}) OVER ({over_clause})"

# =============================================================================
# Expression Generator (SQLsmith style - type-aware)
# =============================================================================

def _gen_expression(ctx, data_type=None):
    """Generate a type-aware expression."""
    depth = _inc_depth(ctx)
    try:
        if depth > MAX_DEPTH or ctx.rng.random() < 0.35:
            if ctx.rng.random() < 0.6 and ctx.tables:
                return _pick_column(ctx, data_type)
            return _gen_constant(ctx, data_type)

        if data_type is None:
            data_type = ctx.rng.choice(['INT', 'TEXT', 'BOOLEAN'])

        data_type_upper = data_type.upper()

        if data_type_upper == 'BOOLEAN':
            return _gen_bool_expression(ctx)
        elif data_type_upper in ['INT', 'INTEGER', 'BIGINT', 'NUMERIC', 'REAL']:
            return _gen_numeric_expression(ctx)
        elif data_type_upper in ['TEXT', 'VARCHAR']:
            return _gen_text_expression(ctx)
        else:
            return _gen_constant(ctx, data_type)
    finally:
        _dec_depth(ctx)

def _gen_bool_expression(ctx):
    """Generate a boolean expression."""
    choice_val = ctx.rng.random()

    if choice_val < 0.15:
        # Postfix (IS NULL, etc.)
        expr = _gen_expression(ctx)
        return f"({expr}) {ctx.rng.choice(POSTFIX_OPS)}"
    elif choice_val < 0.25:
        # NOT
        expr = _gen_expression(ctx, 'BOOLEAN')
        return f"NOT ({expr})"
    elif choice_val < 0.45:
        # Logical (AND/OR)
        left = _gen_expression(ctx, 'BOOLEAN')
        right = _gen_expression(ctx, 'BOOLEAN')
        op = ctx.rng.choice(LOGICAL_OPS)
        return f"({left}) {op} ({right})"
    elif choice_val < 0.65:
        # Comparison
        comp_type = ctx.rng.choice(['INT', 'TEXT'])
        left = _gen_expression(ctx, comp_type)
        right = _gen_expression(ctx, comp_type)
        op = ctx.rng.choice(COMPARISON_OPS)
        return f"({left}) {op} ({right})"
    elif choice_val < 0.75:
        # LIKE/ILIKE
        left = _gen_expression(ctx, 'TEXT')
        pattern = _random_text(ctx)
        op = ctx.rng.choice(['LIKE', 'ILIKE', 'NOT LIKE', 'NOT ILIKE'])
        return f"({left}) {op} {pattern}"
    elif choice_val < 0.85:
        # BETWEEN
        expr = _gen_expression(ctx, 'INT')
        lower = _gen_expression(ctx, 'INT')
        upper = _gen_expression(ctx, 'INT')
        not_between = 'NOT ' if ctx.rng.random() < 0.2 else ''
        return f"({expr}) {not_between}BETWEEN ({lower}) AND ({upper})"
    elif choice_val < 0.92:
        # IN
        expr = _gen_expression(ctx, 'INT')
        num_values = ctx.rng.randint(1, 5)
        values = ', '.join(_gen_constant(ctx, 'INT') for _ in range(num_values))
        not_in = 'NOT ' if ctx.rng.random() < 0.3 else ''
        return f"({expr}) {not_in}IN ({values})"
    elif choice_val < 0.96:
        # EXISTS (subquery)
        subquery = _gen_subquery(ctx)
        not_exists = 'NOT ' if ctx.rng.random() < 0.3 else ''
        return f"{not_exists}EXISTS ({subquery})"
    else:
        return _gen_constant(ctx, 'BOOLEAN')

def _gen_numeric_expression(ctx):
    """Generate a numeric expression."""
    choice_val = ctx.rng.random()

    if choice_val < 0.35:
        # Arithmetic
        left = _gen_expression(ctx, 'INT')
        right = _gen_expression(ctx, 'INT')
        op = ctx.rng.choice(ARITHMETIC_OPS)
        return f"({left}) {op} ({right})"
    elif choice_val < 0.55:
        # Unary
        expr = _gen_expression(ctx, 'INT')
        op = ctx.rng.choice(['+', '-'])
        return f"{op}({expr})"
    elif choice_val < 0.70:
        # CASE expression
        cond = _gen_expression(ctx, 'BOOLEAN')
        then_val = _gen_expression(ctx, 'INT')
        else_val = _gen_expression(ctx, 'INT')
        return f"CASE WHEN {cond} THEN {then_val} ELSE {else_val} END"
    elif choice_val < 0.85:
        # Function call
        return _gen_function_call(ctx, 'INT')
    elif choice_val < 0.92:
        # COALESCE
        args = ', '.join(_gen_expression(ctx, 'INT') for _ in range(ctx.rng.randint(2, 4)))
        return f"COALESCE({args})"
    else:
        # Scalar subquery
        return f"({_gen_scalar_subquery(ctx)})"

def _gen_text_expression(ctx):
    """Generate a text expression."""
    choice_val = ctx.rng.random()

    if choice_val < 0.3:
        # Concatenation
        left = _gen_expression(ctx, 'TEXT')
        right = _gen_expression(ctx, 'TEXT')
        return f"({left}) || ({right})"
    elif choice_val < 0.5:
        # Function call
        return _gen_function_call(ctx, 'TEXT')
    elif choice_val < 0.65:
        # CASE expression
        cond = _gen_expression(ctx, 'BOOLEAN')
        then_val = _gen_expression(ctx, 'TEXT')
        else_val = _gen_expression(ctx, 'TEXT')
        return f"CASE WHEN {cond} THEN {then_val} ELSE {else_val} END"
    elif choice_val < 0.80:
        # COALESCE
        args = ', '.join(_gen_expression(ctx, 'TEXT') for _ in range(ctx.rng.randint(2, 3)))
        return f"COALESCE({args})"
    else:
        return _gen_constant(ctx, 'TEXT')

# =============================================================================
# Subquery Generation (SQLsmith specialty)
# =============================================================================

def _gen_subquery(ctx):
    """Generate a subquery for use in expressions."""
    depth = _inc_depth(ctx)
    try:
        if depth > MAX_DEPTH - 1:
            t_name = _pick_table(ctx)
            col = _pick_column(ctx)
            return f"SELECT {col} FROM {t_name} LIMIT 1"

        t_name = _pick_table(ctx)
        col = _pick_column(ctx)

        # Simple subquery
        where_clause = ''
        if ctx.rng.random() < 0.5:
            where_clause = f" WHERE {_gen_expression(ctx, 'BOOLEAN')}"

        return f"SELECT {col} FROM {t_name}{where_clause}"
    finally:
        _dec_depth(ctx)

def _gen_scalar_subquery(ctx):
    """Generate a scalar subquery (returns single value)."""
    depth = _inc_depth(ctx)
    try:
        t_name = _pick_table(ctx)
        col = _pick_column(ctx, 'INT')

        agg = ctx.rng.choice(['COUNT(*)', f'MAX({col})', f'MIN({col})', f'SUM({col})', f'AVG({col})'])

        where_clause = ''
        if ctx.rng.random() < 0.4:
            where_clause = f" WHERE {_gen_expression(ctx, 'BOOLEAN')}"

        return f"SELECT {agg} FROM {t_name}{where_clause}"
    finally:
        _dec_depth(ctx)

# =============================================================================
# JOIN Generation (SQLsmith style)
# =============================================================================

JOIN_TYPES = ['INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL OUTER JOIN', 'CROSS JOIN']

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
    alias = f"j{ctx.rng.randint(1, 100)}"

    if join_type == 'CROSS JOIN':
        return f" {join_type} {join_table} AS {alias}"

    # Generate ON condition
    on_condition = _gen_expression(ctx, 'BOOLEAN')
    return f" {join_type} {join_table} AS {alias} ON ({on_condition})"

def _gen_lateral_join(ctx):
    """Generate a LATERAL subquery join (SQLsmith feature)."""
    if not ctx.tables:
        return ""

    subquery = _gen_subquery(ctx)
    alias = f"lat{ctx.rng.randint(1, 100)}"

    join_type = ctx.rng.choice(['CROSS JOIN LATERAL', 'LEFT JOIN LATERAL'])

    if 'LEFT' in join_type:
        return f" {join_type} ({subquery}) AS {alias} ON TRUE"
    return f" {join_type} ({subquery}) AS {alias}"

# =============================================================================
# CTE Generation (SQLsmith feature)
# =============================================================================

def _gen_cte(ctx):
    """Generate a CTE (Common Table Expression)."""
    cte_name = f"cte{ctx.rng.randint(1, 100)}"
    ctx.state.setdefault('cte_names', []).append(cte_name)

    t_name = _pick_table(ctx)
    cols = _gen_select_columns(ctx)
    where_clause = _gen_where_clause(ctx)

    return f"{cte_name} AS (SELECT {cols} FROM {t_name} {where_clause})"

def _gen_with_clause(ctx):
    """Generate a WITH clause with one or more CTEs."""
    if ctx.rng.random() < 0.7:
        return ""

    ctx.state['cte_names'] = []
    num_ctes = ctx.rng.randint(1, 3)
    ctes = [_gen_cte(ctx) for _ in range(num_ctes)]

    recursive = 'RECURSIVE ' if ctx.rng.random() < 0.1 else ''
    return f"WITH {recursive}{', '.join(ctes)} "

# =============================================================================
# TABLESAMPLE (SQLsmith feature)
# =============================================================================

def _gen_tablesample(ctx):
    """Generate a TABLESAMPLE clause."""
    if ctx.rng.random() < 0.9:
        return ""

    method = ctx.rng.choice(['BERNOULLI', 'SYSTEM'])
    percentage = ctx.rng.randint(1, 100)
    return f" TABLESAMPLE {method}({percentage})"

# =============================================================================
# SELECT Generation
# =============================================================================

def _gen_select_columns(ctx):
    """Generate SELECT column list."""
    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        return "*"

    if ctx.rng.random() < 0.1:
        return "*"

    table = ctx.tables[t_name]
    cols = list(table.columns.values())
    if not cols:
        return "*"

    num_cols = ctx.rng.randint(1, min(5, len(cols)))
    selected = ctx.rng.sample([c.name for c in cols], num_cols)

    result = []
    for col in selected:
        if ctx.rng.random() < 0.2:
            # Wrap in expression
            expr = _gen_expression(ctx)
            alias = f"col_{ctx.rng.randint(1, 100)}"
            result.append(f"({expr}) AS {alias}")
        else:
            result.append(col)

    # Sometimes add aggregate
    if ctx.rng.random() < 0.15:
        result.append(f"{_gen_aggregate(ctx)} AS agg_result")

    # Sometimes add window function
    if ctx.rng.random() < 0.1:
        result.append(f"{_gen_window_function(ctx)} AS win_result")

    return ', '.join(result)

def _gen_where_clause(ctx):
    """Generate WHERE clause."""
    if ctx.rng.random() < 0.3:
        return ""
    return f"WHERE {_gen_expression(ctx, 'BOOLEAN')}"

def _gen_group_by(ctx):
    """Generate GROUP BY clause."""
    if ctx.rng.random() < 0.75:
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

    # GROUPING SETS / CUBE / ROLLUP (SQLsmith feature)
    if ctx.rng.random() < 0.1:
        grouping_type = ctx.rng.choice(['ROLLUP', 'CUBE'])
        return f"GROUP BY {grouping_type}({', '.join(group_cols)})"

    return f"GROUP BY {', '.join(group_cols)}"

def _gen_having(ctx):
    """Generate HAVING clause."""
    if ctx.rng.random() < 0.85:
        return ""
    return f"HAVING {_gen_expression(ctx, 'BOOLEAN')}"

def _gen_order_by(ctx):
    """Generate ORDER BY clause."""
    if ctx.rng.random() < 0.5:
        return ""

    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        return "ORDER BY 1"

    table = ctx.tables[t_name]
    cols = [c.name for c in table.columns.values()]
    if not cols:
        return "ORDER BY 1"

    num_cols = ctx.rng.randint(1, min(3, len(cols)))
    order_cols = ctx.rng.sample(cols, num_cols)

    order_parts = []
    for col in order_cols:
        direction = ctx.rng.choice(['ASC', 'DESC', ''])
        nulls = ctx.rng.choice(['', ' NULLS FIRST', ' NULLS LAST'])
        order_parts.append(f"{col} {direction}{nulls}".strip())

    return f"ORDER BY {', '.join(order_parts)}"

def _gen_limit(ctx):
    """Generate LIMIT/OFFSET clause."""
    if ctx.rng.random() < 0.6:
        return ""

    limit = ctx.rng.randint(1, 100)
    offset = ""
    if ctx.rng.random() < 0.3:
        offset = f" OFFSET {ctx.rng.randint(0, 50)}"

    return f"LIMIT {limit}{offset}"

def _gen_for_update(ctx):
    """Generate FOR UPDATE/SHARE clause (SQLsmith feature)."""
    if ctx.rng.random() < 0.92:
        return ""

    lock_mode = ctx.rng.choice(['FOR UPDATE', 'FOR NO KEY UPDATE', 'FOR SHARE', 'FOR KEY SHARE'])
    nowait = ' NOWAIT' if ctx.rng.random() < 0.2 else ''
    skip_locked = ' SKIP LOCKED' if ctx.rng.random() < 0.1 and not nowait else ''

    return f" {lock_mode}{nowait}{skip_locked}"

def _gen_select_distinct(ctx):
    """Generate SELECT DISTINCT variation."""
    if ctx.rng.random() < 0.85:
        return "SELECT"

    if ctx.rng.random() < 0.8:
        return "SELECT DISTINCT"

    # DISTINCT ON
    col = _pick_column(ctx)
    return f"SELECT DISTINCT ON ({col})"

def _gen_full_select(ctx):
    """Generate a complete SELECT query (SQLsmith style)."""
    ctx.state['depth'] = 0

    with_clause = _gen_with_clause(ctx)
    t_name = _pick_table(ctx)

    select_type = _gen_select_distinct(ctx)
    columns = _gen_select_columns(ctx)
    tablesample = _gen_tablesample(ctx)
    join_clause = ''
    if ctx.rng.random() < 0.25:
        join_clause = _gen_join_clause(ctx)
    elif ctx.rng.random() < 0.1:
        join_clause = _gen_lateral_join(ctx)

    where = _gen_where_clause(ctx)
    group_by = _gen_group_by(ctx)
    having = _gen_having(ctx) if group_by else ''
    order_by = _gen_order_by(ctx)
    limit = _gen_limit(ctx)
    for_update = _gen_for_update(ctx)

    query = f"{with_clause}{select_type} {columns} FROM {t_name}{tablesample}{join_clause}"
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
    if for_update:
        query += for_update

    return query

# =============================================================================
# Set Operations (UNION, INTERSECT, EXCEPT)
# =============================================================================

def _gen_set_operation(ctx):
    """Generate a set operation (UNION, INTERSECT, EXCEPT)."""
    if ctx.rng.random() < 0.85:
        return _gen_full_select(ctx)

    left = _gen_full_select(ctx)
    op = ctx.rng.choice(['UNION', 'UNION ALL', 'INTERSECT', 'INTERSECT ALL', 'EXCEPT', 'EXCEPT ALL'])
    right = _gen_full_select(ctx)

    return f"({left}) {op} ({right})"

# =============================================================================
# DML Statements with RETURNING (SQLsmith feature)
# =============================================================================

def _gen_returning_clause(ctx):
    """Generate a RETURNING clause."""
    if ctx.rng.random() < 0.7:
        return ""

    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        return " RETURNING *"

    table = ctx.tables[t_name]
    cols = [c.name for c in table.columns.values()]
    if not cols:
        return " RETURNING *"

    if ctx.rng.random() < 0.3:
        return " RETURNING *"

    num_cols = ctx.rng.randint(1, min(3, len(cols)))
    return f" RETURNING {', '.join(ctx.rng.sample(cols, num_cols))}"

def _gen_insert(ctx):
    """Generate an INSERT statement."""
    t_name = _pick_table(ctx)
    if t_name not in ctx.tables:
        return f"INSERT INTO {t_name} (c0) VALUES (1)"

    table = ctx.tables[t_name]
    cols = [c.name for c in table.columns.values() if not (c.is_primary_key and 'generated' in c.data_type.lower())]

    if not cols:
        cols = [c.name for c in table.columns.values()]
    if not cols:
        return f"INSERT INTO {t_name} DEFAULT VALUES{_gen_returning_clause(ctx)}"

    num_cols = ctx.rng.randint(1, len(cols))
    selected_cols = ctx.rng.sample(cols, num_cols)

    # Generate values
    num_rows = ctx.rng.randint(1, 3)
    all_values = []
    for _ in range(num_rows):
        row_values = [ctx.get_column_value(t_name, c) for c in selected_cols]
        all_values.append(f"({', '.join(row_values)})")

    values_str = ', '.join(all_values)
    cols_str = ', '.join(selected_cols)

    # ON CONFLICT (upsert - SQLsmith feature)
    on_conflict = ''
    if ctx.rng.random() < 0.2:
        conflict_action = ctx.rng.choice(['DO NOTHING', f'DO UPDATE SET {selected_cols[0]} = EXCLUDED.{selected_cols[0]}'])
        on_conflict = f" ON CONFLICT ({selected_cols[0]}) {conflict_action}"

    returning = _gen_returning_clause(ctx)

    return f"INSERT INTO {t_name} ({cols_str}) VALUES {values_str}{on_conflict}{returning}"

def _gen_update(ctx):
    """Generate an UPDATE statement with optional RETURNING."""
    t_name = _pick_table(ctx)
    if t_name not in ctx.tables:
        return f"UPDATE {t_name} SET c0 = 1"

    table = ctx.tables[t_name]
    cols = [c.name for c in table.columns.values() if not c.is_primary_key]

    if not cols:
        cols = [c.name for c in table.columns.values()]
    if not cols:
        return f"UPDATE {t_name} SET c0 = 1"

    num_cols = ctx.rng.randint(1, min(3, len(cols)))
    selected_cols = ctx.rng.sample(cols, num_cols)

    set_parts = []
    for col in selected_cols:
        val = ctx.get_column_value(t_name, col)
        set_parts.append(f"{col} = {val}")

    set_clause = ', '.join(set_parts)
    where_clause = _gen_where_clause(ctx)
    returning = _gen_returning_clause(ctx)

    return f"UPDATE {t_name} SET {set_clause} {where_clause}{returning}".strip()

def _gen_delete(ctx):
    """Generate a DELETE statement with optional RETURNING."""
    t_name = _pick_table(ctx)
    where_clause = _gen_where_clause(ctx)
    returning = _gen_returning_clause(ctx)

    # Using clause
    using_clause = ''
    if ctx.rng.random() < 0.1 and len(ctx.tables) > 1:
        other_tables = [t for t in ctx.tables.keys() if t != t_name]
        if other_tables:
            using_clause = f" USING {ctx.rng.choice(other_tables)}"

    return f"DELETE FROM {t_name}{using_clause} {where_clause}{returning}".strip()

# =============================================================================
# MERGE Statement (SQLsmith feature)
# =============================================================================

def _gen_merge(ctx):
    """Generate a MERGE statement (PostgreSQL 15+ / YSQL)."""
    if len(ctx.tables) < 2:
        return _gen_update(ctx)

    tables_list = list(ctx.tables.keys())
    target_table = tables_list[0]
    source_table = tables_list[1] if len(tables_list) > 1 else tables_list[0]

    ctx.state['table'] = target_table

    # ON condition
    on_condition = _gen_expression(ctx, 'BOOLEAN')

    # WHEN MATCHED
    matched_clause = ''
    if ctx.rng.random() < 0.7:
        if ctx.rng.random() < 0.3:
            matched_clause = "WHEN MATCHED THEN DELETE"
        else:
            col = _pick_column(ctx)
            val = _gen_constant(ctx)
            matched_clause = f"WHEN MATCHED THEN UPDATE SET {col} = {val}"

    # WHEN NOT MATCHED
    not_matched_clause = ''
    if ctx.rng.random() < 0.7:
        table = ctx.tables.get(target_table)
        if table:
            cols = [c.name for c in table.columns.values()][:3]
            if cols:
                vals = [_gen_constant(ctx) for _ in cols]
                not_matched_clause = f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join(vals)})"

    if not matched_clause and not not_matched_clause:
        matched_clause = "WHEN MATCHED THEN DELETE"

    return f"MERGE INTO {target_table} USING {source_table} ON {on_condition} {matched_clause} {not_matched_clause}".strip()

# =============================================================================
# DDL Statements
# =============================================================================

def _gen_column_definition(ctx):
    """Generate a column definition for CREATE TABLE."""
    col_name = f"c{ctx.state.get('col_idx', 0)}"
    ctx.state['col_idx'] = ctx.state.get('col_idx', 0) + 1

    data_type = ctx.rng.choice(DATA_TYPES)

    # Add size for certain types
    if data_type == 'VARCHAR':
        size = ctx.rng.randint(1, 255)
        data_type = f"VARCHAR({size})"
    elif data_type == 'NUMERIC':
        precision = ctx.rng.randint(1, 38)
        scale = ctx.rng.randint(0, min(precision, 10))
        data_type = f"NUMERIC({precision},{scale})"

    # Constraints
    constraints = []
    if ctx.rng.random() < 0.3:
        constraints.append(ctx.rng.choice(['NULL', 'NOT NULL']))
    if ctx.rng.random() < 0.1:
        constraints.append('UNIQUE')
    if ctx.rng.random() < 0.1 and not ctx.state.get('has_pk'):
        constraints.append('PRIMARY KEY')
        ctx.state['has_pk'] = True
    if ctx.rng.random() < 0.15:
        default_val = _gen_constant(ctx, data_type.split('(')[0])
        constraints.append(f"DEFAULT {default_val}")

    return f"{col_name} {data_type} {' '.join(constraints)}".strip()

def _gen_create_table(ctx):
    """Generate a CREATE TABLE statement."""
    ctx.state['col_idx'] = 0
    ctx.state['has_pk'] = False

    if_not_exists = 'IF NOT EXISTS ' if ctx.rng.random() < 0.3 else ''
    table_name = f"t{ctx.rng.randint(0, 100)}"

    num_cols = ctx.rng.randint(1, 6)
    columns = [_gen_column_definition(ctx) for _ in range(num_cols)]

    # YSQL-specific: SPLIT INTO TABLETS
    split_clause = ''
    if ctx.rng.random() < 0.2:
        num_tablets = ctx.rng.randint(1, 10)
        split_clause = f" SPLIT INTO {num_tablets} TABLETS"

    return f"CREATE TABLE {if_not_exists}{table_name} ({', '.join(columns)}){split_clause}"

def _gen_create_index(ctx):
    """Generate a CREATE INDEX statement."""
    unique = 'UNIQUE ' if ctx.rng.random() < 0.2 else ''
    t_name = _pick_table(ctx)
    index_name = f"idx_{t_name}_{ctx.rng.randint(1, 100)}"

    # Index method
    method = ''
    if ctx.rng.random() < 0.3:
        method = f" USING {ctx.rng.choice(['BTREE', 'HASH', 'GIN', 'GIST'])}"

    col = _pick_column(ctx)
    order = ctx.rng.choice(['', ' ASC', ' DESC'])
    nulls = ctx.rng.choice(['', ' NULLS FIRST', ' NULLS LAST'])

    # Partial index
    where_clause = ''
    if ctx.rng.random() < 0.15:
        where_clause = f" WHERE {_gen_expression(ctx, 'BOOLEAN')}"

    return f"CREATE {unique}INDEX {index_name} ON {t_name}{method} ({col}{order}{nulls}){where_clause}"

# =============================================================================
# Grammar Rules
# =============================================================================

# Data Type Rules
g.rule("data_type", Lambda(lambda ctx: ctx.rng.choice(DATA_TYPES)))

# Constant Rules
g.rule("constant", Lambda(lambda ctx: _gen_constant(ctx)))
g.rule("int_constant", Lambda(lambda ctx: _gen_constant(ctx, 'INT')))
g.rule("text_constant", Lambda(lambda ctx: _gen_constant(ctx, 'TEXT')))
g.rule("boolean_constant", Lambda(lambda ctx: _gen_constant(ctx, 'BOOLEAN')))

# Expression Rules
g.rule("expression", Lambda(lambda ctx: _gen_expression(ctx)))
g.rule("boolean_expression", Lambda(lambda ctx: _gen_expression(ctx, 'BOOLEAN')))
g.rule("numeric_expression", Lambda(lambda ctx: _gen_expression(ctx, 'INT')))
g.rule("text_expression", Lambda(lambda ctx: _gen_expression(ctx, 'TEXT')))

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
g.rule("cte", Lambda(_gen_cte))

# Statement Rules
g.rule("select", Lambda(_gen_full_select))
g.rule("select_set_op", Lambda(_gen_set_operation))
g.rule("insert", Lambda(_gen_insert))
g.rule("update", Lambda(_gen_update))
g.rule("delete", Lambda(_gen_delete))
g.rule("merge", Lambda(_gen_merge))
g.rule("create_table", Lambda(_gen_create_table))
g.rule("create_index", Lambda(_gen_create_index))

# Main Entry Points
g.rule("query", choice(
    Lambda(_gen_full_select),
    Lambda(_gen_full_select),
    Lambda(_gen_set_operation),
    Lambda(_gen_insert),
    Lambda(_gen_update),
    Lambda(_gen_delete),
    Lambda(_gen_merge),
))

g.rule("ddl", choice(
    Lambda(_gen_create_table),
    Lambda(_gen_create_index),
))

g.rule("dml", choice(
    Lambda(_gen_insert),
    Lambda(_gen_update),
    Lambda(_gen_delete),
    Lambda(_gen_merge),
))

# Export the grammar
grammar = g
