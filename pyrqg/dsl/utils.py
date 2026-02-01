"""
Shared Python utilities for use within Grammar Lambdas.
Reduces logic duplication for common tasks like picking tables/columns.
"""
import uuid
from typing import Optional, List, Any, Callable


def random_id() -> str:
    """Generate a random 8-char identifier."""
    return str(uuid.uuid4()).replace('-', '')[:8]


def pick_table(ctx: Any) -> Optional[str]:
    """Select a random table name from context."""
    if not ctx.tables:
        return None
    return ctx.rng.choice(list(ctx.tables.keys()))


def pick_table_and_store(ctx: Any, fallback: str = "t0") -> str:
    """Pick a random table, store it in context state, and return its name.

    This is the consolidated version of _pick_table() from multiple grammar files.
    Stores the selected table in ctx.state['table'] for subsequent column lookups.
    """
    if ctx.tables:
        t = ctx.rng.choice(list(ctx.tables.keys()))
        ctx.state['table'] = t
        ctx.state.setdefault('available_tables', []).append(t)
        return t
    return fallback


def pick_column(ctx: Any, data_type: Optional[str] = None, is_pk: Optional[bool] = None,
                fallback: str = "c0") -> str:
    """Pick a column from the current table in context state.

    This is the consolidated version of _pick_column() from multiple grammar files.

    Args:
        ctx: The grammar context with tables and state.
        data_type: Optional type filter (e.g., 'INT', 'TEXT', 'BOOLEAN').
        is_pk: Optional primary key filter (True = only PK, False = only non-PK).
        fallback: Return value if no suitable column is found.

    Returns:
        Column name string.
    """
    from pyrqg.core.types import matches_type_category

    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        return fallback

    table = ctx.tables[t_name]
    cols = list(table.columns.values())

    # Filter by primary key status if specified
    if is_pk is not None:
        cols = [c for c in cols if c.is_primary_key == is_pk]

    # Filter by data type if specified
    if data_type and cols:
        filtered = [c for c in cols if matches_type_category(c.data_type, data_type)]
        if filtered:
            cols = filtered

    if not cols:
        # Fall back to all columns
        cols = list(table.columns.values())

    if not cols:
        return fallback

    return ctx.rng.choice([c.name for c in cols])


def get_columns(ctx: Any, table_name: str) -> List[str]:
    """Get column names for a table."""
    if not table_name or table_name not in ctx.tables:
        return []
    return ctx.tables[table_name].get_column_names()


# =============================================================================
# Random Value Generators
# =============================================================================

def random_int(ctx: Any, min_val: int = -1000000, max_val: int = 1000000) -> str:
    """Generate a random integer constant."""
    return str(ctx.rng.randint(min_val, max_val))


def random_bigint(ctx: Any) -> str:
    """Generate a random bigint constant (scaled down to avoid overflow)."""
    return str(ctx.rng.randint(-9223372036854775808 // 1000000, 9223372036854775807 // 1000000))


def random_boolean(ctx: Any) -> str:
    """Generate a random boolean constant."""
    return ctx.rng.choice(['TRUE', 'FALSE'])


def random_text(ctx: Any, max_length: int = 20) -> str:
    """Generate a random text/varchar constant with proper SQL escaping."""
    length = ctx.rng.randint(0, max_length)
    chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 '
    text = ''.join(ctx.rng.choice(chars) for _ in range(length))
    text = text.replace("'", "''")  # SQL escape
    return f"'{text}'"


def random_numeric(ctx: Any) -> str:
    """Generate a random numeric/decimal constant."""
    whole = ctx.rng.randint(-10000, 10000)
    frac = ctx.rng.randint(0, 999999)
    return f"{whole}.{frac}"


def random_float(ctx: Any) -> str:
    """Generate a random float constant."""
    return str(ctx.rng.uniform(-1000000, 1000000))


def random_date(ctx: Any) -> str:
    """Generate a random date constant in SQL format."""
    year = ctx.rng.randint(1970, 2030)
    month = ctx.rng.randint(1, 12)
    day = ctx.rng.randint(1, 28)
    return f"'{year:04d}-{month:02d}-{day:02d}'"


def random_timestamp(ctx: Any) -> str:
    """Generate a random timestamp constant in SQL format."""
    year = ctx.rng.randint(1970, 2030)
    month = ctx.rng.randint(1, 12)
    day = ctx.rng.randint(1, 28)
    hour = ctx.rng.randint(0, 23)
    minute = ctx.rng.randint(0, 59)
    second = ctx.rng.randint(0, 59)
    return f"'{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}'"


def random_inet(ctx: Any) -> str:
    """Generate a random inet constant (IPv4)."""
    octets = [str(ctx.rng.randint(0, 255)) for _ in range(4)]
    return f"'{'.'.join(octets)}'::inet"


def random_bit(ctx: Any, max_length: int = 32) -> str:
    """Generate a random bit string constant."""
    length = ctx.rng.randint(1, max_length)
    bits = ''.join(ctx.rng.choice(['0', '1']) for _ in range(length))
    return f"B'{bits}'"


def random_money(ctx: Any) -> str:
    """Generate a random money constant."""
    amount = ctx.rng.randint(-10000, 10000)
    cents = ctx.rng.randint(0, 99)
    return f"'{amount}.{cents:02d}'::money"


def random_bytea(ctx: Any, max_length: int = 20) -> str:
    """Generate a random bytea constant (hex encoded)."""
    length = ctx.rng.randint(1, max_length)
    hex_chars = '0123456789abcdef'
    hex_str = ''.join(ctx.rng.choice(hex_chars) for _ in range(length * 2))
    return f"'\\x{hex_str}'::bytea"


def random_range(ctx: Any) -> str:
    """Generate a random int4range constant."""
    lower = ctx.rng.randint(-1000, 1000)
    upper = ctx.rng.randint(lower, lower + 1000)
    lower_inc = ctx.rng.choice(['[', '('])
    upper_inc = ctx.rng.choice([']', ')'])
    return f"'{lower_inc}{lower},{upper}{upper_inc}'::int4range"


# Type-to-generator mapping for convenience
VALUE_GENERATORS = {
    'INT': random_int,
    'INTEGER': random_int,
    'SMALLINT': lambda ctx: random_int(ctx, -32768, 32767),
    'BIGINT': random_bigint,
    'BOOLEAN': random_boolean,
    'BOOL': random_boolean,
    'TEXT': random_text,
    'VARCHAR': random_text,
    'CHAR': random_text,
    'NUMERIC': random_numeric,
    'DECIMAL': random_numeric,
    'REAL': random_float,
    'FLOAT': random_float,
    'DOUBLE PRECISION': random_float,
    'DATE': random_date,
    'TIMESTAMP': random_timestamp,
    'TIMESTAMPTZ': random_timestamp,
    'INET': random_inet,
    'BIT': random_bit,
    'MONEY': random_money,
    'BYTEA': random_bytea,
    'RANGE': random_range,
}


def generate_constant(ctx: Any, data_type: Optional[str] = None,
                      null_probability: float = 0.05) -> str:
    """Generate a type-appropriate SQL constant.

    Args:
        ctx: Grammar context with rng.
        data_type: Target data type (e.g., 'INT', 'TEXT'). If None, random type is chosen.
        null_probability: Probability of returning NULL (default 5%).

    Returns:
        SQL literal string.
    """
    if ctx.rng.random() < null_probability:
        return 'NULL'

    if data_type is None:
        data_type = ctx.rng.choice(['INT', 'TEXT', 'BOOLEAN'])

    data_type_upper = data_type.upper().split('(')[0].strip()
    generator = VALUE_GENERATORS.get(data_type_upper, random_text)
    return generator(ctx)


# =============================================================================
# Expression Depth Control
# =============================================================================

def get_depth(ctx: Any) -> int:
    """Get current expression depth."""
    return ctx.state.get('depth', 0)


def inc_depth(ctx: Any) -> int:
    """Increment and return expression depth."""
    depth = ctx.state.get('depth', 0) + 1
    ctx.state['depth'] = depth
    return depth


def dec_depth(ctx: Any) -> None:
    """Decrement expression depth."""
    ctx.state['depth'] = max(0, ctx.state.get('depth', 0) - 1)
