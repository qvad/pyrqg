"""
Centralized PostgreSQL Type Definitions and Classifications.
Removes duplication of type groups across Grammars, Schema, and Generators.
"""

from typing import Set

# Base Categories
NUMERIC_TYPES: Set[str] = {
    'integer', 'int', 'smallint', 'bigint', 'serial', 'bigserial',
    'decimal', 'numeric', 'real', 'double precision', 'float', 'money'
}

STRING_TYPES: Set[str] = {
    'character varying', 'varchar', 'character', 'char', 'text', 'name', 'bpchar'
}

DATETIME_TYPES: Set[str] = {
    'timestamp', 'timestamp without time zone', 'timestamptz', 'timestamp with time zone',
    'date', 'time', 'time without time zone', 'timetz', 'time with time zone', 'interval'
}

BOOLEAN_TYPES: Set[str] = {'boolean', 'bool'}

JSON_TYPES: Set[str] = {'json', 'jsonb'}

NET_TYPES: Set[str] = {'inet', 'cidr', 'macaddr', 'macaddr8'}

GEO_TYPES: Set[str] = {'point', 'line', 'lseg', 'box', 'path', 'polygon', 'circle'}

# Helper to check types loosely (substring matching for types like "varchar(50)")
def is_numeric(dtype: str) -> bool:
    """Check if a data type is numeric (handles parameterized types like NUMERIC(10,2))."""
    d = dtype.lower().split('(')[0].strip()
    return d in NUMERIC_TYPES

def is_string(dtype: str) -> bool:
    d = dtype.lower().split('(')[0].strip()
    return d in STRING_TYPES

def is_datetime(dtype: str) -> bool:
    d = dtype.lower().split('(')[0].strip()
    return d in DATETIME_TYPES or 'timestamp' in d or 'date' in d or 'time' in d

def is_boolean(dtype: str) -> bool:
    d = dtype.lower().split('(')[0].strip()
    return d in BOOLEAN_TYPES

def is_json(dtype: str) -> bool:
    d = dtype.lower().split('(')[0].strip()
    return d in JSON_TYPES


def is_net(dtype: str) -> bool:
    """Check if a data type is a network type."""
    d = dtype.lower().split('(')[0].strip()
    return d in NET_TYPES


def is_geo(dtype: str) -> bool:
    """Check if a data type is a geometric type."""
    d = dtype.lower().split('(')[0].strip()
    return d in GEO_TYPES


# Type category mappings for flexible type matching
TYPE_CATEGORIES = {
    'numeric': NUMERIC_TYPES | {'int', 'integer', 'float4', 'float8', 'serial', 'bigserial'},
    'int': {'integer', 'int', 'smallint', 'bigint', 'serial', 'bigserial', 'tinyint', 'int4', 'int8'},
    'text': STRING_TYPES | {'string', 'varchar', 'character varying'},
    'boolean': BOOLEAN_TYPES,
    'temporal': DATETIME_TYPES | {'date', 'timestamp', 'timestamptz', 'time', 'timetz'},
    'json': JSON_TYPES,
    'net': NET_TYPES,
    'geo': GEO_TYPES,
    'range': {'int4range', 'int8range', 'numrange', 'tsrange', 'daterange'},
    'bit': {'bit', 'bit varying', 'varbit'},
    'money': {'money'},
    'bytea': {'bytea'},
}


def matches_type_category(col_type: str, target_type: str) -> bool:
    """Check if a column type matches a target type or type category.

    This is a consolidated function that replaces _matches_type() duplicated
    across multiple grammar files.

    Args:
        col_type: The column's actual data type (e.g., 'integer', 'varchar(50)').
        target_type: The target type or category (e.g., 'INT', 'numeric', 'TEXT').

    Returns:
        True if the column type matches the target type/category.

    Examples:
        >>> matches_type_category('integer', 'INT')
        True
        >>> matches_type_category('varchar(50)', 'TEXT')
        True
        >>> matches_type_category('bigint', 'numeric')
        True
    """
    col_lower = col_type.lower().split('(')[0].strip()
    target_lower = target_type.lower()

    # Direct match
    if col_lower == target_lower:
        return True

    # Check if target is a category name
    if target_lower in TYPE_CATEGORIES:
        return col_lower in TYPE_CATEGORIES[target_lower]

    # Check if target type belongs to a category and col_type is in same category
    for category, types in TYPE_CATEGORIES.items():
        if target_lower in types:
            return col_lower in types

    # Check using existing helper functions
    if target_lower in ('int', 'integer', 'bigint', 'smallint', 'numeric', 'real', 'float', 'decimal'):
        return is_numeric(col_type)
    if target_lower in ('text', 'varchar', 'char', 'character', 'string'):
        return is_string(col_type)
    if target_lower in ('boolean', 'bool'):
        return is_boolean(col_type)
    if target_lower in ('date', 'timestamp', 'timestamptz', 'time', 'timetz', 'interval'):
        return is_datetime(col_type)
    if target_lower in ('json', 'jsonb'):
        return is_json(col_type)

    return False
