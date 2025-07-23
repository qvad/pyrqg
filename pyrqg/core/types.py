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
