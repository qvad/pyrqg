#!/usr/bin/env python3
"""
YugabyteDB Data Configuration
Replaces the .zz data definition files

This module defines table schemas and initial data for YugabyteDB testing.
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional

@dataclass
class Field:
    """Database field definition"""
    name: str
    type: str
    indexed: bool = False
    
@dataclass
class Index:
    """Database index definition"""
    name: str
    fields: List[str]
    unique: bool = False

@dataclass 
class Table:
    """Database table definition"""
    name: str
    fields: List[Field]
    rows: int
    indexes: List[Index] = None
    
    def __post_init__(self):
        if self.indexes is None:
            self.indexes = []

# ============================================================================
# transactions_postgres.zz Configuration
# ============================================================================

TRANSACTION_TABLES = [
    Table(
        name='A',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=10
    ),
    Table(
        name='B',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=20
    ),
    Table(
        name='C',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=100
    ),
    Table(
        name='D',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=100
    ),
    Table(
        name='E',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=0
    ),
    # Tables with 10-100 rows
    Table(
        name='AA',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=50  # Midpoint of 10-100
    ),
    Table(
        name='BB',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=20
    ),
    Table(
        name='CC',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=300  # Can exceed 100 as per original
    ),
    Table(
        name='DD',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=10
    ),
    Table(
        name='AAA',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=10
    ),
    Table(
        name='BBB',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=10
    ),
    Table(
        name='CCC',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=100
    ),
    Table(
        name='DDD',
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER')
        ],
        rows=1000
    )
]

# Initial value for all integer fields in transaction tables
TRANSACTION_INITIAL_VALUE = 100

# ============================================================================
# outer_join.zz Configuration  
# ============================================================================

OUTER_JOIN_TABLES = []

# Define the 32 tables A through PP
table_configs = [
    # First set: A-H with specific row counts
    ('A', 0), ('B', 1), ('C', 8), ('D', 100), 
    ('E', 128), ('F', 210), ('G', 220), ('H', 255),
    # Second set: I-P (all empty)
    ('I', 0), ('J', 0), ('K', 0), ('L', 0),
    ('M', 0), ('N', 0), ('O', 0), ('P', 0),
    # Third set: AA-HH with specific row counts
    ('AA', 8), ('BB', 100), ('CC', 128), ('DD', 210),
    ('EE', 220), ('FF', 255), ('GG', 0), ('HH', 1),
    # Fourth set: II-PP (all empty)
    ('II', 0), ('JJ', 0), ('KK', 0), ('LL', 0),
    ('MM', 0), ('NN', 0), ('OO', 0), ('PP', 0)
]

# Create tables with full field set
for table_name, row_count in table_configs:
    table = Table(
        name=table_name,
        fields=[
            Field('pk', 'INTEGER', indexed=True),
            Field('col_int', 'INTEGER'),
            Field('col_int_key', 'INTEGER', indexed=True),
            Field('col_bigint', 'BIGINT'),
            Field('col_bigint_key', 'BIGINT', indexed=True),
            Field('col_decimal', 'DECIMAL'),
            Field('col_decimal_key', 'DECIMAL', indexed=True),
            Field('col_float', 'FLOAT'),
            Field('col_float_key', 'FLOAT', indexed=True),
            Field('col_double', 'DOUBLE'),
            Field('col_double_key', 'DOUBLE', indexed=True),
            Field('col_char_255', 'CHAR(255)'),
            Field('col_char_255_key', 'CHAR(255)', indexed=True),
            Field('col_char_10', 'CHAR(10)'),
            Field('col_char_10_key', 'CHAR(10)', indexed=True),
            Field('col_varchar_10', 'VARCHAR(10)'),
            Field('col_varchar_10_key', 'VARCHAR(10)', indexed=True),
            Field('col_text', 'TEXT'),
            Field('col_text_key', 'TEXT', indexed=True),
            Field('col_varchar_255', 'VARCHAR(255)'),
            Field('col_varchar_255_key', 'VARCHAR(255)', indexed=True)
        ],
        rows=row_count,
        indexes=[
            # Composite indexes from original .zz file
            Index('idx1', ['col_char_255', 'col_char_255_key']),
            Index('idx2', ['col_varchar_255', 'col_varchar_255_key']),
            Index('idx3', ['col_text', 'col_varchar_255']),
            Index('idx4', ['col_char_10', 'col_varchar_10', 'col_text']),
            Index('idx5', ['col_int', 'col_char_10', 'col_varchar_10', 'col_text']),
            Index('idx6', ['col_char_255', 'col_int']),
            Index('idx7', ['col_char_10', 'col_char_255'])
        ]
    )
    OUTER_JOIN_TABLES.append(table)

# ============================================================================
# Helper Functions
# ============================================================================

def get_table_config(grammar_name: str) -> List[Table]:
    """Get table configuration for a specific grammar"""
    if grammar_name == 'transactions_postgres':
        return TRANSACTION_TABLES
    elif grammar_name == 'outer_join_portable':
        return OUTER_JOIN_TABLES
    else:
        return []

def get_table_dict(grammar_name: str) -> Dict[str, int]:
    """Get table name to row count mapping"""
    tables = get_table_config(grammar_name)
    return {table.name: table.rows for table in tables}

def get_fields(grammar_name: str) -> List[str]:
    """Get all field names for a grammar"""
    tables = get_table_config(grammar_name)
    fields = set()
    for table in tables:
        for field in table.fields:
            fields.add(field.name)
    return sorted(list(fields))

def get_indexed_fields(grammar_name: str) -> List[str]:
    """Get all indexed field names"""
    tables = get_table_config(grammar_name)
    fields = set()
    for table in tables:
        for field in table.fields:
            if field.indexed:
                fields.add(field.name)
    return sorted(list(fields))

# ============================================================================
# SQL Generation
# ============================================================================

def generate_create_table_sql(table: Table, engine: str = 'InnoDB') -> str:
    """Generate CREATE TABLE SQL statement"""
    sql = f"CREATE TABLE `{table.name}` (\n"
    
    # Add fields
    field_defs = []
    for field in table.fields:
        field_def = f"  `{field.name}` {field.type}"
        if field.name == 'pk':
            field_def += " PRIMARY KEY AUTO_INCREMENT"
        field_defs.append(field_def)
    
    sql += ",\n".join(field_defs)
    
    # Add indexes
    if table.indexes:
        for index in table.indexes:
            index_def = f",\n  KEY `{index.name}` ({', '.join(f'`{f}`' for f in index.fields)})"
            sql += index_def
    
    sql += f"\n) ENGINE={engine};"
    return sql

def generate_insert_sql(table: Table, initial_value: int = 100) -> List[str]:
    """Generate INSERT statements for initial data"""
    if table.rows == 0:
        return []
    
    sqls = []
    int_fields = [f for f in table.fields if 'int' in f.type.lower() and f.name != 'pk']
    
    for i in range(table.rows):
        values = []
        for field in table.fields:
            if field.name == 'pk':
                values.append('DEFAULT')
            elif 'int' in field.type.lower():
                values.append(str(initial_value))
            elif 'char' in field.type.lower() or 'varchar' in field.type.lower():
                values.append(f"'row{i}'")
            elif 'text' in field.type.lower():
                values.append(f"'text for row {i}'")
            else:
                values.append('0')
        
        field_names = ', '.join(f'`{f.name}`' for f in table.fields)
        values_str = ', '.join(values)
        sql = f"INSERT INTO `{table.name}` ({field_names}) VALUES ({values_str});"
        sqls.append(sql)
    
    return sqls

# ============================================================================
# Test
# ============================================================================

if __name__ == "__main__":
    print("YugabyteDB Data Configuration")
    print("=" * 60)
    
    # Show transaction tables
    print("\nTransaction Tables:")
    for table in TRANSACTION_TABLES:
        print(f"  {table.name}: {table.rows} rows, {len(table.fields)} fields")
    
    # Show outer join tables summary
    print(f"\nOuter Join Tables: {len(OUTER_JOIN_TABLES)} tables")
    non_empty = [t for t in OUTER_JOIN_TABLES if t.rows > 0]
    print(f"  Non-empty tables: {len(non_empty)}")
    print(f"  Total rows: {sum(t.rows for t in OUTER_JOIN_TABLES)}")
    
    # Example SQL generation
    print("\nExample CREATE TABLE:")
    print(generate_create_table_sql(TRANSACTION_TABLES[0]))
    
    print("\nExample INSERT statements:")
    for sql in generate_insert_sql(TRANSACTION_TABLES[0])[:3]:
        print(f"  {sql}")