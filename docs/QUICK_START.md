# PyRQG - Python Random Query Generator

A clean, simple library for generating random SQL queries for database testing.

## Quick Start

### As a Library

```python
from pyrqg.api import create_rqg

# Create RQG instance
rqg = create_rqg()
generator = rqg.create_generator()

# Generate queries
select_query = generator.select(where=True, order_by=True)
print(select_query.sql)
# Output: SELECT email, age FROM users WHERE age > 234 ORDER BY email DESC LIMIT 47

insert_query = generator.insert(on_conflict=True, returning=True)
print(insert_query.sql)
# Output: INSERT INTO products (name, price) VALUES ('value_1', 567) ON CONFLICT (product_id) DO NOTHING RETURNING *
```

### Command Line

```bash
# Generate 10 random queries
python rqg.py generate --count 10

# Generate only SELECT queries
python rqg.py generate --types SELECT --count 5

# Generate from a grammar
python rqg.py grammar --name dml --count 10

# Generate DDL for tables
python rqg.py ddl

# Output in JSON format
python rqg.py generate --count 5 --format json
```

## Library API

### Basic Usage

```python
from pyrqg.api import RQG, TableMetadata, create_rqg

# Quick start with default tables
rqg = create_rqg()
generator = rqg.create_generator(seed=42)  # seed for reproducibility

# Generate different query types
query = generator.select()       # SELECT query
query = generator.insert()       # INSERT query
query = generator.update()       # UPDATE query  
query = generator.delete()       # DELETE query

# Access query metadata
print(query.sql)         # The SQL string
print(query.query_type)  # SELECT, INSERT, etc.
print(query.tables)      # Tables used
print(query.features)    # Features like ON CONFLICT, RETURNING
```

### Custom Tables

```python
# Define your own tables
rqg = RQG()
rqg.add_table(TableMetadata(
    name="customers",
    columns=[
        {"name": "id", "type": "integer"},
        {"name": "email", "type": "varchar"},
        {"name": "balance", "type": "decimal"}
    ],
    primary_key="id",
    unique_keys=["email"]
))

# Generate DDL
ddl_statements = rqg.generate_ddl()

# Generate queries for your tables
generator = rqg.create_generator()
queries = generator.generate_batch(100)
```

### Advanced Options

```python
# SELECT with specific options
query = generator.select(
    tables=["users", "orders"],  # Specific tables
    columns=["name", "total"],   # Specific columns
    where=True,                  # Include WHERE clause
    joins=True,                  # Include JOINs
    group_by=True,              # Include GROUP BY
    order_by=True,              # Include ORDER BY
    limit=True                  # Include LIMIT
)

# INSERT with options
query = generator.insert(
    table="users",
    returning=True,      # Add RETURNING clause
    on_conflict=True,    # Add ON CONFLICT clause
    multi_row=True       # Insert multiple rows
)

# Batch generation
queries = generator.generate_batch(
    count=100,
    query_types=["SELECT", "INSERT"]  # Only these types
)
```

## Integration Example

```python
import psycopg2
from pyrqg.api import create_rqg

# Database connection
conn = psycopg2.connect(...)

# Query generator
rqg = create_rqg()
generator = rqg.create_generator()

# Test loop
for i in range(1000):
    query = generator.generate_batch(1)[0]
    
    try:
        cursor = conn.cursor()
        cursor.execute(query.sql)
        
        if query.query_type == "SELECT":
            results = cursor.fetchall()
            print(f"Query {i}: {len(results)} rows")
        else:
            conn.commit()
            print(f"Query {i}: {query.query_type} successful")
            
    except Exception as e:
        print(f"Query {i} failed: {e}")
        conn.rollback()
```

## YugabyteDB Grammar Support

PyRQG includes comprehensive YugabyteDB-specific grammars:

```python
from pyrqg.api import create_rqg

rqg = create_rqg()

# List all available grammars
grammars = rqg.list_grammars()
# Output: {
#   'dml_yugabyte': 'YugabyteDB DML with ON CONFLICT, RETURNING, CTEs',
#   'yugabyte_transactions': 'YugabyteDB transaction patterns',
#   'yugabyte_subquery': 'Complex subqueries and optimizer tests',
#   'yugabyte_outer_join': 'Outer join patterns for YugabyteDB',
#   ...
# }

# Generate YugabyteDB-specific queries
queries = rqg.generate_from_grammar('dml_yugabyte', count=5)
queries = rqg.generate_from_grammar('yugabyte_transactions', count=10)
queries = rqg.generate_from_grammar('yugabyte_subquery', count=3)

# Command line usage
python rqg.py grammar --name dml_yugabyte --count 10
python rqg.py grammar --name yugabyte_transactions --count 5
```

### Available YugabyteDB Grammars

- **`dml_yugabyte`**: Full DML support with ON CONFLICT, RETURNING, CTEs, yb_hash_code()
- **`yugabyte_transactions`**: Transaction patterns with savepoints and isolation levels
- **`yugabyte_subquery`**: Complex subqueries for optimizer testing
- **`yugabyte_outer_join`**: Outer join patterns and edge cases
- **`dml_unique`**: Enhanced DML with 100% query uniqueness

### Loading Custom Grammars

```python
# Load grammar from file
rqg.load_grammar_file('my_grammar', 'path/to/grammar.py')

# Add existing grammar object
from grammars.dml_yugabyte import g as yb_grammar
rqg.add_grammar('custom_yb', yb_grammar)
```

## Features

- **Simple API**: Clean, intuitive interface for query generation
- **YugabyteDB Support**: Comprehensive grammars for YugabyteDB testing
- **Type Safety**: Returns structured `GeneratedQuery` objects with metadata
- **Reproducible**: Use seeds for deterministic query generation
- **Extensible**: Add custom tables, schemas, and grammars
- **Production Ready**: Generates valid SQL for PostgreSQL/YugabyteDB
- **Advanced SQL**: Supports ON CONFLICT, RETURNING, CTEs, etc.

## Workload Testing

PyRQG includes specialized grammars for generating focused database workloads:

```python
from pyrqg.api import create_rqg

rqg = create_rqg()

# Available workload grammars:
# - workload_insert: INSERT-focused queries
# - workload_update: UPDATE-focused queries  
# - workload_delete: DELETE-focused queries
# - workload_upsert: INSERT ON CONFLICT patterns
# - workload_select: SELECT with joins, aggregates, subqueries

# Generate specific query types
inserts = rqg.generate_from_grammar('workload_insert', count=100)
updates = rqg.generate_from_grammar('workload_update', count=50)

# Command line workload generation
python workload_generator.py --duration 60 --qps 100 --distribution select:0.5,insert:0.3,update:0.2
```

### Workload Generator

Generate complete workloads with DDL and queries:

```bash
# Generate 60-second workload at 10 queries/second
python workload_generator.py --duration 60 --qps 10

# Custom query distribution
python workload_generator.py --distribution insert:0.6,select:0.3,upsert:0.1

# Save workload to file
python workload_generator.py --output workload.json --seed 42

# Show DDL and sample queries
python workload_generator.py --print-ddl --print-queries 10
```

The workload generator:
- Creates random tables with proper relationships
- Generates DDL with indexes and foreign keys
- Creates queries matching your distribution
- Outputs JSON for replay in testing frameworks

## File Structure

```
pyrqg/
├── api.py              # Main library interface
├── dsl/                # Grammar DSL framework
├── production/         # Production-scale generation
├── ddl_generator.py    # DDL generation
└── cli.py              # Command-line interface

grammars/               # Pre-built SQL grammars
├── workload/           # Focused workload grammars
└── yugabyte/           # YugabyteDB-specific grammars

scripts/
├── rqg.py              # Universal CLI launcher
├── workload_generator.py  # Workload generation tool
└── db_comparator.py    # Database comparison utility
```

## See Also

- `scripts/rqg.py` - Command-line tool for quick generation
- `scripts/workload_generator.py` - Advanced workload generation
- `grammars/` - Pre-built grammar definitions
- `tests/test_query_uniqueness.py` - Query uniqueness analysis
- `docs/PRODUCTION_REQUIREMENTS.md` - Billion-scale architecture