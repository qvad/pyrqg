# PyRQG - Python Random Query Generator

A high-performance SQL query generator for database testing, supporting PostgreSQL and YugabyteDB with billion-scale capabilities.

## Key Features

- **Billion-Scale Generation**: Generate 10+ billion unique queries
- **High Performance**: 100,000+ queries per second with multithreading
- **Schema-Aware Generation**: 99.99%+ PostgreSQL compatibility
- **256-bit Entropy**: Cryptographically secure randomization
- **Python DSL**: Intuitive grammar definition framework
- **Production Ready**: Monitoring, checkpoints, graceful shutdown
- **PostgreSQL & YugabyteDB**: Full support for both databases

## Installation

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install package in development mode
pip install -e .
```

## Quick Start

### Using the Runner (recommended)

```bash
# List available grammars (discoverability)
python -m pyrqg.runner list
# You can also just run the runner without a mode (default = list)
python -m pyrqg.runner
# Or use the global flag from any mode
python -m pyrqg.runner --list-grammars

# Generate random Yugabyte-focused DDL schema (5 tables)
python -m pyrqg.runner ddl --num-tables 5 --seed 42 --output schema.sql

# Generate a single random table with random PK, indexes and properties
python -m pyrqg.runner ddl --table demo --num-columns 8 --num-constraints 4 --seed 7

# Generate 100 queries from a built-in grammar
python -m pyrqg.runner grammar --grammar dml_yugabyte --count 100 --seed 123 --output queries.sql

# Delegate to the production runner (predefined configs retained)
python -m pyrqg.runner production --config yugabyte --count 100000

# Run a production scenario workload (grammar file)
python -m pyrqg.runner scenario --file production_scenarios\workloads\01_ecommerce_workload.py --count 1000

# Execute end-to-end against local PostgreSQL (creates random tables, applies ALTERs, runs queries)
python -m pyrqg.runner exec --dsn "postgresql://postgres:password@localhost:5432/postgres" --num-tables 20 --count 100000 --use-filter --progress-every 1000
# Add --echo-queries to print every executed statement
```

### Local Database (PostgreSQL) - Quick Launch

Use Docker to start a local PostgreSQL that matches the default DSN used in this repo (user=postgres, password=password, db=postgres, port=5432):

```bash
# Linux/macOS shell
docker run --name pyrqg-postgres \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_DB=postgres \
  -p 5432:5432 \
  -v pgdata_pyrqg:/var/lib/postgresql/data \
  -d postgres:16
```

Windows PowerShell:

```powershell
# One line (recommended in PowerShell)
docker run --name pyrqg-postgres -e POSTGRES_PASSWORD=password -e POSTGRES_USER=postgres -e POSTGRES_DB=postgres -p 5432:5432 -v pgdata_pyrqg:/var/lib/postgresql/data -d postgres:16
```

Quick check the database is up:

```bash
psql "postgresql://postgres:password@localhost:5432/postgres" -c "SELECT version();"
```

If you don't have psql installed locally on Windows, you can check via Docker:

```powershell
docker exec -it pyrqg-postgres psql -U postgres -d postgres -c "SELECT version();"
```

Run PyRQG end-to-end against this local PostgreSQL (Windows PowerShell):

```powershell
# Activate venv if not yet active
python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt

# Execute: create random tables, apply ALTERs, then run 1,000 queries
python -m pyrqg.runner exec --dsn "postgresql://postgres:password@localhost:5432/postgres" --num-tables 10 --count 1000 --use-filter --print-errors --error-samples 5
```

Notes:
- The configs (e.g., configs/quick_test.json) assume a database named "postgres"; the tool will create and use the "pyrqg" schema automatically via DDL.
- Stop and remove when done: `docker rm -f pyrqg-postgres`.
- For a clean slate, also remove the volume: `docker volume rm pgdata_pyrqg`.
- Make sure Python deps are installed (PowerShell): `python -m venv .venv; .\.venv\Scripts\Activate.ps1; pip install -r requirements.txt`.

### Local Database (YugabyteDB, port 5433) - Quick Launch

YugabyteDB speaks the PostgreSQL wire protocol. Launch a local single-node cluster with tserver on port 5433:

```bash
docker network create ybnet || true

# Start master
docker run -d --name yb-master --net ybnet \
  -p 7000:7000 \
  yugabytedb/yugabyte:latest \
  bin/yb-master \
    --master_addresses=yb-master:7100 \
    --rpc_bind_addresses=yb-master:7100 \
    --webserver_interface=0.0.0.0

# Start tserver (PostgreSQL compatible listener on 5433)
docker run -d --name yb-tserver --net ybnet \
  -p 5433:5433 -p 9000:9000 \
  -e YB_MASTER_ADDRESSES=yb-master:7100 \
  yugabytedb/yugabyte:latest \
  bin/yb-tserver \
    --tserver_master_addrs=yb-master:7100 \
    --rpc_bind_addresses=yb-tserver:9100 \
    --pgsql_proxy_bind_address=0.0.0.0:5433 \
    --cql_proxy_bind_address=0.0.0.0:9042 \
    --webserver_interface=0.0.0.0
```

Quick check:

```bash
psql "postgresql://postgres:password@localhost:5433/postgres" -c "SELECT version();"
```

Run PyRQG against YugabyteDB and print sample SQL errors:

```bash
# Generate DDL + DML and execute, showing up to 10 syntax errors
python -m pyrqg.runner exec \
  --dsn "postgresql://postgres:password@localhost:5433/postgres" \
  --num-tables 10 \
  --count 200 \
  --print-errors --error-samples 10
```

Alternatively, use the provided config:

```bash
python -m pyrqg.runner grammar --grammar dml_yugabyte --count 50
# Or run production settings oriented for Yugabyte (see docs)
python -m pyrqg.runner production --config yugabyte --count 1000
```

Notes:
- YugabyteDB uses port 5433 by default; DSN scheme remains postgresql.
- A convenience config is included: configs/yugabyte_local.json.
- For a quick schema, our DDL uses schema pyrqg and sets search_path accordingly.


### Using the Python API

```python
from pyrqg.api import create_rqg

rqg = create_rqg()

# Generate queries from grammar
queries = rqg.generate_from_grammar('dml_unique', count=10)
for query in queries:
    print(query)

# Generate schema-aware queries for PostgreSQL
rqg_pg = create_rqg()
queries = rqg_pg.generate_from_grammar('dml_unique', count=10)
```

### Schema-Aware Generation (NEW)

```python
from pyrqg.schema_aware_generator import get_schema_aware_generator

# Connect to your database
generator = get_schema_aware_generator()

# Generate queries that match your actual schema
query = generator.generate_insert("users")    # Uses real columns
query = generator.generate_update("products") # Type-aware values
query = generator.generate_select("orders")   # Valid joins
```

## Available Grammars

### Core Grammars
- `dml_unique` - DML with maximum uniqueness
- `ddl_focused` - Complex DDL generation
- `functions_ddl` - PostgreSQL functions and stored procedures
- `advanced_query_patterns` - Complex query patterns
- `postgresql15_types` - PostgreSQL 15 data types
- `json_sql_pg15` - PostgreSQL 15 JSON/SQL features

### Workload Grammars
- `workload/insert_focused` - INSERT-heavy workload
- `workload/update_focused` - UPDATE-heavy workload
- `workload/delete_focused` - DELETE-heavy workload
- `workload/select_focused` - SELECT with complex joins
- `workload/upsert_focused` - INSERT ON CONFLICT patterns

### YugabyteDB Grammars
- `yugabyte/transactions_postgres` - Distributed transactions
- `yugabyte/optimizer_subquery_portable` - Optimizer testing
- `yugabyte/outer_join_portable` - Complex join patterns

## Production Usage

### Billion-Scale Generation

```bash
# Use predefined configuration (e.g., yugabyte, test, performance, minimal, billion)
python -m pyrqg.runner production --config yugabyte

# Custom configuration
python -m pyrqg.runner production --custom --queries 1000000000 --grammars dml_unique,functions_ddl --threads 16

# Override target count and threads on predefined config
python -m pyrqg.runner production --config yugabyte --count 1000000 --threads 8

# Resume from checkpoint
python -m pyrqg.runner production --config yugabyte --checkpoint checkpoint.json

# Production scenario (DDL + mixed workload for scenario + general workload)
python -m pyrqg.runner production --production-scenario bank --count 1000 --seed 42 --output prod_bank.sql

# Directly execute the scenario against a running DB (no intermediate file)
python -m pyrqg.runner production --production-scenario bank --count 1000 --seed 42  --dsn "postgresql://yugabyte:yugabyte@localhost:5433/postgres" --use-filter --print-errors --error-samples 10 --progress-every 100 --echo-queries

# Or generate to file and apply with psql
psql "postgresql://yugabyte:yugabyte@localhost:5433/postgres" -f prod_bank.sql
```

### Performance Benchmarks

On a modern 16-core machine:
- **Query Generation**: 100,000+ queries/second
- **Schema-Aware**: 7,200 queries/second with 99.99% success
- **PostgreSQL Filtered**: 3,000-5,000 queries/second
- **Memory Usage**: <10GB for billion-query runs
- **Uniqueness**: >99.999% unique queries

## Advanced Features

### PostgreSQL Compatibility

PyRQG includes advanced PostgreSQL compatibility features:

```python
# Enable PostgreSQL filtering
from pyrqg.filters.postgres_filter import PostgreSQLFilter

pg_filter = PostgreSQLFilter()
valid_query = pg_filter.filter_query(query)  # Returns validated query or None

# Analyze query patterns
from pyrqg.filters.query_analyzer import QueryAnalyzer

analyzer = QueryAnalyzer()
stats = analyzer.analyze_queries(queries)
print(stats.error_distribution)
```

### Custom Grammar Definition

```python
from pyrqg.dsl.core import Grammar, choice, template, repeat

grammar = Grammar(
    main=choice(
        template("SELECT $columns FROM $table WHERE $condition"),
        template("INSERT INTO $table ($columns) VALUES ($values)")
    ),
    columns=repeat("column", min=1, max=5, separator=", "),
    column=choice("id", "name", "email", "created_at"),
    table=choice("users", "products", "orders"),
    condition=template("$column = $value"),
    value=choice("'test'", "123", "true", "NULL"),
    values=repeat("value", min=1, max=5, separator=", ")
)
```

### Monitoring and Analysis

Use the Runner to generate queries (to files if needed), then feed them to your preferred execution/monitoring tooling or test harness.

## Architecture

PyRQG uses a modular architecture optimized for performance:

1. **DSL Framework**: Grammar definition using Python primitives
2. **Execution Engine**: Recursive grammar processing with caching
3. **Production System**: Multi-threaded generation with batching
4. **Entropy Manager**: Thread-safe 256-bit randomization
5. **Uniqueness Tracker**: Bloom filter with <0.001% false positives
6. **PostgreSQL Filter**: Query validation and fixing
7. **Schema Awareness**: Real-time database introspection

## Testing

```bash
# Run all tests
./run_tests.sh

# Run specific test suites
python -m pytest tests/test_dsl_core.py -v
python -m pytest tests/test_production.py -v
python -m pytest tests/test_grammars.py -v

# Run with coverage
python -m pytest --cov=pyrqg --cov-report=html
```

## Documentation

- [Architecture Overview](docs/ARCHITECTURE.md)
- [Grammar Development Guide](docs/GRAMMAR_GUIDE.md)
- [Production Configuration](docs/PRODUCTION_CONFIG.md)
- [PostgreSQL Compatibility](docs/POSTGRES_COMPAT.md)

## Cleanup

To clean up non-essential artifacts (logs, generated reports, temp outputs, etc.) based on the curated list in CLEANUP_DELETE_LIST.txt:

```bash
# Dry-run (recommended): shows what would be deleted
python scripts/cleanup_from_list.py --dry-run

# Execute cleanup
python scripts/cleanup_from_list.py --execute

# Verbose output
python scripts/cleanup_from_list.py --execute --verbose
```

Notes:
- The script reads only the "Deletion candidates" section and ignores [KEEP] entries.
- docs/ is always protected and will not be deleted by the script.
- Review and adjust CLEANUP_DELETE_LIST.txt before running with --execute.

## License

This project is licensed under the MIT License - see the LICENSE file for details.