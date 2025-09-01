# PyRQG - Python Random Query Generator

A high-performance SQL query generator for database testing, supporting PostgreSQL and YugabyteDB with billion-scale capabilities.

## Key Features

- **Billion-Scale Generation**: Generate 10+ billion unique queries
- **High Performance**: 100,000+ queries per second with multithreading
- **Schema-Aware Generation**: 99.99%+ PostgreSQL compatibility
- **256-bit Entropy**: Cryptographically secure randomization
- **Python DSL**: Intuitive grammar definition framework
- **Production Ready**: Monitoring, checkpoints, graceful shutdown
- **PostgreSQL & YugabyteDB**: Runs via PostgreSQL-compatible DSNs (PostgreSQL executor only)

## Installation

You can use PyRQG directly from source or install it as a package.

### From source (development)

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install package (development, editable)
pip install -e .

# Or build/install a wheel from the repository root (optional)
python -m pip install --upgrade build
python -m build
pip install dist/*.whl
```

## Python Usage (API-first)

Use PyRQG directly from Python without invoking the CLI. These examples show how to create schemas and generate queries inside your app or test framework.

```python
import os
from pyrqg.ddl_generator import DDLGenerator
from pyrqg.core.executor import create_executor
from pyrqg.api import RQG

DSN = "postgresql://user:pass@localhost:5433/yugabyte"  # or PostgreSQL

# 1) Generate and apply a random schema (with profiles/knobs)
gen = DDLGenerator(
    seed=42,
    profile="core",              # core|json_heavy|time_series|network_heavy|wide_range (PG only)
    fk_ratio=0.4,
    index_ratio=0.8,
    composite_index_ratio=0.35,
    partial_index_ratio=0.25,
)
ddl_sql = ";\n".join(gen.generate_schema(num_tables=20)) + ";\n"

exec = create_executor(DSN)
exec.execute("SET search_path TO public, pg_catalog;")
exec.execute(ddl_sql)

# 2) Schema-aware workloads (discover real tables/columns via the live DB)
os.environ["PYRQG_DSN"] = DSN
os.environ["PYRQG_SCHEMA"] = "public"   # or your target schema

rqg = RQG()  # loads built-in grammars

# Examples: SELECT/UPDATE/INSERT workloads using live schema metadata
selects = rqg.generate_from_grammar("workload/select_schema_aware", count=100, seed=1)
updates = rqg.generate_from_grammar("workload/update_schema_aware", count=100, seed=2)
inserts = rqg.generate_from_grammar("workload/insert_focused", count=100, seed=3)

# 3) Execute generated queries
for q in selects + updates + inserts:
    exec.execute(q)

# 4) Grammar-driven (non schema-aware) generation
queries = rqg.generate_from_grammar("dml_yugabyte", count=50, seed=99)
```

Tips:
- Set `PYRQG_DSN` and `PYRQG_SCHEMA` in your process before generating schema-aware workloads.
- Prefer profiles `core`, `json_heavy`, or `time_series` on Yugabyte; `wide_range` includes PostgreSQL-only types (e.g., ranges).
- List grammars in code via `RQG().list_grammars()`; add custom ones using `rqg.add_grammar(name, grammar)`.

## Packaging and Publishing (pip / PyPI)

This project includes a minimal pyproject.toml so you can build and publish wheels.

1) Install build tools:

```bash
python -m pip install --upgrade build twine
```

2) Build sdist and wheel:

```bash
python -m build
# Artifacts will appear in ./dist
ls dist
```

3) Upload to TestPyPI first (recommended):

```bash
# Create an account on https://test.pypi.org
# Then upload:
twine upload -r testpypi dist/*

# Install from TestPyPI to verify:
pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple pyrqg
```

4) Upload to PyPI when ready:

```bash
# Make sure you increment the version in pyproject.toml before re-uploading
twine upload dist/*
```

Notes:
- This minimal package installs the pyrqg Python package and the `pyrqg` console script. The top-level grammars/ directory is not packaged as an importable module in this minimal example. The library loads built-in grammars from installed code; repository-only grammars remain available when running from source.
- If you need to distribute additional grammar modules as part of the package, consider converting grammars/ into a Python package (add __init__.py) or include them as package data and import accordingly.

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

# One-shot random generator (schema + constraints/functions + inserts + workload)
python -m pyrqg.runner random --num-tables 5 --constraints 10 --functions 5 --rows-per-table 10 --workload-count 50 --seed 42 --output bundle.sql

# Delegate to the production runner (predefined configs retained)
python -m pyrqg.runner production --config yugabyte --count 100000

# Run a production scenario workload (grammar file)
python -m pyrqg.runner scenario --file production_scenarios\workloads\01_ecommerce_workload.py --count 1000

# Execute end-to-end against local PostgreSQL (creates random tables, applies ALTERs, runs queries)
python -m pyrqg.runner exec --dsn "postgresql://postgres:password@localhost:5432/postgres" --num-tables 20 --count 100000 --use-filter --progress-every 1000
# Add --echo-queries to print every executed statement

# Generate from a specific rule within a grammar (not just the default 'query')
python -m pyrqg.runner grammar --grammar functions_ddl --grammar-rule create_function --count 5 --seed 99

# Time-limited generation (no DSN): stream queries for N seconds
python -m pyrqg.runner production --custom --grammars dml_yugabyte --duration 10 --output out.sql
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


### Using the Python API (Quickstart)

```python
from pyrqg.api import create_rqg

# Create the generator
rqg = create_rqg()

# 1) Generate a random schema (complex DDL)
schema_sql = rqg.generate_random_schema(num_tables=5)
print("-- Schema")
print(";\n".join(schema_sql) + ";\n")

# 2) Generate random constraints and functions (DDL)
cf_sql = rqg.generate_random_constraints_and_functions(constraints=10, functions=5, include_procedures=True, seed=42)
print("-- Constraints & Functions")
print(";\n".join(cf_sql) + ";\n")

# 3) Generate random data inserts for current tables
inserts = rqg.generate_random_data_inserts(rows_per_table=3, seed=42, on_conflict=True)
print("-- Data Inserts")
print(";\n".join(inserts) + ";\n")

# 4) Generate a mixed workload (SELECT/INSERT/UPDATE/DELETE), optionally with functions mixed in
workload = rqg.run_mixed_workload(count=20, seed=42, include_functions=True)
print("-- Workload")
print(";\n".join(workload) + ";\n")

# Generate directly from a specific grammar/rule if needed
samples = rqg.generate_from_grammar('functions_ddl', rule='create_function', count=3, seed=100)
for s in samples:
    print(s)
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
  
### Schema-Aware Workloads (live DB introspection)
- `workload/select_schema_aware` - SELECTs built from actual tables/columns
- `workload/update_schema_aware` - UPDATEs with type-aware SET/WHERE
- `workload/insert_focused` - already schema-aware for values/types

### YugabyteDB Grammars
- `yugabyte/transactions_postgres` - Distributed transactions
- `yugabyte/optimizer_subquery_portable` - Optimizer testing
- `yugabyte/outer_join_portable` - Complex join patterns

### Grammar Catalog (Descriptions)
- `advanced_query_patterns`: Recursive CTEs, LATERAL joins, window functions, set operations, analytical queries.
- `concurrent_isolation_testing`: Concurrency/isolation tests (locking, MVCC, deadlocks) using live schema.
- `concurrent_isolation_testing_improved`: Higher‑uniqueness, broader table/column sampling for concurrency.
- `data_integrity_testing`: Constraint and trigger stress (FKs, CHECK/NOT NULL), violation scenarios.
- `ddl_focused`: CREATE/ALTER/DROP for tables/indexes/views; constraint/index variants.
- `dml_unique`: High‑uniqueness DML mix with broad predicates/values.
- `dml_with_functions`: DML with SQL/PL function calls and computed expressions.
- `dml_yugabyte`: Yugabyte‑oriented DML (ON CONFLICT, RETURNING, CTEs, MERGE subset, yb_hash_code); includes schema DDL rules.
- `functions_ddl`: Functions/procedures DDL (params/returns/bodies), management (DROP/ALTER), invocation.
- `json_sql_pg15`: PG15 JSON features (JSON_TABLE/EXISTS/QUERY/VALUE, JSON path, aggregates).
- `merge_statement`: PostgreSQL 15 MERGE patterns (simple/CTE/multi‑action) using real table/column sampling.
- `performance_edge_cases`: Planner/perf edge cases; large tables, partial indexes, skewed predicates.
- `postgresql15_types`: Extended types (ranges, multiranges, geometric, network, FTS) with create/insert/select.
- `security_testing`: GRANT/REVOKE, default privileges, RLS policies, security barrier views.
- `simple_transaction`: Transactional patterns via simple DSL.
- `subquery_dsl`: Scalar/EXISTS/IN/ANY‑ALL subqueries; DSL examples over user/order/product tables.
- `yugabyte_transactions_dsl`: Transactions adapted for Yugabyte using DSL.

Workloads
- `workload/insert_focused`: Schema‑aware inserts (single/multi‑row, INSERT…SELECT, DEFAULT).
- `workload/update_focused`: Schema‑aware updates (SET/RETURNING, UPDATE…FROM).
- `workload/delete_focused`: Schema‑aware deletes (WHERE, subqueries, DELETE USING).
- `workload/select_focused`: Schema‑aware selects (columns/filters/order/joins).
- `workload/upsert_focused`: INSERT ON CONFLICT (DO NOTHING/UPDATE), EXCLUDED usage.
- `workload/select_schema_aware`: Live‑schema SELECTs (simple/join forms).
- `workload/update_schema_aware`: Live‑schema UPDATEs (arithmetic SET, WHERE/RETURNING, UPDATE…FROM).

Yugabyte‑specific
- `yugabyte/outer_join_portable`: Portable outer‑join patterns for Yugabyte’s optimizer.
- `yugabyte/optimizer_subquery_portable`: Yugabyte‑friendly subquery/optimizer stress.
- `yugabyte/transactions_postgres`: Transaction/locking patterns on PG wire for Yugabyte.

## Production Usage (custom)

### Time-limited runs
You can run production in a time-based mode using `--duration N`.

Example:

```bash
python -m pyrqg.runner production --custom \
  --workload-grammars yugabyte/outer_join_portable \
  --duration 30 \
  --threads 8 \
  --dsn "postgresql://yugabyte:yugabyte@localhost:5433/yugabyte"
```

### Error samples visibility (improved)
At the end of a production --custom run, PyRQG will always print a small set of syntax error samples if any occurred (default up to 10).

- Use --error-samples N to adjust how many to keep/show.
- Use --print-errors to also sample during execution (in addition to the final summary).

### Selecting a rule within a grammar
Both grammar mode and production `--custom` accept `--grammar-rule` to generate from a specific rule instead of the default `query`.

Examples:
```bash
# Grammar mode
python -m pyrqg.runner grammar --grammar functions_ddl --grammar-rule create_function --count 5

# Production custom (generating a GROUP BY variant from the outer-join grammar)
python -m pyrqg.runner production --custom \
  --workload-grammars yugabyte/outer_join_portable \
  --grammar-rule select_with_group_by \
  --duration 15 --threads 8 \
  --dsn "postgresql://yugabyte:yugabyte@localhost:5433/yugabyte"
```

### Custom Schema Input (files)
Provide your customer schema as SQL and generate queries against it.

Flags:
- `--schema-file path1.sql[,path2.sql]`: apply SQL files before workloads
- `--schema-name <name>`: target schema and search_path (default `pyrqg`; use `public` to see tables in psql without changing search_path)

Example:
```bash
python -m pyrqg.runner production --custom \
  --schema-name public \
  --schema-file /path/to/customer.sql \
  --workload-grammars workload/select_schema_aware,workload/update_schema_aware,workload/insert_focused \
  --count 3000 \
  --dsn "postgresql://user:pass@localhost:5433/db" \
  --print-errors --progress-every 200
```

### Schema Profiles & Density Knobs
Control schema diversity and realism when auto-generating schemas.

Flags:
- `--schema-profile`: `core` (default), `json_heavy`, `time_series`, `network_heavy`, `wide_range` (PG-only types)
- `--fk-ratio`: cross-table FK density (0..1)
- `--index-ratio`: index density per table (0..1)
- `--composite-index-ratio`: probability of composite indexes (0..1)
- `--partial-index-ratio`: probability of partial indexes (0..1)
- `--num-tables`, `--num-functions`, `--num-views`: counts for generated objects

Example (Yugabyte-friendly):
```bash
python -m pyrqg.runner production --custom \
  --schema-name public --schema-profile core \
  --fk-ratio 0.4 --index-ratio 0.8 --composite-index-ratio 0.35 --partial-index-ratio 0.25 \
  --num-tables 30 --num-functions 3 --num-views 3 \
  --workload-grammars dml_yugabyte,workload/insert_focused,workload/update_schema_aware,yugabyte/outer_join_portable \
  --count 5000 --dsn "postgresql://yugabyte:yugabyte@localhost:5433/yugabyte" \
  --print-errors --progress-every 250
```

Note: `wide_range` includes PostgreSQL-only types (e.g., ranges, money). Prefer `core`, `json_heavy`, or `time_series` on Yugabyte.

### Long-running Query Watchdog (formatted SQL)
Report full, formatted SQL for queries exceeding a threshold (defaults: 300s, 5s interval):

```bash
python -m pyrqg.runner production --custom \
  --workload-grammars dml_yugabyte \
  --count 5000 \
  --dsn "postgresql://user:pass@localhost:5432/db" \
  --watch-threshold 120 --watch-interval 2
```

### Run All Grammars
Run every registered grammar sequentially (defaults: 5k queries per grammar). Use env vars to control DSN, schema, and progress.

```bash
COUNT=5000 \
DSN="postgresql://yugabyte:yugabyte@localhost:5433/yugabyte" \
SCHEMA_NAME=public \
PROGRESS_EVERY=500 \
scripts/run_all_grammars.sh --print-errors
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

# Example input(s)
query = "SELECT 1"
queries = ["SELECT 1", "SELECT 2"]

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
from pyrqg.dsl.core import Grammar, choice, template, repeat, ref

# Define a small grammar using curly-brace placeholders and explicit refs
g = Grammar("custom_example")

g.rule("query",
    choice(
        template("SELECT {columns} FROM {table} WHERE {condition}",
                 columns=ref("columns"),
                 table=ref("table"),
                 condition=ref("condition")),
        template("INSERT INTO {table} ({columns}) VALUES ({values})",
                 table=ref("table"),
                 columns=ref("columns"),
                 values=ref("values"))
    )
)

g.rule("columns", repeat(ref("column"), min=1, max=5, sep=", "))

g.rule("column", choice("id", "name", "email", "created_at"))

g.rule("table", choice("users", "products", "orders"))

g.rule("condition", template("{column} = {value}", column=ref("column"), value=ref("value")))

g.rule("value", choice("'test'", "123", "true", "NULL"))

g.rule("values", repeat(ref("value"), min=1, max=5, sep=", "))

# Usage: g.generate("query")
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

- [Grammar Style Guide](docs/GRAMMAR_STYLE.md)
- Advanced docs were moved to `contrib/docs/` to keep the core lean.

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
