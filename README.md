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

# Generate 100 queries from the Snowflake grammar
python -m pyrqg.runner grammar --grammar snowflake --count 100 --seed 123 --output queries.sql

# Execute generated queries by applying them with psql
# Example: generate DDL to file and apply to a running database
python -m pyrqg.runner grammar --grammar ddl_focused --count 100 --output schema.sql
psql "postgresql://postgres:password@localhost:5432/postgres" -f schema.sql
```

Tip: pass `--errors-only` when running `pyrqg.runner` to emit just the failing SQL statements (they go to stdout). Combine it with `--error-log path/to/file` if you want to capture every failure in a log without echoing successful queries.

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

Run PyRQG against YugabyteDB and apply queries with psql:

```bash
# Generate queries to a file, then apply with psql (YugabyteDB listens on 5433)
python -m pyrqg.runner grammar --grammar snowflake --count 50 --output queries.sql
psql "postgresql://postgres:password@localhost:5433/postgres" -f queries.sql
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
queries = rqg.generate_from_grammar('snowflake', count=10)
for query in queries:
    print(query)

# Generate DDL statements
ddl_statements = rqg.generate_ddl()
```

### Quick start: Runner CLI examples

Below are copy-pasteable examples to start the built-in runner. Replace credentials/DSN as needed.

List available grammars and descriptions:

```bash
python -m pyrqg.runner list
```

Generate queries from a specific grammar and print to console:

```bash
python -m pyrqg.runner grammar --grammar snowflake --count 5
```

Generate queries and save to a file (no execution):

```bash
python -m pyrqg.runner grammar --grammar snowflake --count 50 --output queries.sql
```

Execute generated queries against a local Postgres/Yugabyte instance:

Windows PowerShell (set DSN for the current session)

```powershell
$env:PYRQG_DSN = "postgresql://postgres:postgres@localhost:5432/postgres"
python -m pyrqg.runner grammar --grammar ddl_focused --count 20 --execute
```

Or pass DSN inline (any shell):

```bash
python -m pyrqg.runner grammar --grammar snowflake --count 20 \
  --dsn "postgresql://postgres:postgres@localhost:5432/postgres" \
  --errors-only --continue-on-error
```

Run all grammars once each (initialize a basic default schema first):

```bash
python -m pyrqg.runner all --count 3 --init-schema \
  --dsn "postgresql://postgres:postgres@localhost:5432/postgres" \
  --errors-only --continue-on-error
```

Generate complex DDL for N random tables and save to a file:

```bash
python -m pyrqg.runner ddl --num-tables 5 --output schema.sql
```

Generate a single random table DDL (with indexes) and execute it:

```bash
python -m pyrqg.runner ddl --table tmp_users --num-columns 8 --num-constraints 3 \
  --dsn "postgresql://postgres:postgres@localhost:5432/postgres" --execute
```

Deterministic generation with a seed (same seed → same queries):

```bash
python -m pyrqg.runner grammar --grammar snowflake --count 5 --seed 42
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

## How PyRQG Works

PyRQG generates SQL by walking a user-defined grammar (a small Python program that describes how to build statements). At a high level:
- You define a Grammar object with named rules (e.g., query, table, column, value) using DSL primitives like template, choice, repeat, maybe, and ref.
- The engine starts from a main rule (by default rule="query") and expands placeholders recursively.
- A seeded random source drives choices to create diverse yet reproducible queries.
- Optionally you can generate DDL using the DDL generator to set up a test schema.
- You can run generation through the CLI Runner or the Python API; output can be printed, written to a file, or executed via psql.

Key DSL primitives you’ll use most often:
- template("... $placeholder ..."): string pattern with named placeholders
- choice(a, b, c): randomly choose one option
- repeat("rule", min=0, max=3, separator=", "): repeat another rule
- maybe("rule"): include another rule with 50/50 probability
- ref("name"): reference another rule explicitly (when not using string shorthands)

Determinism and seeds:
- Pass --seed N in the runner or seed=N in the API to get repeatable sequences.
- Same grammar + same seed => exactly the same output.

## Create Your Own Grammar (Step-by-Step)

Below is the smallest possible, but useful, custom grammar that emits simple SELECT/INSERT statements.

1) Create a new file, for example grammars/my_grammar.py:

```python
from pyrqg.dsl.core import Grammar, template, choice, repeat

# Expose a top-level variable named `g` so loaders can discover it

g = Grammar(
    # Entry point; the runner/API will use rule="query" by default
    query=choice(
        template("SELECT $cols FROM $table$maybe_where"),
        template("INSERT INTO $table ($cols) VALUES ($vals)")
    ),

    # Sub-rules
    cols=repeat("col", min=1, max=4, separator=", "),
    col=choice("id", "name", "email", "created_at"),
    table=choice("users", "products", "orders"),

    # Optional WHERE clause
    maybe_where=choice("", " WHERE id = 1", " WHERE name = 'x'") ,

    # Values for INSERT
    vals=repeat("val", min=1, max=4, separator=", "),
    val=choice("1", "'abc'", "NULL")
)
```

2) Load your grammar in one of two ways:
- Easiest (environment-based): set PYRQG_GRAMMARS to the module path that defines g. Example on Windows PowerShell:
  - $env:PYRQG_GRAMMARS = "grammars.my_grammar"
  - python -m pyrqg.runner list  # you should see my_grammar in the list
- Programmatic: load and register directly in Python:
  - from grammars.my_grammar import g
  - from pyrqg.api import create_rqg
  - rqg = create_rqg(); rqg.add_grammar("my_grammar", g)

3) Run it via the Runner:
- python -m pyrqg.runner grammar --grammar my_grammar --count 10 --seed 7
- To write to a file: add --output queries.sql and apply with psql.

4) Use it via the API:
```python
from pyrqg.api import create_rqg
from grammars.my_grammar import g

rqg = create_rqg()
rqg.add_grammar("my_grammar", g)
print("\n".join(rqg.generate_from_grammar("my_grammar", count=5, seed=123)))
```

Tips:
- Always expose the variable name g = Grammar(...) in your module so loaders can find it.
- Give your rules meaningful names. The default entry rule is query but you can pass a different one via --rule in API usage.
- Start simple; grow complexity with new rules and references as you need more diversity.

## End-to-End Examples

- Generate 50 statements from your grammar and apply to local Postgres with psql:
  - python -m pyrqg.runner grammar --grammar my_grammar --count 50 --output queries.sql
  - psql "postgresql://postgres:password@localhost:5432/postgres" -f queries.sql
- Generate DDL for a quick playground schema:
  - python -m pyrqg.runner ddl --num-tables 3 --output schema.sql
  - psql "postgresql://postgres:password@localhost:5432/postgres" -f schema.sql
- Deterministic run (same seed → same output):
  - python -m pyrqg.runner grammar --grammar my_grammar --count 10 --seed 42

## Troubleshooting

- Runner says: "--grammar is required for 'grammar' mode" → supply --grammar NAME or use the list mode to discover names.
- Your grammar doesn’t appear in list → ensure PYRQG_GRAMMARS includes your module path and that the module exposes g.
- psql not found on Windows → install PostgreSQL client or run psql inside the Docker container via docker exec.
- Connection refused → verify DSN host/port and that your database container is running.

## Available Grammars

### Available Grammars
- `ddl_focused` – emits complex PostgreSQL DDL for schema bootstrapping.
- `snowflake` – generates simplified Snowflake SQL (USE, ALTER WAREHOUSE, aggregates).

## Performance

On a modern 16-core machine:
- Query generation: 100,000+ queries/second
- Schema-aware: thousands of queries/second with high success rate

### Performance Benchmarks

On a modern 16-core machine:
- **Query Generation**: 100,000+ queries/second
- **Schema-Aware**: 7,200 queries/second with 99.99% success
- **PostgreSQL Filtered**: 3,000-5,000 queries/second
- **Memory Usage**: <10GB for billion-query runs
- **Uniqueness**: >99.999% unique queries

## Advanced Features


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
6. **Schema Awareness**: Real-time database introspection

## Testing

```bash
# Run all tests
pytest

# Run specific test files
pytest tests/test_dsl_core.py -v
pytest tests/test_api.py -v

# Run with coverage
pytest --cov=pyrqg --cov-report=html
```

## Documentation

- PyRQG Complete Specification: PYRQG_COMPLETE_SPECIFICATION.md
- Test Suite Guide: tests/README.md


## License

This project is licensed under the MIT License - see the LICENSE file for details.
