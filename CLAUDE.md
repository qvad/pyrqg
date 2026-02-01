# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PyRQG (Python Random Query Generator) is a SQL fuzzer for PostgreSQL-compatible databases (PostgreSQL, YugabyteDB). It generates complex, syntactically valid SQL queries to identify correctness bugs, robustness issues, and performance regressions.

## Commands

```bash
# Install dependencies
.venv/bin/pip install -r requirements.txt
.venv/bin/pip install -e .

# Run all tests
.venv/bin/pytest -v

# Run a single test file
.venv/bin/pytest tests/test_dsl.py -v

# Run a single test
.venv/bin/pytest tests/test_dsl.py::test_template -v

# Run a grammar (dry run, no DB)
.venv/bin/pyrqg grammar --grammar real_workload --count 5

# List available database runners
.venv/bin/pyrqg runners

# Execute against database (auto-detects runner from grammar)
.venv/bin/pyrqg grammar --grammar real_workload --count 1000 --execute --continue-on-error --dsn "postgresql://user:pass@localhost:5432/dbname"

# Explicitly specify a runner
.venv/bin/pyrqg grammar --grammar basic_crud --runner ysql --count 100 --dsn "postgresql://yugabyte:yugabyte@localhost:5433/yugabyte"

# Run YCQL grammar against YugabyteDB YCQL API
.venv/bin/pyrqg grammar --grammar yugabyte_ycql --count 100 --execute --ycql-host localhost --ycql-port 9042

# Docker integration test
docker compose up --build --abort-on-container-exit --exit-code-from pyrqg-runner
```

## Architecture

### Layered Design

1. **Interface Layer** (`pyrqg/runner.py`): CLI entry point, handles arguments and output
2. **API Layer** (`pyrqg/api.py`): Main facade (`RQG` class), orchestrates grammar loading and schema management
3. **Runner Layer** (`pyrqg/core/runners/`): Pluggable database execution backends
4. **DSL Layer** (`pyrqg/dsl/`): Grammar rules, evaluation engine, and primitives

### Pluggable Runners

Database runners provide a unified interface for executing queries against different databases:

```python
from pyrqg.core.runners import RunnerRegistry, RunnerConfig

# List available runners
runners = RunnerRegistry.list_runners()  # {'postgresql', 'ysql', 'ycql'}

# Get a runner by name
config = RunnerConfig(dsn="postgresql://localhost:5432/testdb")
runner = RunnerRegistry.get("postgresql", config=config)

# Or auto-detect from grammar target_api
runner = RunnerRegistry.get_for_api("ysql", config=config)

# Execute queries
stats = runner.execute_queries(queries)
print(stats.summary())
```

Built-in runners:
- `postgresql` - Standard PostgreSQL (psycopg2)
- `ysql` - YugabyteDB YSQL (PostgreSQL-compatible, port 5433)
- `ycql` - YugabyteDB YCQL (Cassandra-compatible, port 9042, requires cassandra-driver)

Create custom runners by subclassing `Runner`:
```python
from pyrqg.core.runners import Runner, RunnerRegistry

class MyDBRunner(Runner):
    name = "mydb"
    description = "My custom database"

    def connect(self): ...
    def close(self): ...
    def execute_one(self, query: str) -> tuple[str, Optional[str]]: ...

RunnerRegistry.register(MyDBRunner)
```

### Key Components

- **`pyrqg/dsl/core.py`**: Core DSL engine - `Grammar`, `Rule`, `Context`, and elements (`Template`, `Choice`, `Lambda`, `Repeat`, `Maybe`)
- **`pyrqg/core/schema.py`**: Data classes for database structure (`Table`, `Column`, `TableConstraint`, `Index`)
- **`pyrqg/core/introspection.py`**: `SchemaProvider` connects to DB and populates `Table` objects
- **`pyrqg/core/valgen.py`**: `ValueGenerator` generates random SQL literals based on column types
- **`pyrqg/core/types.py`**: Type sets (`NUMERIC_TYPES`, `STRING_TYPES`) and helpers (`is_numeric`, `is_string`)
- **`pyrqg/core/grammar_loader.py`**: Dynamically loads grammar files from `grammars/` directory

### Grammars

Grammars live in `grammars/` and export a `grammar` object. Entry point is a rule named `"query"`. Key built-in grammars:
- `real_workload.py` - Mixed SELECT/INSERT/UPDATE/DELETE (the critical grammar)
- `basic_crud.py` - Simple CRUD operations
- `ddl_focused.py` - Schema modification statements

## Grammar DSL

```python
from pyrqg.dsl.core import Grammar, template, choice, repeat, maybe, Lambda
from pyrqg.dsl.utils import pick_table, random_id

g = Grammar("my_grammar")
g.rule("query", template("SELECT {cols} FROM {table};"))
g.rule("cols", choice("*", "id", "name"))
g.rule("table", Lambda(pick_table))  # Schema-aware
grammar = g  # Export required
```

## Important Conventions

- Always use `ctx.rng` (seeded) instead of global `random` for determinism
- Use `pyrqg.core.types` helpers (`is_numeric`, `is_string`, `matches_type_category`) instead of hardcoded type string matching
- Use `pyrqg.dsl.utils` for common grammar operations:
  - `pick_table_and_store(ctx)` - select table and store in state
  - `pick_column(ctx, data_type=None, is_pk=None)` - flexible column selection
  - `generate_constant(ctx, data_type)` - type-appropriate value generation
  - `inc_depth(ctx)` / `dec_depth(ctx)` - expression depth control
- `Context.tables` provides introspected schema as dict of `Table` objects
- DDL statements pause the thread pool to prevent catalog locking issues
- Use `--seed` for reproducible test runs
