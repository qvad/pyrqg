# Python PyRQG

Modern Python implementation of the Random Query Generator with **billion-scale production capabilities**.

## Directory Structure
- `pyrqg/` - Core Python package
- `grammars/` - Python grammar definitions
- `tests/` - Test suite
- `scripts/` - Utility scripts
- `docs/` - Documentation

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

### Using the API:
```python
from pyrqg.api import create_rqg

rqg = create_rqg()

# Generate queries from grammar
queries = rqg.generate_from_grammar('dml_unique', count=10)
for query in queries:
    print(query)

# Generate DDL with complex constraints
ddl_statements = rqg.generate_complex_ddl(num_tables=5)
for ddl in ddl_statements:
    print(ddl)
```

### Using the CLI:
```bash
# Run with a grammar file for 60 seconds
python -m pyrqg.cli --grammar grammars/dml_unique.py --duration 60

# Run 1000 queries
python -m pyrqg.cli --grammar grammars/dml_unique.py --queries 1000

# Use the simple launcher script
python scripts/rqg.py generate --count 10

# Generate workload
python scripts/workload_generator.py --duration 60 --qps 10
```

## Key Features
- **Billion-Scale Generation** - Generate 10+ billion unique queries
- **High Performance** - 100,000+ queries per second with multithreading
- **256-bit Entropy** - Cryptographically secure randomization
- **Python DSL** for grammar definition
- **Enhanced DDL** with composite keys, check constraints, foreign keys
- **Query Uniqueness** - Probabilistic tracking with <0.001% false positives
- **Dynamic Data** - Realistic names, emails, addresses, correlations
- **YugabyteDB Support** - Specialized grammars and validators
- **Workload Generation** - Configurable query distributions
- **Production Ready** - Monitoring, checkpoints, graceful shutdown

## Available Grammars
- `dml_unique` - DML with maximum uniqueness
- `dml_yugabyte` - YugabyteDB DML with ON CONFLICT, RETURNING
- `ddl_focused` - Complex DDL generation
- `functions_ddl` - PostgreSQL functions and stored procedures
- `dml_with_functions` - DML with function calls
- `advanced_query_patterns` - Complex query patterns
- `postgresql15_types` - PostgreSQL 15 data types
- `json_sql_pg15` - PostgreSQL 15 JSON/SQL features
- `simple_transaction` - Transaction patterns
- `yugabyte_transactions` - YugabyteDB transaction patterns
- **Workload grammars:**
  - `insert_workload` - INSERT-focused queries
  - `update_workload` - UPDATE-focused queries
  - `delete_workload` - DELETE-focused queries
  - `select_workload` - SELECT with joins
  - `upsert_workload` - INSERT ON CONFLICT patterns
- **YugabyteDB grammars:**
  - `transactions_postgres` - PostgreSQL-compatible transactions
  - `optimizer_subquery_portable` - Subquery optimizer tests
  - `outer_join_portable` - Outer join patterns

## Production Usage

### Billion-Scale Generation
```bash
# Use predefined configurations
python run_production.py --config billion      # 1 billion queries
python run_production.py --config test         # 10k test queries
python run_production.py --config performance  # Benchmark mode
python run_production.py --config yugabyte    # YugabyteDB testing

# Custom configuration
python run_production.py --custom --queries 100000 --grammars dml_unique,functions_ddl

# Override settings
python run_production.py --config test --count 50000 --threads 8 --no-uniqueness

# Save to file
python run_production.py --config test --output queries.sql

# Resume from checkpoint
python run_production.py --config billion --checkpoint output/billion_scale/checkpoint.json
```

### Configuration
Production PyRQG uses Python-based configuration for type safety and IDE support:

```python
from pyrqg.production.configs import billion_scale_config, custom_config

# Use predefined configuration
config = billion_scale_config()  # 1 billion queries, all optimizations

# Create custom configuration
config = custom_config(
    name="my_test",
    queries=100_000,
    grammars=["dml_unique", "functions_ddl"],
    threads=8,
    uniqueness=True
)

# Modify configuration
config.threading.num_threads = 16
config.uniqueness.false_positive_rate = 0.001
```

Available configurations:
- `billion` - 1 billion queries with maximum performance
- `test` - 10k queries with deterministic settings
- `performance` - Benchmark mode (no uniqueness checking)
- `minimal` - Minimal configuration
- `yugabyte` - YugabyteDB-specific testing

## Performance

On a modern 16-core machine:
- **Query Generation**: 100,000+ queries/second
- **Memory Usage**: <10GB for billion-query runs
- **Uniqueness**: >99.999% unique queries
- **Entropy**: 2^256 possible states

## Documentation
- [Quick Start Guide](docs/QUICK_START.md) - Simple examples and basic usage
- [Production Requirements](docs/PRODUCTION_REQUIREMENTS.md) - Billion-scale design
