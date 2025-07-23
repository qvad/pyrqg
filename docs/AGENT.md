# PyRQG Agent Documentation

This document provides comprehensive information about PyRQG (Python Random Query Generator) for future Claude AI sessions. It includes all essential details about the project structure, features, requirements, and usage.

## Project Overview

PyRQG is a modern Python implementation of the Random Query Generator with **billion-scale production capabilities**. It's designed for database testing, particularly for PostgreSQL and YugabyteDB.

### Key Features
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

## System Requirements

### Python Dependencies (requirements.txt)
```
psycopg2-binary>=2.9.0
numpy>=1.20.0
mmh3>=3.0.0
psutil>=5.9.0
```

### Python Version
- Python 3.8 or higher required
- Tested with Python 3.9-3.11

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

## Project Structure

```
pyrqg/
├── pyrqg/                  # Core Python package
│   ├── api.py             # Main library interface
│   ├── cli.py             # Command-line interface
│   ├── ddl_generator.py   # DDL generation with constraints
│   ├── core/              # Core engine and execution
│   │   ├── engine.py      # Grammar execution engine
│   │   ├── executor.py    # Query executor
│   │   ├── validator.py   # Result validators
│   │   └── reporter.py    # Result reporting
│   ├── dsl/               # Grammar DSL framework
│   │   └── core.py        # DSL primitives (Grammar, choice, template, etc.)
│   ├── production/        # Production-scale features
│   │   ├── entropy.py     # 256-bit entropy management
│   │   ├── threading.py   # Multithreading architecture
│   │   ├── uniqueness.py  # Bloom filter uniqueness tracking
│   │   ├── data_generator.py  # Realistic data generation
│   │   ├── configs.py     # Python-based configuration
│   │   └── production_rqg.py  # Main production runner
│   ├── generators/        # Data generators
│   └── filters/           # Query filters
├── grammars/              # SQL grammar definitions
│   ├── dml_unique.py      # DML with maximum uniqueness
│   ├── dml_yugabyte.py    # YugabyteDB-specific DML
│   ├── ddl_focused.py     # Complex DDL generation
│   ├── functions_ddl.py   # PostgreSQL functions/procedures
│   ├── dml_with_functions.py  # DML with function calls
│   ├── workload/          # Workload-specific grammars
│   │   ├── select_focused.py
│   │   ├── insert_focused.py
│   │   ├── update_focused.py
│   │   ├── delete_focused.py
│   │   └── upsert_focused.py
│   └── yugabyte/          # YugabyteDB grammars
│       ├── transactions_postgres.py
│       ├── optimizer_subquery_portable.py
│       └── outer_join_portable.py
├── scripts/               # Utility scripts
│   ├── rqg.py            # Universal launcher
│   ├── workload_generator.py  # Workload generation
│   └── db_comparator.py  # Database comparison
├── tests/                 # Test suite
├── docs/                  # Documentation
│   ├── QUICK_START.md    # Simple examples
│   ├── PRODUCTION_REQUIREMENTS.md  # Billion-scale design
│   └── AGENT.md          # This file
└── run_production.py      # Production runner
```

## Usage Examples

### 1. Basic Query Generation

```python
from pyrqg.api import create_rqg

# Create RQG instance
rqg = create_rqg()
generator = rqg.create_generator(seed=42)

# Generate different query types
select_query = generator.select(where=True, order_by=True)
insert_query = generator.insert(on_conflict=True, returning=True)
update_query = generator.update(where=True)
delete_query = generator.delete(where=True)

# Access query metadata
print(query.sql)         # The SQL string
print(query.query_type)  # SELECT, INSERT, etc.
print(query.tables)      # Tables used
print(query.features)    # Features like ON CONFLICT, RETURNING
```

### 2. Grammar-Based Generation

```python
# Generate from specific grammar
queries = rqg.generate_from_grammar('dml_unique', count=10)
queries = rqg.generate_from_grammar('yugabyte_transactions', count=5)

# List available grammars
grammars = rqg.list_grammars()
```

### 3. DDL Generation

```python
# Simple DDL
rqg._add_default_tables()  # Load default tables
ddl_statements = rqg.generate_ddl()

# Complex DDL with constraints
ddl_statements = rqg.generate_complex_ddl(num_tables=5)

# Random table DDL
table_ddl = rqg.generate_random_table_ddl('my_table')
```

### 4. Command Line Usage

```bash
# Basic generation
python scripts/rqg.py generate --count 10

# Grammar-based generation
python scripts/rqg.py grammar --name dml_unique --count 100

# DDL generation
python scripts/rqg.py ddl

# List grammars
python scripts/rqg.py list grammars

# Workload generation
python scripts/workload_generator.py --duration 60 --qps 10

# Production run
python run_production.py --config test --count 10000
```

### 5. Production Usage

```python
# Use predefined configurations
python run_production.py --config billion      # 1 billion queries
python run_production.py --config test         # 10k test queries
python run_production.py --config performance  # Benchmark mode
python run_production.py --config yugabyte    # YugabyteDB testing

# Custom configuration
python run_production.py --custom --queries 100000 --grammars dml_unique,functions_ddl

# Override settings
python run_production.py --config test --count 50000 --threads 8 --no-uniqueness

# Resume from checkpoint
python run_production.py --config billion --checkpoint output/billion_scale/checkpoint.json
```

## Available Grammars

### General Grammars
- `dml_unique` - DML with maximum uniqueness
- `dml_yugabyte` - YugabyteDB DML with ON CONFLICT, RETURNING
- `ddl_focused` - Complex DDL generation
- `functions_ddl` - PostgreSQL functions and stored procedures
- `dml_with_functions` - DML with function calls
- `advanced_query_patterns` - Complex query patterns
- `postgresql15_types` - PostgreSQL 15 data types
- `json_sql_pg15` - PostgreSQL 15 JSON/SQL features
- `simple_transaction` - Transaction patterns

### Workload Grammars
- `insert_workload` - INSERT-focused queries
- `update_workload` - UPDATE-focused queries
- `delete_workload` - DELETE-focused queries
- `select_workload` - SELECT with joins
- `upsert_workload` - INSERT ON CONFLICT patterns

### YugabyteDB Grammars
- `yugabyte_transactions` - YugabyteDB transaction patterns
- `transactions_postgres` - PostgreSQL-compatible transactions
- `optimizer_subquery_portable` - Subquery optimizer tests
- `outer_join_portable` - Outer join patterns

## Production Configuration

PyRQG uses Python-based configuration for type safety:

```python
from pyrqg.production.configs import billion_scale_config, custom_config

# Predefined configs
config = billion_scale_config()   # 1 billion queries
config = test_config()            # 10k queries, deterministic
config = performance_test_config() # Benchmark mode
config = minimal_config()         # Minimal setup
config = yugabyte_config()        # YugabyteDB specific

# Custom config
config = custom_config(
    name="my_test",
    queries=100_000,
    grammars=["dml_unique", "functions_ddl"],
    threads=8,
    uniqueness=True
)
```

### Key Configuration Parameters

1. **Entropy Configuration**
   - Primary source: "urandom", "deterministic"
   - State bits: 128, 256
   - Reseed interval
   - Thread-local RNG

2. **Threading Configuration**
   - Number of threads (None for auto-detect)
   - Queue size
   - Batch size
   - Backpressure threshold
   - CPU affinity

3. **Uniqueness Configuration**
   - Mode: PROBABILISTIC, EXACT, NONE
   - False positive rate
   - Bloom filter size
   - Rotation interval

4. **Data Generation**
   - Distribution types
   - Vocabulary size
   - Realistic data (names, emails, addresses)
   - Data correlations

5. **Monitoring**
   - Metrics: qps, memory, uniqueness_rate
   - Export formats
   - Alert thresholds

## Grammar DSL Reference

PyRQG uses a Python-based DSL for defining grammars:

```python
from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat

g = Grammar("my_grammar")

# Define rules
g.rule("query",
    choice(
        ref("select"),
        ref("insert"),
        weights=[70, 30]  # 70% SELECT, 30% INSERT
    )
)

g.rule("select",
    template("SELECT {columns} FROM {table} WHERE {condition}",
        columns=ref("column_list"),
        table=ref("table_name"),
        condition=ref("where_condition")
    )
)

# Primitives
g.rule("table_name", choice("users", "products", "orders"))
g.rule("column_name", choice("id", "name", "email", "price"))
g.rule("value", number(1, 1000))

# Modifiers
g.rule("optional_where", maybe(ref("where_clause")))
g.rule("column_list", repeat(ref("column_name"), min=1, max=5, sep=", "))
```

## Performance Characteristics

On a modern 16-core machine:
- **Query Generation**: 100,000+ queries/second
- **Memory Usage**: <10GB for billion-query runs
- **Uniqueness**: >99.999% unique queries
- **Entropy**: 2^256 possible states
- **Thread Scaling**: Near-linear up to CPU count

## PostgreSQL/YugabyteDB Coverage

### Supported Features
1. **Basic DML**: SELECT, INSERT, UPDATE, DELETE
2. **Advanced DML**: 
   - ON CONFLICT (UPSERT)
   - RETURNING clause
   - CTEs (WITH clause)
   - Multi-row INSERT
3. **Joins**: INNER, LEFT, RIGHT, FULL OUTER, CROSS
4. **Subqueries**: Correlated, uncorrelated, IN, EXISTS
5. **Aggregates**: COUNT, SUM, AVG, MIN, MAX, etc.
6. **Window Functions**: ROW_NUMBER, RANK, LEAD, LAG
7. **Functions**: Built-in, UDFs, stored procedures
8. **Transactions**: BEGIN, COMMIT, ROLLBACK, SAVEPOINT
9. **DDL**: 
   - Tables with constraints
   - Indexes (B-tree, Hash, GiST, GIN)
   - Views, Materialized views
   - Functions and procedures
10. **Data Types**: All PostgreSQL 15 types including JSON, arrays, ranges

### Coverage Statistics (from analysis)
- Overall PostgreSQL coverage: ~72%
- DML coverage: ~85%
- DDL coverage: ~80%
- Functions/Procedures: ~85%
- YugabyteDB-specific: ~90%

## Important Code Patterns

### 1. Creating a Grammar
```python
from pyrqg.dsl.core import Grammar, choice, template

g = Grammar("grammar_name")
g.rule("rule_name", choice(...))
```

### 2. Production Runner Pattern
```python
from pyrqg.production.production_rqg import ProductionRQG
from pyrqg.production.configs import billion_scale_config

config = billion_scale_config()
runner = ProductionRQG(config)
runner.run()
```

### 3. Custom Table Definition
```python
from pyrqg.api import TableMetadata

table = TableMetadata(
    name="my_table",
    columns=[
        {"name": "id", "type": "integer"},
        {"name": "data", "type": "jsonb"}
    ],
    primary_key="id"
)
rqg.add_table(table)
```

## Troubleshooting

### Common Issues

1. **ModuleNotFoundError: No module named 'pyrqg'**
   - Run from project root or install with `pip install -e .`

2. **ModuleNotFoundError: No module named 'numpy'**
   - Install dependencies: `pip install -r requirements.txt`

3. **Empty DDL generation**
   - Call `rqg._add_default_tables()` before `generate_ddl()`

4. **Grammar not found**
   - Check available grammars with `rqg.list_grammars()`
   - Grammar names are case-sensitive

## Development Guidelines

### Adding a New Grammar
1. Create file in `grammars/` directory
2. Import DSL: `from pyrqg.dsl.core import Grammar, ...`
3. Create grammar: `g = Grammar("name")`
4. Define rules
5. Test with: `python scripts/rqg.py grammar --name your_grammar`

### Adding Production Features
1. Add module in `pyrqg/production/`
2. Update `ProductionConfig` in `config.py`
3. Integrate in `production_rqg.py`
4. Add configuration in `configs.py`

### Testing
- Unit tests in `tests/`
- Integration tests use Docker for real databases
- Uniqueness verification with `test_query_uniqueness.py`
- Performance benchmarks with performance config

## Future Enhancements

1. **Additional Database Support**
   - MySQL/MariaDB grammars
   - Oracle compatibility
   - SQL Server patterns

2. **Enhanced Features**
   - Query mutation for fuzzing
   - Automatic grammar learning
   - Query complexity scoring
   - Performance prediction

3. **Production Improvements**
   - Distributed generation
   - Cloud storage integration
   - Real-time analytics
   - Query replay capabilities

## Contact and Support

- GitHub Issues: Report bugs and feature requests
- Documentation: See docs/ directory
- Examples: Check scripts/ directory for usage examples

## Important Implementation Details

### 1. Thread-Safe Random Generation
The production system uses thread-local RNG with automatic cleanup:
```python
# In entropy.py
def _cleanup_terminated_threads(self):
    """Remove generators for terminated threads"""
    active_threads = {t.ident for t in threading.enumerate()}
    dead_threads = [tid for tid in self._thread_generators 
                   if tid not in active_threads]
    for tid in dead_threads:
        del self._thread_generators[tid]
```

### 2. Bloom Filter Configuration
For billion-scale uniqueness tracking:
- Size: 4GB (4096 MB)
- Hash functions: 7
- False positive rate: 0.001%
- Rotation interval: 100M queries

### 3. Backpressure Handling
Threading uses exponential backoff:
```python
wait_time = 0.001  # Start with 1ms
max_wait = 0.1     # Max 100ms
while not batch.completed:
    time.sleep(wait_time)
    wait_time = min(wait_time * 1.5, max_wait)
```

### 4. Grammar Compilation
Regex patterns are pre-compiled for performance:
```python
# Compiled once at module level
IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
```

## Known Issues and Solutions

### 1. Import Path Issues
**Problem**: Scripts fail with "ModuleNotFoundError: No module named 'pyrqg'"
**Solution**: Scripts need parent directory in path:
```python
sys.path.insert(0, str(Path(__file__).parent.parent))
```

### 2. Empty DDL Generation
**Problem**: `rqg.generate_ddl()` returns empty list
**Solution**: Call `rqg._add_default_tables()` first

### 3. Grammar Names
**Problem**: Some grammars created without names
**Solution**: Always specify name: `Grammar("name")`

### 4. Workload Grammar Duplication
**Issue**: ~25-30% code duplication in workload files
**Status**: Acceptable for now, could be refactored with base class

## Code Quality Notes

### Removed Files
1. **Duplicate executors**: Consolidated to single `executor.py`
2. **Config files**: Migrated from YAML/HOCON to Python
3. **Test duplicates**: Removed `test_ddl_then_dml.py`
4. **Lua files**: All grammars now in Python
5. **Python cache**: Cleaned `__pycache__` directories

### Architecture Decisions
1. **Production vs Simple API**: Intentionally separate for different use cases
2. **Grammar files**: Kept separate for clarity despite some duplication
3. **Thread safety**: All production components are thread-safe
4. **Memory management**: Automatic cleanup of terminated threads

## Testing Checklist

### Basic Functionality
- [ ] `python scripts/rqg.py generate --count 10`
- [ ] `python scripts/rqg.py grammar --name dml_unique --count 5`
- [ ] `python scripts/rqg.py ddl`
- [ ] `python scripts/rqg.py list grammars`

### Production Features
- [ ] `python run_production.py --config test --count 1000`
- [ ] Checkpoint/resume functionality
- [ ] Thread scaling
- [ ] Uniqueness verification

### Database Integration
- [ ] PostgreSQL connection and execution
- [ ] YugabyteDB specific features
- [ ] Error handling and validation

## Version History

- 1.0.0 - Initial production release with billion-scale capabilities
- Added PostgreSQL 15 support
- Added function/procedure generation
- Migrated from YAML/HOCON to Python configs
- Removed Lua dependencies
- Fixed memory leaks in thread management
- Optimized regex compilation

This document should provide all necessary information for future Claude sessions to understand and work with PyRQG effectively.