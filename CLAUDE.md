# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## CRITICAL RULES - MUST FOLLOW

### 1. DO NOT CREATE NEW RUNNERS OR LAUNCHERS!
- **USE ONLY** the existing `pyrqg_launcher.py` 
- **FIX** issues in existing code instead of creating new files
- **IMPROVE** existing logic rather than replacing it
- If something doesn't work, **FIX THE EXISTING CODE**

### 2. DO NOT CREATE EXTRA TEST SCRIPTS!
- Use existing test infrastructure in `tests/` directory
- Run tests with `pytest` or `run_tests.sh`
- For manual testing, use `pyrqg_launcher.py` commands

### 3. WORK WITH EXISTING CODE
- The codebase already has all necessary infrastructure
- Focus on fixing and improving what exists
- Creating new runners/scripts is NOT acceptable

## Project Overview

PyRQG (Python Random Query Generator) is a high-performance SQL query generator for database testing, supporting PostgreSQL and YugabyteDB with billion-scale capabilities. It generates diverse, syntactically correct SQL queries using a Python DSL framework.

## Architecture Overview

### Core Components

1. **DSL Framework** (`pyrqg/dsl/`)
   - `core.py`: Grammar definition primitives (Grammar, choice, template, repeat, optional)
   - `schema_aware_context.py`: Context management for schema-aware generation

2. **Production System** (`pyrqg/production/`)
   - `production_rqg.py`: Main production runner with billion-scale capabilities
   - `entropy.py`: 256-bit cryptographic randomness management
   - `threading.py`: Multi-threaded execution with batching
   - `uniqueness.py`: Bloom filter for duplicate detection
   - `data_generator.py`: Realistic data generation (names, emails, addresses)

3. **Query Execution** (`pyrqg/core/`)
   - `engine.py`: Grammar execution engine
   - `executor.py`: Database query executor
   - `filtered_executor.py`: Filtered execution with validation
   - `validator.py`: Query validation framework
   - `reporter.py`: Execution reporting

4. **PostgreSQL Compatibility** (`pyrqg/filters/`)
   - `postgres_filter.py`: Query validation and fixing for PostgreSQL
   - `schema_validator.py`: Schema-based query validation
   - `query_analyzer.py`: Query pattern analysis and error tracking

5. **Schema-Aware Generation**
   - `schema_aware_generator.py`: Generates queries based on live database schema
   - `perfect_schema_registry.py`: Schema caching and management
   - `schemas/postgres_schema.py`: PostgreSQL-specific schema handling

## Recent Updates

### Schema-Aware Query Generation
- **New Feature**: Schema-aware query generation that achieves 99.99%+ success rate
- **PostgreSQL Filter**: Smart filtering system that validates queries before execution
- **Array Type Support**: Proper handling of PostgreSQL ARRAY columns

## Common Development Commands

### Setup and Installation
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies and package
pip install -r requirements.txt
pip install -e .

# Install test dependencies
pip install pytest pytest-cov
```

### Running Tests
```bash
# Run all tests
python3 -m pytest tests/ -v

# Run specific test files
python3 -m pytest tests/test_dsl_core.py -v
python3 -m pytest tests/test_api.py -v
python3 -m pytest tests/test_production.py -v
python3 -m pytest tests/test_grammars.py -v

# Run integration tests
python3 -m pytest tests/test_integration.py -v -m integration

# Run tests with coverage
python3 -m pytest --cov=pyrqg --cov-report=html

# Quick test suite
./run_tests.sh
```

### Query Generation Commands (Universal Launcher)
```bash
# Basic query generation
python pyrqg_launcher.py generate --count 1000 --grammar dml_unique

# Generate specific query types
python pyrqg_launcher.py generate --count 100 --types SELECT INSERT

# Production-scale generation (billion queries)
python pyrqg_launcher.py production --config configs/billion_scale.conf

# Custom production run
python pyrqg_launcher.py production --queries 100000 --grammars dml_unique functions_ddl --threads 8

# Execute queries against database with monitoring
python pyrqg_launcher.py execute --database "dbname=postgres" --duration 3600 --grammars workload_insert workload_select

# Generate and execute workloads
python pyrqg_launcher.py workload --duration 60 --qps 10 --create-tables --execute --database "dbname=postgres"

# Validate queries with PyRQG validators
python pyrqg_launcher.py validate --grammar grammars/dml_unique.py --database "postgresql://localhost/test" --validators error_message performance

# Schema-aware PostgreSQL queries (NEW)
python pyrqg_launcher.py generate --count 1000 --grammar dml_unique --postgres-filter

# List available resources
python pyrqg_launcher.py list grammars
python pyrqg_launcher.py list validators
python pyrqg_launcher.py list configs
```

## High-Level Architecture

### Core DSL Framework (`pyrqg/dsl/core.py`)
The DSL provides grammar definition primitives:
- `Grammar` class: Top-level grammar container
- `choice()`: Random selection from options
- `template()`: SQL template with placeholders
- `optional()`: Include element with probability
- `repeat()`: Generate multiple instances
- `sequence()`: Ordered element generation

### Production System Architecture
1. **EntropyManager** (`production/entropy.py`): Manages 256-bit cryptographic randomness across threads
2. **ThreadPoolManager** (`production/threading.py`): Coordinates multi-threaded query generation with batching
3. **UniquenessTracker** (`production/uniqueness.py`): Bloom filter-based duplicate detection (<0.001% false positives)
4. **DynamicDataGenerator** (`production/data_generator.py`): Generates realistic correlated data (names, emails, addresses)

### PostgreSQL Compatibility Layer (NEW)
1. **PostgresFilter** (`filters/postgres_filter.py`): Validates and fixes queries for PostgreSQL
2. **SchemaAwareGenerator** (`schema_aware_generator.py`): Generates queries based on actual database schema
3. **QueryAnalyzer** (`filters/query_analyzer.py`): Analyzes query patterns and errors
4. **SchemaValidator** (`filters/schema_validator.py`): Validates queries against live schema

### Grammar Loading System
- Grammars are Python modules in `grammars/` directory
- Each grammar exports a `grammar` variable of type `Grammar`
- Grammar loader dynamically imports modules and validates structure
- Support for grammar directories: `workload/`, `yugabyte/`

### Query Generation Flow
1. **API Layer** (`api.py`): High-level interface via `RQG` class
2. **Grammar Execution** (`core/engine.py`): Processes DSL elements recursively
3. **PostgreSQL Filtering** (optional): Validates queries against PostgreSQL rules
4. **Schema Awareness** (optional): Uses actual database schema for valid queries
5. **Data Injection**: Dynamic data generator provides realistic values
6. **Uniqueness Check**: Bloom filter ensures query uniqueness
7. **Output**: Queries returned as generators for memory efficiency

### Key Design Patterns

#### Grammar Definition Pattern
```python
from pyrqg.dsl.core import Grammar, choice, template

grammar = Grammar(
    main=choice(
        template("SELECT $columns FROM $table WHERE $condition"),
        template("INSERT INTO $table ($columns) VALUES ($values)")
    ),
    columns=["id", "name", "email"],
    table=["users", "customers"],
    condition=template("$column = $value")
)
```

#### Production Configuration Pattern
```python
from pyrqg.production.configs import custom_config

config = custom_config(
    name="test_run",
    queries=100_000,
    grammars=["dml_unique", "functions_ddl"],
    threads=8,
    uniqueness=True,
    postgres_filter=True  # Enable PostgreSQL filtering
)
```

#### Schema-Aware Generation Pattern (NEW)
```python
from pyrqg.schema_aware_generator import get_schema_aware_generator

generator = get_schema_aware_generator()
# Generates queries based on actual database schema
query = generator.generate_insert("users")  # Only uses valid columns
```

### Performance Optimizations
- **Batch Processing**: Queries generated in configurable batches (default 1000)
- **Lock-Free Design**: Thread-local entropy states minimize contention
- **Memory Efficiency**: Streaming output, periodic garbage collection
- **Checkpoint System**: Resume billion-scale runs after interruption
- **Query Caching**: PostgreSQL filter caches validation results

### Database Support
- **PostgreSQL**: Full support with schema-aware generation and filtering
- **YugabyteDB**: Specialized grammars in `grammars/yugabyte/`
- Support for distributed SQL features
- Transaction patterns optimized for distributed systems
- Portable grammars work across PostgreSQL and YugabyteDB

## Important Implementation Notes

1. **Thread Safety**: All production components are thread-safe. EntropyManager uses thread-local storage.

2. **Memory Management**: Bloom filter size calculated based on target queries and false positive rate.

3. **Grammar Validation**: Grammar loader validates structure before execution to prevent runtime errors.

4. **Error Handling**: Production system gracefully handles interrupts with checkpoint saves.

5. **Data Correlation**: DynamicDataGenerator maintains realistic relationships (e.g., email matches name).

6. **Query Diversity**: 256-bit entropy ensures 2^256 possible random states for maximum diversity.

7. **PostgreSQL Compatibility**: Schema-aware generation achieves 99.99%+ success rate by using actual database metadata.

8. **Array Type Handling**: PostgreSQL ARRAY columns properly formatted as `'{item1,item2}'`

## Test Commands

### PostgreSQL Schema-Aware Test
```bash
# Run 2-hour schema-aware test
python pyrqg_launcher.py execute --database "dbname=postgres" --duration 7200 --schema-aware --report

# Analyze test results
python pyrqg_launcher.py analyze --latest
```

### Performance Benchmarks
- Query Generation: ~7,000-10,000 queries/second
- Schema-Aware Generation: ~7,200 queries/second
- PostgreSQL Filtering: ~3,000-5,000 queries/second
- Billion-scale runs: 8-12 hours on 8 cores

## Additional Development Commands

### Running Single Tests
```bash
# Run specific test function
python3 -m pytest tests/test_dsl_core.py::TestDSLCore::test_basic_template -v

# Run tests matching pattern
python3 -m pytest -k "test_template" -v

# Run with debug output
python3 -m pytest tests/test_api.py -v -s --log-cli-level=DEBUG
```

### Linting and Type Checking
```bash
# Run linting with flake8 (if available)
flake8 pyrqg/ tests/

# Run type checking with mypy (if available)
mypy pyrqg/

# Format code with black (if available)
black pyrqg/ tests/

# Sort imports with isort (if available)
isort pyrqg/ tests/
```

### Available PyRQG Commands

The `pyrqg_launcher.py` supports these commands:
- `generate` - Basic query generation with grammar support
- `production` - Large-scale query generation with threading
- `execute` - Execute queries against database with monitoring
- `workload` - Generate workload patterns (currently being refactored)
- `ddl` - DDL generation commands
- `list` - List available grammars, validators, configs
- `validate` - Validate queries with various validators
- `analyze` - Analyze query patterns and results

### Grammar Locations
- Core grammars: `grammars/*.py`
- Workload grammars: `grammars/workload/*.py`
- YugabyteDB grammars: `grammars/yugabyte/*.py`
- Example grammars: `examples/grammars/*.py`

### Virtual Environment
```bash
# Activate virtual environment
source pyrqg_venv/bin/activate  # or venv/bin/activate

# Deactivate when done
deactivate
```