# PyRQG (Python Random Query Generator)

PyRQG is an enterprise-grade SQL fuzzer and benchmarking tool designed for PostgreSQL-compatible databases (PostgreSQL, YugabyteDB). It generates complex, syntactically valid, and semantically meaningful SQL queries to identify correctness bugs, robustness issues, and performance regressions.

## Key Features

*   **State-Aware Fuzzing:** Generates queries based on the actual database schema (tables, columns, types) via introspection.
*   **Complex SQL Support:** Supports recursive expressions, CTEs, window functions, set operations, and transactional logic.
*   **High Performance:** Multi-threaded execution engine with backpressure handling and connection pooling.
*   **Determinism:** Fully reproducible test runs using seed control.
*   **Flexible DSL:** A Python-native Domain Specific Language for defining custom query generation grammars.

## Installation

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-org/pyrqg.git
    cd pyrqg
    ```

2.  **Set up the environment:**
    ```bash
    python3 -m venv .venv
    .venv/bin/pip install -r requirements.txt
    .venv/bin/pip install -e .
    ```

## Usage

The primary interface is the `pyrqg` CLI.

### 1. Initialize Schema
Before running complex workloads, you typically need a database schema. You can define this yourself or let PyRQG generate a random one.

```bash
# Initialize a default random schema
pyrqg ddl --execute --dsn "postgresql://user:pass@localhost:5432/dbname"
```

### 2. Run a Fuzz Test
Run the `real_workload` grammar, which generates a mix of `SELECT`, `INSERT`, `UPDATE`, and `DELETE` statements adapted to your schema.

```bash
# Run 1000 queries with 10 threads, ignoring errors (standard fuzzing mode)
pyrqg grammar \
  --grammar real_workload \
  --count 1000 \
  --dsn "postgresql://user:pass@localhost:5432/dbname" \
  --execute \
  --continue-on-error
```

### 3. List Available Grammars
```bash
pyrqg list
```

### 4. Docker Integration
For safe, isolated testing, use the included Docker Compose setup.

```bash
# Runs the full test suite against a containerized PostgreSQL instance
docker compose up --build --abort-on-container-exit
```

## Documentation

*   **[How to Create Grammars](docs/creating_grammars.md):** Learn how to write your own query generators using the PyRQG DSL.
*   **[Project Analysis](PROJECT_ANALYSIS.md):** Detailed architectural overview and system design.

## Project Structure

*   `pyrqg/`: Core library code.
    *   `dsl/`: The grammar engine and DSL primitives.
    *   `core/`: Execution engine and schema management.
    *   `runner.py`: CLI entry point.
*   `grammars/`: Built-in grammars (e.g., `real_workload.py`).
*   `tests/`: Unit and integration tests.

## Development

To run the unit tests:
```bash
.venv/bin/pytest
```

## License
MIT