# AI Interaction Guidelines for PyRQG

This document serves as the primary reference for AI agents working on the PyRQG project. It consolidates architectural details, operational guidelines, and specific instructions for maintaining and extending the codebase.

## 1. Project Context

**PyRQG** (Python Random Query Generator) is a SQL fuzzer designed to stress-test PostgreSQL-compatible databases (PostgreSQL, YugabyteDB). It generates high volumes of complex, syntactically valid SQL to uncover correctness bugs, crashes, and performance regressions.

### Key Characteristics
*   **DSL-Based:** Queries are generated via a Python-native Domain Specific Language (DSL).
*   **State-Aware:** The system can introspect a database schema to generate semantically valid queries (e.g., joining on correct keys, respecting types).
*   **Deterministic:** All generation is seeded (`--seed`), allowing for reproducible crash reports.
*   **Multi-Threaded:** The runner executes queries in parallel to maximize throughput.

## 2. Operational Guidelines

### 2.1. Environment & Commands
*   **Virtual Environment:** Always use the `.venv` directory. Do not modify the system Python.
    *   Command prefix: `.venv/bin/python` or `.venv/bin/pip`.
*   **Package Installation:**
    *   Install dependencies: `.venv/bin/pip install -r requirements.txt`
    *   Install package in editable mode: `.venv/bin/pip install -e .`
*   **Testing:**
    *   Unit/Integration tests: `.venv/bin/pytest -v`
    *   Docker Integration (Full Stack): `docker compose up --build --abort-on-container-exit --exit-code-from pyrqg-runner`

### 2.2. Code Style & Conventions
*   **Language:** Python 3.8+
*   **Type Hinting:** Use standard `typing` (e.g., `List`, `Optional`, `Dict`).
*   **Docstrings:** Required for all public modules, classes, and functions.
*   **Imports:** Absolute imports preferred (e.g., `from pyrqg.dsl.core import ...`).

### 2.3. Safety & Stability
*   **Do Not Break Existing Grammars:** The `real_workload.py` grammar is critical. Modify it with extreme caution and always verify with `pyrqg grammar --grammar real_workload --count 10`.
*   **Database Safety:** When running against a DB (`--execute`), the fuzzer generates massive load. Ensure the target is a test instance.
*   **Connection Handling:** The runner automatically handles `OperationalError` (reconnects). Do not suppress these errors in the grammar logic unless intentional.

## 3. Architecture & Components

### 3.1. Directory Structure
*   `pyrqg/dsl/`: The core grammar engine (`core.py`) and primitives (`primitives.py`).
*   `pyrqg/core/`: Execution engine (`execution.py`) and schema definitions (`schema.py`).
*   `pyrqg/api.py`: High-level library facade (`RQG`, `QueryGenerator`).
*   `pyrqg/runner.py`: CLI entry point.
*   `grammars/`: Definition files for query generation.

### 3.2. The DSL Engine (`pyrqg.dsl.core`)
*   **`Grammar`**: Container for rules.
*   **`Context`**: Holds `rng` (random), `state` (dict), and `tables` (schema metadata).
*   **Elements**:
    *   `template("SELECT {cols} FROM {table}")`: String interpolation.
    *   `choice("a", "b")`: Random selection.
    *   `Lambda(func)`: Executes a Python function for complex logic.
    *   `repeat(elem)`: Generates a list.

### 3.3. The Runner (`pyrqg.runner`)
*   **Entry Point:** `pyrqg` (via `entry_points` in `pyproject.toml`).
*   **Subcommands:**
    *   `list`: Show grammars.
    *   `grammar`: Run a specific grammar.
    *   `all`: Run all grammars.
    *   `ddl`: Generate schema operations.

## 4. Workflows for AI Agents

### 4.1. Creating a New Grammar
1.  **File Location:** Create `grammars/<name>.py`.
2.  **Structure:**
    ```python
    from pyrqg.dsl.core import Grammar, template, choice
    g = Grammar("my_grammar")
    g.rule("query", template("SELECT 1"))
    grammar = g  # Export required
    ```
3.  **Verification:** Run `.venv/bin/pyrqg grammar --grammar my_grammar --count 5`.

### 4.2. Modifying the DSL
1.  **Locate:** `pyrqg/dsl/core.py`.
2.  **Implement:** Add new classes inheriting from `Element`.
3.  **Test:** Add unit tests in `tests/test_dsl.py`.

### 4.3. Debugging Execution Issues
1.  **Logs:** Use `--verbose` (prints all queries) or `--log-errors` (prints only failures).
2.  **Reproduction:** Always ask for or capture the `--seed` value.
3.  **Isolation:** Simplify the grammar to the smallest rule that reproduces the issue.

## 5. Common CLI Reference
```bash
# Basic Dry Run
.venv/bin/pyrqg grammar --grammar real_workload --count 5

# Full Fuzzing Run (PostgreSQL/YugabyteDB)
.venv/bin/pyrqg grammar \
  --grammar real_workload \
  --count 1000 \
  --execute \
  --continue-on-error \
  --dsn "postgresql://postgres:postgres@localhost:5432/postgres"

# Introspect Schema & Run
.venv/bin/pyrqg all --init-schema --count 50 --execute --dsn "..."
```