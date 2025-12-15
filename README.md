# PyRQG Documentation & Developer Guide

## 1. Introduction

Welcome to PyRQG, the Python Random Query Generator.

PyRQG is a powerful, enterprise-grade tool designed for testing database systems like PostgreSQL and YugabyteDB. It generates a high volume of varied and **highly complex, syntactically and semantically valid SQL queries** using a sophisticated, state-aware grammar. This fuzzer explores a vast space of SQL, which can be used to find correctness bugs, identify performance regressions, or benchmark new database versions under realistic, challenging workloads.

The core of PyRQG is a flexible, Python-native Domain-Specific Language (DSL) that allows you to define query-generating grammars in a declarative and composable way, now featuring recursive expression generation, robust type tracking, and advanced DDL/DML.

This guide covers how to set up the project, use its command-line interface, and extend it by writing your own grammars.

## 2. Installation and Setup

To get started with PyRQG, you need to set up a Python virtual environment and install the necessary dependencies.

**Prerequisites:**
*   Python 3.9+
*   `git`
*   `docker` and `docker compose` (for integration testing)

**Steps:**

1.  **Clone the repository (if you haven't already):**
    ```bash
    git clone <repository_url>
    cd pyrqg
    ```

2.  **Create and activate a Python virtual environment:**
    This project includes a `.venv` directory, and it's best practice to use it.
    ```bash
    # Create the virtual environment (if it doesn't exist)
    python3 -m venv .venv

    # NOTE: You do not need to "activate" the venv for the commands below.
    # The commands will use the executables directly from the venv's bin/ directory.
    ```

3.  **Install dependencies:**
    The project's dependencies are listed in `requirements.txt`. Install them into the virtual environment using `pip`. You also need to install `pytest` for validation and `psycopg2-binary` for database interaction.
    ```bash
    .venv/bin/pip install -r requirements.txt
    .venv/bin/pip install pytest psycopg2-binary
    ```

4.  **Install PyRQG in Editable Mode:**
    To make the `pyrqg` module itself available for use by the runner and tests, install it in "editable" mode (`-e`). This links the installation to your source code.
    ```bash
    .venv/bin/pip install -e .
    ```

You are now set up to run the tool and its tests.

## 3. How to Run PyRQG (The CLI)

PyRQG is controlled via the `pyrqg` command-line script, which is an entry point to the `pyrqg.runner` module.

You can get help at any time with the `--help` flag:
```bash
# Show help for the main command and list subcommands
.venv/bin/pyrqg --help

# Show help for a specific subcommand (e.g., 'grammar')
.venv/bin/pyrqg grammar --help
```

### Common Commands

Here are the primary workflows for using the tool. Note that any command that connects to a database requires the `--dsn` argument, which is a standard PostgreSQL connection string.

**1. List Available Grammars**
Shows all the query generators that are ready to use.
```bash
.venv/bin/pyrqg list
```

**2. Generate and Print Queries to Console**
Quickly see the output of a grammar. The `real_workload` grammar is now stateful, so running it directly without a database will produce DDL and queries that rely on tables created within the same run.
```bash
.venv/bin/pyrqg grammar --grammar real_workload --count 5
```

**3. Initialize a Database Schema**
This is a critical step before running schema-dependent workloads. This command generates the DDL for the default schema and executes it against your target database.
```bash
.venv/bin/pyrqg ddl --execute --dsn "postgresql://user:pass@host:port/dbname"
```

**4. Generate and Execute a Single Grammar (e.g., `real_workload`)**
This runs queries from the `real_workload` grammar against your database. It will now generate a mix of DDL, DML, and complex SELECT statements. It will continue even if some queries fail, collecting statistics.
```bash
.venv/bin/pyrqg grammar --grammar real_workload --count 100 --execute --continue-on-error --dsn "postgresql://user:pass@host:port/dbname"
```

**5. Run a Full Workload Test**
This is the most powerful command. It first initializes the schema, then runs 50 queries from *every* available grammar against the database.
```bash
.venv/bin/pyrqg all --init-schema --count 50 --execute --continue-on-error --dsn "postgresql://user:pass@host:port/dbname"
```

## 4. Advanced Grammar Development: The Enterprise-Grade `real_workload`

The `grammars/real_workload.py` has been significantly upgraded to an "Enterprise-Grade" fuzzer, generating highly complex and semantically aware SQL. This grammar is designed to rigorously stress-test database systems.

**Key Features of `real_workload.py`:**

*   **Stateful Schema (`ctx.db_state`):** The grammar maintains a persistent internal model of the created tables, their columns, and their properties (type, nullability). This ensures that generated queries always reference existing entities.
*   **Recursive Expression Generation (`_gen_expr`):** SQL expressions are built recursively, creating deep and complex Abstract Syntax Trees (ASTs). This includes:
    *   Arithmetic operations (`+`, `-`, `*`, `/`, `%`) with robust type handling and `NULLIF(denominator, 0)` for safe division.
    *   String manipulations (`LOWER`, `UPPER`, `TRIM`, `MD5`, `||` for concatenation).
    *   Boolean logic (`AND`, `OR`, `NOT`, `IS NULL`, `IS NOT NULL`).
    *   Conditional logic (`CASE WHEN ... THEN ... ELSE ... END`).
    *   Comparison operators (`=`, `<>`, `>`, `<`, `>=`, `<=`, `LIKE`, `ILIKE`, `BETWEEN`, `IN`).
*   **Strict Type Enforcement:** During expression generation, the grammar strictly tracks and enforces type compatibility to minimize `DatatypeMismatch` and `UndefinedFunction` errors.
*   **Advanced DDL (`_gen_ddl`, `_gen_index`):** Dynamically creates tables with:
    *   Varied data types (including arrays).
    *   `PRIMARY KEY` and `NOT NULL` constraints.
    *   `CHECK` constraints.
    *   Indexes (excluding types where unique indexing is problematic in some DBs).
    *   Nullability is tracked and respected.
*   **Complex Query Topologies (`_gen_complex_select`):**
    *   Generates `SELECT` statements with random joins, aggregate functions, window functions (`ROW_NUMBER`), and nested subqueries.
    *   Includes `WITH` clauses (CTEs) for modular query construction.
    *   Supports `UNION ALL`, `UNION`, `INTERSECT`, and `EXCEPT` operations, ensuring type and column count compatibility between query arms.
*   **Safe DML (`_gen_dml`):**
    *   `INSERT` statements respect `NOT NULL` constraints by ensuring valid (non-NULL) data is generated.
    *   `UPDATE` statements modify data using complex expressions.
    *   `DELETE` statements remove data.

This grammar aims to generate syntactically correct SQL that pushes the boundaries of database parsers, query planners, and execution engines, making it an invaluable tool for finding robustness issues.

## 5. Running Integration Tests (Docker Compose)

For comprehensive testing against real database instances, PyRQG provides a Docker Compose setup. This allows you to run the fuzzer against different database versions (e.g., PostgreSQL, YugabyteDB) in isolated, reproducible environments.

**Prerequisites:**
*   Docker Desktop or Docker Engine installed and running.

**Steps:**

1.  **Ensure Docker environment is ready:**
    ```bash
    docker info
    ```
    (Ensure Docker is running and you have access.)

2.  **Run the tests using Docker Compose:**
    This command will build the `pyrqg-runner` Docker image, start the database service(s) (currently PostgreSQL), wait for them to become healthy, and then execute the PyRQG test suite. The `--abort-on-container-exit` flag ensures that the entire setup is torn down once the tests complete.

    ```bash
    docker compose up --build --abort-on-container-exit --exit-code-from pyrqg-runner
    ```
    *(Note: The `yugabytedb` service is currently disabled in `docker-compose.yml` to ensure the PostgreSQL tests can run reliably due to observed stability issues with YugabyteDB in the Docker Compose setup. It can be re-enabled and debugged if needed.)*

3.  **Interpreting Test Results (Statistics Output):**
    The `pyrqg-runner` will execute a large number of queries (e.g., 100,000 for PostgreSQL) and print a detailed statistics report for each database.

    ```
    --- Starting Fuzz Test on postgres with 100000 queries ---

    === Fuzzing Statistics ===
    Total Queries: 100000
    Successful:    84259
    Failed:        15741
    Error Breakdown:
      - UndefinedFunction: 11618
      - UndefinedTable: 2174
      - StringDataRightTruncation: 133
      - DatatypeMismatch: 1598
      - NumericValueOutOfRange: 5
      - CheckViolation: 149
      - UniqueViolation: 64
    ==========================
    ```

    *   **Successful queries:** Indicate SQL that the database parsed and executed without error.
    *   **Failed queries:** Represent queries that caused an error in the database. These are often the most valuable findings for a fuzzer!
    *   **Error Breakdown:** Provides a count of specific `psycopg2.Error` types encountered. Common errors like `UndefinedFunction`, `DatatypeMismatch`, and `DivisionByZero` often point to:
        *   Edge cases in type coercion.
        *   Limitations or strictness in the database's query planner.
        *   Runtime semantic issues triggered by complex, randomized data.
        *   Unexpected behavior in the database system itself.

    A high rate of syntax errors (`SyntaxError`) from Python's perspective would indicate a bug in the grammar itself. The current grammar primarily produces valid SQL, and failures typically reflect semantic or runtime issues within the database.

## 6. How to Write Grammars (Legacy)

*(This section is from the previous version of the README.md and is kept for historical context. For advanced grammar development, refer to Section 4 and the `grammars/real_workload.py` example.)*

Creating a new grammar is the primary way to extend PyRQG. A grammar is a Python file in the `grammars/` directory that defines a set of rules for generating text.

### Core Concepts

*   **`Grammar` Object:** The container for your rules. You start by creating an instance: `g = Grammar("my_new_grammar")`.
*   **Rules:** A rule is a named component that generates a piece of text. You define a rule with `g.rule("my_rule_name", <definition>)
*   **The DSL:** The `<definition>` is created using PyRQG's simple DSL elements, imported from `pyrqg.dsl.core`.

### The DSL Elements

Here are the most important DSL elements:

*   **`choice(*options, weights=None)`**
    Selects one option at random from a list. Options can be strings or other DSL elements.
    ```python
    g.rule("status", choice("'active'", "'inactive'", "'pending'"))
    ```

*   **`template(string, **kwargs)`**
    A string with placeholders that get replaced by other rules.
    Placeholders can be `{name}` (implicitly refers to a rule named `name`) or `{key:rule}` (explicitly uses `rule` to fill the placeholder `key`).
    ```python
    g.rule("query", template("SELECT {columns} FROM {table};\n"))
    g.rule("columns", "*")
    g.rule("table", "users")
    # Generates: "SELECT * FROM users;\n"
    ```

*   **`ref("rule_name")`**
    Explicitly creates a reference to another rule. This is useful inside other elements like `choice`.
    ```python
    g.rule("query", choice(ref("select_query"), ref("update_query")))
    ```

*   **`Lambda(callable)`**
    For complex or imperative logic, you can use a Python function. The function receives a `context` object and must return a string.
    ```python
    import uuid
    def _generate_unique_id(ctx):
        return f"'{uuid.uuid4()}'"

    g.rule("unique_id", Lambda(_generate_unique_id))
    ```

*   **`repeat(element, min, max, sep)`**
    Repeats an element a random number of times between `min` and `max`, joined by `sep`.
    ```python
    g.rule("column_list", repeat(ref("column_name"), min=2, max=5, sep=", "))
    ```

*   **`maybe(element, probability)`**
    Includes the element with a given probability (0.0 to 1.0).
    ```python
    g.rule("query", template("SELECT * FROM users {maybe_limit}"))
    g.rule("maybe_limit", maybe("LIMIT 100", probability=0.5))
    ```

### Example: A Simple Grammar

Let's create a file `grammars/greetings.py`.

```python
from pyrqg.dsl.core import Grammar, choice, template, repeat, ref

# 1. Create a grammar container
g = Grammar("greetings")

# 2. Define basic rules
g.rule("greeting_word", choice("Hello", "Greetings", "Hi"))
g.rule("name", choice("Alice", "Bob", "World"))
g.rule("punctuation", choice(".", "!"))

# 3. Define a structural rule using a template
g.rule("full_greeting", template("{greeting_word}, {name}{punctuation}"))

# 4. Define the root 'query' rule
# This will be the entry point when the grammar is run.
g.rule("query", repeat(ref("full_greeting"), min=1, max=3, sep="\n"))

# 5. Export the grammar object
grammar = g
```
If you run this with `.venv/bin/pyrqg grammar --grammar greetings --count 1`, you might get an output like:
```
Hello, World!
Greetings, Alice.
```

### Schema-Aware vs. Self-Contained Grammars

*(This section is largely superseded by the advanced state-aware grammar in `real_workload.py`)*

There are two main approaches to writing grammars:

1.  **Schema-Aware (like `ddl_focused.py`):**
    These grammars rely on knowledge of a database schema. PyRQG provides a "virtual" schema via the `SchemaCatalog`. You can access it to get valid table and column names. This is the recommended approach for generating queries that should run against the default schema.

    ```python
    from pyrqg.schema_support import get_schema_catalog

    CATALOG = get_schema_catalog() # Get the offline catalog

    def _random_table(ctx):
        # Use the catalog to pick a real table name
        return ctx.rng.choice(CATALOG.list_tables())

    g.rule("any_real_table", Lambda(_random_table))
    ```

2.  **Self-Contained (like older `real_workload.py`):**
    These grammars have no external dependencies. They generated their own data on-the-fly using **Common Table Expressions (CTEs)** in a `WITH` clause. This made them highly portable and good for testing specific SQL features without needing a pre-initialized database.

## 7. How to Validate the Project (Running Tests)

The project includes a test suite that was significantly improved. Running it is the best way to validate that the core components are working correctly and to exercise the fuzzing capabilities.

**1. Running Unit Tests:**
After completing the installation, run the following command from the project root. This will discover and run all basic unit tests in the `tests/` directory.
```bash
.venv/bin/pytest -v
```

**2. Running Integration Tests (Docker Compose):**
For comprehensive, high-volume fuzz testing against real database instances (PostgreSQL, YugabyteDB), use the Docker Compose setup.

**Prerequisites:**
*   Docker Desktop or Docker Engine installed and running.

**Execution:**
This command will build the `pyrqg-runner` Docker image, start the database service(s) (currently PostgreSQL), wait for them to become healthy, and then execute the PyRQG test suite with 100,000 queries. The `--abort-on-container-exit` flag ensures that the entire setup is torn down once the tests complete.

```bash
docker compose up --build --abort-on-container-exit --exit-code-from pyrqg-runner
```
*(Note: The `yugabytedb` service is currently disabled in `docker-compose.yml` to ensure the PostgreSQL tests can run reliably due to observed stability issues with YugabyteDB in the Docker Compose setup. It can be re-enabled and debugged if needed.)*

**Interpreting Test Results (Statistics Output):**
The `pyrqg-runner` will execute a large number of queries (e.g., 100,000 for PostgreSQL) and print a detailed statistics report. This report categorizes errors encountered during query execution in the database.

Example Output:
```
--- Starting Fuzz Test on postgres with 100000 queries ---

=== Fuzzing Statistics ===
Total Queries: 100000
Successful:    84259
Failed:        15741
Error Breakdown:
  - UndefinedFunction: 11618
  - UndefinedTable: 2174
  - StringDataRightTruncation: 133
  - DatatypeMismatch: 1598
  - NumericValueOutOfRange: 5
  - CheckViolation: 149
  - UniqueViolation: 64
==========================
```

*   **Successful queries:** Indicate SQL that the database parsed and executed without error.
*   **Failed queries:** Represent queries that caused an error in the database. These are often the most valuable findings for a fuzzer! The types of errors (e.g., `UndefinedFunction`, `DatatypeMismatch`, `DivisionByZero`) highlight:
    *   Edge cases in type coercion.
    *   Limitations or strictness in the database's query planner.
    *   Runtime semantic issues triggered by complex, randomized data.
    *   Potential bugs or unexpected behavior in the database system itself.

This report is crucial for understanding the robustness of the database under a high-stress, randomized workload.

## 8. Future Work

*  Can
*   **Expanded Type Coverage:** Include more complex data types (e.g., geometric, network types, range types) in expression generation.
*   **Advanced SQL Constructs:** Add support for `MERGE`, `LATERAL JOIN`, `WINDOW` clauses in `WHERE`/`HAVING`, and `TABLESAMPLE`.
*   **Error Categorization Enhancement:** Refine error parsing to provide more actionable insights into database behavior.
*   **Performance Benchmarking Integration:** Add modules to collect and report query execution times, CPU/memory usage, etc.
*   **Grammar Configuration:** Externalize more parameters for grammar generation (e.g., depth of expressions, number of joins) for easier tuning.
