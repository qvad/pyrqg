# PyRQG Documentation & Developer Guide

## 1. Introduction

Welcome to PyRQG, the Python Random Query Generator.

PyRQG is a powerful tool designed for testing database systems like PostgreSQL and YugabyteDB. It generates a high volume of varied and complex SQL queries, which can be used to find correctness bugs (fuzzing), identify performance regressions, or benchmark new database versions.

The core of PyRQG is a flexible, Python-native Domain-Specific Language (DSL) that allows you to define query-generating grammars in a declarative and composable way.

This guide covers how to set up the project, use its command-line interface, and extend it by writing your own grammars.

## 2. Installation and Setup

To get started with PyRQG, you need to set up a Python virtual environment and install the necessary dependencies.

**Prerequisites:**
*   Python 3.8+
*   `git`

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

    # NOTE: You do not need to "activate" the venv. The following commands
    # will use the executables directly from the venv's bin/ directory.
    ```

3.  **Install dependencies:**
    The project's dependencies are listed in `requirements.txt`. Install them into the virtual environment using `pip`. You also need to install `pytest` for validation.
    ```bash
    .venv/bin/pip install -r requirements.txt
    .venv/bin/pip install pytest
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
Quickly see the output of a grammar. This example generates 5 queries from `real_workload` and prints them to stdout.
```bash
.venv/bin/pyrqg grammar --grammar real_workload --count 5
```

**3. Initialize a Database Schema**
This is a critical step before running schema-dependent workloads. This command generates the DDL for the default schema and executes it against your target database.
```bash
.venv/bin/pyrqg ddl --execute --dsn "postgresql://user:pass@host:port/dbname"
```

**4. Generate and Execute a Single Grammar**
This runs 100 queries from the `ddl_focused` grammar against your database. It will continue even if some queries fail.
```bash
.venv/bin/pyrqg grammar --grammar ddl --count 100 --execute --continue-on-error --dsn "postgresql://user:pass@host:port/dbname"
```

**5. Run a Full Workload Test**
This is the most powerful command. It first initializes the schema, then runs 50 queries from *every* available grammar against the database.
```bash
.venv/bin/pyrqg all --init-schema --count 50 --execute --continue-on-error --dsn "postgresql://user:pass@host:port/dbname"
```

## 4. How to Write Grammars

Creating a new grammar is the primary way to extend PyRQG. A grammar is a Python file in the `grammars/` directory that defines a set of rules for generating text.

### Core Concepts

*   **`Grammar` Object:** The container for your rules. You start by creating an instance: `g = Grammar("my_new_grammar")`.
*   **Rules:** A rule is a named component that generates a piece of text. You define a rule with `g.rule("my_rule_name", <definition>)`.
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

2.  **Self-Contained (like `real_workload.py`):**
    These grammars have no external dependencies. They generate their own data on-the-fly using **Common Table Expressions (CTEs)** in a `WITH` clause. This makes them highly portable and good for testing specific SQL features without needing a pre-initialized database.

    ```python
    g.rule("with_clause", template("WITH my_data(id, value) AS (VALUES (1, 'a'), (2, 'b'))"))
g.rule("query", template("{with_clause} SELECT value FROM my_data;"))
    ```

## 5. How to Validate the Project (Running Tests)

The project includes a test suite that was significantly improved during the refactoring. Running it is the best way to validate that the core components are working correctly.

After completing the installation, run the following command from the project root:
```bash
.venv/bin/pytest -v
```

This will discover and run all tests in the `tests/` directory. All tests should pass if the project is in a good state.