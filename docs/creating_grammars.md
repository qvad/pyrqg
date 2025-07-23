# How to Create Grammars in PyRQG

PyRQG uses a Python-native Domain Specific Language (DSL) to define how random queries are generated. A "grammar" is simply a Python file that uses this DSL to construct SQL statements.

## 1. The Basics

A grammar file must export a `grammar` object (an instance of `pyrqg.dsl.core.Grammar`). The entry point for generation is a rule named `"query"`.

### Minimal Example

Create a file `grammars/simple.py`:

```python
from pyrqg.dsl.core import Grammar, template, choice

# 1. Create the grammar container
g = Grammar("simple")

# 2. Define the entry point rule 'query'
# Using a template with placeholders {cols} and {table}
g.rule("query", template("SELECT {cols} FROM {table};"))

# 3. Define the supporting rules
g.rule("cols", choice("*", "id, name", "count(*)"))
g.rule("table", choice("users", "products", "orders"))

# 4. Export as 'grammar' (required by the loader)
grammar = g
```

Run it:
```bash
pyrqg grammar --grammar simple --count 5
```

## 2. DSL Elements

The DSL provides building blocks in `pyrqg.dsl.core`:

*   **`template(string, **kwargs)`**: String interpolation.
    *   `g.rule("query", template("SELECT * FROM {table}"))`
*   **`choice(*options, weights=None)`**: Randomly selects one option.
    *   `g.rule("op", choice("=", "<", ">"))`
*   **`repeat(element, min, max, sep)`**: Generates a list of elements.
    *   `g.rule("cols", repeat("col_name", min=1, max=3, sep=", "))`
*   **`maybe(element, probability)`**: Optionally includes the element.
    *   `g.rule("limit", maybe("LIMIT 10"))`
*   **`Lambda(func)`**: Executes a Python function receiving the `Context`.

## 3. Using Shared Utilities

To avoid rewriting common logic (like picking a random table or generating a unique ID), use the shared utilities in `pyrqg.dsl.utils`.

```python
from pyrqg.dsl.core import Grammar, Lambda, template
from pyrqg.dsl.utils import pick_table, random_id

g = Grammar("util_example")

# Use pick_table to get a random table name from the connected DB
g.rule("table", Lambda(pick_table))

# Use random_id to generate a unique string
g.rule("alias", Lambda(lambda ctx: f"alias_{random_id()}"))

g.rule("query", template("SELECT * FROM {table} AS {alias}"))
```

## 4. Advanced: Schema Awareness

PyRQG can introspect your database. You can access this metadata via `ctx.tables` in a `Lambda` function. `ctx.tables` is a dictionary of `Table` objects (defined in `pyrqg.core.schema`).

```python
from pyrqg.dsl.core import Grammar, Lambda

g = Grammar("schema_aware")

def pick_numeric_col(ctx):
    # Check if we have tables
    if not ctx.tables: return "id"
    
    # Pick a random table
    t_name = ctx.rng.choice(list(ctx.tables.keys()))
    table = ctx.tables[t_name]
    
    # Use helper method to find numeric columns
    nums = table.get_numeric_columns()
    if nums:
        return f"{t_name}.{ctx.rng.choice(nums)}"
    return "1"

g.rule("num_col", Lambda(pick_numeric_col))
g.rule("query", template("SELECT {num_col} * 2"))
```

## 5. Best Practices

1.  **Determinism**: Always use `ctx.rng` (which is seeded) instead of the global `random` module.
2.  **Shared Types**: If checking data types, use `pyrqg.core.types` (e.g., `is_numeric`, `is_string`) instead of hardcoded string matching.
3.  **Safety**: When generating math, handle edge cases (like division by zero) using SQL functions like `NULLIF`.