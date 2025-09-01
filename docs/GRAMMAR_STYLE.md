# Grammar Style Guide

This guide standardizes how we write human‑readable, composable grammars.

## Principles
- Small rules, clear names: prefer `snake_case` and short building blocks.
- One entrypoint: expose a single `query` rule per grammar.
- DRY helpers: reuse `pyrqg.dsl.primitives` for identifiers, values, and clauses.
- Compose with `template`, `choice`, `repeat`, `maybe`; avoid huge SQL blobs.

## Structure (suggested)
1) Tokens/identifiers: table/column names, aliases, constraints
2) Values/expressions: numbers, strings, value lists, predicates
3) Statements: INSERT/UPDATE/DELETE/SELECT, CTEs, MERGE
4) Optional schema blocks: tables/functions/views using `pyrqg.dsl.schema_primitives`
5) Entrypoint: `rule("query", choice(..., weights=[...]))`

## Conventions
- Rules: `verb_object` (e.g., `insert_returning`, `cte_update`).
- Columns/tables: use `common_table_names()` and `common_column_names()`.
- Values: use `basic_value()` and `string_value_common()`; add DB‑specific options via `choice(...)`.
- WHERE/JOIN: use `basic_where_condition(...)` and `id_join_condition(...)`.
- Lists: use `column_list_of(...)` and `value_list_of(...)`.

## Example
```python
from pyrqg.dsl.core import Grammar, template, ref
from pyrqg.dsl.primitives import (
  common_table_names, common_column_names, basic_value,
  column_list_of, value_list_of, basic_where_condition
)

g = Grammar("dml_minimal")
g.rule("table_name", common_table_names())
g.rule("column_name", common_column_names())
g.rule("value", basic_value())
g.rule("column_list", column_list_of(ref("column_name")))
g.rule("value_list", value_list_of(ref("value")))
g.rule("where_condition", basic_where_condition(ref("column_name"), ref("value")))
g.rule("insert_simple", template("INSERT INTO {table} ({cols}) VALUES ({vals})",
    table=ref("table_name"), cols=ref("column_list"), vals=ref("value_list")))
g.rule("query", ref("insert_simple"))
```

Stick to these patterns for readability and easier reuse across grammars.

## Embedding Schemas in Grammars

Use `pyrqg.dsl.schema_primitives` to add DDL alongside queries:

```python
from pyrqg.dsl.schema_primitives import (
  random_schema_element, random_functions_element, random_views_element, schema_bundle_element
)

g.rule("random_schema", random_schema_element(num_tables=5))
g.rule("random_functions", random_functions_element(count=3, include_procedures=True))
g.rule("random_views", random_views_element(count=2))
g.rule("schema_bundle", schema_bundle_element(num_tables=5, functions=3, views=2))
```

These rules return SQL strings; join them with other templates as needed (e.g., produce a “setup + workload” bundle).
