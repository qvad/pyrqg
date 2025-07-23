"""
Basic CRUD Grammar
Replaces legacy QueryGenerator with a DSL-based implementation.
"""

from pyrqg.dsl.core import Grammar, Lambda, choice, template, maybe, repeat
from pyrqg.dsl.utils import pick_table

g = Grammar("basic_crud")

def _pick_and_set_table(ctx):
    t = pick_table(ctx)
    if t:
        ctx.state['table'] = t
    return t or "table1"

def _get_columns(ctx):
    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        return "*"
    table = ctx.tables[t_name]
    cols = table.get_column_names()
    if not cols:
        return "*"
    # Pick 1-all columns
    k = ctx.rng.randint(1, len(cols))
    selected = ctx.rng.sample(cols, k)
    return ", ".join(selected)

def _get_numeric_col(ctx):
    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        return "id"
    table = ctx.tables[t_name]
    nums = table.get_numeric_columns()
    if not nums:
        # Fallback to primary key if numeric, or just any column
        return table.primary_key or "id"
    return ctx.rng.choice(nums)

def _gen_insert_cols(ctx):
    t_name = ctx.state.get('table')
    if not t_name or t_name not in ctx.tables:
        return "col1"
    table = ctx.tables[t_name]
    # Exclude auto-generated PK if possible
    cols = [c.name for c in table.columns_list if not (c.is_primary_key and "GENERATED" in c.data_type)]
    if not cols:
        cols = table.get_column_names()
    
    selected = ctx.rng.sample(cols, ctx.rng.randint(1, len(cols)))
    ctx.state['insert_cols'] = selected
    return ", ".join(selected)

def _gen_insert_vals(ctx):
    t_name = ctx.state.get('table')
    cols = ctx.state.get('insert_cols', [])
    if not t_name or not cols:
        return "'val'"
    
    vals = []
    for c in cols:
        val = ctx.get_column_value(t_name, c)
        vals.append(val)
    return ", ".join(vals)

def _gen_update_set(ctx):
    t_name = ctx.state.get('table')
    if not t_name: return "col1 = 1"
    table = ctx.tables.get(t_name)
    if not table: return "col1 = 1"
    
    cols = [c.name for c in table.columns_list if not c.is_primary_key]
    if not cols: return "col1 = 1"
    
    selected = ctx.rng.sample(cols, ctx.rng.randint(1, min(3, len(cols))))
    parts = []
    for c in selected:
        val = ctx.get_column_value(t_name, c)
        parts.append(f"{c} = {val}")
    return ", ".join(parts)

# Rules
g.rule("table_name", Lambda(_pick_and_set_table))

# SELECT
g.rule("select", template("SELECT {columns} FROM {table:table_name} {where_clause} {limit}"))
g.rule("columns", Lambda(_get_columns))
g.rule("where_clause", maybe(template("WHERE {num_col} > {val}"), probability=0.5))
g.rule("num_col", Lambda(_get_numeric_col))
g.rule("val", Lambda(lambda ctx: str(ctx.rng.randint(1, 1000))))
g.rule("limit", maybe(template("LIMIT {n}"), probability=0.5))
g.rule("n", Lambda(lambda ctx: str(ctx.rng.randint(10, 100))))

# INSERT
g.rule("insert", template("INSERT INTO {table:table_name} ({insert_cols}) VALUES ({insert_vals})"))
g.rule("insert_cols", Lambda(_gen_insert_cols))
g.rule("insert_vals", Lambda(_gen_insert_vals))

# UPDATE
g.rule("update", template("UPDATE {table:table_name} SET {update_set} {where_clause}"))
g.rule("update_set", Lambda(_gen_update_set))

# DELETE
g.rule("delete", template("DELETE FROM {table:table_name} {where_clause}"))

# Main Entry Point
g.rule("query", choice("select", "insert", "update", "delete"))

grammar = g
