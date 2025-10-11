"""
Schema-aware UPDATE-focused Grammar
Picks up tables/columns from the live database via PerfectSchemaRegistry.

Respects env:
  - PYRQG_DSN: DSN for psycopg2 (set by runner when --dsn is provided)
  - PYRQG_SCHEMA: target schema (default 'pyrqg', runner can set to 'public')
"""

from pyrqg.dsl.core import Grammar, choice, template, Lambda
from pyrqg.perfect_schema_registry import get_perfect_registry

g = Grammar("update_workload_schema_aware")


def _pick_table(ctx):
    reg = get_perfect_registry()
    tables = reg.get_tables()
    return ctx.rng.choice(tables) if tables else "public_dummy"


def _pick_update_columns(ctx, table):
    reg = get_perfect_registry()
    cols = reg.get_insertable_columns(table)
    if not cols:
        return []
    k = ctx.rng.randint(1, min(3, len(cols)))
    return ctx.rng.sample(cols, k)


def _where_clause(ctx, table):
    reg = get_perfect_registry()
    # Prefer id or a numeric column
    if reg.column_exists(table, 'id'):
        return f"id = {reg.get_column_value('id', ctx.rng, table)}"
    for cand in ('user_id', 'product_id', 'order_id', 'quantity', 'total', 'amount'):
        if reg.column_exists(table, cand):
            return f"{cand} = {reg.get_column_value(cand, ctx.rng, table)}"
    # Fallback: first insertable column IS NOT NULL
    cols = reg.get_insertable_columns(table)
    if cols:
        return f"{cols[0]} IS NOT NULL"
    return "1=1"


def _set_clause(ctx, table, cols):
    reg = get_perfect_registry()
    parts = []
    for c in cols:
        # Occasionally use self-referential increment for numerics
        val = reg.get_column_value(c, ctx.rng, table)
        if c.endswith('_id') or c == 'id':
            parts.append(f"{c} = {val}")
        else:
            # Hint: try additive update sometimes
            if isinstance(val, str) and val.isdigit():
                if ctx.rng.random() < 0.5:
                    parts.append(f"{c} = {c} + {val}")
                    continue
            parts.append(f"{c} = {val}")
    return ", ".join(parts) if parts else ""


def _gen_simple_update(ctx):
    table = _pick_table(ctx)
    cols = _pick_update_columns(ctx, table)
    if not cols:
        return f"UPDATE {table} SET id = id WHERE 1=0"  # harmless no-op if no cols
    set_sql = _set_clause(ctx, table, cols)
    where_sql = _where_clause(ctx, table)
    return f"UPDATE {table} SET {set_sql} WHERE {where_sql}"


def _gen_update_returning(ctx):
    table = _pick_table(ctx)
    cols = _pick_update_columns(ctx, table)
    if not cols:
        return f"UPDATE {table} SET id = id RETURNING *"  # harmless no-op
    set_sql = _set_clause(ctx, table, cols)
    where_sql = _where_clause(ctx, table)
    return f"UPDATE {table} SET {set_sql} WHERE {where_sql} RETURNING *"


def _gen_update_from(ctx):
    reg = get_perfect_registry()
    t1 = _pick_table(ctx)
    t2 = _pick_table(ctx)
    cols = _pick_update_columns(ctx, t1)
    if not cols:
        return f"UPDATE {t1} SET id = id"
    set_col = cols[0]
    # Join on id if possible
    join_col = 'id' if reg.column_exists(t1, 'id') and reg.column_exists(t2, 'id') else None
    if not join_col:
        return _gen_simple_update(ctx)
    return (
        f"UPDATE {t1} AS t SET {set_col} = s.{set_col} "
        f"FROM {t2} AS s WHERE t.{join_col} = s.{join_col}"
    )


g.rule("simple_update", Lambda(_gen_simple_update))
g.rule("update_returning", Lambda(_gen_update_returning))
g.rule("update_from", Lambda(_gen_update_from))

g.rule("query", choice(
    Lambda(_gen_simple_update),
    Lambda(_gen_update_returning),
    Lambda(_gen_update_from),
    weights=[60, 25, 15]
))

# Export
grammar = g

