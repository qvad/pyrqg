"""
Schema-aware SELECT-focused Grammar
Uses PerfectSchemaRegistry to pick real tables/columns and build valid queries.
"""

from pyrqg.dsl.core import Grammar, choice, template, Lambda
from pyrqg.perfect_schema_registry import get_perfect_registry

g = Grammar("select_workload_schema_aware")


def _pick_table(ctx):
    reg = get_perfect_registry()
    tables = reg.get_tables()
    return ctx.rng.choice(tables) if tables else "public_dummy"


def _columns(ctx, table, kmin=1, kmax=4):
    reg = get_perfect_registry()
    cols = reg.get_insertable_columns(table)
    if not cols:
        cols = ['*']
    k = ctx.rng.randint(kmin, min(kmax, len(cols))) if cols != ['*'] else 1
    return ", ".join(ctx.rng.sample(cols, k)) if cols != ['*'] else '*'


def _where(ctx, table):
    reg = get_perfect_registry()
    for cand in ('id', 'user_id', 'product_id', 'order_id', 'quantity', 'total', 'amount', 'price'):
        if reg.column_exists(table, cand):
            return f"{cand} = {reg.get_column_value(cand, ctx.rng, table)}"
    cols = reg.get_insertable_columns(table)
    if cols:
        return f"{cols[0]} IS NOT NULL"
    return "1=1"


def _order_by(ctx, table):
    reg = get_perfect_registry()
    cols = reg.get_insertable_columns(table)
    if not cols:
        return ""
    col = ctx.rng.choice(cols)
    dirn = ctx.rng.choice(["ASC", "DESC"])
    return f" ORDER BY {col} {dirn}"


def _gen_simple_select(ctx):
    t = _pick_table(ctx)
    cols = _columns(ctx, t)
    where = _where(ctx, t)
    ob = _order_by(ctx, t)
    limit = ctx.rng.randint(10, 100)
    return f"SELECT {cols} FROM {t} WHERE {where}{ob} LIMIT {limit}"


def _gen_join_select(ctx):
    reg = get_perfect_registry()
    t1 = _pick_table(ctx)
    t2 = _pick_table(ctx)
    cols = _columns(ctx, t1)
    join_col = 'id' if reg.column_exists(t1, 'id') and reg.column_exists(t2, 'id') else None
    if not join_col:
        return _gen_simple_select(ctx)
    limit = ctx.rng.randint(10, 100)
    return (
        f"SELECT {cols} FROM {t1} a JOIN {t2} b ON a.{join_col} = b.{join_col} LIMIT {limit}"
    )


g.rule("simple_select", Lambda(_gen_simple_select))
g.rule("join_select", Lambda(_gen_join_select))
g.rule("query", choice(Lambda(_gen_simple_select), Lambda(_gen_join_select), weights=[70, 30]))

grammar = g

