"""Randgen-inspired outer join workload.

This grammar mirrors the shapes produced by the legacy randgen outer_join_portable
workload while remaining portable by synthesizing the referenced tables via CTEs.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Sequence

from pyrqg.dsl.core import Grammar, Lambda

TABLE_NAMES: Sequence[str] = (
    "A", "AA", "B", "BB", "C", "CC", "D", "DD",
    "E", "EE", "F", "FF", "G", "GG", "H", "HH",
    "I", "II", "J", "JJ", "K", "KK", "L", "LL",
    "M", "MM", "N", "NN", "O", "OO", "P", "PP",
)

NUMERIC_COLUMNS = ["pk", "col_int", "col_bigint", "col_decimal_5_2"]
NUMERIC_INDEXED = ["pk", "col_int_key", "col_bigint_key", "col_decimal_5_2_key"]
CHAR_COLUMNS = ["col_char_10", "col_varchar_1024"]
CHAR_INDEXED = ["col_char_10_key", "col_varchar_1024_key"]
def _unique(seq: Sequence[str]) -> List[str]:
    seen = set()
    unique_items: List[str] = []
    for item in seq:
        if item in seen:
            continue
        seen.add(item)
        unique_items.append(item)
    return unique_items


ALL_COLUMNS = _unique(NUMERIC_COLUMNS + NUMERIC_INDEXED + CHAR_COLUMNS + CHAR_INDEXED)
COLUMN_TYPES: Dict[str, str] = {
    "pk": "BIGINT",
    "col_int": "INT",
    "col_bigint": "BIGINT",
    "col_decimal_5_2": "NUMERIC(5,2)",
    "col_int_key": "INT",
    "col_bigint_key": "BIGINT",
    "col_decimal_5_2_key": "NUMERIC(5,2)",
    "col_char_10": "CHAR(10)",
    "col_varchar_1024": "VARCHAR(1024)",
    "col_char_10_key": "CHAR(10)",
    "col_varchar_1024_key": "VARCHAR(1024)",
}

AGG_FUNCS = ["SUM", "MIN", "MAX", "COUNT"]
HINTS = [
    "/*+ disable_hashmerge */",
    "/*+ disable_seq_or_bitmapscan disable_hashagg_or_sort */",
    "/*+ disable_seq_or_bitmapscan disable_hashagg_or_sort disable_hashmerge */",
    "/*+ IndexScanRegexp(.*) */",
]
SELECT_TAGS = ["STRAIGHT_JOIN", "SQL_SMALL_RESULT"]


@dataclass
class TableInstance:
    name: str
    alias: str


def _weighted_choice(ctx, items: Sequence[str], weights: Sequence[int]) -> str:
    total = sum(weights)
    pick = ctx.rng.randint(1, total)
    upto = 0
    for item, weight in zip(items, weights):
        upto += weight
        if pick <= upto:
            return item
    return items[-1]


def _random_table_name(ctx) -> str:
    return ctx.rng.choice(TABLE_NAMES)


def _random_number(ctx) -> str:
    roll = ctx.rng.random()
    if roll < 0.4:
        return f"{ctx.rng.randint(-900, 900)}"
    if roll < 0.8:
        return f"{ctx.rng.randint(0, 900)}.{ctx.rng.randint(0, 99):02d}"
    return "NULL"


def _random_string(ctx) -> str:
    base = ["'alpha'", "'beta'", "'gamma'", "''", "' '"]
    if ctx.rng.random() < 0.4:
        letters = ''.join(ctx.rng.choice(list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')) for _ in range(3))
        base.append(f"'{letters}'")
    if ctx.rng.random() < 0.3:
        base.append("'randgen'")
    return ctx.rng.choice(base)


def _comparator(ctx) -> str:
    return ctx.rng.choice(["=", "!=", "<", ">", "<=", ">="])


def _numeric_ref(ctx, tables: List[TableInstance], indexed: bool = False) -> str:
    alias = ctx.rng.choice(tables).alias
    column = ctx.rng.choice(NUMERIC_INDEXED if indexed else NUMERIC_COLUMNS)
    return f"{alias}.{column}"


def _char_ref(ctx, tables: List[TableInstance], indexed: bool = False) -> str:
    alias = ctx.rng.choice(tables).alias
    column = ctx.rng.choice(CHAR_INDEXED if indexed else CHAR_COLUMNS)
    return f"{alias}.{column}"


def _aggregate_expr(ctx, tables: List[TableInstance]) -> str:
    func = ctx.rng.choice(AGG_FUNCS)
    if func == "COUNT" and ctx.rng.random() < 0.3:
        inner = "*"
    else:
        target = _numeric_ref(ctx, tables)
        if ctx.rng.random() < 0.25:
            inner = f"DISTINCT {target}"
        else:
            inner = target
    return f"{func}({inner})"


def _build_tables(ctx) -> List[TableInstance]:
    count = ctx.rng.randint(3, 7)
    return [TableInstance(name=_random_table_name(ctx), alias=f"table{i+1}") for i in range(count)]


def _build_select_list(ctx, tables: List[TableInstance]):
    mode = _weighted_choice(ctx, ["simple", "mixed", "aggregate"], [2, 5, 3])
    count = ctx.rng.randint(3, 6)
    select_items: List[str] = []
    nonaggs: List[str] = []
    agg_exprs: List[str] = []
    for idx in range(count):
        alias_name = f"field{idx + 1}"
        if mode == "simple" or (mode == "mixed" and ctx.rng.random() < 0.6):
            expr = _numeric_ref(ctx, tables)
            select_items.append(f"{expr} AS {alias_name}")
            nonaggs.append(expr)
        else:
            expr = _aggregate_expr(ctx, tables)
            select_items.append(f"{expr} AS {alias_name}")
            agg_exprs.append(expr)
    if not select_items:
        expr = _numeric_ref(ctx, tables)
        select_items.append(f"{expr} AS field1")
        nonaggs.append(expr)
    return select_items, nonaggs, agg_exprs


def _build_from_clause(ctx, tables: List[TableInstance]) -> str:
    first = tables[0]
    lines = [f"{first.name} AS {first.alias}"]
    attached = [first.alias]
    for table in tables[1:]:
        partner = ctx.rng.choice(attached)
        join_type = ctx.rng.choice(["LEFT", "RIGHT", "FULL"])
        join_column = ctx.rng.choice(NUMERIC_INDEXED + CHAR_INDEXED)
        lines.append(
            f"{join_type} OUTER JOIN {table.name} AS {table.alias} ON {partner}.{join_column} = {table.alias}.{join_column}"
        )
        attached.append(table.alias)
    return "\n  ".join(lines)


def _where_clause(ctx, tables: List[TableInstance]) -> str:
    terms: List[str] = []
    clause_builders = [
        lambda: f"{_numeric_ref(ctx, tables)} {_comparator(ctx)} {_numeric_ref(ctx, tables)}",
        lambda: f"{_numeric_ref(ctx, tables)} {_comparator(ctx)} {_random_number(ctx)}",
        lambda: f"{_char_ref(ctx, tables)} {_comparator(ctx)} {_char_ref(ctx, tables)}",
        lambda: f"{_char_ref(ctx, tables)} {_comparator(ctx)} {_random_string(ctx)}",
        lambda: f"{_numeric_ref(ctx, tables)} NOT BETWEEN {_random_number(ctx)} AND {_random_number(ctx)}",
        lambda: f"{_char_ref(ctx, tables)} NOT LIKE CONCAT({_random_string(ctx)}, '%')",
        lambda: f"{_char_ref(ctx, tables)} IS NOT NULL",
        lambda: f"{_numeric_ref(ctx, tables)} IN ({', '.join(_random_number(ctx) for _ in range(3))})",
    ]
    for _ in range(ctx.rng.randint(0, 3)):
        terms.append(ctx.rng.choice(clause_builders)())
    if not terms:
        return ""
    connector = ctx.rng.choice([" AND ", " OR "])
    return "WHERE " + connector.join(terms)


def _group_by_clause(ctx, nonaggs: List[str], agg_exprs: List[str]) -> str:
    if not nonaggs:
        return ""
    if agg_exprs or len(nonaggs) > 1 or (nonaggs and ctx.rng.random() < 0.5):
        return "GROUP BY " + ", ".join(nonaggs)
    return ""


def _having_clause(ctx, has_group_by: bool, agg_exprs: List[str]) -> str:
    if not agg_exprs or (not has_group_by and ctx.rng.random() < 0.5):
        return ""
    target = ctx.rng.choice(agg_exprs)
    return f"HAVING {target} {_comparator(ctx)} {_random_number(ctx)}"


def _order_by_clause(ctx, select_aliases: List[str]) -> str:
    if not select_aliases or ctx.rng.random() < 0.3:
        return ""
    shuffled = select_aliases[:]
    ctx.rng.shuffle(shuffled)
    count = ctx.rng.randint(1, len(shuffled))
    items = []
    for alias in shuffled[:count]:
        direction = ctx.rng.choice(["", " DESC"])
        items.append(f"{alias}{direction}")
    return "ORDER BY " + ", ".join(items)


def _limit_clause(ctx) -> str:
    if ctx.rng.random() < 0.5:
        return ""
    limit = ctx.rng.choice([1, 2, 10, 100, 1000])
    clause = f"LIMIT {limit}"
    if ctx.rng.random() < 0.3:
        clause += f" OFFSET {ctx.rng.randint(0, 9)}"
    return clause


def _select_intro(ctx) -> str:
    tokens = ["SELECT"]
    if ctx.rng.random() < 0.3:
        tokens.append("DISTINCT")
    tags = [tag for tag in SELECT_TAGS if ctx.rng.random() < 0.3]
    if tags:
        tokens.append("/* " + " ".join(tags) + " */")
    return " ".join(tokens)


def _hint_block(ctx) -> str:
    if ctx.rng.random() < 0.5:
        return ""
    return ctx.rng.choice(HINTS)


def _build_table_cte(ctx, table_name: str) -> str:
    rows = []
    for _ in range(ctx.rng.randint(3, 6)):
        values = []
        for column in ALL_COLUMNS:
            kind = COLUMN_TYPES[column]
            if "CHAR" in kind:
                literal = _random_string(ctx)
            else:
                literal = _random_number(ctx)
            literal = literal.strip()
            if literal.upper() == "NULL":
                values.append(f"NULL::{kind}")
            else:
                values.append(f"{literal}::{kind}")
        rows.append(f"({', '.join(values)})")
    columns = ", ".join(ALL_COLUMNS)
    values_blob = ",\n        ".join(rows)
    return (
        f"{table_name} AS (\n"
        f"    SELECT * FROM (VALUES\n"
        f"        {values_blob}\n"
        f"    ) AS src({columns})\n"
        ")"
    )


def _build_query(ctx) -> str:
    tables = _build_tables(ctx)
    select_items, nonaggs, agg_exprs = _build_select_list(ctx, tables)
    select_intro = _select_intro(ctx)
    select_aliases = [f"field{i+1}" for i in range(len(select_items))]
    group_by = _group_by_clause(ctx, nonaggs, agg_exprs)
    having = _having_clause(ctx, bool(group_by), agg_exprs)
    ctes = sorted({table.name for table in tables})
    cte_sql = ",\n".join(_build_table_cte(ctx, name) for name in ctes)
    parts: List[str] = []
    hint = _hint_block(ctx)
    if hint:
        parts.append(hint)
    parts.append(f"WITH {cte_sql}")
    parts.append(f"{select_intro} {', '.join(select_items)}")
    parts.append("FROM " + _build_from_clause(ctx, tables))
    where_clause = _where_clause(ctx, tables)
    if where_clause:
        parts.append(where_clause)
    if group_by:
        parts.append(group_by)
    if having:
        parts.append(having)
    order_by = _order_by_clause(ctx, select_aliases)
    if order_by:
        parts.append(order_by)
    limit_clause = _limit_clause(ctx)
    if limit_clause:
        parts.append(limit_clause)
    return "\n".join(parts)


g = Grammar("outer_join_portable")

g.rule("query", Lambda(_build_query))

grammar = g
