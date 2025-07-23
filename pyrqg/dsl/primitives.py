"""
Shared DSL primitives for composing readable, DRY grammars.

These helpers return DSL Elements (Choice/Template/Repeat/etc.) so they can be
used directly inside grammar rule definitions without cross-grammar rule refs.
Keep primitives small and composable; avoid project-specific policy here.
"""

from .core import (
    choice,
    template,
    repeat,
    number,
    Lambda,
    Literal,
    table,
    field,
)


# -------------------------- Identifiers --------------------------

def common_table_names(names=None):
    """Return a Table element that resolves at runtime via Context"""
    return table()


def common_column_names(names=None):
    """Return a Field element that resolves at runtime via Context"""
    return field()


def unique_columns(names=None):
    """Return a Field element prioritizing ID/unique columns"""
    return field(data_type="id")


def alias_names():
    return choice("new_id", "old_value", "result", "hash_code")


def index_name_default():
    return Lambda(
        lambda ctx: f"idx_{ctx.rng.choice(['users', 'orders', 'products'])}_{ctx.rng.randint(1, 100)}"
    )


# -------------------------- Values --------------------------

def string_value_common():
    return choice(
        Literal("'active'"),
        Literal("'inactive'"),
        Literal("'pending'"),
        Literal("'completed'"),
        Lambda(lambda ctx: f"'user{ctx.rng.randint(1, 100)}@test.com'"),
        Lambda(lambda ctx: f"'Product {ctx.rng.randint(1, 100)}'"),
    )


def basic_value(include_current_ts=True):
    base = [number(1, 1000), string_value_common(), Literal("NULL"), Literal("DEFAULT")]
    if include_current_ts:
        base.append(Literal("CURRENT_TIMESTAMP"))
    return choice(*base)


def column_list_of(column_element, min_len=2, max_len=4):
    return repeat(column_element, min=min_len, max=max_len, sep=", ")


def value_list_of(value_element, min_len=2, max_len=4):
    return repeat(value_element, min=min_len, max=max_len, sep=", ")


# -------------------------- Clauses --------------------------

def basic_where_condition(column_element, value_element):
    return choice(
        template("{field} = {value}", field=column_element, value=value_element),
        template("{field} > {value}", field=column_element, value=number(1, 100)),
        template(
            "{field} IN ({values})",
            field=column_element,
            values=repeat(value_element, min=2, max=4, sep=", "),
        ),
        template("{field} IS NOT NULL", field=column_element),
    )


def id_join_condition(unique_col_element):
    return template("target.{field} = source.{field}", field=unique_col_element)


def returning_clause_basic(column_element):
    return choice(
        Literal("*"),
        column_element,
        template("{col1}, {col2}", col1=column_element, col2=column_element),
        template("{col} AS {alias}", col=column_element, alias=alias_names()),
    )

