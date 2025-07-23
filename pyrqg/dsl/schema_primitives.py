"""
Schema-generation elements to embed DDL into grammars.

Provides Lambda-based elements that produce SQL strings for:
- Random table schemas (via DDLGenerator)
- Functions/procedures (via functions_ddl grammar if available; fallback otherwise)
- Simple views (derived from table names)
- Bundles that combine tables + functions + views
"""

from typing import List, Optional

from .core import Lambda


def _join_sql(stmts: List[str]) -> str:
    stmts = [s.strip().rstrip(";") for s in stmts if s and s.strip()]
    if not stmts:
        return ""
    return ";\n".join(stmts) + ";"


def random_schema_element(
    num_tables: int = 5,
    dialect: str = "postgresql",
    profile: str = "core",
    fk_ratio: float = 0.3,
    index_ratio: float = 0.7,
    composite_index_ratio: float = 0.3,
    partial_index_ratio: float = 0.2,
):
    """Return a Lambda element that generates CREATE TABLE (+indexes/constraints) SQL."""

    def _gen(ctx) -> str:
        try:
            from pyrqg.ddl_generator import DDLGenerator
            seed = getattr(ctx, "seed", None)
            gen = DDLGenerator(
                dialect=dialect,
                seed=seed,
                profile=profile,
                fk_ratio=fk_ratio,
                index_ratio=index_ratio,
                composite_index_ratio=composite_index_ratio,
                partial_index_ratio=partial_index_ratio,
            )
            stmts = gen.generate_schema(num_tables)
            return _join_sql(stmts)
        except Exception:
            return "-- failed to generate schema"

    return Lambda(_gen)


def random_functions_element(count: int = 3, include_procedures: bool = True):
    """Return a Lambda element that generates function/procedure DDL.
    Tries grammars.functions_ddl; falls back to simple SQL functions.
    """

    def _fallback(ctx) -> List[str]:
        out: List[str] = []
        rng = ctx.rng
        for i in range(count):
            fn = f"fn_auto_{rng.randint(1000, 9999)}"
            body = f"SELECT {rng.randint(1, 100)};"
            out.append(
                f"CREATE OR REPLACE FUNCTION {fn}() RETURNS INT LANGUAGE SQL AS $$ {body} $$"
            )
            if include_procedures and (i % 3 == 2):
                pr = f"pr_auto_{rng.randint(1000, 9999)}"
                out.append(
                    f"CREATE OR REPLACE PROCEDURE {pr}() LANGUAGE PLPGSQL AS $$ BEGIN PERFORM {fn}(); END $$"
                )
        return out

    def _gen(ctx) -> str:
        try:
            # Try to import the functions grammar dynamically
            from importlib import import_module
            mod = import_module('grammars.functions_ddl')
            g = getattr(mod, 'g', None)
            if g is None:
                raise ImportError('functions grammar missing g')
            out: List[str] = []
            base_seed = getattr(ctx, 'seed', None) or ctx.rng.randint(1, 1_000_000)
            for i in range(count):
                rule = 'create_procedure' if include_procedures and (i % 3 == 2) else 'create_function'
                out.append(g.generate(rule, seed=base_seed + i))
            return _join_sql(out)
        except Exception:
            return _join_sql(_fallback(ctx))

    return Lambda(_gen)


def random_views_element(count: int = 2, table_names: Optional[List[str]] = None):
    """Return a Lambda element that generates simple CREATE VIEW statements.
    If table_names is None, uses common e-commerce style tables.
    """
    default_tables = table_names or ["users", "orders", "products", "inventory"]

    def _gen(ctx) -> str:
        rng = ctx.rng
        out: List[str] = []
        for _ in range(max(0, count)):
            t = rng.choice(default_tables)
            v = f"v_{t}_{rng.randint(1, 999)}"
            if t == "orders":
                out.append(
                    f"CREATE OR REPLACE VIEW {v} AS SELECT user_id, COUNT(*) AS order_count FROM {t} GROUP BY user_id"
                )
            elif t == "products":
                out.append(
                    f"CREATE OR REPLACE VIEW {v} AS SELECT id, name, price FROM {t} WHERE status = 'active'"
                )
            else:
                out.append(
                    f"CREATE OR REPLACE VIEW {v} AS SELECT * FROM {t}"
                )
        return _join_sql(out)

    return Lambda(_gen)


def schema_bundle_element(
    num_tables: int = 5,
    functions: int = 3,
    views: int = 2,
    dialect: str = "postgresql",
    profile: str = "core",
    fk_ratio: float = 0.3,
    index_ratio: float = 0.7,
    composite_index_ratio: float = 0.3,
    partial_index_ratio: float = 0.2,
):
    """Return a Lambda element that concatenates tables + functions + views into a single SQL bundle."""

    def _gen(ctx) -> str:
        schema_sql = random_schema_element(
            num_tables=num_tables,
            dialect=dialect,
            profile=profile,
            fk_ratio=fk_ratio,
            index_ratio=index_ratio,
            composite_index_ratio=composite_index_ratio,
            partial_index_ratio=partial_index_ratio,
        ).generate(ctx)
        funcs_sql = random_functions_element(count=functions).generate(ctx)
        views_sql = random_views_element(count=views).generate(ctx)
        parts = [s for s in [schema_sql, funcs_sql, views_sql] if s and s.strip()]
        return "\n\n".join(parts)

    return Lambda(_gen)
