from __future__ import annotations

import pytest

from pyrqg.api import create_rqg
from pyrqg.dsl.core import Grammar

CORE_GRAMMARS = [
    "ddl_focused",
    "real_workload",
    "outer_join_portable",
]


def load_grammar(name: str) -> Grammar:
    module = __import__(f"grammars.{name}", fromlist=["grammar", "g"])
    return getattr(module, "grammar", getattr(module, "g"))


class TestGrammarCatalog:
    @pytest.mark.parametrize("name", CORE_GRAMMARS)
    def test_grammar_generates(self, name: str) -> None:
        grammar = load_grammar(name)
        for seed in range(3):
            sql = grammar.generate("query", seed=seed)
            assert sql.strip()


class TestDDLGeneration:
    def test_default_schema_emits_tables(self) -> None:
        rqg = create_rqg()
        ddl = rqg.generate_ddl()
        assert ddl, "DDL generation should yield statements"
        blob = "\n".join(stmt.lower() for stmt in ddl)
        assert "create table users" in blob


class TestRealWorkloadGrammar:
    def test_emits_orders_and_revenue_queries(self) -> None:
        grammar = load_grammar("real_workload")
        queries = [grammar.generate("query", seed=i) for i in range(10)]
        assert any("FROM orders" in q for q in queries)
        assert any("FROM revenue" in q for q in queries)


class TestOuterJoinPortableGrammar:
    def test_generates_outer_join_queries(self) -> None:
        grammar = load_grammar("outer_join_portable")
        queries = [grammar.generate("query", seed=i) for i in range(6)]
        assert any("OUTER JOIN" in q for q in queries)
        assert any("GROUP BY" in q or "HAVING" in q for q in queries)
