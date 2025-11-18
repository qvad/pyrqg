from __future__ import annotations

import pytest

from pyrqg.api import create_rqg
from pyrqg.dsl.core import Grammar

CORE_GRAMMARS = [
    "ddl_focused",
    "snowflake",
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


class TestSnowflakeGrammar:
    def test_includes_snowflake_statements(self) -> None:
        grammar = load_grammar("snowflake")
        queries = [grammar.generate("query", seed=i) for i in range(10)]
        assert any("ALTER WAREHOUSE" in q for q in queries)
        assert any("USE DATABASE" in q for q in queries)
