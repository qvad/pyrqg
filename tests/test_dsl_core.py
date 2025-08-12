"""
Comprehensive test suite for pyrqg.dsl.core module
Tests all DSL components including Grammar, Elements, and generation
"""

import pytest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrqg.dsl.core import (
    Grammar, Element, Literal, Choice, Template, Optional, 
    Repeat, Lambda, RuleRef, Context,
    literal, choice, template, maybe, repeat, ref, 
    table, field, number, digit
)


class TestGrammar:
    """Test Grammar class functionality"""
    
    def test_grammar_creation(self):
        """Test basic grammar creation"""
        g = Grammar("test_grammar")
        assert g.name == "test_grammar"
        assert g.rules == {}
        assert g.tables == {}
        assert g.fields == []
        
    def test_grammar_without_name(self):
        """Test grammar creation without name"""
        g = Grammar()
        assert g.name == "unnamed"
        
    def test_define_tables(self):
        """Test table definition"""
        g = Grammar("test")
        g.define_tables(users=100, orders=500, products=50)
        
        assert g.tables == {"users": 100, "orders": 500, "products": 50}
        
    def test_define_fields(self):
        """Test field definition"""
        g = Grammar("test")
        g.define_fields("id", "name", "email", "created_at")
        
        assert g.fields == ["id", "name", "email", "created_at"]
        
    def test_add_rule(self):
        """Test adding rules to grammar"""
        g = Grammar("test")
        g.rule("test_rule", Literal("SELECT * FROM users"))
        
        assert "test_rule" in g.rules
        assert isinstance(g.rules["test_rule"], Literal)
        
    def test_rule_with_string(self):
        """Test adding rule with string shorthand"""
        g = Grammar("test")
        g.rule("test_rule", "SELECT * FROM users")
        
        assert isinstance(g.rules["test_rule"], Literal)
        
    def test_generate_rule(self):
        """Test generating from a rule"""
        g = Grammar("test")
        g.rule("test_rule", Literal("SELECT 1"))
        
        result = g.generate("test_rule")
        assert result == "SELECT 1"
        
    def test_generate_with_seed(self):
        """Test deterministic generation with seed"""
        g = Grammar("test")
        g.rule("test_rule", Choice("option1", "option2", "option3"))
        
        # Same seed should produce same result
        result1 = g.generate("test_rule", seed=42)
        result2 = g.generate("test_rule", seed=42)
        assert result1 == result2
        
        # Different seed should (likely) produce different result
        result3 = g.generate("test_rule", seed=123)
        # Note: might occasionally be same due to randomness
        
    def test_generate_missing_rule(self):
        """Test generating from non-existent rule"""
        g = Grammar("test")
        
        with pytest.raises(KeyError):
            g.generate("missing_rule")


class TestLiteral:
    """Test Literal element"""
    
    def test_literal_generation(self):
        """Test literal string generation"""
        lit = Literal("SELECT * FROM users")
        ctx = Context()
        
        assert lit.generate(ctx) == "SELECT * FROM users"
        
    def test_literal_immutable(self):
        """Test that literal always returns same value"""
        lit = Literal("test_value")
        ctx = Context()
        
        for _ in range(10):
            assert lit.generate(ctx) == "test_value"


class TestChoice:
    """Test Choice element"""
    
    def test_choice_basic(self):
        """Test basic choice between options"""
        ch = Choice("option1", "option2", "option3")
        ctx = Context()
        
        result = ch.generate(ctx)
        assert result in ["option1", "option2", "option3"]
        
    def test_choice_with_weights(self):
        """Test weighted choice"""
        ch = Choice("common", "rare", weights=[90, 10])
        ctx = Context(seed=42)
        
        # Generate many times and check distribution
        results = [ch.generate(Context(seed=i)) for i in range(100)]
        common_count = results.count("common")
        rare_count = results.count("rare")
        
        # Should be roughly 90/10 split (with some variance)
        assert common_count > rare_count
        
    def test_choice_single_option(self):
        """Test choice with single option"""
        ch = Choice("only_option")
        ctx = Context()
        
        assert ch.generate(ctx) == "only_option"
        
    def test_choice_empty(self):
        """Test empty choice raises error"""
        with pytest.raises(ValueError):
            Choice()


class TestTemplate:
    """Test Template element"""
    
    def test_template_basic(self):
        """Test basic template substitution"""
        tmpl = Template("SELECT {field} FROM {table}")
        ctx = Context()
        ctx.set("field", "name")
        ctx.set("table", "users")
        
        assert tmpl.generate(ctx) == "SELECT name FROM users"
        
    def test_template_with_dict_params(self):
        """Test template with dictionary parameters"""
        tmpl = Template(
            "INSERT INTO {table} ({col1}, {col2}) VALUES ({val1}, {val2})",
            table="users",
            col1="name", 
            col2="email",
            val1="'John'",
            val2="'john@example.com'"
        )
        ctx = Context()
        
        expected = "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
        assert tmpl.generate(ctx) == expected
        
    def test_template_with_elements(self):
        """Test template with Element parameters"""
        tmpl = Template(
            "SELECT * FROM {table} WHERE {field} = {value}",
            table=Choice("users", "orders"),
            field=Literal("status"),
            value=Lambda(lambda ctx: ctx.rng.randint(1, 10))
        )
        ctx = Context(seed=42)
        
        result = tmpl.generate(ctx)
        assert "SELECT * FROM" in result
        assert "WHERE status =" in result
        
    def test_template_missing_params(self):
        """Test template with missing parameters"""
        tmpl = Template("SELECT {field} FROM {table}", table="users")
        ctx = Context()
        
        # Should use placeholder for missing field
        result = tmpl.generate(ctx)
        assert result == "SELECT {field} FROM users"
        
    def test_template_labeled_params(self):
        """Test template with labeled parameters"""
        tmpl = Template(
            "SELECT {field1:column}, {field2:column} FROM {table}",
            column=Choice("id", "name", "email"),
            table="users"
        )
        ctx = Context(seed=42)
        
        result = tmpl.generate(ctx)
        # Both field1 and field2 should be replaced with column choices
        assert "SELECT" in result
        assert "FROM users" in result


class TestOptional:
    """Test Optional element"""
    
    def test_optional_basic(self):
        """Test optional element generation"""
        opt = Optional("WHERE status = 'active'")
        ctx = Context()
        
        # Generate multiple times to test both cases
        results = set()
        for i in range(20):
            ctx = Context(seed=i)
            results.add(opt.generate(ctx))
            
        # Should have both empty and non-empty results
        assert "" in results
        assert "WHERE status = 'active'" in results
        
    def test_optional_with_probability(self):
        """Test optional with custom probability"""
        opt = Optional("LIMIT 10", probability=0.9)
        
        # Generate many times
        results = []
        for i in range(100):
            ctx = Context(seed=i)
            results.append(opt.generate(ctx))
            
        # Count non-empty results
        non_empty = [r for r in results if r != ""]
        # Should be roughly 90% (with some variance)
        assert len(non_empty) > 80


class TestRepeat:
    """Test Repeat element"""
    
    def test_repeat_basic(self):
        """Test basic repeat functionality"""
        rep = Repeat(Literal("value"), min=2, max=4, sep=", ")
        ctx = Context(seed=42)
        
        result = rep.generate(ctx)
        values = result.split(", ")
        
        assert len(values) >= 2
        assert len(values) <= 4
        assert all(v == "value" for v in values)
        
    def test_repeat_fixed_count(self):
        """Test repeat with fixed count"""
        rep = Repeat(Literal("x"), min=3, max=3, sep="")
        ctx = Context()
        
        assert rep.generate(ctx) == "xxx"
        
    def test_repeat_with_choice(self):
        """Test repeat with choice element"""
        rep = Repeat(Choice("A", "B", "C"), min=3, max=5, sep="-")
        ctx = Context(seed=42)
        
        result = rep.generate(ctx)
        parts = result.split("-")
        
        assert len(parts) >= 3
        assert len(parts) <= 5
        assert all(p in ["A", "B", "C"] for p in parts)


class TestLambda:
    """Test Lambda element"""
    
    def test_lambda_basic(self):
        """Test basic lambda functionality"""
        lam = Lambda(lambda ctx: f"Random: {ctx.rng.randint(1, 100)}")
        ctx = Context(seed=42)
        
        result = lam.generate(ctx)
        assert result.startswith("Random: ")
        
    def test_lambda_deterministic(self):
        """Test lambda with same seed produces same result"""
        lam = Lambda(lambda ctx: ctx.rng.randint(1, 1000000))
        
        ctx1 = Context(seed=42)
        ctx2 = Context(seed=42)
        
        assert lam.generate(ctx1) == lam.generate(ctx2)
        
    def test_lambda_access_context(self):
        """Test lambda accessing context properties"""
        g = Grammar("test")
        g.define_tables(users=100, orders=200)
        g.define_fields("id", "name")
        
        lam = Lambda(lambda ctx: f"Tables: {list(ctx.tables.keys())}")
        ctx = Context(grammar=g)
        
        result = lam.generate(ctx)
        assert "users" in result
        assert "orders" in result


class TestRuleRef:
    """Test RuleRef element"""
    
    def test_rule_ref_basic(self):
        """Test basic rule reference"""
        g = Grammar("test")
        g.rule("table_name", Choice("users", "orders", "products"))
        g.rule("query", Template("SELECT * FROM {table}", table=RuleRef("table_name")))
        
        result = g.generate("query", seed=42)
        assert result in [
            "SELECT * FROM users",
            "SELECT * FROM orders", 
            "SELECT * FROM products"
        ]
        
    def test_rule_ref_missing(self):
        """Test rule reference to missing rule"""
        g = Grammar("test")
        ref = RuleRef("missing_rule")
        ctx = Context(grammar=g)
        
        # Should return placeholder
        assert ref.generate(ctx) == "{missing_rule}"
        
    def test_rule_ref_circular(self):
        """Test circular rule references"""
        g = Grammar("test")
        # Create circular reference
        g.rule("rule1", Template("A {ref}", ref=RuleRef("rule2")))
        g.rule("rule2", Template("B {ref}", ref=RuleRef("rule1")))
        
        # Should handle gracefully (with max depth or detection)
        # This is a known issue in current implementation


class TestContext:
    """Test Context class"""
    
    def test_context_creation(self):
        """Test context creation"""
        ctx = Context()
        assert ctx.grammar is None
        assert ctx.seed is None
        assert hasattr(ctx, 'rng')
        
    def test_context_with_seed(self):
        """Test context with seed"""
        ctx = Context(seed=42)
        assert ctx.seed == 42
        
        # Same seed should produce same random sequence
        val1 = ctx.rng.randint(1, 1000000)
        
        ctx2 = Context(seed=42)
        val2 = ctx2.rng.randint(1, 1000000)
        
        assert val1 == val2
        
    def test_context_values(self):
        """Test context value storage"""
        ctx = Context()
        
        ctx.set("key1", "value1")
        ctx.set("key2", 42)
        
        assert ctx.get("key1") == "value1"
        assert ctx.get("key2") == 42
        assert ctx.get("missing") is None
        
    def test_context_grammar_properties(self):
        """Test context accessing grammar properties"""
        g = Grammar("test")
        g.define_tables(users=100)
        g.define_fields("id", "name")
        
        ctx = Context(grammar=g)
        
        assert ctx.tables == {"users": 100}
        assert ctx.fields == ["id", "name"]


class TestHelperFunctions:
    """Test helper functions"""
    
    def test_literal_helper(self):
        """Test literal() helper function"""
        lit = literal("test")
        assert isinstance(lit, Literal)
        assert lit.generate(Context()) == "test"
        
    def test_choice_helper(self):
        """Test choice() helper function"""
        ch = choice("a", "b", "c")
        assert isinstance(ch, Choice)
        
    def test_template_helper(self):
        """Test template() helper function"""
        tmpl = template("SELECT {field}", field="name")
        assert isinstance(tmpl, Template)
        
    def test_maybe_helper(self):
        """Test maybe() helper function"""
        opt = maybe("optional text")
        assert isinstance(opt, Optional)
        
    def test_repeat_helper(self):
        """Test repeat() helper function"""
        rep = repeat(literal("x"), min=2, max=4)
        assert isinstance(rep, Repeat)
        
    def test_ref_helper(self):
        """Test ref() helper function"""
        rule_ref = ref("some_rule")
        assert isinstance(rule_ref, RuleRef)
        
    def test_table_helper(self):
        """Test table() helper function"""
        g = Grammar("test")
        g.define_tables(users=100, orders=200)
        
        tbl = table()
        ctx = Context(grammar=g, seed=42)
        
        result = tbl.generate(ctx)
        assert result in ["users", "orders"]
        
    def test_field_helper(self):
        """Test field() helper function"""
        g = Grammar("test")
        g.define_fields("id", "name", "email")
        
        fld = field()
        ctx = Context(grammar=g, seed=42)
        
        result = fld.generate(ctx)
        assert result in ["id", "name", "email"]
        
    def test_number_helper(self):
        """Test number() helper function"""
        num = number(1, 100)
        ctx = Context(seed=42)
        
        result = int(num.generate(ctx))
        assert 1 <= result <= 100
        
    def test_digit_helper(self):
        """Test digit() helper function"""
        d = digit()
        ctx = Context(seed=42)
        
        result = int(d.generate(ctx))
        assert 0 <= result <= 9


class TestIntegration:
    """Integration tests for complete grammar usage"""
    
    def test_simple_query_grammar(self):
        """Test a simple query grammar"""
        g = Grammar("simple_queries")
        g.define_tables(users=100, orders=500)
        g.define_fields("id", "name", "email", "status")
        
        g.rule("query", choice(
            ref("select_query"),
            ref("insert_query"),
            ref("update_query")
        ))
        
        g.rule("select_query", template(
            "SELECT {fields} FROM {table} WHERE {condition}",
            fields=choice("*", field(), template("{f1}, {f2}", f1=field(), f2=field())),
            table=table(),
            condition=ref("where_condition")
        ))
        
        g.rule("where_condition", template(
            "{field} = {value}",
            field=field(),
            value=choice(
                number(1, 100),
                literal("'active'"),
                literal("'inactive'")
            )
        ))
        
        g.rule("insert_query", template(
            "INSERT INTO {table} ({field1}, {field2}) VALUES ({val1}, {val2})",
            table=table(),
            field1=field(),
            field2=field(),
            val1=number(1, 100),
            val2=literal("'test'")
        ))
        
        g.rule("update_query", template(
            "UPDATE {table} SET {field} = {value} WHERE id = {id}",
            table=table(),
            field=field(),
            value=choice(number(1, 100), literal("'updated'")),
            id=number(1, 100)
        ))
        
        # Generate several queries
        for i in range(5):
            query = g.generate("query", seed=i)
            assert query  # Not empty
            assert any(keyword in query for keyword in ["SELECT", "INSERT", "UPDATE"])
            
    def test_complex_nested_grammar(self):
        """Test complex grammar with nested structures"""
        g = Grammar("complex")
        
        g.rule("query", template(
            "WITH {cte_name} AS ({cte_query}) {main_query}",
            cte_name=choice("cte1", "cte2", "temp_data"),
            cte_query=ref("simple_select"),
            main_query=ref("main_select")
        ))
        
        g.rule("simple_select", 
            "SELECT id, name FROM users WHERE status = 'active'"
        )
        
        g.rule("main_select", template(
            "SELECT * FROM {cte} JOIN orders ON {cte}.id = orders.user_id",
            cte=choice("cte1", "cte2", "temp_data")
        ))
        
        query = g.generate("query")
        assert "WITH" in query
        assert "AS" in query
        assert "JOIN" in query


class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_empty_grammar(self):
        """Test generating from empty grammar"""
        g = Grammar("empty")
        
        with pytest.raises(KeyError):
            g.generate("any_rule")
            
    def test_none_element_in_template(self):
        """Test template with None parameter"""
        tmpl = Template("SELECT {field}", field=None)
        ctx = Context()
        
        # Should handle gracefully
        result = tmpl.generate(ctx)
        assert result == "SELECT {field}"
        
    def test_recursive_template_params(self):
        """Test template with recursive parameter references"""
        # This tests the template parameter resolution
        tmpl = Template(
            "SELECT {a}, {b:a}, {c:b}",
            a=choice("col1", "col2"),
            b=choice("col3", "col4")
        )
        ctx = Context(seed=42)
        
        result = tmpl.generate(ctx)
        parts = result.split(", ")
        # All three should be from appropriate choices
        
    def test_very_long_repeat(self):
        """Test repeat with large range"""
        rep = Repeat(literal("x"), min=1, max=1000, sep="")
        ctx = Context(seed=42)
        
        result = rep.generate(ctx)
        assert 1 <= len(result) <= 1000
        
    def test_probability_edge_cases(self):
        """Test optional with edge probability values"""
        opt1 = Optional("always", probability=1.0)
        opt2 = Optional("never", probability=0.0)
        
        ctx = Context()
        
        # Always should always appear
        for _ in range(10):
            assert opt1.generate(ctx) == "always"
            
        # Never should never appear  
        for _ in range(10):
            assert opt2.generate(ctx) == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])