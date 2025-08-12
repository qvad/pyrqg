#!/usr/bin/env python3
"""
03_grammar_composition.py - Composing Multiple Grammars

This example demonstrates grammar composition techniques:
- Combining multiple grammars
- Grammar inheritance
- Modular grammar design
- Grammar mixins
- Dynamic grammar composition

Key concepts:
- Grammar modularity
- Rule namespacing
- Grammar extension
- Composition patterns
- Reusable components
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional, Callable

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, Element, Context, template, choice, ref, number, maybe


class BaseGrammar(Grammar):
    """Base grammar with common SQL components."""
    
    def __init__(self, name: str = "base"):
        super().__init__(name)
        self._setup_base_rules()
    
    def _setup_base_rules(self):
        """Setup common rules used across all SQL."""
        
        # Basic data types
        self.rule("integer", number(1, 10000))
        self.rule("small_int", number(1, 100))
        self.rule("big_int", number(100000, 999999))
        self.rule("boolean", choice("true", "false"))
        self.rule("null", "NULL")
        
        # Common operators
        self.rule("comparison_op", choice("=", "!=", "<", ">", "<=", ">="))
        self.rule("logical_op", choice("AND", "OR"))
        
        # Basic identifiers
        self.rule("identifier", choice(
            "id", "name", "email", "status", "created_at", 
            "updated_at", "type", "value", "data"
        ))
        
        # Common values
        self.rule("string_value", choice(
            "'test'", "'example'", "'data'", "'value'"
        ))
        
        self.rule("status_value", choice(
            "'active'", "'inactive'", "'pending'", "'completed'"
        ))
        
        self.rule("timestamp_value", choice(
            "CURRENT_TIMESTAMP",
            "CURRENT_DATE",
            "CURRENT_DATE - INTERVAL '1 day'",
            "CURRENT_DATE - INTERVAL '7 days'"
        ))


class SelectGrammar(BaseGrammar):
    """Grammar specialized for SELECT queries."""
    
    def __init__(self):
        super().__init__("select")
        self._setup_select_rules()
    
    def _setup_select_rules(self):
        """Setup SELECT-specific rules."""
        
        # SELECT clause variations
        self.rule("select_clause", choice(
            "SELECT *",
            "SELECT DISTINCT *",
            template("SELECT {columns}"),
            template("SELECT {aggregate}")
        ))
        
        self.rule("columns", choice(
            ref("identifier"),
            template("{col1}, {col2}"),
            template("{col1}, {col2}, {col3}")
        ))
        
        self.rule("col1", ref("identifier"))
        self.rule("col2", ref("identifier"))
        self.rule("col3", ref("identifier"))
        
        self.rule("aggregate", choice(
            "COUNT(*)",
            template("COUNT(DISTINCT {identifier})"),
            template("SUM({identifier})"),
            template("AVG({identifier})"),
            template("MAX({identifier})"),
            template("MIN({identifier})")
        ))
        
        # WHERE clause
        self.rule("where_clause", maybe(
            template("WHERE {condition}"),
            probability=0.7
        ))
        
        self.rule("condition", choice(
            ref("simple_condition"),
            ref("compound_condition")
        ))
        
        self.rule("simple_condition", template(
            "{identifier} {comparison_op} {value}"
        ))
        
        self.rule("compound_condition", template(
            "{condition1} {logical_op} {condition2}",
            condition1=ref("simple_condition"),
            condition2=ref("simple_condition")
        ))
        
        self.rule("value", choice(
            ref("integer"),
            ref("string_value"),
            ref("boolean"),
            ref("null")
        ))
        
        # Complete SELECT query
        self.rule("query", template(
            "{select_clause} FROM {table} {where_clause} {order_by} {limit}"
        ))
        
        self.rule("table", choice("users", "products", "orders"))
        
        self.rule("order_by", maybe(
            template("ORDER BY {identifier} {direction}"),
            probability=0.4
        ))
        
        self.rule("direction", choice("ASC", "DESC"))
        
        self.rule("limit", maybe(
            template("LIMIT {small_int}"),
            probability=0.3
        ))


class JoinGrammar(SelectGrammar):
    """Extends SelectGrammar with JOIN capabilities."""
    
    def __init__(self):
        super().__init__()
        self.name = "join"
        self._setup_join_rules()
    
    def _setup_join_rules(self):
        """Add JOIN-specific rules."""
        
        # Override query to include joins
        self.rule("query", template(
            "{select_clause} FROM {table_with_alias} {joins} {where_clause} {order_by} {limit}"
        ))
        
        self.rule("table_with_alias", choice(
            template("users u"),
            template("products p"),
            template("orders o"),
            template("categories c")
        ))
        
        self.rule("joins", choice(
            ref("single_join"),
            ref("multiple_joins"),
            weights=[70, 30]
        ))
        
        self.rule("single_join", template(
            "{join_type} {joined_table} ON {join_condition}"
        ))
        
        self.rule("multiple_joins", template(
            "{join1} {join2}",
            join1=ref("single_join"),
            join2=ref("single_join")
        ))
        
        self.rule("join_type", choice(
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL OUTER JOIN",
            weights=[50, 30, 10, 10]
        ))
        
        self.rule("joined_table", choice(
            template("products p2"),
            template("orders o2"),
            template("users u2"),
            template("order_items oi")
        ))
        
        self.rule("join_condition", choice(
            template("u.id = o.user_id"),
            template("p.id = o.product_id"),
            template("p.category_id = c.id"),
            template("o.id = oi.order_id")
        ))


class InsertGrammar(BaseGrammar):
    """Grammar for INSERT queries."""
    
    def __init__(self):
        super().__init__("insert")
        self._setup_insert_rules()
    
    def _setup_insert_rules(self):
        """Setup INSERT-specific rules."""
        
        self.rule("query", choice(
            ref("simple_insert"),
            ref("multi_insert"),
            ref("insert_select")
        ))
        
        # Simple INSERT
        self.rule("simple_insert", template(
            "INSERT INTO {table} ({columns}) VALUES ({values})"
        ))
        
        self.rule("table", choice("users", "products", "orders"))
        
        self.rule("columns", choice(
            "name, email",
            "name, price, stock",
            "user_id, product_id, quantity"
        ))
        
        self.rule("values", choice(
            template("'{name}', '{email}'"),
            template("'{product}', {price}, {stock}"),
            template("{user_id}, {product_id}, {quantity}")
        ))
        
        # Value generators
        self.rule("name", choice("John", "Jane", "Bob", "Alice"))
        self.rule("email", template("{name}@example.com"))
        self.rule("product", choice("Widget", "Gadget", "Tool"))
        self.rule("price", number(10, 1000))
        self.rule("stock", number(0, 100))
        self.rule("user_id", ref("small_int"))
        self.rule("product_id", ref("small_int"))
        self.rule("quantity", number(1, 10))
        
        # Multi-row INSERT
        self.rule("multi_insert", template(
            "INSERT INTO {table} ({columns}) VALUES {value_list}"
        ))
        
        self.rule("value_list", template(
            "({values1}), ({values2}), ({values3})",
            values1=ref("values"),
            values2=ref("values"),
            values3=ref("values")
        ))
        
        # INSERT SELECT
        self.rule("insert_select", template(
            "INSERT INTO {table} ({columns}) {select_query}"
        ))
        
        self.rule("select_query", template(
            "SELECT {columns} FROM {source_table} WHERE {condition}"
        ))
        
        self.rule("source_table", ref("table"))
        self.rule("condition", template("id < {small_int}"))


class CompositeGrammar(Grammar):
    """Composes multiple grammars into one."""
    
    def __init__(self, name: str = "composite"):
        super().__init__(name)
        self.sub_grammars = {}
    
    def add_grammar(self, prefix: str, grammar: Grammar):
        """Add a sub-grammar with a namespace prefix."""
        self.sub_grammars[prefix] = grammar
        
        # Import rules with prefix
        for rule_name, rule in grammar.rules.items():
            prefixed_name = f"{prefix}_{rule_name}"
            self.rule(prefixed_name, rule)
    
    def compose(self, *grammars: List[tuple]):
        """Compose multiple grammars."""
        for prefix, grammar in grammars:
            self.add_grammar(prefix, grammar)
    
    def create_mixed_rule(self, name: str, components: List[tuple]):
        """Create a rule that mixes components from different grammars."""
        
        def mixed_generator(ctx):
            prefix, rule = ctx.rng.choice(components)
            full_rule = f"{prefix}_{rule}"
            
            if full_rule in self.rules:
                element = self.rules[full_rule]
                if isinstance(element, Element):
                    return element.generate(ctx)
                else:
                    return str(element)
            return f"-- Error: {full_rule} not found"
        
        self.rule(name, Lambda(mixed_generator))


def demonstrate_grammar_composition():
    """Show different composition techniques."""
    
    print("Grammar Composition Examples")
    print("=" * 50)
    
    # 1. Basic Inheritance
    print("\n1. Grammar Inheritance:")
    select_grammar = SelectGrammar()
    
    print("  Base SELECT queries:")
    for i in range(3):
        print(f"  - {select_grammar.generate('query', seed=i)}")
    
    # 2. Extended Grammar
    print("\n\n2. Extended Grammar with JOINs:")
    join_grammar = JoinGrammar()
    
    print("  JOIN queries:")
    for i in range(3):
        query = join_grammar.generate('query', seed=i*10)
        print(f"  - {query[:80]}...")
    
    # 3. Composite Grammar
    print("\n\n3. Composite Grammar:")
    composite = CompositeGrammar()
    composite.compose(
        ("select", SelectGrammar()),
        ("insert", InsertGrammar())
    )
    
    # Create mixed rule
    composite.create_mixed_rule("mixed_query", [
        ("select", "query"),
        ("insert", "query")
    ])
    
    print("  Mixed queries from composite grammar:")
    for i in range(6):
        query = composite.generate("mixed_query", seed=i)
        print(f"  {i+1}. {query[:70]}...")


def create_modular_grammar_system():
    """Create a modular grammar system."""
    
    print("\n\nModular Grammar System")
    print("=" * 50)
    
    # Component grammars
    class WhereClauseGrammar(Grammar):
        """Reusable WHERE clause grammar."""
        
        def __init__(self):
            super().__init__("where")
            
            self.rule("where", maybe(
                template("WHERE {conditions}"),
                probability=0.8
            ))
            
            self.rule("conditions", choice(
                ref("simple"),
                ref("complex")
            ))
            
            self.rule("simple", template("{field} = {value}"))
            
            self.rule("complex", template(
                "({c1}) AND ({c2})",
                c1=ref("simple"),
                c2=ref("simple")
            ))
            
            self.rule("field", choice("id", "status", "type"))
            self.rule("value", choice("1", "'active'", "'test'"))
    
    class OrderByGrammar(Grammar):
        """Reusable ORDER BY grammar."""
        
        def __init__(self):
            super().__init__("orderby")
            
            self.rule("order_by", maybe(
                template("ORDER BY {order_list}"),
                probability=0.5
            ))
            
            self.rule("order_list", choice(
                ref("single_order"),
                ref("multi_order")
            ))
            
            self.rule("single_order", template(
                "{field} {dir}",
                field=choice("id", "created_at", "name"),
                dir=choice("ASC", "DESC")
            ))
            
            self.rule("multi_order", template(
                "{o1}, {o2}",
                o1=ref("single_order"),
                o2=ref("single_order")
            ))
    
    class QueryBuilder(Grammar):
        """Builds queries using modular components."""
        
        def __init__(self):
            super().__init__("builder")
            
            # Compose from modules
            self.where_module = WhereClauseGrammar()
            self.order_module = OrderByGrammar()
            
            # Import modular rules
            self._import_module(self.where_module, "where_")
            self._import_module(self.order_module, "order_")
            
            # Build queries using modules
            self.rule("select_query", template(
                "SELECT * FROM {table} {where} {order}",
                table=choice("users", "products"),
                where=ref("where_where"),
                order=ref("order_order_by")
            ))
        
        def _import_module(self, module: Grammar, prefix: str):
            """Import rules from a module with prefix."""
            for name, rule in module.rules.items():
                self.rule(f"{prefix}{name}", rule)
    
    builder = QueryBuilder()
    
    print("  Queries built from modular components:")
    for i in range(5):
        query = builder.generate("select_query", seed=i*2)
        # Clean up spaces
        query = " ".join(query.split())
        print(f"  {i+1}. {query}")


def create_grammar_factory():
    """Create a grammar factory system."""
    
    print("\n\nGrammar Factory Pattern")
    print("=" * 50)
    
    class GrammarFactory:
        """Factory for creating specialized grammars."""
        
        @staticmethod
        def create_crud_grammar(table: str, fields: List[str]) -> Grammar:
            """Create CRUD grammar for a specific table."""
            
            grammar = Grammar(f"crud_{table}")
            
            # SELECT
            grammar.rule("select", template(
                "SELECT {fields} FROM {table} WHERE {condition}",
                fields=choice("*", ", ".join(fields)),
                table=table,
                condition=template(
                    "{field} = {value}",
                    field=choice(*fields),
                    value=choice("1", "'test'")
                )
            ))
            
            # INSERT
            field_list = ", ".join(fields)
            value_list = ", ".join([f"{{{f}_value}}" for f in fields])
            
            grammar.rule("insert", template(
                f"INSERT INTO {table} ({field_list}) VALUES ({value_list})",
                **{f"{f}_value": choice("'value'", "123", "NULL") for f in fields}
            ))
            
            # UPDATE
            grammar.rule("update", template(
                "UPDATE {table} SET {assignments} WHERE {condition}",
                table=table,
                assignments=choice(*[f"{f} = {{value}}" for f in fields]),
                value=choice("'new'", "456"),
                condition="id = 1"
            ))
            
            # DELETE
            grammar.rule("delete", template(
                "DELETE FROM {table} WHERE {condition}",
                table=table,
                condition=template(
                    "{field} = {value}",
                    field=choice(*fields),
                    value=choice("'old'", "0")
                )
            ))
            
            # Main rule
            grammar.rule("query", choice(
                ref("select"),
                ref("insert"),
                ref("update"),
                ref("delete")
            ))
            
            return grammar
        
        @staticmethod
        def create_analytical_grammar(tables: List[str]) -> Grammar:
            """Create grammar for analytical queries."""
            
            grammar = Grammar("analytical")
            
            # Window functions
            grammar.rule("window_query", template(
                """SELECT 
  {column},
  {window_func} OVER (PARTITION BY {partition} ORDER BY {order}) as {alias}
FROM {table}""",
                column=choice("id", "name", "value"),
                window_func=choice("ROW_NUMBER()", "RANK()", "SUM(amount)", "AVG(value)"),
                partition=choice("category", "type", "user_id"),
                order=choice("created_at", "id", "value DESC"),
                alias=choice("rn", "rnk", "running_sum", "moving_avg"),
                table=choice(*tables)
            ))
            
            # CTEs
            grammar.rule("cte_query", template(
                """WITH {cte_name} AS (
  SELECT {columns} FROM {table1} WHERE {condition}
)
SELECT * FROM {cte_name} JOIN {table2} ON {join_condition}""",
                cte_name=choice("base_data", "filtered_results", "temp_view"),
                columns=choice("*", "id, name", "COUNT(*) as cnt"),
                table1=choice(*tables),
                table2=choice(*tables),
                condition="status = 'active'",
                join_condition="1=1"
            ))
            
            # Aggregations
            grammar.rule("agg_query", template(
                """SELECT 
  {group_by},
  {agg_funcs}
FROM {table}
GROUP BY {group_by}
HAVING {having}""",
                group_by=choice("type", "category", "DATE(created_at)"),
                agg_funcs=choice(
                    "COUNT(*) as count",
                    "SUM(amount) as total, AVG(amount) as average",
                    "MIN(value) as min_val, MAX(value) as max_val"
                ),
                table=choice(*tables),
                having=choice("COUNT(*) > 10", "SUM(amount) > 1000", "AVG(value) < 100")
            ))
            
            grammar.rule("query", choice(
                ref("window_query"),
                ref("cte_query"),
                ref("agg_query")
            ))
            
            return grammar
    
    # Use factory to create grammars
    factory = GrammarFactory()
    
    # Create CRUD grammar for users table
    users_grammar = factory.create_crud_grammar(
        "users", 
        ["id", "name", "email", "status"]
    )
    
    print("  CRUD operations for users table:")
    for i in range(4):
        query = users_grammar.generate("query", seed=i*3)
        print(f"  {i+1}. {query}")
    
    # Create analytical grammar
    analytical = factory.create_analytical_grammar(
        ["orders", "products", "users", "transactions"]
    )
    
    print("\n  Analytical queries:")
    for i in range(3):
        query = analytical.generate("query", seed=i*5)
        print(f"\n  {i+1}. {query}")


def create_dynamic_composition():
    """Demonstrate dynamic grammar composition."""
    
    print("\n\nDynamic Grammar Composition")
    print("=" * 50)
    
    class DynamicGrammar(Grammar):
        """Grammar that can be modified at runtime."""
        
        def __init__(self):
            super().__init__("dynamic")
            self.plugins = {}
        
        def register_plugin(self, name: str, plugin_func: Callable):
            """Register a plugin that adds rules."""
            self.plugins[name] = plugin_func
            plugin_func(self)
        
        def enable_feature(self, feature: str):
            """Enable a feature by adding its rules."""
            
            if feature == "json":
                self._add_json_support()
            elif feature == "uuid":
                self._add_uuid_support()
            elif feature == "encryption":
                self._add_encryption_support()
        
        def _add_json_support(self):
            """Add JSON query support."""
            self.rule("json_query", choice(
                template("SELECT data->'{field}' FROM {table}"),
                template("SELECT * FROM {table} WHERE data @> '{{{key}: {value}}}'"),
                template("UPDATE {table} SET data = jsonb_set(data, '{{field}}', '{value}')")
            ))
            
            self.rule("field", choice("name", "email", "settings"))
            self.rule("key", choice("type", "status", "active"))
            self.rule("value", choice("true", "false", '"test"'))
            self.rule("table", "json_data")
        
        def _add_uuid_support(self):
            """Add UUID support."""
            self.rule("uuid_value", choice(
                "gen_random_uuid()",
                "'550e8400-e29b-41d4-a716-446655440000'::uuid"
            ))
            
            self.rule("uuid_query", template(
                "INSERT INTO {table} (id, data) VALUES ({uuid}, {data})",
                table="uuid_table",
                uuid=ref("uuid_value"),
                data="'test data'"
            ))
        
        def _add_encryption_support(self):
            """Add encryption function support."""
            self.rule("encrypt_query", choice(
                template("SELECT pgp_sym_encrypt({data}, {key})"),
                template("SELECT pgp_sym_decrypt({encrypted}, {key})"),
                template("UPDATE {table} SET {field} = pgp_sym_encrypt({field}, {key})")
            ))
            
            self.rule("data", "'sensitive data'")
            self.rule("key", "'secret_key'")
            self.rule("encrypted", "'encrypted_value'::bytea")
            self.rule("field", choice("password", "ssn", "credit_card"))
    
    # Create and configure dynamic grammar
    dynamic = DynamicGrammar()
    
    # Add base query
    dynamic.rule("query", choice("SELECT 1"))
    
    print("  Base grammar:")
    print(f"  - {dynamic.generate('query')}")
    
    # Enable features dynamically
    print("\n  After enabling JSON support:")
    dynamic.enable_feature("json")
    dynamic.rule("query", choice(
        ref("query"),  # Keep existing
        ref("json_query")  # Add new
    ))
    
    for i in range(2):
        print(f"  - {dynamic.generate('json_query', seed=i)}")
    
    print("\n  After enabling UUID support:")
    dynamic.enable_feature("uuid")
    
    print(f"  - {dynamic.generate('uuid_query')}")
    
    # Plugin system
    def spatial_plugin(grammar):
        """Add spatial query support."""
        grammar.rule("spatial_query", choice(
            "SELECT * FROM locations WHERE ST_Distance(point, 'POINT(0 0)') < 1000",
            "SELECT ST_AsText(geom) FROM geometries",
            "INSERT INTO locations (name, point) VALUES ('test', ST_MakePoint(1.0, 2.0))"
        ))
    
    print("\n  After registering spatial plugin:")
    dynamic.register_plugin("spatial", spatial_plugin)
    
    print(f"  - {dynamic.generate('spatial_query')}")


def main():
    """Run all composition examples."""
    
    demonstrate_grammar_composition()
    create_modular_grammar_system()
    create_grammar_factory()
    create_dynamic_composition()
    
    print("\n" + "=" * 50)
    print("Grammar Composition Summary:")
    print("- Use inheritance for extending grammars")
    print("- Compose multiple grammars with namespacing")
    print("- Create modular, reusable components")
    print("- Use factories for consistent grammar creation")
    print("- Enable dynamic composition for flexibility")


if __name__ == "__main__":
    main()