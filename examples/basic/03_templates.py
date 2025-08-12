#!/usr/bin/env python3
"""
03_templates.py - Template System Deep Dive

This example explores PyRQG's template system:
- Basic placeholder substitution
- Nested templates
- Dynamic value generation
- Template composition
- Advanced placeholder patterns

Key concepts:
- Template syntax
- Placeholder resolution
- Value injection
- Template reusability
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, template, choice, number, maybe, repeat, ref, Lambda


def basic_templates():
    """Demonstrate basic template usage."""
    
    print("Basic Template Examples")
    print("=" * 50)
    
    grammar = Grammar("basic_templates")
    
    # Simple template with literal values
    grammar.rule("simple", template(
        "SELECT {columns} FROM {table}",
        columns="id, name",
        table="users"
    ))
    
    print("1. Simple template with literals:")
    print(f"   {grammar.generate('simple')}")
    
    # Template with element values
    grammar.rule("with_elements", template(
        "SELECT * FROM {table} WHERE id = {id}",
        table=choice("users", "products", "orders"),
        id=number(1, 100)
    ))
    
    print("\n2. Template with elements:")
    for i in range(3):
        print(f"   {grammar.generate('with_elements', seed=i)}")
    
    # Template with rule references
    grammar.rule("table_name", choice("customers", "employees", "departments"))
    grammar.rule("with_refs", template(
        "DELETE FROM {table_name} WHERE created_at < {date}",
        date="'2024-01-01'"
    ))
    
    print("\n3. Template with rule references:")
    for i in range(3):
        print(f"   {grammar.generate('with_refs', seed=i+10)}")


def nested_templates():
    """Show nested template patterns."""
    
    print("\n\nNested Template Examples")
    print("=" * 50)
    
    grammar = Grammar("nested_templates")
    
    # Define nested components
    grammar.rule("column_def", template(
        "{name} {type} {constraints}",
        name=choice("id", "name", "email", "created_at"),
        type=choice("INTEGER", "VARCHAR(255)", "TIMESTAMP"),
        constraints=maybe("NOT NULL", 0.7)
    ))
    
    grammar.rule("create_table", template(
        "CREATE TABLE {table_name} (\n  {columns}\n)",
        table_name=choice("users", "products"),
        columns=repeat(ref("column_def"), min=2, max=4, separator=",\n  ")
    ))
    
    print("1. Nested CREATE TABLE:")
    print(grammar.generate("create_table", seed=42))
    
    # Multi-level nesting
    grammar.rule("where_condition", template(
        "{column} {op} {value}",
        column=choice("status", "type", "category"),
        op=choice("=", "!=", "IN"),
        value=choice("'active'", "'pending'", "('A', 'B', 'C')")
    ))
    
    grammar.rule("complex_where", template(
        "WHERE {primary} AND ({secondary})",
        primary=ref("where_condition"),
        secondary=template(
            "{cond1} OR {cond2}",
            cond1=ref("where_condition"),
            cond2=ref("where_condition")
        )
    ))
    
    grammar.rule("complex_query", template(
        "SELECT * FROM {table} {where}",
        table="orders",
        where=ref("complex_where")
    ))
    
    print("\n2. Multi-level nested query:")
    print(grammar.generate("complex_query", seed=123))


def dynamic_templates():
    """Demonstrate dynamic template generation."""
    
    print("\n\nDynamic Template Examples")
    print("=" * 50)
    
    grammar = Grammar("dynamic_templates")
    
    # Lambda-based dynamic values
    grammar.rule("current_timestamp", Lambda(
        lambda ctx: "CURRENT_TIMESTAMP - INTERVAL '{} days'".format(
            ctx.rng.randint(1, 30)
        )
    ))
    
    grammar.rule("dynamic_query", template(
        "SELECT * FROM events WHERE created_at > {timestamp}",
        timestamp=ref("current_timestamp")
    ))
    
    print("1. Dynamic timestamp generation:")
    for i in range(3):
        print(f"   {grammar.generate('dynamic_query', seed=i)}")
    
    # Context-aware templates
    grammar.define_tables(
        small_table=100,
        medium_table=10000,
        large_table=1000000
    )
    
    grammar.rule("smart_query", Lambda(lambda ctx: 
        f"SELECT * FROM {ctx.rng.choice(list(ctx.tables.keys()))} "
        f"{'TABLESAMPLE SYSTEM (1)' if max(ctx.tables.values()) > 100000 else ''}"
    ))
    
    print("\n2. Context-aware query:")
    print(f"   {grammar.generate('smart_query')}")
    
    # Template factory pattern
    def create_insert_template(table, columns):
        """Factory function to create INSERT templates."""
        col_list = ", ".join(columns)
        val_placeholders = ", ".join([f"{{{col}_value}}" for col in columns])
        
        return template(
            f"INSERT INTO {table} ({col_list}) VALUES ({val_placeholders})",
            **{f"{col}_value": choice("'test'", "123", "NULL") for col in columns}
        )
    
    grammar.rule("user_insert", create_insert_template("users", ["name", "email", "age"]))
    
    print("\n3. Template factory pattern:")
    print(f"   {grammar.generate('user_insert', seed=99)}")


def template_composition():
    """Show how to compose templates for complex queries."""
    
    print("\n\nTemplate Composition Examples")
    print("=" * 50)
    
    grammar = Grammar("composition")
    
    # Build query from components
    grammar.rule("select_clause", template(
        "SELECT {distinct} {columns}",
        distinct=maybe("DISTINCT", 0.2),
        columns=choice("*", "id, name", "COUNT(*)")
    ))
    
    grammar.rule("from_clause", template(
        "FROM {table} {alias}",
        table=choice("users", "orders", "products"),
        alias=maybe(choice("u", "o", "p"), 0.5)
    ))
    
    grammar.rule("where_clause", maybe(template(
        "WHERE {conditions}",
        conditions=choice(
            "id > 100",
            "status = 'active'",
            "created_at >= CURRENT_DATE - INTERVAL '7 days'"
        )
    ), 0.7))
    
    grammar.rule("order_clause", maybe(template(
        "ORDER BY {column} {direction}",
        column=choice("id", "created_at", "name"),
        direction=choice("ASC", "DESC")
    ), 0.4))
    
    grammar.rule("limit_clause", maybe(template(
        "LIMIT {count}",
        count=choice(10, 50, 100)
    ), 0.3))
    
    # Compose all parts
    grammar.rule("full_query", template(
        "{select} {from} {where} {order} {limit}",
        select=ref("select_clause"),
        from=ref("from_clause"),
        where=ref("where_clause"),
        order=ref("order_clause"),
        limit=ref("limit_clause")
    ))
    
    print("Composed queries:")
    for i in range(5):
        query = grammar.generate("full_query", seed=i*7)
        # Clean up extra spaces
        query = " ".join(query.split())
        print(f"\n{i+1}. {query}")


def advanced_patterns():
    """Advanced template patterns and techniques."""
    
    print("\n\nAdvanced Template Patterns")
    print("=" * 50)
    
    grammar = Grammar("advanced")
    
    # Recursive templates (be careful!)
    grammar.rule("expression", choice(
        ref("simple_expr"),
        ref("complex_expr")
    ))
    
    grammar.rule("simple_expr", choice(
        template("{column}"),
        template("{value}"),
        template("{function}({column})")
    ))
    
    grammar.rule("complex_expr", template(
        "({expr1} {op} {expr2})",
        expr1=ref("simple_expr"),  # Don't recurse here to avoid infinite loop
        op=choice("+", "-", "*", "/", "||"),
        expr2=ref("simple_expr")
    ))
    
    grammar.rule("column", choice("price", "quantity", "tax_rate"))
    grammar.rule("value", choice("10", "1.5", "0.08"))
    grammar.rule("function", choice("ABS", "ROUND", "CEIL"))
    
    print("1. Expression generation:")
    for i in range(3):
        print(f"   {grammar.generate('expression', seed=i+50)}")
    
    # Multi-line template formatting
    grammar.rule("formatted_query", template("""
WITH monthly_sales AS (
  SELECT 
    DATE_TRUNC('month', order_date) AS month,
    SUM(total) AS revenue
  FROM orders
  WHERE order_date >= {start_date}
  GROUP BY 1
)
SELECT 
  month,
  revenue,
  LAG(revenue) OVER (ORDER BY month) AS prev_month,
  revenue - LAG(revenue) OVER (ORDER BY month) AS growth
FROM monthly_sales
ORDER BY month DESC
LIMIT {limit}""".strip(),
        start_date="'2024-01-01'",
        limit=choice(6, 12)
    ))
    
    print("\n2. Multi-line formatted query:")
    print(grammar.generate("formatted_query", seed=77))
    
    # Conditional templates
    def conditional_template(include_joins=True):
        if include_joins:
            return template(
                "SELECT {cols} FROM {t1} JOIN {t2} ON {condition}",
                cols="*",
                t1="orders",
                t2="users",
                condition="orders.user_id = users.id"
            )
        else:
            return template(
                "SELECT {cols} FROM {table}",
                cols="*",
                table="orders"
            )
    
    grammar.rule("conditional", conditional_template(include_joins=True))
    
    print("\n3. Conditional template:")
    print(f"   {grammar.generate('conditional')}")


def template_debugging():
    """Tips for debugging template issues."""
    
    print("\n\nTemplate Debugging Tips")
    print("=" * 50)
    
    print("""
Common template issues and solutions:

1. Undefined placeholders:
   Problem:  template("SELECT {column} FROM {table}")  # 'column' not defined
   Solution: Add column definition or pass as parameter
   
2. Circular references:
   Problem:  rule("a", template("{b}")); rule("b", template("{a}"))
   Solution: Add base cases to break cycles
   
3. Extra spaces:
   Problem:  "SELECT  * FROM  users  WHERE  id = 1"
   Solution: Use " ".join(query.split()) to normalize
   
4. Missing values:
   Problem:  Template generates "{placeholder}" in output
   Solution: Ensure all placeholders have definitions
   
5. Type mismatches:
   Problem:  Numeric placeholders with quotes
   Solution: Use appropriate generators for each type

Debugging helpers:
- Use fixed seeds for reproducible results
- Print intermediate rule values
- Test rules in isolation
- Check grammar.rules.keys() for available rules
""")


def main():
    """Run all template examples."""
    
    basic_templates()
    nested_templates()
    dynamic_templates()
    template_composition()
    advanced_patterns()
    template_debugging()
    
    print("\n" + "=" * 50)
    print("Template System Summary:")
    print("- Templates provide flexible query patterns")
    print("- Placeholders can be literals, elements, or rule references")
    print("- Templates can be nested and composed")
    print("- Dynamic generation enables context-aware queries")
    print("- Careful design prevents circular references")


if __name__ == "__main__":
    main()