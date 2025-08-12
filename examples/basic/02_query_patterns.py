#!/usr/bin/env python3
"""
02_query_patterns.py - Common SQL Query Patterns

This example demonstrates various SQL query patterns:
- Complex WHERE clauses
- JOIN operations
- Aggregations and GROUP BY
- Subqueries
- ORDER BY and LIMIT

Key concepts:
- Weighted choices for realistic distributions
- Optional elements with probabilities
- Nested templates
- Complex condition generation
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, number, maybe, repeat, ref


def create_pattern_grammar():
    """Create grammar with common SQL query patterns."""
    
    grammar = Grammar("query_patterns")
    
    # Define schema
    grammar.define_tables(
        users=10000,
        orders=50000,
        products=1000,
        order_items=100000,
        categories=50
    )
    
    # ========== Complex WHERE Clauses ==========
    
    grammar.rule("where_clause", maybe(
        template("WHERE {conditions}"),
        probability=0.8  # 80% of queries have WHERE
    ))
    
    grammar.rule("conditions", choice(
        ref("simple_condition"),
        ref("and_conditions"),
        ref("or_conditions"),
        ref("complex_conditions"),
        weights=[50, 25, 15, 10]  # Prefer simple conditions
    ))
    
    grammar.rule("simple_condition", choice(
        template("{column} = {value}"),
        template("{column} != {value}"),
        template("{column} > {value}"),
        template("{column} < {value}"),
        template("{column} BETWEEN {value} AND {value2}"),
        template("{column} IN ({value_list})"),
        template("{column} LIKE '{pattern}'"),
        template("{column} IS NULL"),
        template("{column} IS NOT NULL")
    ))
    
    grammar.rule("and_conditions", template(
        "{condition1} AND {condition2}",
        condition1=ref("simple_condition"),
        condition2=ref("simple_condition")
    ))
    
    grammar.rule("or_conditions", template(
        "{condition1} OR {condition2}",
        condition1=ref("simple_condition"),
        condition2=ref("simple_condition")
    ))
    
    grammar.rule("complex_conditions", template(
        "({and_group}) OR ({simple})",
        and_group=ref("and_conditions"),
        simple=ref("simple_condition")
    ))
    
    # ========== JOIN Patterns ==========
    
    grammar.rule("join_query", template(
        "SELECT {select_list} FROM {table1} {joins} {where_clause} {group_by} {order_by} {limit}",
        select_list=ref("select_list"),
        table1=ref("table"),
        joins=ref("join_clause"),
        where_clause=ref("where_clause"),
        group_by=maybe(ref("group_by_clause"), 0.3),
        order_by=maybe(ref("order_by_clause"), 0.5),
        limit=maybe(ref("limit_clause"), 0.3)
    ))
    
    grammar.rule("join_clause", repeat(
        ref("single_join"),
        min=1,
        max=3,
        separator=" "
    ))
    
    grammar.rule("single_join", template(
        "{join_type} {table2} ON {join_condition}",
        join_type=choice(
            "INNER JOIN",
            "LEFT JOIN",
            "RIGHT JOIN",
            "FULL OUTER JOIN",
            weights=[60, 30, 5, 5]
        ),
        table2=ref("table_with_alias"),
        join_condition=ref("join_condition")
    ))
    
    grammar.rule("join_condition", choice(
        template("{t1}.{col1} = {t2}.{col2}"),
        template("{t1}.{col1} = {t2}.{col2} AND {t1}.status = 'active'")
    ))
    
    # ========== Aggregation Patterns ==========
    
    grammar.rule("aggregation_query", template(
        "SELECT {group_columns}, {aggregates} "
        "FROM {table} {joins} {where_clause} "
        "GROUP BY {group_columns} {having} {order_by}",
        group_columns=ref("group_column_list"),
        aggregates=ref("aggregate_list"),
        table=ref("table"),
        joins=maybe(ref("join_clause"), 0.4),
        where_clause=maybe(ref("where_clause"), 0.6),
        having=maybe(ref("having_clause"), 0.3),
        order_by=maybe(ref("order_by_clause"), 0.7)
    ))
    
    grammar.rule("aggregate_list", repeat(
        ref("aggregate_function"),
        min=1,
        max=3,
        separator=", "
    ))
    
    grammar.rule("aggregate_function", choice(
        template("COUNT(*) AS count"),
        template("SUM({numeric_column}) AS total_{numeric_column}"),
        template("AVG({numeric_column}) AS avg_{numeric_column}"),
        template("MIN({numeric_column}) AS min_{numeric_column}"),
        template("MAX({numeric_column}) AS max_{numeric_column}"),
        template("COUNT(DISTINCT {column}) AS unique_{column}")
    ))
    
    grammar.rule("having_clause", template(
        "HAVING {having_condition}",
        having_condition=choice(
            "COUNT(*) > 10",
            "SUM(amount) > 1000",
            "AVG(price) < 100"
        )
    ))
    
    # ========== Subquery Patterns ==========
    
    grammar.rule("subquery_patterns", choice(
        ref("in_subquery"),
        ref("exists_subquery"),
        ref("scalar_subquery"),
        ref("from_subquery")
    ))
    
    grammar.rule("in_subquery", template(
        "SELECT * FROM {table1} WHERE {column} IN (SELECT {column2} FROM {table2} WHERE {condition})"
    ))
    
    grammar.rule("exists_subquery", template(
        "SELECT * FROM {table1} t1 WHERE EXISTS (SELECT 1 FROM {table2} t2 WHERE t2.{fk} = t1.id AND {condition})"
    ))
    
    grammar.rule("scalar_subquery", template(
        "SELECT *, (SELECT {aggregate}({column}) FROM {table2} WHERE {table2}.{fk} = {table1}.id) AS {alias} FROM {table1}"
    ))
    
    grammar.rule("from_subquery", template(
        "SELECT * FROM (SELECT {columns} FROM {table} WHERE {condition} ORDER BY {column} LIMIT 100) AS subq"
    ))
    
    # ========== Common Components ==========
    
    grammar.rule("table", choice("users", "orders", "products", "order_items", "categories"))
    
    grammar.rule("table_with_alias", choice(
        template("users u"),
        template("orders o"),
        template("products p"),
        template("order_items oi"),
        template("categories c")
    ))
    
    grammar.rule("column", choice(
        "id", "name", "email", "status", "created_at",
        "user_id", "product_id", "price", "quantity"
    ))
    
    grammar.rule("numeric_column", choice("price", "quantity", "amount", "total"))
    
    grammar.rule("value", choice(
        number(1, 1000),
        "'active'",
        "'pending'",
        "CURRENT_DATE"
    ))
    
    grammar.rule("value2", number(1001, 2000))
    
    grammar.rule("value_list", repeat(
        number(1, 100),
        min=2,
        max=5,
        separator=", "
    ))
    
    grammar.rule("pattern", choice("%test%", "user_%", "%@example.com"))
    
    grammar.rule("select_list", choice(
        "*",
        ref("column_list"),
        template("{table_alias}.*"),
        template("{table_alias}.{column}, {table_alias2}.{column2}")
    ))
    
    grammar.rule("column_list", repeat(
        ref("column"),
        min=1,
        max=5,
        separator=", "
    ))
    
    grammar.rule("group_column_list", repeat(
        ref("column"),
        min=1,
        max=3,
        separator=", "
    ))
    
    grammar.rule("order_by_clause", template(
        "ORDER BY {order_list}",
        order_list=repeat(
            template("{column} {direction}"),
            min=1,
            max=3,
            separator=", "
        )
    ))
    
    grammar.rule("direction", choice("ASC", "DESC", weights=[30, 70]))
    
    grammar.rule("limit_clause", template(
        "LIMIT {limit_value}",
        limit_value=choice(10, 20, 50, 100, 1000)
    ))
    
    # Aliases and references
    grammar.rule("t1", choice("t1", "u", "o", "p"))
    grammar.rule("t2", choice("t2", "u2", "o2", "p2"))
    grammar.rule("col1", ref("column"))
    grammar.rule("col2", ref("column"))
    grammar.rule("fk", choice("user_id", "product_id", "order_id"))
    grammar.rule("alias", choice("total", "count", "average", "maximum"))
    grammar.rule("table_alias", choice("u", "o", "p", "oi", "c"))
    grammar.rule("table_alias2", choice("u2", "o2", "p2"))
    grammar.rule("aggregate", choice("COUNT", "SUM", "AVG", "MAX", "MIN"))
    
    return grammar


def demonstrate_patterns():
    """Show different query pattern examples."""
    
    grammar = create_pattern_grammar()
    
    patterns = {
        "Complex WHERE": "simple_condition",
        "AND/OR Conditions": "complex_conditions",
        "JOIN Query": "join_query",
        "Aggregation": "aggregation_query",
        "IN Subquery": "in_subquery",
        "EXISTS Subquery": "exists_subquery",
        "Scalar Subquery": "scalar_subquery"
    }
    
    print("SQL Query Pattern Examples")
    print("=" * 70)
    
    for pattern_name, rule_name in patterns.items():
        print(f"\n{pattern_name}:")
        print("-" * 70)
        
        # Generate 2 examples of each pattern
        for i in range(2):
            try:
                query = grammar.generate(rule_name, seed=i*10)
                print(f"\nExample {i+1}:")
                
                # Format multi-line queries nicely
                if len(query) > 80:
                    # Simple formatting for readability
                    query = query.replace(" FROM ", "\nFROM ")
                    query = query.replace(" WHERE ", "\nWHERE ")
                    query = query.replace(" GROUP BY ", "\nGROUP BY ")
                    query = query.replace(" ORDER BY ", "\nORDER BY ")
                    query = query.replace(" JOIN ", "\n  JOIN ")
                
                print(query)
            except Exception as e:
                print(f"Error generating {rule_name}: {e}")


def main():
    """Run pattern demonstration."""
    
    demonstrate_patterns()
    
    print("\n" + "=" * 70)
    print("Key Pattern Concepts:")
    print("-" * 70)
    print("1. Weighted choices create realistic query distributions")
    print("2. Optional elements (maybe) add variety")
    print("3. Repeat creates lists (columns, conditions, joins)")
    print("4. Nested templates enable complex structures")
    print("5. Rule references (ref) promote reusability")
    print("\nTip: Adjust weights and probabilities to match your workload!")


if __name__ == "__main__":
    main()