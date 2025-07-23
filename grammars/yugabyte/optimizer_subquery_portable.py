#!/usr/bin/env python3
"""
YugabyteDB Optimizer Subquery Grammar (Portable)
Converted from optimizer_subquery_portable.yy

This grammar generates complex subqueries for optimizer testing.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, maybe, template, table, field, number, digit, ref, Lambda, repeat

# Create grammar
g = Grammar("optimizer_subquery_portable")

# Use same tables as transactions grammar
g.define_tables(
    A=10, B=20, C=100, D=100, E=0,
    AA=50, BB=20, CC=300, DD=10,
    AAA=10, BBB=10, CCC=100, DDD=1000
)

# Define fields
g.define_fields(
    'pk', 'col_int_key', 'col_int',
    'col_varchar_10_key', 'col_varchar_1024_key',
    'col_varchar_10', 'col_varchar_1024'
)

# ============================================================================
# Main Query Types
# ============================================================================

g.rule("query",
    choice(
        ref("dml"),
        ref("dml_list"),
        ref("select")
    )
)

g.rule("dml",
    choice(
        ref("update"),
        ref("insert"),
        ref("insert_select"),
        ref("delete")
    )
)

g.rule("dml_list",
    repeat(ref("dml"), min=2, max=2, sep=" ; ")
)

# ============================================================================
# DML Operations
# ============================================================================

g.rule("update",
    template("UPDATE {table} SET {field} = {value} {where}",
        table=ref("_table"),
        field=ref("update_field"),
        value=ref("int_value"),
        where=ref("where_list")
    )
)

g.rule("insert",
    template("INSERT INTO {table} ( {field} ) VALUES ( {value} )",
        table=ref("_table"),
        field=ref("field_name"),
        value=ref("value")
    )
)

g.rule("insert_select",
    template("INSERT INTO {table} ( {field} ) {select}",
        table=ref("_table"),
        field=ref("field_name"),
        select=ref("select")
    )
)

g.rule("delete",
    template("DELETE FROM {table} {where} {order_by} LIMIT {limit}",
        table=ref("_table"),
        where=ref("where_list"),
        order_by=ref("order_by"),
        limit=ref("small_digit")
    )
)

# ============================================================================
# SELECT Queries with Subqueries
# ============================================================================

g.rule("select",
    template("{explain} SELECT {hint} {select_list} FROM {table_list} {where} {group_by} {having} {order_by} {limit}",
        explain=maybe(ref("explain")),
        hint=maybe(ref("hint")),
        select_list=ref("select_list"),
        table_list=ref("table_list"),
        where=ref("where_list"),
        group_by=maybe(ref("group_by_clause")),
        having=maybe(ref("having_clause")),
        order_by=maybe(ref("order_by")),
        limit=maybe(template("LIMIT {n}", n=ref("small_digit")))
    )
)

g.rule("explain", 
    choice("EXPLAIN", "EXPLAIN ( ANALYZE , BUFFERS )")
)

g.rule("hint",
    choice(
        "/*+ SeqScan(table1) */",
        "/*+ IndexScan(table1) */",
        "/*+ HashJoin(table1 table2) */",
        "/*+ NestLoop(table1 table2) */",
        "/*+ Leading(table1 table2) */"
    )
)

# ============================================================================
# Subquery Types
# ============================================================================

g.rule("subquery",
    choice(
        ref("scalar_subquery"),
        ref("exists_subquery"),
        ref("in_subquery"),
        ref("any_all_subquery"),
        ref("correlated_subquery"),
        ref("nested_subquery")
    )
)

g.rule("scalar_subquery",
    template("( SELECT {agg}({field}) FROM {table} {where} )",
        agg=choice("MIN", "MAX", "SUM", "COUNT"),
        field=ref("field_name"),
        table=ref("_table"),
        where=maybe(ref("where_list"))
    )
)

g.rule("exists_subquery",
    template("{not_kw} EXISTS ( {select} )",
        not_kw=maybe("NOT"),
        select=ref("correlated_subquery_select")
    )
)

g.rule("in_subquery",
    template("{not_kw} IN ( SELECT {field} FROM {table} {where} )",
        not_kw=maybe("NOT"),
        field=ref("field_name"),
        table=ref("_table"),
        where=maybe(ref("where_list"))
    )
)

g.rule("any_all_subquery",
    template("{comp_op} {any_all} ( SELECT {field} FROM {table} {where} )",
        comp_op=ref("comparison_operator"),
        any_all=choice("ANY", "ALL", "SOME"),
        field=ref("field_name"),
        table=ref("_table"),
        where=maybe(ref("where_list"))
    )
)

g.rule("correlated_subquery",
    template("( SELECT {field} FROM {table} AS {alias2} WHERE {correlation} )",
        field=ref("field_name"),
        table=ref("_table"),
        alias2=ref("table_alias"),
        correlation=ref("correlation_condition")
    )
)

g.rule("correlated_subquery_select",
    template("SELECT * FROM {table} AS {alias2} WHERE {correlation}",
        table=ref("_table"),
        alias2=ref("table_alias"),
        correlation=ref("correlation_condition")
    )
)

g.rule("correlation_condition",
    template("{alias1}.{field1} = {alias2}.{field2}",
        alias1=ref("existing_table_alias"),
        field1=ref("field_name"),
        alias2=ref("table_alias"),
        field2=ref("field_name")
    )
)

g.rule("nested_subquery",
    template("( SELECT {field} FROM ( {inner_select} ) AS {alias} )",
        field=ref("field_name"),
        inner_select=ref("select"),
        alias=ref("table_alias")
    )
)

# ============================================================================
# WHERE Conditions with Subqueries
# ============================================================================

g.rule("where_list",
    maybe(template("WHERE {conditions}", conditions=ref("where_conditions")))
)

g.rule("where_conditions",
    choice(
        ref("simple_condition"),
        ref("subquery_condition"),
        ref("compound_condition")
    )
)

g.rule("simple_condition",
    choice(
        template("{field} {op} {value}",
            field=ref("field_name"),
            op=ref("comparison_operator"),
            value=ref("value")
        ),
        template("{field} BETWEEN {v1} AND {v2}",
            field=ref("field_name"),
            v1=ref("value"),
            v2=ref("value")
        ),
        template("{field} {in_list}",
            field=ref("field_name"),
            in_list=ref("in_value_list")
        ),
        template("{field} IS {not_kw} NULL",
            field=ref("field_name"),
            not_kw=maybe("NOT")
        )
    )
)

g.rule("subquery_condition",
    choice(
        template("{field} = {subquery}",
            field=ref("field_name"),
            subquery=ref("scalar_subquery")
        ),
        ref("exists_subquery"),
        template("{field} {in_subquery}",
            field=ref("field_name"),
            in_subquery=ref("in_subquery")
        ),
        template("{field} {any_all}",
            field=ref("field_name"),
            any_all=ref("any_all_subquery")
        )
    )
)

g.rule("compound_condition",
    template("{cond1} {logical_op} {cond2}",
        cond1=ref("where_conditions"),
        logical_op=choice("AND", "OR"),
        cond2=ref("where_conditions")
    )
)

# ============================================================================
# Table and Field References
# ============================================================================

g.rule("table_list",
    choice(
        ref("single_table"),
        ref("table_join")
    )
)

g.rule("single_table",
    template("{table} AS {alias}",
        table=ref("_table"),
        alias=ref("table_alias")
    )
)

g.rule("table_join",
    template("{t1} AS {a1} {join_type} JOIN {t2} AS {a2} ON {a1}.{f1} = {a2}.{f2}",
        t1=ref("_table"),
        a1=ref("table_alias"),
        join_type=choice("INNER", "LEFT", "RIGHT", ""),
        t2=ref("_table"),
        a2=ref("table_alias"),
        f1=ref("field_name"),
        f2=ref("field_name")
    )
)

g.rule("select_list",
    choice(
        "*",
        ref("field_name"),
        ref("field_list"),
        ref("aggregate_list")
    )
)

g.rule("field_list",
    repeat(ref("field_name"), min=2, max=5, sep=", ")
)

g.rule("aggregate_list",
    template("{agg1}({f1}) AS agg1, {agg2}({f2}) AS agg2",
        agg1=ref("aggregate_function"),
        agg2=ref("aggregate_function"),
        f1=ref("field_name"),
        f2=ref("field_name")
    )
)

# ============================================================================
# GROUP BY and HAVING
# ============================================================================

g.rule("group_by_clause",
    template("GROUP BY {fields}",
        fields=ref("field_list")
    )
)

g.rule("having_clause",
    template("HAVING {condition}",
        condition=choice(
            template("{agg}({field}) > {value}",
                agg=ref("aggregate_function"),
                field=ref("field_name"),
                value=ref("int_value")
            ),
            ref("exists_subquery")
        )
    )
)

# ============================================================================
# ORDER BY
# ============================================================================

g.rule("order_by",
    template("ORDER BY {field} {dir}",
        field=ref("field_name"),
        dir=choice("ASC", "DESC", "")
    )
)

# ============================================================================
# Values and Operators
# ============================================================================

g.rule("value",
    choice(
        ref("int_value"),
        ref("char_value"),
        ref("null_value")
    )
)

g.rule("int_value", 
    choice(
        ref("_digit"),
        ref("_tinyint"),
        ref("_integer")
    )
)

g.rule("char_value",
    choice(
        ref("_char"),
        ref("_varchar")
    )
)

g.rule("null_value", "NULL")

g.rule("comparison_operator",
    choice("=", "!=", "<", ">", "<=", ">=", "<>")
)

g.rule("in_value_list",
    template("IN ( {values} )",
        values=repeat(ref("value"), min=2, max=5, sep=", ")
    )
)

# ============================================================================
# Basic Elements
# ============================================================================

g.rule("_table",
    choice('A', 'B', 'C', 'D', 'AA', 'BB', 'CC', 'DD')
)

g.rule("field_name",
    choice('pk', 'col_int_key', 'col_int', 'col_varchar_10_key', 'col_varchar_10')
)

g.rule("update_field",
    choice('col_int_key', 'col_int')
)

g.rule("table_alias",
    choice('table1', 'table2', 'table3', 't1', 't2', 't3')
)

g.rule("existing_table_alias", 
    choice('table1', 't1')
)

g.rule("aggregate_function",
    choice("COUNT", "SUM", "MIN", "MAX", "AVG")
)

g.rule("_digit", digit())
g.rule("small_digit", choice('1', '2', '3', '4', '5'))
g.rule("_tinyint", number(0, 255))
g.rule("_integer", number(-2147483648, 2147483647))
g.rule("_char", Lambda(lambda ctx: ctx.rng.choice(['a', 'b', 'c', 'd', 'e'])))
g.rule("_varchar", Lambda(lambda ctx: ''.join(ctx.rng.choices(['a', 'b', 'c'], k=5))))

# ============================================================================
# Test
# ============================================================================

if __name__ == "__main__":
    print("YugabyteDB Optimizer Subquery Grammar")
    print("=" * 60)
    
    # Generate different query types
    for query_type in ["scalar_subquery", "exists_subquery", "in_subquery", 
                      "any_all_subquery", "correlated_subquery", "select"]:
        print(f"\n{query_type}:")
        for i in range(2):
            query = g.generate(query_type, seed=100 + i)
            print(f"  {query}")
    
    print("\n\nFull queries:")
    for i in range(5):
        query = g.generate("query", seed=200 + i)
        print(f"\n{i+1}. {query}")