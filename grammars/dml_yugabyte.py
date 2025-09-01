"""
YugabyteDB-specific DML Grammar with advanced features
Includes INSERT ON CONFLICT, RETURNING, CTEs, and YugabyteDB-specific syntax
"""

import sys
from pathlib import Path

import os
from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda, Literal
from pyrqg.dsl.primitives import (
    common_table_names,
    common_column_names,
    unique_columns,
    string_value_common,
    basic_value,
    column_list_of,
    value_list_of,
    basic_where_condition,
    id_join_condition,
    returning_clause_basic,
    alias_names,
    index_name_default,
)
from pyrqg.dsl.schema_primitives import (
    random_schema_element,
    random_functions_element,
    random_views_element,
    schema_bundle_element,
)

# Create grammar instance
g = Grammar("dml_yugabyte")

# ============================================================================
# Main Query Types
# ============================================================================

g.rule("query",
    choice(
        ref("insert_on_conflict"),
        ref("insert_returning"),
        ref("update_returning"),
        ref("delete_returning"),
        ref("upsert_multiple"),
        ref("cte_insert"),
        ref("cte_update"),
        ref("cte_delete"),
        ref("truncate"),
        ref("merge_statement"),
        weights=[20, 15, 15, 10, 10, 10, 10, 5, 3, 2]
    )
)

# ============================================================================
# INSERT ON CONFLICT (PostgreSQL UPSERT)
# ============================================================================

g.rule("insert_on_conflict",
    template("INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT {conflict_target} DO {conflict_action}",
        table=ref("table_name"),
        columns=ref("column_list"),
        values=ref("value_list"),
        conflict_target=ref("conflict_target"),
        conflict_action=ref("conflict_action")
    )
)

g.rule("conflict_target",
    choice(
        template("({column})", column=ref("unique_column")),
        template("({col1}, {col2})", col1=ref("unique_column"), col2=ref("unique_column")),
        template("ON CONSTRAINT {constraint}", constraint=ref("constraint_name"))
    )
)

g.rule("conflict_action",
    choice(
        Literal("NOTHING"),
        template("UPDATE SET {assignments}",
            assignments=ref("update_assignments")
        ),
        template("UPDATE SET {assignments} WHERE {condition}",
            assignments=ref("update_assignments"),
            condition=ref("where_condition")
        ),
        template("UPDATE SET ({columns}) = ({values})",
            columns=ref("column_list"),
            values=ref("value_list")
        )
    )
)

# ============================================================================
# RETURNING Clauses
# ============================================================================

g.rule("insert_returning",
    template("INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING {returning}",
        table=ref("table_name"),
        columns=ref("column_list"),
        values=ref("value_list"),
        returning=ref("returning_clause")
    )
)

g.rule("update_returning",
    template("UPDATE {table} SET {assignments} WHERE {condition} RETURNING {returning}",
        table=ref("table_name"),
        assignments=ref("update_assignments"),
        condition=ref("where_condition"),
        returning=ref("returning_clause")
    )
)

g.rule("delete_returning",
    template("DELETE FROM {table} WHERE {condition} RETURNING {returning}",
        table=ref("table_name"),
        condition=ref("where_condition"),
        returning=ref("returning_clause")
    )
)

g.rule("returning_clause",
    choice(
        returning_clause_basic(ref("column_name")),
        template("yb_hash_code({col})", col=ref("column_name")),  # YugabyteDB specific
    )
)

# ============================================================================
# CTEs (Common Table Expressions)
# ============================================================================

g.rule("cte_insert",
    template("""WITH {cte_name} AS (
    {select_query}
)
INSERT INTO {table} ({columns})
SELECT {select_columns} FROM {cte_name}""",
        cte_name=ref("cte_name"),
        select_query=ref("select_query"),
        table=ref("table_name"),
        columns=ref("column_list"),
        select_columns=ref("column_list")
    )
)

g.rule("cte_update",
    template("""WITH {cte_name} AS (
    SELECT {columns} FROM {source_table} WHERE {condition}
)
UPDATE {table} SET {assignments}
FROM {cte_name}
WHERE {join_condition}""",
        cte_name=ref("cte_name"),
        columns=ref("column_list"),
        source_table=ref("table_name"),
        condition=ref("where_condition"),
        table=ref("table_name"),
        assignments=ref("update_assignments"),
        join_condition=ref("join_condition")
    )
)

g.rule("cte_delete",
    template("""WITH deleted AS (
    DELETE FROM {table1}
    WHERE {condition1}
    RETURNING {returning}
)
DELETE FROM {table2}
WHERE {column} IN (SELECT {column} FROM deleted)""",
        table1=ref("table_name"),
        condition1=ref("where_condition"),
        returning=ref("column_name"),
        table2=ref("table_name"),
        column=ref("column_name")
    )
)

# ============================================================================
# Advanced DML
# ============================================================================

g.rule("upsert_multiple",
    template("""INSERT INTO {table} ({columns}) VALUES 
{multi_values}
ON CONFLICT ({conflict_column}) DO UPDATE SET {assignments}""",
        table=ref("table_name"),
        columns=ref("column_list"),
        multi_values=repeat(
            template("({values})", values=ref("value_list")),
            min=2, max=5, sep=",\n"
        ),
        conflict_column=ref("unique_column"),
        assignments=ref("update_assignments")
    )
)

g.rule("truncate",
    choice(
        template("TRUNCATE {table}", table=ref("table_name")),
        template("TRUNCATE {table} RESTART IDENTITY", table=ref("table_name")),
        template("TRUNCATE {table} CASCADE", table=ref("table_name"))
    )
)

g.rule("merge_statement",
    template("""MERGE INTO {target_table} AS target
USING {source_table} AS source
ON {merge_condition}
WHEN MATCHED THEN
    UPDATE SET {update_assignments}
WHEN NOT MATCHED THEN
    INSERT ({columns}) VALUES ({values})""",
        target_table=ref("table_name"),
        source_table=ref("table_name"),
        merge_condition=ref("join_condition"),
        update_assignments=ref("update_assignments"),
        columns=ref("column_list"),
        values=ref("value_list")
    )
)

# ============================================================================
# YugabyteDB-specific Features
# ============================================================================

g.rule("create_table_colocated",
    template("""CREATE TABLE {table} (
    {column_definitions}
) WITH (colocation = {colocation})""",
        table=ref("table_name"),
        column_definitions=ref("column_definitions"),
        colocation=choice("true", "false")
    )
)

if os.environ.get("PYRQG_YB"):
    # Yugabyte-safe: default btree index
    g.rule("create_index_hash",
        template("CREATE INDEX {index_name} ON {table} ({column})",
            index_name=ref("index_name"),
            table=ref("table_name"),
            column=ref("column_name")
        )
    )
else:
    g.rule("create_index_hash",
        template("CREATE INDEX {index_name} ON {table} USING HASH ({column})",
            index_name=ref("index_name"),
            table=ref("table_name"),
            column=ref("column_name")
        )
    )

g.rule("split_table",
    template("ALTER TABLE {table} SPLIT AT VALUES ({split_values})",
        table=ref("table_name"),
        split_values=ref("split_values")
    )
)

# ============================================================================
# Helper Rules
# ============================================================================

g.rule("table_name",
    common_table_names()
)

g.rule("column_name",
    common_column_names()
)

g.rule("unique_column",
    unique_columns()
)

g.rule("constraint_name",
    choice(
        Literal("users_pkey"),
        Literal("users_email_key"),
        Literal("orders_pkey"),
        Literal("products_pkey")
    )
)

g.rule("column_list",
    column_list_of(ref("column_name"))
)

g.rule("value_list",
    value_list_of(ref("value"))
)

g.rule("value",
    choice(
        basic_value(),
        template("yb_hash_code({num})", num=number(1, 100)),  # YugabyteDB function
    )
)

g.rule("string_value",
    string_value_common()
)

g.rule("update_assignments",
    repeat(
        template("{field} = {value}",
            field=ref("column_name"),
            value=ref("update_value")
        ),
        min=1, max=3, sep=", "
    )
)

g.rule("update_value",
    choice(
        ref("value"),
        template("{field} + {increment}", field=ref("column_name"), increment=number(1, 10)),
        template("EXCLUDED.{field}", field=ref("column_name")),  # For ON CONFLICT
        Literal("DEFAULT")
    )
)

g.rule("where_condition",
    choice(
        basic_where_condition(ref("column_name"), ref("value")),
        template("yb_hash_code({field}) = {hash}", field=ref("column_name"), hash=number(1, 1000)),
    )
)

g.rule("join_condition",
    id_join_condition(ref("unique_column"))
)

g.rule("select_query",
    template("SELECT {columns} FROM {table} WHERE {condition}",
        columns=ref("column_list"),
        table=ref("table_name"),
        condition=ref("where_condition")
    )
)

g.rule("cte_name",
    choice("updated_rows", "deleted_rows", "source_data", "temp_data")
)

g.rule("alias",
    alias_names()
)

g.rule("index_name",
    index_name_default()
)

g.rule("column_definitions",
    Literal("""id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP""")
)

g.rule("split_values",
    repeat(number(100, 1000), min=1, max=3, sep=", ")
)

# ============================================================================
# Schema generation rules (DDL inside grammar)
# ============================================================================

g.rule("random_schema", random_schema_element(num_tables=5, dialect="postgresql"))
g.rule("random_functions", random_functions_element(count=3, include_procedures=True))
g.rule("random_views", random_views_element(count=2))
g.rule("schema_bundle", schema_bundle_element(num_tables=5, functions=3, views=2, dialect="postgresql"))

if __name__ == "__main__":
    # Test the grammar
    print("Testing YugabyteDB DML Grammar")
    print("="*60)
    # Example: generate only DDL bundle
    print("\n-- Random schema bundle --")
    print(schema_bundle_element(num_tables=3, functions=2, views=1).generate(g.context))

    # Example: generate just tables/functions/views
    print("\n-- Random tables --\n", random_schema_element(num_tables=2).generate(g.context))
    print("\n-- Random functions --\n", random_functions_element(count=2).generate(g.context))
    print("\n-- Random views --\n", random_views_element(count=1).generate(g.context))

    for i in range(15):
        try:
            query = g.generate("query", seed=i)
            print(f"\n-- Query {i}:")
            print(query)
        except Exception as e:
            print(f"\nError {i}: {e}")
