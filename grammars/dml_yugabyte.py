"""
YugabyteDB-specific DML Grammar with advanced features
Includes INSERT ON CONFLICT, RETURNING, CTEs, and YugabyteDB-specific syntax
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda, Literal

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
        Literal("*"),
        ref("column_name"),
        template("{col1}, {col2}", col1=ref("column_name"), col2=ref("column_name")),
        template("{col} AS {alias}", col=ref("column_name"), alias=ref("alias")),
        template("yb_hash_code({col})", col=ref("column_name"))  # YugabyteDB specific
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
    choice("users", "orders", "products", "inventory", "transactions", "logs")
)

g.rule("column_name",
    choice("id", "user_id", "product_id", "name", "email", "status", 
           "quantity", "price", "total", "created_at", "updated_at")
)

g.rule("unique_column",
    choice("id", "email", "product_id", "user_id")
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
    repeat(ref("column_name"), min=2, max=4, sep=", ")
)

g.rule("value_list",
    repeat(ref("value"), min=2, max=4, sep=", ")
)

g.rule("value",
    choice(
        number(1, 1000),
        ref("string_value"),
        Literal("NULL"),
        Literal("DEFAULT"),
        Literal("CURRENT_TIMESTAMP"),
        template("yb_hash_code({num})", num=number(1, 100))  # YugabyteDB function
    )
)

g.rule("string_value",
    choice(
        Literal("'active'"),
        Literal("'inactive'"),
        Literal("'pending'"),
        Literal("'completed'"),
        Lambda(lambda ctx: f"'user{ctx.rng.randint(1, 100)}@test.com'"),
        Lambda(lambda ctx: f"'Product {ctx.rng.randint(1, 100)}'")
    )
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
        template("{field} = {value}", field=ref("column_name"), value=ref("value")),
        template("{field} > {value}", field=ref("column_name"), value=number(1, 100)),
        template("{field} IN ({values})", field=ref("column_name"), 
                values=repeat(ref("value"), min=2, max=4, sep=", ")),
        template("{field} IS NOT NULL", field=ref("column_name")),
        template("yb_hash_code({field}) = {hash}", field=ref("column_name"), hash=number(1, 1000))
    )
)

g.rule("join_condition",
    template("target.{field} = source.{field}", field=ref("unique_column"))
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
    choice("new_id", "old_value", "result", "hash_code")
)

g.rule("index_name",
    Lambda(lambda ctx: f"idx_{ctx.rng.choice(['users', 'orders', 'products'])}_{ctx.rng.randint(1, 100)}")
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

if __name__ == "__main__":
    # Test the grammar
    print("Testing YugabyteDB DML Grammar")
    print("="*60)
    
    for i in range(15):
        try:
            query = g.generate("query", seed=i)
            print(f"\n-- Query {i}:")
            print(query)
        except Exception as e:
            print(f"\nError {i}: {e}")