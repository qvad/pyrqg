"""
SELECT-focused Grammar for Workload Testing
Generates various SELECT patterns with different complexity levels
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda

g = Grammar("select_workload")

# ============================================================================
# Main rule - different SELECT patterns
# ============================================================================

g.rule("query",
    choice(
        ref("simple_select"),          # 25% - Basic SELECT
        ref("select_where"),           # 20% - With WHERE
        ref("select_join"),            # 15% - With JOIN
        ref("select_aggregate"),       # 15% - With aggregation
        ref("select_subquery"),        # 15% - With subquery
        ref("select_complex"),         # 10% - Complex queries
        weights=[25, 20, 15, 15, 15, 10]
    )
)

# ============================================================================
# SELECT patterns
# ============================================================================

g.rule("simple_select",
    template("SELECT {columns} FROM {table}",
        table=ref("table_name"),
        columns=ref("column_selection")
    )
)

g.rule("select_where",
    template("SELECT {columns} FROM {table} WHERE {condition}",
        table=ref("table_name"),
        columns=ref("column_selection"),
        condition=ref("where_condition")
    )
)

g.rule("select_join",
    template("SELECT {columns} FROM {table1} {join_type} JOIN {table2} ON {join_condition} WHERE {condition}",
        columns=ref("join_column_selection"),
        table1=ref("table_name"),
        join_type=choice("INNER", "LEFT", "RIGHT"),
        table2=ref("table_name"),
        join_condition=ref("join_condition"),
        condition=ref("where_condition")
    )
)

g.rule("select_aggregate",
    template("SELECT {agg_columns} FROM {table} {where} GROUP BY {group_cols} {having} {order}",
        agg_columns=ref("aggregate_selection"),
        table=ref("table_name"),
        where=maybe(template("WHERE {cond}", cond=ref("where_condition"))),
        group_cols=ref("group_by_columns"),
        having=maybe(template("HAVING {cond}", cond=ref("having_condition"))),
        order=maybe(template("ORDER BY {col} {dir}", 
            col=ref("order_column"),
            dir=choice("ASC", "DESC")
        ))
    )
)

g.rule("select_subquery",
    choice(
        # Subquery in WHERE
        template("SELECT {columns} FROM {table} WHERE {column} IN (SELECT {sub_col} FROM {sub_table} WHERE {sub_cond})",
            columns=ref("column_selection"),
            table=ref("table_name"),
            column=ref("column_name"),
            sub_col=ref("column_name"),
            sub_table=ref("table_name"),
            sub_cond=ref("where_condition")
        ),
        
        # Correlated subquery
        template("SELECT {columns}, (SELECT {agg}({sub_col}) FROM {sub_table} WHERE {corr_cond}) AS {alias} FROM {table}",
            columns=ref("column_selection"),
            agg=choice("COUNT", "MAX", "MIN", "AVG"),
            sub_col=ref("column_name"),
            sub_table=ref("table_name"),
            corr_cond=ref("correlation_condition"),
            alias=ref("alias_name"),
            table=ref("table_name")
        )
    )
)

g.rule("select_complex",
    choice(
        # CTE - Fixed to use actual column values
        Lambda(lambda ctx: f"WITH {g.generate("cte_name", seed=ctx.seed)} AS (SELECT {g.generate("column_selection", seed=ctx.seed)} FROM {g.generate("table_name", seed=ctx.seed)} WHERE {g.generate("where_condition", seed=ctx.seed)}) SELECT {g.generate("column_selection", seed=ctx.seed)} FROM {g.generate("cte_name", seed=ctx.seed)}"),
        
        # UNION - Fixed to ensure same columns
        Lambda(lambda ctx: (lambda cols, t1, t2, c1, c2: f"SELECT {cols} FROM {t1} WHERE {c1} UNION SELECT {cols} FROM {t2} WHERE {c2}")(
            g.generate("column_selection", seed=ctx.seed),
            g.generate("table_name", seed=ctx.seed),
            g.generate("table_name", seed=ctx.seed),
            g.generate("where_condition", seed=ctx.seed),
            g.generate("where_condition", seed=ctx.seed)
        )),
        
        # Window function
        Lambda(lambda ctx: f"SELECT {g.generate("column_selection", seed=ctx.seed)}, ROW_NUMBER() OVER (PARTITION BY {g.generate("column_name", seed=ctx.seed)} ORDER BY {g.generate("column_name", seed=ctx.seed)}) AS {g.generate("alias_name", seed=ctx.seed)} FROM {g.generate("table_name", seed=ctx.seed)}")
    )
)

# ============================================================================
# Column selections
# ============================================================================

g.rule("column_selection",
    choice(
        "*",
        ref("column_list"),
        ref("specific_columns"),
        ref("calculated_columns")
    )
)

g.rule("join_column_selection",
    choice(
        Lambda(lambda ctx: f"{g.generate("table_alias", seed=ctx.seed)}.*, {g.generate("table_alias", seed=ctx.seed)}.{g.generate("column_name", seed=ctx.seed)}"),
        Lambda(lambda ctx: f"{g.generate("table_alias", seed=ctx.seed)}.{g.generate("column_name", seed=ctx.seed)}, {g.generate("table_alias", seed=ctx.seed)}.{g.generate("column_name", seed=ctx.seed)}"),
        ref("column_list")
    )
)

g.rule("aggregate_selection",
    choice(
        template("COUNT(*), {agg}", agg=ref("aggregate_function")),
        template("{col}, {agg}", col=ref("column_name"), agg=ref("aggregate_function")),
        template("{agg1}, {agg2}", agg1=ref("aggregate_function"), agg2=ref("aggregate_function"))
    )
)

g.rule("aggregate_function",
    choice(
        template("COUNT({col})", col=ref("column_name")),
        template("SUM({col})", col=ref("numeric_column")),
        template("AVG({col})", col=ref("numeric_column")),
        template("MAX({col})", col=ref("column_name")),
        template("MIN({col})", col=ref("column_name"))
    )
)

# ============================================================================
# Conditions
# ============================================================================

g.rule("where_condition",
    choice(
        template("{col} = {val}", col=ref("column_name"), val=ref("value")),
        template("{col} > {val}", col=ref("column_name"), val=ref("value")),
        Lambda(lambda ctx: f"{g.generate('column_name', seed=ctx.seed)} BETWEEN {ctx.rng.randint(1, 100)} AND {ctx.rng.randint(101, 1000)}"),
        Lambda(lambda ctx: f"{g.generate('column_name', seed=ctx.seed)} IN ({', '.join(str(ctx.rng.randint(1, 100)) for _ in range(ctx.rng.randint(2, 5)))})"),
        Lambda(lambda ctx: f"{g.generate('column_name', seed=ctx.seed)} LIKE '{ctx.rng.choice(['A', 'B', 'C'])}%'"),
        template("{col} IS NOT NULL", col=ref("column_name"))
    )
)

g.rule("having_condition",
    choice(
        Lambda(lambda ctx: f"COUNT(*) > {ctx.rng.randint(1, 10)}"),
        Lambda(lambda ctx: f"SUM({g.generate("numeric_column", seed=ctx.seed)}) > {ctx.rng.randint(100, 10000)}"),
        Lambda(lambda ctx: f"AVG({g.generate("numeric_column", seed=ctx.seed)}) < {ctx.rng.randint(50, 500)}")
    )
)

g.rule("join_condition",
    Lambda(lambda ctx: f"t1.{g.generate("join_column", seed=ctx.seed)} = t2.{g.generate("join_column", seed=ctx.seed)}")
)

g.rule("correlation_condition",
    Lambda(lambda ctx: f"{g.generate("table_name", seed=ctx.seed)}.{g.generate("join_column", seed=ctx.seed)} = {g.generate("table_name", seed=ctx.seed)}.{g.generate("join_column", seed=ctx.seed)}")
)

# ============================================================================
# Helper rules
# ============================================================================

g.rule("table_name",
    choice(
        "users", "orders", "products", "customers",
        "transactions", "accounts", "inventory", "sales"
    )
)

g.rule("table_alias",
    choice("t1", "t2", "u", "o", "p", "c")
)

g.rule("column_name",
    choice(
        "id", "name", "status", "created_at",
        "user_id", "product_id", "quantity", "price"
    )
)

g.rule("numeric_column",
    choice(
        "quantity", "price", "total", "balance",
        "amount", "score", "count", "age"
    )
)

g.rule("join_column",
    choice("id", "user_id", "product_id", "customer_id", "order_id")
)

g.rule("column_list",
    choice(
        "id, name, status",
        "user_id, created_at, status",
        "product_id, quantity, price"
    )
)

g.rule("specific_columns",
    repeat(ref("column_name"), min=1, max=5, sep=", ")
)

g.rule("calculated_columns",
    choice(
        Lambda(lambda ctx: f"{g.generate("numeric_column", seed=ctx.seed)} * {ctx.rng.uniform(0.8, 1.2):.2f} AS adjusted_value"),
        Lambda(lambda ctx: f"CASE WHEN {g.generate("column_name", seed=ctx.seed)} = '{ctx.rng.choice(['active', 'pending'])}' THEN 1 ELSE 0 END AS is_active"),
        Lambda(lambda ctx: f"EXTRACT(YEAR FROM created_at) AS year")
    )
)

g.rule("group_by_columns",
    choice(
        ref("column_name"),
        Lambda(lambda ctx: f"{g.generate("column_name", seed=ctx.seed)}, {g.generate("column_name", seed=ctx.seed)}")
    )
)

g.rule("order_column",
    choice(
        ref("column_name"),
        "1", "2",  # Positional
        Lambda(lambda ctx: f"{g.generate("aggregate_function", seed=ctx.seed)}")
    )
)

g.rule("value",
    choice(
        Lambda(lambda ctx: str(ctx.rng.randint(1, 1000))),
        Lambda(lambda ctx: f"'{ctx.rng.choice(['active', 'inactive', 'pending'])}'"),
        "CURRENT_DATE",
        Lambda(lambda ctx: f"'{ctx.rng.randint(2020, 2024)}-{ctx.rng.randint(1, 12):02d}-{ctx.rng.randint(1, 28):02d}'")
    )
)

g.rule("cte_name",
    choice("cte_data", "temp_results", "filtered_data", "summary")
)

g.rule("alias_name",
    choice("total", "count", "avg_value", "max_value", "rank")
)

if __name__ == "__main__":
    print("SELECT-focused Grammar Test")
    print("=" * 60)
    
    for i in range(10):
        query = g.generate("query", seed=i)
        print(f"\n{i+1}. {query}")