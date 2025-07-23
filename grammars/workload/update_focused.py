"""
UPDATE-focused Grammar for Workload Testing
Generates various UPDATE patterns with different complexity levels
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda

g = Grammar("update_workload")

# ============================================================================
# Main rule - different UPDATE patterns
# ============================================================================

g.rule("query",
    choice(
        ref("simple_update"),          # 30% - Basic update
        ref("conditional_update"),     # 25% - With WHERE clause
        ref("multi_column_update"),    # 20% - Multiple columns
        ref("update_from_select"),     # 15% - With subquery
        ref("update_returning"),       # 10% - With RETURNING
        weights=[30, 25, 20, 15, 10]
    )
)

# ============================================================================
# UPDATE patterns
# ============================================================================

g.rule("simple_update",
    template("UPDATE {table} SET {assignment}",
        table=ref("table_name"),
        assignment=ref("single_assignment")
    )
)

g.rule("conditional_update",
    template("UPDATE {table} SET {assignment} WHERE {condition}",
        table=ref("table_name"),
        assignment=ref("assignment_list"),
        condition=ref("where_condition")
    )
)

g.rule("multi_column_update",
    template("UPDATE {table} SET {assignments} WHERE {condition}",
        table=ref("table_name"),
        assignments=repeat(ref("assignment"), min=2, max=5, sep=", "),
        condition=ref("where_condition")
    )
)

g.rule("update_from_select",
    template("UPDATE {table} SET {column} = (SELECT {agg}({col2}) FROM {table2} WHERE {join_cond}) WHERE {condition}",
        table=ref("table_name"),
        column=ref("column_name"),
        agg=choice("MAX", "MIN", "AVG", "COUNT"),
        col2=ref("column_name"),
        table2=ref("table_name"),
        join_cond=ref("join_condition"),
        condition=ref("where_condition")
    )
)

g.rule("update_returning",
    template("UPDATE {table} SET {assignment} WHERE {condition} RETURNING {return_cols}",
        table=ref("table_name"),
        assignment=ref("assignment_list"),
        condition=ref("where_condition"),
        return_cols=choice("*", ref("column_name"), "id, updated_at")
    )
)

# ============================================================================
# Assignment patterns
# ============================================================================

g.rule("assignment",
    choice(
        ref("simple_assignment"),
        ref("increment_assignment"),
        ref("expression_assignment"),
        ref("case_assignment")
    )
)

g.rule("single_assignment",
    template("{column} = {value}",
        column=ref("column_name"),
        value=ref("update_value")
    )
)

g.rule("simple_assignment",
    template("{column} = {value}",
        column=ref("column_name"),
        value=ref("update_value")
    )
)

g.rule("assignment_list",
    repeat(ref("assignment"), min=1, max=3, sep=", ")
)

g.rule("increment_assignment",
    template("{column} = {column} {op} {value}",
        column=ref("numeric_column"),
        op=choice("+", "-", "*"),
        value=Lambda(lambda ctx: ctx.rng.randint(1, 100))
    )
)

g.rule("expression_assignment",
    choice(
        template("{col1} = {col2} * {factor}",
            col1=ref("numeric_column"),
            col2=ref("numeric_column"),
            factor=Lambda(lambda ctx: ctx.rng.uniform(0.8, 1.2))
        ),
        template("{col} = COALESCE({col}, {default})",
            col=ref("column_name"),
            default=ref("update_value")
        ),
        template("{col} = UPPER({col})", col=ref("string_column")),
        template("{col} = SUBSTRING({col} FROM 1 FOR {len})",
            col=ref("string_column"),
            len=Lambda(lambda ctx: ctx.rng.randint(5, 20))
        )
    )
)

g.rule("case_assignment",
    template("{column} = CASE WHEN {condition} THEN {val1} ELSE {val2} END",
        column=ref("column_name"),
        condition=ref("simple_condition"),
        val1=ref("update_value"),
        val2=ref("update_value")
    )
)

# ============================================================================
# Helper rules
# ============================================================================

g.rule("table_name",
    choice(
        "users", "orders", "products", "inventory",
        "transactions", "accounts", "customers", "employees"
    )
)

g.rule("column_name",
    choice(
        "status", "quantity", "price", "total_amount",
        "last_updated", "modified_by", "version", "active"
    )
)

g.rule("numeric_column",
    choice(
        "quantity", "price", "total_amount", "balance",
        "score", "count", "version", "retry_count"
    )
)

g.rule("string_column",
    choice(
        "name", "description", "status", "category",
        "email", "address", "notes", "tags"
    )
)

g.rule("update_value",
    choice(
        Lambda(lambda ctx: f"'{ctx.rng.choice(['active', 'inactive', 'pending', 'completed'])}'"),
        Lambda(lambda ctx: str(ctx.rng.randint(0, 1000))),
        Lambda(lambda ctx: f"{ctx.rng.uniform(0.01, 9999.99):.2f}"),
        "CURRENT_TIMESTAMP",
        "NULL",
        Lambda(lambda ctx: f"'{ctx.rng.choice(['Updated', 'Modified', 'Changed'])} {ctx.rng.randint(1, 999)}'"),
        "DEFAULT"
    )
)

g.rule("where_condition",
    choice(
        Lambda(lambda ctx: f"id = {ctx.rng.randint(1, 1000)}"),
        Lambda(lambda ctx: f"status = '{ctx.rng.choice(['active', 'pending', 'draft'])}'"),
        Lambda(lambda ctx: f"{ctx.grammar.rules['numeric_column'].generate(ctx)} > {ctx.rng.randint(0, 100)}"),
        Lambda(lambda ctx: f"{ctx.grammar.rules['numeric_column'].generate(ctx)} BETWEEN {ctx.rng.randint(1, 50)} AND {ctx.rng.randint(51, 100)}"),
        Lambda(lambda ctx: f"created_at < CURRENT_DATE - INTERVAL '{ctx.rng.randint(1, 90)} days'"),
        Lambda(lambda ctx: f"id IN ({', '.join(str(ctx.rng.randint(1, 100)) for _ in range(ctx.rng.randint(2, 5)))})")
    )
)

g.rule("simple_condition",
    choice(
        Lambda(lambda ctx: f"{ctx.grammar.rules['numeric_column'].generate(ctx)} > {ctx.rng.randint(0, 100)}"),
        Lambda(lambda ctx: f"status = '{ctx.rng.choice(['active', 'inactive'])}'"),
        Lambda(lambda ctx: f"modified_at IS NULL")
    )
)

g.rule("join_condition",
    choice(
        Lambda(lambda ctx: f"{ctx.grammar.rules['table_name'].generate(ctx)}.id = {ctx.grammar.rules['table_name'].generate(ctx)}.{ctx.rng.choice(['user_id', 'product_id', 'customer_id'])}"),
        Lambda(lambda ctx: f"{ctx.grammar.rules['table_name'].generate(ctx)}.{ctx.rng.choice(['category', 'type', 'status'])} = {ctx.grammar.rules['table_name'].generate(ctx)}.{ctx.rng.choice(['category', 'type', 'status'])}")
    )
)

if __name__ == "__main__":
    print("UPDATE-focused Grammar Test")
    print("=" * 60)
    
    for i in range(10):
        query = g.generate("query", seed=i)
        print(f"\n{i+1}. {query}")