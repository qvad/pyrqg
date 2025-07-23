"""
DELETE-focused Grammar for Workload Testing
Generates various DELETE patterns with different complexity levels
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda

g = Grammar("delete_workload")

# ============================================================================
# Main rule - different DELETE patterns
# ============================================================================

g.rule("query",
    choice(
        ref("simple_delete"),          # 35% - Basic delete with WHERE
        ref("delete_limit"),           # 20% - With LIMIT
        ref("delete_subquery"),        # 20% - With subquery
        ref("delete_returning"),       # 15% - With RETURNING
        ref("delete_using"),           # 10% - DELETE USING (PostgreSQL)
        weights=[35, 20, 20, 15, 10]
    )
)

# ============================================================================
# DELETE patterns
# ============================================================================

g.rule("simple_delete",
    template("DELETE FROM {table} WHERE {condition}",
        table=ref("table_name"),
        condition=ref("where_condition")
    )
)

g.rule("delete_limit",
    template("DELETE FROM {table} WHERE {condition} ORDER BY {order_col} {order_dir} LIMIT {limit}",
        table=ref("table_name"),
        condition=ref("where_condition"),
        order_col=ref("column_name"),
        order_dir=choice("ASC", "DESC"),
        limit=Lambda(lambda ctx: ctx.rng.randint(1, 100))
    )
)

g.rule("delete_subquery",
    template("DELETE FROM {table} WHERE {column} IN (SELECT {column} FROM {table2} WHERE {sub_condition})",
        table=ref("table_name"),
        column=ref("column_name"),
        table2=ref("table_name"),
        sub_condition=ref("where_condition")
    )
)

g.rule("delete_returning",
    template("DELETE FROM {table} WHERE {condition} RETURNING {return_cols}",
        table=ref("table_name"),
        condition=ref("where_condition"),
        return_cols=choice("*", ref("column_list"), "id", "id, deleted_at")
    )
)

g.rule("delete_using",
    template("DELETE FROM {table1} USING {table2} WHERE {join_condition} AND {delete_condition}",
        table1=ref("table_name"),
        table2=ref("table_name"),
        join_condition=ref("join_condition"),
        delete_condition=ref("delete_specific_condition")
    )
)

# ============================================================================
# Condition patterns
# ============================================================================

g.rule("where_condition",
    choice(
        # Simple conditions
        Lambda(lambda ctx: f"id = {ctx.rng.randint(1, 1000)}"),
        Lambda(lambda ctx: f"status = '{ctx.rng.choice(['deleted', 'inactive', 'expired', 'cancelled'])}'"),
        
        # Range conditions
        Lambda(lambda ctx: f"created_at < CURRENT_DATE - INTERVAL '{ctx.rng.randint(30, 365)} days'"),
        Lambda(lambda ctx: f"last_login < CURRENT_DATE - INTERVAL '{ctx.rng.randint(90, 180)} days'"),
        Lambda(lambda ctx: f"expiry_date < CURRENT_TIMESTAMP"),
        
        # Numeric conditions
        Lambda(lambda ctx: f"quantity = 0"),
        Lambda(lambda ctx: f"balance < {ctx.rng.randint(-100, 0)}"),
        Lambda(lambda ctx: f"retry_count > {ctx.rng.randint(3, 10)}"),
        
        # IN conditions
        Lambda(lambda ctx: f"id IN ({', '.join(str(ctx.rng.randint(1, 1000)) for _ in range(ctx.rng.randint(2, 10)))}"),
        Lambda(lambda ctx: f"user_id IN (SELECT id FROM users WHERE status = 'deleted')"),
        
        # Complex conditions
        Lambda(lambda ctx: f"status = 'pending' AND created_at < CURRENT_DATE - INTERVAL '7 days'"),
        Lambda(lambda ctx: f"(quantity = 0 OR expiry_date < CURRENT_DATE) AND status != 'archived'")
    )
)

g.rule("delete_specific_condition",
    choice(
        Lambda(lambda ctx: f"{ctx.grammar.rules['table_name'].generate(ctx)}.status = 'inactive'"),
        Lambda(lambda ctx: f"{ctx.grammar.rules['table_name'].generate(ctx)}.deleted = true"),
        Lambda(lambda ctx: f"{ctx.grammar.rules['table_name'].generate(ctx)}.expiry_date < CURRENT_DATE")
    )
)

g.rule("join_condition",
    choice(
        Lambda(lambda ctx: f"{ctx.grammar.rules['table_name'].generate(ctx)}.id = {ctx.grammar.rules['table_name'].generate(ctx)}.{ctx.rng.choice(['user_id', 'product_id', 'order_id', 'customer_id'])}"),
        Lambda(lambda ctx: f"{ctx.grammar.rules['table_name'].generate(ctx)}.{ctx.rng.choice(['email', 'username', 'code'])} = {ctx.grammar.rules['table_name'].generate(ctx)}.{ctx.rng.choice(['email', 'username', 'code'])}")
    )
)

# ============================================================================
# Helper rules
# ============================================================================

g.rule("table_name",
    choice(
        "users", "orders", "products", "sessions",
        "logs", "temp_data", "audit_logs", "notifications",
        "expired_tokens", "deleted_items", "archive", "cache"
    )
)

g.rule("column_name",
    choice(
        "id", "user_id", "product_id", "order_id",
        "status", "created_at", "updated_at", "deleted_at",
        "expiry_date", "last_accessed", "quantity", "retry_count"
    )
)

g.rule("column_list",
    choice(
        "id, status, deleted_at",
        "id, user_id, created_at",
        "id, status",
        "user_id, product_id, quantity"
    )
)

if __name__ == "__main__":
    print("DELETE-focused Grammar Test")
    print("=" * 60)
    
    for i in range(10):
        query = g.generate("query", seed=i)
        print(f"\n{i+1}. {query}")