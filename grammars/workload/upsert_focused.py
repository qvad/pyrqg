"""
UPSERT-focused Grammar for Workload Testing
Generates INSERT ... ON CONFLICT patterns (PostgreSQL/YugabyteDB UPSERT)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda

g = Grammar("upsert_workload")

# ============================================================================
# Main rule - different UPSERT patterns
# ============================================================================

g.rule("query",
    choice(
        ref("upsert_do_nothing"),      # 25% - ON CONFLICT DO NOTHING
        ref("upsert_do_update"),       # 30% - ON CONFLICT DO UPDATE
        ref("upsert_conditional"),     # 20% - Conditional UPDATE
        ref("upsert_multi_row"),       # 15% - Multi-row UPSERT
        ref("upsert_returning"),       # 10% - With RETURNING
        weights=[25, 30, 20, 15, 10]
    )
)

# ============================================================================
# UPSERT patterns
# ============================================================================

g.rule("upsert_do_nothing",
    template("INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT ({conflict_col}) DO NOTHING",
        table=ref("table_name"),
        columns=ref("column_list"),
        values=ref("value_list"),
        conflict_col=ref("unique_column")
    )
)

g.rule("upsert_do_update",
    template("INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT ({conflict_col}) DO UPDATE SET {updates}",
        table=ref("table_name"),
        columns=ref("column_list"),
        values=ref("value_list"),
        conflict_col=ref("unique_column"),
        updates=ref("update_set_list")
    )
)

g.rule("upsert_conditional",
    template("INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT ({conflict_col}) DO UPDATE SET {updates} WHERE {condition}",
        table=ref("table_name"),
        columns=ref("column_list"),
        values=ref("value_list"),
        conflict_col=ref("unique_column"),
        updates=ref("update_set_list"),
        condition=ref("update_condition")
    )
)

g.rule("upsert_multi_row",
    template("INSERT INTO {table} ({columns}) VALUES {rows} ON CONFLICT ({conflict_col}) DO UPDATE SET {updates}",
        table=ref("table_name"),
        columns=ref("column_list"),
        rows=repeat(
            template("({values})", values=ref("value_list")),
            min=2, max=5, sep=", "
        ),
        conflict_col=ref("unique_column"),
        updates=ref("update_set_list")
    )
)

g.rule("upsert_returning",
    template("INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT ({conflict_col}) DO UPDATE SET {updates} RETURNING {return_cols}",
        table=ref("table_name"),
        columns=ref("column_list"),
        values=ref("value_list"),
        conflict_col=ref("unique_column"),
        updates=ref("update_set_list"),
        return_cols=choice("*", "id, updated_at", ref("column_list"))
    )
)

# ============================================================================
# Update patterns for ON CONFLICT
# ============================================================================

g.rule("update_set_list",
    choice(
        # Update with EXCLUDED values
        ref("excluded_updates"),
        
        # Mixed updates
        ref("mixed_updates"),
        
        # Increment/merge updates
        ref("merge_updates"),
        
        # Timestamp updates
        "updated_at = CURRENT_TIMESTAMP"
    )
)

g.rule("excluded_updates",
    repeat(
        template("{col} = EXCLUDED.{col}", col=ref("updatable_column")),
        min=1, max=4, sep=", "
    )
)

g.rule("mixed_updates",
    choice(
        template("{col1} = EXCLUDED.{col1}, {col2} = {value}",
            col1=ref("updatable_column"),
            col2=ref("updatable_column"),
            value=ref("update_value")
        ),
        template("{col} = COALESCE({table}.{col}, EXCLUDED.{col})",
            col=ref("updatable_column"),
            table=ref("table_name")
        ),
        template("{col} = CASE WHEN {table}.{col} IS NULL THEN EXCLUDED.{col} ELSE {table}.{col} END",
            col=ref("updatable_column"),
            table=ref("table_name")
        )
    )
)

g.rule("merge_updates",
    choice(
        template("quantity = {table}.quantity + EXCLUDED.quantity",
            table=ref("table_name")
        ),
        template("balance = {table}.balance + EXCLUDED.amount",
            table=ref("table_name")
        ),
        template("visit_count = {table}.visit_count + 1",
            table=ref("table_name")
        ),
        template("data = {table}.data || EXCLUDED.data",
            table=ref("table_name")
        )
    )
)

g.rule("update_condition",
    choice(
        Lambda(lambda ctx: f"{g.rules['table_name'].generate(ctx)}.updated_at < EXCLUDED.updated_at"),
        Lambda(lambda ctx: f"{g.rules['table_name'].generate(ctx)}.version < EXCLUDED.version"),
        Lambda(lambda ctx: f"{g.rules['table_name'].generate(ctx)}.status != 'locked'"),
        Lambda(lambda ctx: f"EXCLUDED.priority > {g.rules['table_name'].generate(ctx)}.priority")
    )
)

# ============================================================================
# Helper rules
# ============================================================================

g.rule("table_name",
    choice(
        "users", "products", "inventory", "accounts",
        "settings", "cache", "sessions", "metrics"
    )
)

g.rule("unique_column",
    choice(
        "id", "email", "username", "product_code",
        "session_id", "api_key", "(user_id, setting_key)"
    )
)

g.rule("column_list",
    choice(
        "id, email, name, status",
        "product_code, name, price, quantity",
        "user_id, setting_key, setting_value",
        "session_id, user_id, data, last_accessed",
        "metric_name, metric_value, timestamp"
    )
)

g.rule("updatable_column",
    choice(
        "name", "status", "quantity", "price",
        "last_seen", "data", "settings", "metadata"
    )
)

g.rule("value_list",
    choice(
        # User values
        Lambda(lambda ctx: f"{ctx.rng.randint(1, 10000)}, '{ctx.rng.choice(['user', 'admin'])}_{ctx.rng.randint(1, 999)}@example.com', '{ctx.rng.choice(['John', 'Jane', 'Bob'])} {ctx.rng.choice(['Smith', 'Doe', 'Johnson'])}', '{ctx.rng.choice(['active', 'pending'])}'"),
        
        # Product values
        Lambda(lambda ctx: f"'PROD{ctx.rng.randint(1000, 9999)}', '{ctx.rng.choice(['Widget', 'Gadget', 'Tool'])} {ctx.rng.randint(1, 99)}', {ctx.rng.uniform(9.99, 999.99):.2f}, {ctx.rng.randint(0, 1000)}"),
        
        # Settings values
        Lambda(lambda ctx: f"{ctx.rng.randint(1, 100)}, '{ctx.rng.choice(['theme', 'language', 'timezone'])}', '{ctx.rng.choice(['dark', 'light', 'en_US', 'UTC'])}'"),
        
        # Session values
        Lambda(lambda ctx: f"'{ctx.rng.choice(['sess_', 'sid_'])}{ctx.rng.randint(100000, 999999)}', {ctx.rng.randint(1, 1000)}, '{{\"last_page\": \"/home\"}}', CURRENT_TIMESTAMP")
    )
)

g.rule("update_value",
    choice(
        Lambda(lambda ctx: f"'{ctx.rng.choice(['updated', 'modified', 'changed'])}'"),
        "CURRENT_TIMESTAMP",
        Lambda(lambda ctx: str(ctx.rng.randint(0, 1000))),
        "DEFAULT"
    )
)

# Export grammar
grammar = g

if __name__ == "__main__":
    print("UPSERT-focused Grammar Test")
    print("=" * 60)
    
    for i in range(10):
        query = g.generate("query", seed=i)
        print(f"\n{i+1}. {query}")