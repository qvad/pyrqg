"""
INSERT-focused Grammar for Workload Testing
Generates various INSERT patterns with different complexity levels
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda

g = Grammar("insert_workload")

# ============================================================================
# Main rule - different INSERT patterns
# ============================================================================

g.rule("query",
    choice(
        ref("simple_insert"),          # 30% - Basic single row
        ref("multi_row_insert"),       # 25% - Multiple rows
        ref("insert_select"),          # 20% - INSERT ... SELECT
        ref("insert_returning"),       # 15% - With RETURNING
        ref("insert_default"),         # 10% - With DEFAULT values
        weights=[30, 25, 20, 15, 10]
    )
)

# ============================================================================
# Simple INSERT patterns
# ============================================================================

g.rule("simple_insert",
    template("INSERT INTO {table} ({columns}) VALUES ({values})",
        table=ref("table_name"),
        columns=ref("column_list"),
        values=ref("value_list")
    )
)

g.rule("multi_row_insert",
    template("INSERT INTO {table} ({columns}) VALUES {rows}",
        table=ref("table_name"),
        columns=ref("column_list"),
        rows=repeat(
            template("({values})", values=ref("value_list")),
            min=2, max=10, sep=", "
        )
    )
)

g.rule("insert_select",
    template("INSERT INTO {table1} ({columns}) SELECT {select_cols} FROM {table2} WHERE {condition}",
        table1=ref("table_name"),
        columns=ref("column_list"),
        select_cols=ref("select_columns"),
        table2=ref("table_name"),
        condition=ref("where_condition")
    )
)

g.rule("insert_returning",
    template("INSERT INTO {table} ({columns}) VALUES ({values}) RETURNING {return_cols}",
        table=ref("table_name"),
        columns=ref("column_list"),
        values=ref("value_list"),
        return_cols=choice("*", ref("column_list"), "id")
    )
)

g.rule("insert_default",
    template("INSERT INTO {table} ({columns}) VALUES ({values})",
        table=ref("table_name"),
        columns=ref("partial_column_list"),
        values=ref("value_list_with_defaults")
    )
)

# ============================================================================
# Helper rules
# ============================================================================

g.rule("table_name",
    choice(
        "users", "orders", "products", "inventory", 
        "transactions", "logs", "sessions", "analytics",
        "customers", "suppliers", "employees", "departments"
    )
)

g.rule("column_list",
    choice(
        "id, name, email, status",
        "user_id, product_id, quantity, price",
        "customer_id, order_date, total_amount, status",
        "product_code, description, unit_price, stock_quantity",
        "employee_id, first_name, last_name, department_id, hire_date",
        "transaction_id, account_id, amount, transaction_type, timestamp"
    )
)

g.rule("partial_column_list",
    choice(
        "name, email",
        "product_id, quantity",
        "customer_id, total_amount",
        "description, unit_price",
        "first_name, last_name, department_id"
    )
)

g.rule("select_columns",
    choice(
        ref("column_list"),
        "id, name, status",
        "COUNT(*), MAX(price), MIN(quantity)",
        "DISTINCT product_id, SUM(quantity)"
    )
)

g.rule("value_list",
    choice(
        # Numeric and string values
        Lambda(lambda ctx: f"{ctx.rng.randint(1, 1000)}, '{ctx.rng.choice(['John', 'Jane', 'Bob', 'Alice'])}', '{ctx.rng.choice(['user', 'admin', 'guest'])}@example.com', '{ctx.rng.choice(['active', 'inactive', 'pending'])}'"),
        
        # Order-like values
        Lambda(lambda ctx: f"{ctx.rng.randint(1, 100)}, {ctx.rng.randint(1, 50)}, {ctx.rng.randint(1, 20)}, {ctx.rng.uniform(10.0, 999.99):.2f}"),
        
        # Timestamp values
        Lambda(lambda ctx: f"{ctx.rng.randint(1000, 9999)}, {ctx.rng.randint(1, 100)}, {ctx.rng.uniform(0.01, 9999.99):.2f}, '{ctx.rng.choice(['debit', 'credit', 'transfer'])}', CURRENT_TIMESTAMP"),
        
        # Mixed with NULLs
        Lambda(lambda ctx: f"NULL, '{ctx.rng.choice(['Product', 'Service', 'Item'])} {ctx.rng.randint(1, 999)}', {ctx.rng.uniform(1.0, 999.99):.2f}, {ctx.rng.randint(0, 1000)}")
    )
)

g.rule("value_list_with_defaults",
    choice(
        Lambda(lambda ctx: f"'{ctx.rng.choice(['Test', 'Demo', 'Sample'])} {ctx.rng.randint(1, 999)}', '{ctx.rng.choice(['test', 'demo', 'sample'])}_{ctx.rng.randint(1, 999)}@example.com'"),
        Lambda(lambda ctx: f"{ctx.rng.randint(1, 100)}, {ctx.rng.randint(1, 999)}"),
        Lambda(lambda ctx: f"{ctx.rng.randint(1, 100)}, {ctx.rng.uniform(10.0, 9999.99):.2f}"),
        "DEFAULT, DEFAULT",
        Lambda(lambda ctx: f"'{ctx.rng.choice(['Alpha', 'Beta', 'Gamma'])}', {ctx.rng.uniform(1.0, 100.0):.2f}")
    )
)

g.rule("where_condition",
    choice(
        Lambda(lambda ctx: f"status = '{ctx.rng.choice(['active', 'pending', 'completed'])}'"),
        Lambda(lambda ctx: f"id > {ctx.rng.randint(1, 100)}"),
        Lambda(lambda ctx: f"created_at > CURRENT_DATE - INTERVAL '{ctx.rng.randint(1, 30)} days'"),
        Lambda(lambda ctx: f"price BETWEEN {ctx.rng.randint(10, 100)} AND {ctx.rng.randint(101, 1000)}"),
        Lambda(lambda ctx: f"quantity > {ctx.rng.randint(0, 50)} AND status != 'deleted'")
    )
)

if __name__ == "__main__":
    print("INSERT-focused Grammar Test")
    print("=" * 60)
    
    for i in range(10):
        query = g.generate("query", seed=i)
        print(f"\n{i+1}. {query}")