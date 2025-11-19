"""
Generates a workload of realistic analytical queries based on common patterns
found in real-world data warehouses, such as those analyzed in recent research.

This grammar aims for high query shape uniqueness by combining various SQL features
in a modular and declarative way.
"""

from pyrqg.dsl.core import Grammar, choice, template, ref, repeat, maybe, Lambda

# --- Constants ---
REGIONS = ["APAC", "EMEA", "AMER-EAST", "AMER-WEST"]
STATUSES = ["NEW", "PROCESSING", "SHIPPED", "RETURNED", "CLOSED"]
PRODUCTS = ["Keyboard", "Mouse", "Monitor", "Webcam", "Laptop", "Dock"]

# --- Grammar Definition ---
g = Grammar("real_workload_v2")

# ============================================================================
# 1. CTE (Data Generation) Definitions
# ============================================================================

def _generate_rows(ctx, num_rows, row_generator_func):
    """Helper to generate multiple VALUES rows."""
    rows = [row_generator_func(ctx, i) for i in range(num_rows)]
    return ",\n        ".join(rows)

# --- Customers CTE ---
def _customer_row(ctx, i):
    id = 1000 + i
    name = ctx.rng.choice(["Alice", "Bob", "Charlie", "David", "Eve", "Frank"])
    region = ctx.rng.choice(REGIONS)
    return f"({id}, '{name}{i}', '{region}')"

g.rule(
    "customers_cte",
    Lambda(lambda ctx: template(
        "customers(customer_id, name, region) AS (VALUES\n        {rows}\n    )",
        rows=_generate_rows(ctx, 15, _customer_row),
    ).generate(ctx))
)

# --- Orders CTE ---
def _order_row(ctx, i):
    order_id = 5000 + i
    customer_id = 1000 + ctx.rng.randint(0, 14)
    year = ctx.rng.randint(2021, 2023)
    month = ctx.rng.randint(1, 12)
    day = ctx.rng.randint(1, 28)
    order_date = f"DATE '{year}-{month:02d}-{day:02d}'"
    status = ctx.rng.choice(STATUSES)
    return f"({order_id}, {customer_id}, {order_date}, '{status}')"

g.rule(
    "orders_cte",
    Lambda(lambda ctx: template(
        "orders(order_id, customer_id, order_date, status) AS (VALUES\n        {rows}\n    )",
        rows=_generate_rows(ctx, 30, _order_row),
    ).generate(ctx))
)

# --- Order Items CTE ---
def _item_row(ctx, i):
    order_id = 5000 + ctx.rng.randint(0, 29)
    product = ctx.rng.choice(PRODUCTS)
    quantity = ctx.rng.randint(1, 5)
    price = f"{ctx.rng.randint(10, 500)}.{ctx.rng.randint(0, 99):02d}"
    return f"({order_id}, '{product}', {quantity}, {price})"

g.rule(
    "order_items_cte",
    Lambda(lambda ctx: template(
        "order_items(order_id, product, quantity, price) AS (VALUES\n        {rows}\n    )",
        rows=_generate_rows(ctx, 50, _item_row),
    ).generate(ctx))
)

# --- Combined WITH Clause ---
g.rule("with_clause", template("WITH {customers_cte},\n     {orders_cte},\n     {order_items_cte}"))

# ============================================================================
# 2. Reusable Clause Definitions
# ============================================================================

# --- Column & Expression Rules ---
g.rule("agg_func", choice("SUM", "AVG", "COUNT", "MIN", "MAX"))
g.rule("numeric_col", choice("oi.quantity", "oi.price"))
g.rule("grouping_col", choice("c.region", "o.status", "oi.product", "c.name", "o.order_date"))

g.rule("simple_aggregation", template("{agg_func}({numeric_col})"))

g.rule("window_func", choice(
    template("ROW_NUMBER() OVER ({window_spec})"),
    template("RANK() OVER ({window_spec})"),
    template("LAG({numeric_col}, 1, 0) OVER ({window_spec})"),
    template("LEAD({numeric_col}, 1, 0) OVER ({window_spec})"),
    template("SUM({numeric_col}) OVER ({window_spec})")
))
g.rule("window_spec", template("PARTITION BY {partition_col} ORDER BY {order_col} DESC"))
g.rule("partition_col", choice("c.region", "o.status", "oi.product"))
g.rule("order_col", choice("oi.price", "oi.quantity", "o.order_date"))

# --- FROM/JOIN Clause ---
g.rule("base_tables", template("customers c\n     JOIN orders o ON c.customer_id = o.customer_id\n     JOIN order_items oi ON o.order_id = oi.order_id"))

# --- WHERE Clause ---
g.rule("string_literal", Lambda(lambda ctx: f"'{ctx.rng.choice(REGIONS + STATUSES + PRODUCTS)}'"))
g.rule("string_predicate", template("{col} {op} {val}",
    col=choice("c.region", "o.status", "oi.product"),
    op=choice("=", "!="),
    val=ref("string_literal")
))
g.rule("numeric_predicate", template("{col} {op} {val}",
    col=ref("numeric_col"),
    op=choice(">", "<", ">=", "<="),
    val=Lambda(lambda ctx: str(ctx.rng.randint(5, 100)))
))
g.rule("between_predicate", template("{numeric_col} BETWEEN {val1} AND {val2}",
    val1=Lambda(lambda ctx: str(ctx.rng.randint(1, 50))),
    val2=Lambda(lambda ctx: str(ctx.rng.randint(51, 200)))
))
g.rule("in_predicate", template("oi.product IN ({products})",
    products=Lambda(lambda ctx: ", ".join([f"'{p}'" for p in ctx.rng.sample(PRODUCTS, k=ctx.rng.randint(2,3))]))
))
g.rule("subquery_predicate", template("c.customer_id IN (SELECT DISTINCT customer_id FROM orders WHERE status = 'CLOSED')"))

g.rule("predicate", choice(
    ref("string_predicate"), ref("numeric_predicate"), ref("between_predicate"),
    ref("in_predicate"), ref("subquery_predicate"),
    weights=[30, 20, 15, 20, 10]
))
g.rule("where_clause", template("WHERE {predicates}", predicates=repeat(ref("predicate"), min=1, max=3, sep=" AND ")))

# --- ORDER BY / LIMIT Clauses ---
g.rule("order_by_clause", template("ORDER BY {order_by_cols}",
    order_by_cols=repeat(template("{col} {dir}", col=choice("1", "2"), dir=choice("ASC", "DESC")), min=1, max=2, sep=", ")
))
g.rule("limit_clause", template("LIMIT {val}", val=Lambda(lambda ctx: str(ctx.rng.choice([10, 50, 100, 500])))))


# ============================================================================
# 3. Top-Level Query Construction
# ============================================================================

# --- Query Type 1: Simple selection with joins and filters (no aggregations) ---
g.rule("simple_select_list", repeat(choice("c.name", "c.region", "o.order_date", "o.status", "oi.product", "oi.quantity", "oi.price"), min=2, max=5, sep=", "))
g.rule("simple_join_query", template(
    "{with_clause}\n"
    "SELECT {simple_select_list}\n"
    "FROM {base_tables}\n"
    "{where_clause}\n"
    "{order_by_clause}\n"
    "{limit_clause}"
))

# --- Query Type 2: Aggregation query ---
def _build_aggregation_query(ctx):
    """Builds a valid GROUP BY query, ensuring select list and group by columns match."""
    group_cols = [g.rules["grouping_col"].generate(ctx) for _ in range(ctx.rng.randint(1, 3))]
    group_cols = sorted(list(set(group_cols))) # unique columns

    select_cols = list(group_cols)
    # Add 1-2 aggregate functions
    for _ in range(ctx.rng.randint(1, 2)):
        agg = g.rules["simple_aggregation"].generate(ctx)
        select_cols.append(agg)
    
    ctx.rng.shuffle(select_cols)

    query_parts = [
        g.rules["with_clause"].generate(ctx),
        f"SELECT {', '.join(select_cols)}",
        "FROM " + g.rules["base_tables"].generate(ctx),
        g.rules["where_clause"].generate(ctx) if ctx.rng.random() < 0.8 else "",
        f"GROUP BY {', '.join(group_cols)}",
    ]
    if ctx.rng.random() < 0.7: # optional having
        having_agg = g.rules["simple_aggregation"].generate(ctx)
        op = ctx.rng.choice(['>', '<', '='])
        val = ctx.rng.randint(100, 1000)
        query_parts.append(f"HAVING {having_agg} {op} {val}")
    if ctx.rng.random() < 0.8:
        query_parts.append(g.rules["order_by_clause"].generate(ctx))
    if ctx.rng.random() < 0.6:
        query_parts.append(g.rules["limit_clause"].generate(ctx))

    return "\n".join(filter(None, query_parts))

g.rule("aggregation_query", Lambda(_build_aggregation_query))


# --- Query Type 3: Window function query (no GROUP BY) ---
def _build_window_query(ctx):
    """Builds a query with window functions, which are incompatible with GROUP BY."""
    select_cols = [
        g.rules["grouping_col"].generate(ctx),
        g.rules["numeric_col"].generate(ctx),
        g.rules["window_func"].generate(ctx)
    ]
    
    query_parts = [
        g.rules["with_clause"].generate(ctx),
        f"SELECT {', '.join(select_cols)}",
        "FROM " + g.rules["base_tables"].generate(ctx),
        g.rules["where_clause"].generate(ctx) if ctx.rng.random() < 0.8 else "",
    ]
    if ctx.rng.random() < 0.8:
        query_parts.append(g.rules["order_by_clause"].generate(ctx))
    if ctx.rng.random() < 0.6:
        query_parts.append(g.rules["limit_clause"].generate(ctx))
        
    return "\n".join(filter(None, query_parts))

g.rule("window_query", Lambda(_build_window_query))

# --- Root rule to select a query type ---
g.rule("query", choice(
    ref("simple_join_query"), 
    ref("aggregation_query"),
    ref("window_query")
))

# Export the grammar object
grammar = g