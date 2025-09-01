"""
Enhanced DML Grammar with improved uniqueness
Uses dynamic values and more variety to ensure unique queries
"""

import sys
from pathlib import Path

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda, Literal
import hashlib

# Create grammar instance
g = Grammar("dml_unique")

# Global counter for ensuring uniqueness
_query_counter = [0]

def unique_id(ctx):
    """Generate a unique identifier based on context"""
    _query_counter[0] += 1
    seed_part = ctx.seed if ctx.seed is not None else ctx.rng.randint(0, 999999)
    return f"{seed_part}_{_query_counter[0]}"

def unique_email(ctx):
    """Generate unique email"""
    uid = unique_id(ctx)
    domains = ['test.com', 'example.org', 'demo.net', 'sample.io']
    return f"'user_{uid}@{ctx.rng.choice(domains)}'"

def unique_name(ctx):
    """Generate unique name"""
    first_names = ['John', 'Jane', 'Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
    uid = unique_id(ctx)
    return f"'{ctx.rng.choice(first_names)} {ctx.rng.choice(last_names)} {uid}'"

def unique_product(ctx):
    """Generate unique product name"""
    types = ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse', 'Speaker']
    brands = ['Pro', 'Plus', 'Max', 'Ultra', 'Elite', 'Prime']
    uid = unique_id(ctx)
    return f"'{ctx.rng.choice(types)} {ctx.rng.choice(brands)} {uid}'"

def random_timestamp(ctx):
    """Generate random timestamp"""
    year = ctx.rng.randint(2020, 2024)
    month = ctx.rng.randint(1, 12)
    day = ctx.rng.randint(1, 28)
    hour = ctx.rng.randint(0, 23)
    minute = ctx.rng.randint(0, 59)
    return f"'{year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:00'"

# ============================================================================
# Main Query Types with more variety
# ============================================================================

g.rule("query",
    choice(
        ref("insert_complex"),
        ref("update_complex"),
        ref("delete_complex"),
        ref("upsert_complex"),
        ref("cte_complex"),
        weights=[25, 25, 20, 20, 10]
    )
)

# ============================================================================
# Complex INSERT with high uniqueness
# ============================================================================

g.rule("insert_complex",
    choice(
        # Single row with many dynamic values
        template("INSERT INTO {table} ({columns}) VALUES ({values})",
            table=ref("table_name"),
            columns=ref("column_list_dynamic"),
            values=ref("value_list_dynamic")
        ),
        
        # Multi-row with unique values
        template("INSERT INTO {table} ({columns}) VALUES {multi_values}",
            table=ref("table_name"),
            columns=ref("column_list_dynamic"),
            multi_values=repeat(
                template("({values})", values=ref("value_list_dynamic")),
                min=2, max=5, sep=", "
            )
        ),
        
        # INSERT SELECT with calculations
        template("INSERT INTO {table1} ({columns}) SELECT {select_expr} FROM {table2} WHERE {condition}",
            table1=ref("table_name"),
            columns=ref("column_list_dynamic"),
            select_expr=ref("select_expression"),
            table2=ref("table_name"),
            condition=ref("complex_condition")
        )
    )
)

# ============================================================================
# Complex UPDATE with high uniqueness
# ============================================================================

g.rule("update_complex",
    choice(
        # Update with expressions
        template("UPDATE {table} SET {assignments} WHERE {condition}",
            table=ref("table_name"),
            assignments=ref("update_assignments_dynamic"),
            condition=ref("complex_condition")
        ),
        
        # Update with subquery
        template("""UPDATE {table1} SET {field} = (
    SELECT {agg}({field2}) FROM {table2} 
    WHERE {table2}.{link} = {table1}.{link} AND {condition}
) WHERE {outer_condition}""",
            table1=ref("table_name"),
            field=ref("field_name"),
            agg=choice("MAX", "MIN", "AVG", "COUNT"),
            field2=ref("field_name"),
            table2=ref("table_name"),
            link=ref("link_field"),
            condition=ref("complex_condition"),
            outer_condition=ref("complex_condition")
        )
    )
)

# ============================================================================
# Complex conditions for uniqueness
# ============================================================================

g.rule("complex_condition",
    choice(
        # Range condition with random values
        template("{field} BETWEEN {low} AND {high}",
            field=ref("field_name"),
            low=Lambda(lambda ctx: ctx.rng.randint(1, 500)),
            high=Lambda(lambda ctx: ctx.rng.randint(501, 1000))
        ),
        
        # Pattern matching
        template("{field} LIKE '{pattern}%'",
            field=ref("field_name"),
            pattern=Lambda(lambda ctx: f"{ctx.rng.choice(['A', 'B', 'C'])}{ctx.rng.randint(100, 999)}")
        ),
        
        # Complex IN clause
        template("{field} IN ({values})",
            field=ref("field_name"),
            values=Lambda(lambda ctx: ', '.join(str(ctx.rng.randint(1, 1000)) for _ in range(ctx.rng.randint(2, 5))))
        ),
        
        # Date condition
        template("{field} > {timestamp}",
            field=choice("created_at", "updated_at"),
            timestamp=Lambda(random_timestamp)
        ),
        
        # Compound condition
        template("({cond1}) {op} ({cond2})",
            cond1=ref("simple_condition"),
            op=choice("AND", "OR"),
            cond2=ref("simple_condition")
        )
    )
)

# ============================================================================
# Dynamic value generation
# ============================================================================

g.rule("value_list_dynamic",
    repeat(ref("dynamic_value"), min=2, max=6, sep=", ")
)

g.rule("dynamic_value",
    choice(
        # Numeric with wide range
        Lambda(lambda ctx: ctx.rng.randint(1, 100000)),
        
        # Unique strings
        Lambda(unique_email),
        Lambda(unique_name),
        Lambda(unique_product),
        
        # Timestamps
        Lambda(random_timestamp),
        
        # Expressions
        Lambda(lambda ctx: f"{ctx.rng.randint(1, 100)} * {ctx.rng.randint(1, 100)}"),
        
        # UUIDs (simulated)
        Lambda(lambda ctx: f"'{hashlib.md5(str(ctx.rng.random()).encode()).hexdigest()}'"),
        
        # JSON data
        Lambda(lambda ctx: f"'{{\"id\": {ctx.rng.randint(1, 1000)}, \"value\": \"{ctx.rng.choice(['A', 'B', 'C'])}\"}}'"),
        
        weights=[20, 15, 15, 10, 10, 10, 10, 10]
    )
)

# ============================================================================
# Dynamic assignments
# ============================================================================

g.rule("update_assignments_dynamic",
    repeat(
        choice(
            # Simple assignment with dynamic value
            template("{field} = {value}",
                field=ref("field_name"),
                value=ref("dynamic_value")
            ),
            
            # Expression assignment
            template("{field} = {field} {op} {value}",
                field=ref("field_name"),
                op=choice("+", "-", "*", "/"),
                value=Lambda(lambda ctx: ctx.rng.randint(1, 100))
            ),
            
            # Complex expression
            template("{field} = CASE WHEN {condition} THEN {val1} ELSE {val2} END",
                field=ref("field_name"),
                condition=ref("simple_condition"),
                val1=ref("dynamic_value"),
                val2=ref("dynamic_value")
            )
        ),
        min=1, max=4, sep=", "
    )
)

# ============================================================================
# Enhanced UPSERT for uniqueness
# ============================================================================

g.rule("upsert_complex",
    template("""INSERT INTO {table} ({columns}) 
VALUES ({values})
ON CONFLICT ({conflict_col}) DO UPDATE SET 
{assignments}
WHERE {condition}""",
        table=ref("table_name"),
        columns=ref("column_list_dynamic"),
        values=ref("value_list_dynamic"),
        conflict_col=ref("unique_column"),
        assignments=ref("conflict_assignments"),
        condition=ref("complex_condition")
    )
)

g.rule("conflict_assignments",
    repeat(
        template("{col} = EXCLUDED.{col} || '_updated_' || {suffix}",
            col=ref("field_name"),
            suffix=Lambda(lambda ctx: f"'{ctx.rng.randint(1000, 9999)}'")
        ),
        min=1, max=3, sep=", "
    )
)

# ============================================================================
# Complex CTEs
# ============================================================================

g.rule("cte_complex",
    template("""WITH {cte1} AS (
    SELECT {cols1} FROM {table1} 
    WHERE {cond1}
    LIMIT {limit1}
),
{cte2} AS (
    SELECT {cols2} FROM {cte1}
    JOIN {table2} ON {join_cond}
    WHERE {cond2}
)
{final_query}""",
        cte1=Lambda(lambda ctx: f"cte_{ctx.rng.randint(1000, 9999)}"),
        cols1=ref("select_expression"),
        table1=ref("table_name"),
        cond1=ref("complex_condition"),
        limit1=Lambda(lambda ctx: ctx.rng.randint(10, 100)),
        cte2=Lambda(lambda ctx: f"cte_{ctx.rng.randint(1000, 9999)}"),
        cols2=ref("select_expression"),
        table2=ref("table_name"),
        join_cond=ref("join_condition_dynamic"),
        cond2=ref("complex_condition"),
        final_query=ref("cte_final_query")
    )
)

# ============================================================================
# Helper rules with more variety
# ============================================================================

g.rule("table_name",
    Lambda(lambda ctx: ctx.rng.choice([
        "users", "orders", "products", "inventory", 
        "transactions", "logs", "sessions", "analytics"
    ]))
)

g.rule("field_name",
    Lambda(lambda ctx: ctx.rng.choice([
        "id", "user_id", "product_id", "order_id",
        "name", "email", "status", "quantity",
        "price", "total", "created_at", "updated_at",
        "data", "metadata", "score", "rating"
    ]))
)

g.rule("unique_column",
    choice("id", "email", "product_id", "order_id", "session_id")
)

g.rule("column_list_dynamic",
    Lambda(lambda ctx: ', '.join(
        ctx.rng.sample(
            ["id", "user_id", "name", "email", "status", "quantity", "price", "data"],
            k=ctx.rng.randint(2, 5)
        )
    ))
)

g.rule("select_expression",
    Lambda(lambda ctx: ', '.join([
        (lambda f:
            # Avoid invalid per-column DISTINCT; only allow function wrappers here
            f if any(agg in f for agg in ['COUNT(', 'SUM(', 'AVG(', 'MIN(', 'MAX(']) else
            (lambda prefix: f"{prefix}{f}{')'}" if '(' in prefix else f"{prefix}{f}")(
                ctx.rng.choice(['', 'UPPER(', 'LOWER('])
            )
        )(field)
        for field in ctx.rng.sample(
            ["id", "name", "email", "COUNT(*)", "SUM(price)", "AVG(quantity)"],
            k=ctx.rng.randint(2, 4)
        )
    ]))
)

g.rule("simple_condition",
    template("{field} {op} {value}",
        field=ref("field_name"),
        op=choice("=", "!=", ">", "<", ">=", "<="),
        value=ref("dynamic_value")
    )
)

g.rule("join_condition_dynamic",
    Lambda(lambda ctx: 
        f"{ctx.rng.choice(['t1', 't2'])}.{ctx.rng.choice(['id', 'user_id', 'product_id'])} = "
        f"{ctx.rng.choice(['t2', 't1'])}.{ctx.rng.choice(['user_id', 'product_id', 'order_id'])}"
    )
)

g.rule("link_field",
    choice("id", "user_id", "product_id", "order_id")
)

g.rule("delete_complex",
    choice(
        template("DELETE FROM {table} WHERE {condition}",
            table=ref("table_name"),
            condition=ref("complex_condition")
        ),
        
        template("""DELETE FROM {table1} 
WHERE {field} IN (
    SELECT {field} FROM {table2} 
    WHERE {condition}
    ORDER BY {order_field} {order}
    LIMIT {limit}
)""",
            table1=ref("table_name"),
            field=ref("field_name"),
            table2=ref("table_name"),
            condition=ref("complex_condition"),
            order_field=ref("field_name"),
            order=choice("ASC", "DESC"),
            limit=Lambda(lambda ctx: ctx.rng.randint(5, 50))
        )
    )
)

g.rule("cte_final_query",
    choice(
        template("SELECT {expr} FROM {cte}",
            expr=ref("select_expression"),
            cte=Lambda(lambda ctx: f"cte_{ctx.rng.randint(1000, 9999)}")
        ),
        
        template("INSERT INTO {table} ({cols}) SELECT {expr} FROM {cte}",
            table=ref("table_name"),
            cols=ref("column_list_dynamic"),
            expr=ref("select_expression"),
            cte=Lambda(lambda ctx: f"cte_{ctx.rng.randint(1000, 9999)}")
        ),
        
        template("UPDATE {table} SET {assignments} WHERE {field} IN (SELECT {field} FROM {cte})",
            table=ref("table_name"),
            assignments=ref("update_assignments_dynamic"),
            field=ref("field_name"),
            cte=Lambda(lambda ctx: f"cte_{ctx.rng.randint(1000, 9999)}")
        )
    )
)

if __name__ == "__main__":
    # Test uniqueness
    print("Testing Enhanced DML Grammar for Uniqueness")
    print("="*60)
    
    queries = []
    for i in range(100):
        query = g.generate("query", seed=i)
        queries.append(query)
    
    unique_queries = len(set(queries))
    print(f"Generated: {len(queries)} queries")
    print(f"Unique: {unique_queries} queries")
    print(f"Uniqueness: {unique_queries/len(queries)*100:.1f}%")
    
    # Show some examples
    print("\nSample queries:")
    for i in range(5):
        print(f"\n{i+1}. {queries[i][:80]}...")