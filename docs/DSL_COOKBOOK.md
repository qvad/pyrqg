# PyRQG DSL Cookbook

A collection of practical examples and recipes for common SQL generation patterns using PyRQG's DSL.

## Table of Contents

1. [Basic Queries](#basic-queries)
2. [Complex SELECT Patterns](#complex-select-patterns)
3. [INSERT Patterns](#insert-patterns)
4. [UPDATE and DELETE](#update-and-delete)
5. [JOIN Patterns](#join-patterns)
6. [Subqueries](#subqueries)
7. [CTEs and Window Functions](#ctes-and-window-functions)
8. [Transactions](#transactions)
9. [DDL Patterns](#ddl-patterns)
10. [Performance Testing Patterns](#performance-testing-patterns)
11. [Data Type Specific Patterns](#data-type-specific-patterns)
12. [Advanced Techniques](#advanced-techniques)

## Basic Queries

### Simple SELECT with Optional Clauses

```python
from pyrqg.dsl.core import Grammar, template, choice, maybe, number

grammar = Grammar("basic_select")

grammar.rule("query", template(
    "SELECT {columns} FROM {table} {where} {order} {limit}",
    columns=choice("*", "id, name", "id, email", "COUNT(*)"),
    table=choice("users", "orders", "products"),
    where=maybe(template("WHERE {condition}"), 0.7),
    order=maybe(template("ORDER BY {column} {dir}"), 0.3),
    limit=maybe(template("LIMIT {n}"), 0.2)
))

grammar.rule("condition", choice(
    template("id = {n}", n=number(1, 1000)),
    template("status = '{s}'", s=choice("active", "pending", "completed")),
    template("created_at > CURRENT_DATE - INTERVAL '{d} days'", d=number(1, 30))
))

grammar.rule("column", choice("id", "name", "created_at", "status"))
grammar.rule("dir", choice("ASC", "DESC"))
grammar.rule("n", number(10, 100))

# Generates queries like:
# SELECT * FROM users WHERE id = 42 ORDER BY created_at DESC LIMIT 20
# SELECT id, name FROM orders WHERE status = 'pending'
# SELECT COUNT(*) FROM products
```

### Parameterized Queries

```python
grammar = Grammar("parameterized")

# Using positional parameters
grammar.rule("pg_query", template(
    "SELECT * FROM users WHERE id = $1 AND status = $2"
))

# Using named parameters
grammar.rule("named_query", template(
    "SELECT * FROM orders WHERE user_id = :user_id AND total > :min_total"
))

# Dynamic parameter count
grammar.rule("in_query", template(
    "SELECT * FROM products WHERE id IN ({params})",
    params=repeat("$1", min=1, max=5, separator=", ")
))
```

## Complex SELECT Patterns

### Multi-condition WHERE Clauses

```python
grammar = Grammar("complex_where")

grammar.rule("query", template(
    "SELECT * FROM {table} WHERE {conditions}"
))

grammar.rule("conditions", choice(
    ref("simple_condition"),
    ref("and_conditions"),
    ref("or_conditions"),
    ref("complex_conditions")
))

grammar.rule("simple_condition", choice(
    template("{col} = {val}"),
    template("{col} > {val}"),
    template("{col} BETWEEN {val1} AND {val2}"),
    template("{col} IN ({vals})"),
    template("{col} IS NULL"),
    template("{col} IS NOT NULL")
))

grammar.rule("and_conditions", template(
    "{cond1} AND {cond2}",
    cond1=ref("simple_condition"),
    cond2=ref("simple_condition")
))

grammar.rule("or_conditions", template(
    "{cond1} OR {cond2}",
    cond1=ref("simple_condition"),
    cond2=ref("simple_condition")
))

grammar.rule("complex_conditions", template(
    "({and_conds}) OR ({simple})",
    and_conds=ref("and_conditions"),
    simple=ref("simple_condition")
))

grammar.rule("col", choice("id", "status", "price", "created_at"))
grammar.rule("val", choice("1", "'active'", "100.00", "CURRENT_DATE"))
grammar.rule("val1", number(1, 50))
grammar.rule("val2", number(51, 100))
grammar.rule("vals", repeat(number(1, 10), min=2, max=5, separator=", "))
```

### Aggregation Queries

```python
grammar = Grammar("aggregation")

grammar.rule("agg_query", template(
    "SELECT {group_cols}, {agg_exprs} "
    "FROM {table} "
    "{where} "
    "GROUP BY {group_cols} "
    "{having} "
    "{order}"
))

grammar.rule("group_cols", choice(
    "status",
    "DATE(created_at)",
    "user_id",
    "category, subcategory"
))

grammar.rule("agg_exprs", choice(
    "COUNT(*)",
    "SUM(amount), AVG(amount)",
    "MIN(price), MAX(price), AVG(price)",
    "COUNT(DISTINCT user_id)",
    "STRING_AGG(name, ', ')"
))

grammar.rule("having", maybe(template(
    "HAVING {having_cond}",
    having_cond=choice(
        "COUNT(*) > 10",
        "SUM(amount) > 1000",
        "AVG(price) BETWEEN 10 AND 100"
    )
), 0.5))

grammar.rule("order", maybe(template(
    "ORDER BY {order_expr}",
    order_expr=choice(
        "COUNT(*) DESC",
        "SUM(amount) DESC",
        "1, 2"  # Position references
    )
), 0.7))
```

## INSERT Patterns

### Single and Multi-row INSERT

```python
grammar = Grammar("insert_patterns")

# Single row insert
grammar.rule("single_insert", template(
    "INSERT INTO {table} ({columns}) VALUES ({values})"
))

# Multi-row insert
grammar.rule("multi_insert", template(
    "INSERT INTO {table} ({columns}) VALUES {value_lists}"
))

grammar.rule("value_lists", repeat(
    template("({values})"),
    min=2,
    max=10,
    separator=", "
))

# INSERT ... SELECT
grammar.rule("insert_select", template(
    "INSERT INTO {target_table} ({columns}) "
    "SELECT {columns} FROM {source_table} WHERE {condition}"
))

# INSERT with RETURNING
grammar.rule("insert_returning", template(
    "INSERT INTO {table} ({columns}) VALUES ({values}) "
    "RETURNING {return_cols}"
))

grammar.rule("columns", choice(
    "name, email",
    "user_id, product_id, quantity",
    "title, content, author_id"
))

grammar.rule("values", choice(
    "'John Doe', 'john@example.com'",
    "123, 456, 5",
    "'Test Post', 'Content here', 1"
))

grammar.rule("return_cols", choice("id", "*", "id, created_at"))
```

### UPSERT Patterns (INSERT ON CONFLICT)

```python
grammar = Grammar("upsert")

# Basic upsert
grammar.rule("upsert_basic", template(
    "INSERT INTO {table} ({columns}) VALUES ({values}) "
    "ON CONFLICT ({conflict_cols}) DO UPDATE "
    "SET {updates}"
))

# Upsert with WHERE clause
grammar.rule("upsert_conditional", template(
    "INSERT INTO {table} ({columns}) VALUES ({values}) "
    "ON CONFLICT ({conflict_cols}) DO UPDATE "
    "SET {updates} "
    "WHERE {table}.{condition}"
))

# Do nothing on conflict
grammar.rule("insert_ignore", template(
    "INSERT INTO {table} ({columns}) VALUES ({values}) "
    "ON CONFLICT ({conflict_cols}) DO NOTHING"
))

grammar.rule("conflict_cols", choice(
    "id",
    "email",
    "(user_id, product_id)",
    "lower(email)"
))

grammar.rule("updates", choice(
    "updated_at = EXCLUDED.updated_at",
    "count = {table}.count + 1",
    "data = EXCLUDED.data, updated_at = CURRENT_TIMESTAMP"
))
```

## UPDATE and DELETE

### UPDATE Patterns

```python
grammar = Grammar("update_patterns")

# Basic update
grammar.rule("basic_update", template(
    "UPDATE {table} SET {assignments} WHERE {condition}"
))

# Update with FROM clause
grammar.rule("update_from", template(
    "UPDATE {table1} "
    "SET {assignments} "
    "FROM {table2} "
    "WHERE {table1}.{fk} = {table2}.id AND {condition}"
))

# Update with subquery
grammar.rule("update_subquery", template(
    "UPDATE {table} "
    "SET {column} = ({subquery}) "
    "WHERE {condition}"
))

grammar.rule("assignments", choice(
    "status = 'active'",
    "updated_at = CURRENT_TIMESTAMP",
    "price = price * 1.1, updated_at = CURRENT_TIMESTAMP",
    "data = jsonb_set(data, '{path}', '{value}')"
))

grammar.rule("subquery", choice(
    "SELECT AVG(price) FROM products WHERE category = {table}.category",
    "SELECT COUNT(*) FROM orders WHERE user_id = {table}.id"
))
```

### DELETE Patterns

```python
grammar = Grammar("delete_patterns")

# Basic delete
grammar.rule("basic_delete", template(
    "DELETE FROM {table} WHERE {condition}"
))

# Delete with USING (join)
grammar.rule("delete_using", template(
    "DELETE FROM {table1} "
    "USING {table2} "
    "WHERE {table1}.{fk} = {table2}.id AND {condition}"
))

# Delete with RETURNING
grammar.rule("delete_returning", template(
    "DELETE FROM {table} WHERE {condition} RETURNING *"
))

# Cascading delete pattern
grammar.rule("cascade_delete", template(
    "WITH deleted AS ("
    "  DELETE FROM {parent_table} WHERE {condition} RETURNING id"
    ") "
    "DELETE FROM {child_table} WHERE {fk} IN (SELECT id FROM deleted)"
))
```

## JOIN Patterns

### Various JOIN Types

```python
grammar = Grammar("join_patterns")

# Basic joins
grammar.rule("inner_join", template(
    "SELECT {cols} FROM {t1} "
    "INNER JOIN {t2} ON {t1}.{fk} = {t2}.id"
))

grammar.rule("left_join", template(
    "SELECT {cols} FROM {t1} "
    "LEFT JOIN {t2} ON {t1}.{fk} = {t2}.id "
    "WHERE {t2}.id IS NULL"  # Find unmatched records
))

# Multiple joins
grammar.rule("multi_join", template(
    "SELECT {cols} "
    "FROM orders o "
    "INNER JOIN users u ON o.user_id = u.id "
    "INNER JOIN products p ON o.product_id = p.id "
    "LEFT JOIN reviews r ON p.id = r.product_id AND r.user_id = u.id"
))

# Self join
grammar.rule("self_join", template(
    "SELECT e1.name AS employee, e2.name AS manager "
    "FROM employees e1 "
    "LEFT JOIN employees e2 ON e1.manager_id = e2.id"
))

# Cross join with condition
grammar.rule("cross_join", template(
    "SELECT {cols} "
    "FROM {t1} CROSS JOIN {t2} "
    "WHERE {condition}"
))

grammar.rule("cols", choice(
    "*",
    "t1.*, t2.name",
    "t1.id, t1.name, t2.id AS other_id, t2.name AS other_name"
))
```

### Complex JOIN Conditions

```python
grammar = Grammar("complex_joins")

grammar.rule("multi_condition_join", template(
    "SELECT * FROM {t1} "
    "JOIN {t2} ON {t1}.id = {t2}.{t1}_id "
    "AND {t2}.status = 'active' "
    "AND {t2}.created_at > {t1}.created_at"
))

grammar.rule("lateral_join", template(
    "SELECT u.*, recent_orders.* "
    "FROM users u "
    "CROSS JOIN LATERAL ("
    "  SELECT * FROM orders o "
    "  WHERE o.user_id = u.id "
    "  ORDER BY created_at DESC "
    "  LIMIT 5"
    ") AS recent_orders"
))
```

## Subqueries

### Scalar Subqueries

```python
grammar = Grammar("scalar_subqueries")

grammar.rule("select_with_scalar", template(
    "SELECT "
    "  name, "
    "  (SELECT COUNT(*) FROM orders WHERE user_id = u.id) AS order_count, "
    "  (SELECT SUM(amount) FROM payments WHERE user_id = u.id) AS total_paid "
    "FROM users u"
))

grammar.rule("where_scalar", template(
    "SELECT * FROM products "
    "WHERE price > (SELECT AVG(price) FROM products)"
))
```

### IN/EXISTS Subqueries

```python
grammar = Grammar("in_exists")

grammar.rule("in_subquery", template(
    "SELECT * FROM {table1} "
    "WHERE {column} IN ("
    "  SELECT {column} FROM {table2} WHERE {condition}"
    ")"
))

grammar.rule("not_in_subquery", template(
    "SELECT * FROM {table1} "
    "WHERE {column} NOT IN ("
    "  SELECT {column} FROM {table2} WHERE {column} IS NOT NULL"
    ")"
))

grammar.rule("exists_subquery", template(
    "SELECT * FROM users u "
    "WHERE EXISTS ("
    "  SELECT 1 FROM orders o "
    "  WHERE o.user_id = u.id AND o.created_at > CURRENT_DATE - INTERVAL '30 days'"
    ")"
))

grammar.rule("not_exists_subquery", template(
    "SELECT * FROM products p "
    "WHERE NOT EXISTS ("
    "  SELECT 1 FROM order_items oi WHERE oi.product_id = p.id"
    ")"
))
```

### Correlated Subqueries

```python
grammar = Grammar("correlated")

grammar.rule("correlated_where", template(
    "SELECT * FROM employees e1 "
    "WHERE salary > ("
    "  SELECT AVG(salary) FROM employees e2 "
    "  WHERE e2.department_id = e1.department_id"
    ")"
))

grammar.rule("correlated_select", template(
    "SELECT "
    "  p.*, "
    "  (SELECT COUNT(*) FROM order_items oi WHERE oi.product_id = p.id) AS times_ordered, "
    "  (SELECT MAX(o.created_at) FROM orders o "
    "   JOIN order_items oi ON o.id = oi.order_id "
    "   WHERE oi.product_id = p.id) AS last_ordered "
    "FROM products p"
))
```

## CTEs and Window Functions

### Common Table Expressions

```python
grammar = Grammar("cte_patterns")

# Simple CTE
grammar.rule("simple_cte", template("""
WITH active_users AS (
  SELECT * FROM users WHERE status = 'active'
)
SELECT * FROM active_users WHERE created_at > CURRENT_DATE - INTERVAL '7 days'
"""))

# Multiple CTEs
grammar.rule("multiple_ctes", template("""
WITH 
user_stats AS (
  SELECT user_id, COUNT(*) as order_count, SUM(total) as total_spent
  FROM orders
  GROUP BY user_id
),
product_stats AS (
  SELECT product_id, COUNT(*) as times_ordered, AVG(quantity) as avg_quantity
  FROM order_items
  GROUP BY product_id
)
SELECT u.name, us.order_count, us.total_spent, ps.times_ordered
FROM users u
JOIN user_stats us ON u.id = us.user_id
CROSS JOIN product_stats ps
"""))

# Recursive CTE
grammar.rule("recursive_cte", template("""
WITH RECURSIVE category_tree AS (
  -- Base case
  SELECT id, name, parent_id, 0 as level
  FROM categories
  WHERE parent_id IS NULL
  
  UNION ALL
  
  -- Recursive case
  SELECT c.id, c.name, c.parent_id, ct.level + 1
  FROM categories c
  JOIN category_tree ct ON c.parent_id = ct.id
)
SELECT * FROM category_tree ORDER BY level, name
"""))
```

### Window Functions

```python
grammar = Grammar("window_functions")

# Ranking functions
grammar.rule("ranking", template("""
SELECT 
  name,
  department,
  salary,
  ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
  RANK() OVER (ORDER BY salary DESC) as overall_rank,
  DENSE_RANK() OVER (ORDER BY salary DESC) as dense_rank,
  NTILE(4) OVER (ORDER BY salary) as salary_quartile
FROM employees
"""))

# Aggregate window functions
grammar.rule("window_aggregates", template("""
SELECT 
  date,
  amount,
  SUM(amount) OVER (ORDER BY date) as running_total,
  AVG(amount) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_avg_7day,
  MAX(amount) OVER (PARTITION BY EXTRACT(MONTH FROM date)) as month_max,
  COUNT(*) OVER () as total_rows
FROM transactions
"""))

# Lead/Lag functions
grammar.rule("lead_lag", template("""
SELECT 
  user_id,
  login_time,
  LAG(login_time) OVER (PARTITION BY user_id ORDER BY login_time) as previous_login,
  LEAD(login_time) OVER (PARTITION BY user_id ORDER BY login_time) as next_login,
  login_time - LAG(login_time) OVER (PARTITION BY user_id ORDER BY login_time) as time_since_last
FROM user_logins
"""))
```

## Transactions

### Transaction Patterns

```python
grammar = Grammar("transactions")

# Basic transaction
grammar.rule("basic_transaction", template("""
BEGIN;
{operation1};
{operation2};
{operation3};
COMMIT;
"""))

# Transaction with savepoint
grammar.rule("savepoint_transaction", template("""
BEGIN;
{operation1};
SAVEPOINT sp1;
{risky_operation};
{check_operation};
ROLLBACK TO sp1;  -- Or RELEASE sp1 if successful
{safe_operation};
COMMIT;
"""))

# Transaction with error handling pattern
grammar.rule("conditional_transaction", template("""
BEGIN;
UPDATE accounts SET balance = balance - {amount} WHERE id = {from_id};
UPDATE accounts SET balance = balance + {amount} WHERE id = {to_id};
-- Check constraint would fail if balance < 0
COMMIT;
"""))

grammar.rule("operation1", choice(
    "INSERT INTO logs (message) VALUES ('Transaction started')",
    "UPDATE users SET last_active = CURRENT_TIMESTAMP WHERE id = 1"
))

grammar.rule("operation2", choice(
    "UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 100",
    "INSERT INTO orders (user_id, total) VALUES (1, 99.99)"
))

grammar.rule("operation3", choice(
    "INSERT INTO audit_log (action, timestamp) VALUES ('purchase', CURRENT_TIMESTAMP)",
    "UPDATE statistics SET total_sales = total_sales + 1"
))
```

## DDL Patterns

### Table Creation

```python
grammar = Grammar("ddl_patterns")

# Basic table
grammar.rule("create_table", template("""
CREATE TABLE {table_name} (
  id SERIAL PRIMARY KEY,
  {columns},
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""))

# Table with constraints
grammar.rule("table_with_constraints", template("""
CREATE TABLE {table_name} (
  id SERIAL PRIMARY KEY,
  email VARCHAR(255) UNIQUE NOT NULL,
  age INTEGER CHECK (age >= 0 AND age <= 150),
  status VARCHAR(50) DEFAULT 'pending',
  parent_id INTEGER REFERENCES {table_name}(id) ON DELETE CASCADE,
  CONSTRAINT {constraint_name} CHECK (status IN ('pending', 'active', 'disabled'))
)
"""))

# Partitioned table
grammar.rule("partitioned_table", template("""
CREATE TABLE measurements (
  id SERIAL,
  device_id INTEGER,
  recorded_at TIMESTAMP NOT NULL,
  temperature NUMERIC,
  humidity NUMERIC
) PARTITION BY RANGE (recorded_at);

CREATE TABLE measurements_2024_01 PARTITION OF measurements
  FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
"""))

grammar.rule("columns", repeat(
    choice(
        "name VARCHAR(255) NOT NULL",
        "email VARCHAR(255) UNIQUE",
        "price DECIMAL(10,2) DEFAULT 0.00",
        "quantity INTEGER NOT NULL DEFAULT 1",
        "data JSONB",
        "tags TEXT[]"
    ),
    min=2,
    max=5,
    separator=",\n  "
))
```

### Index Patterns

```python
grammar = Grammar("index_patterns")

# Various index types
grammar.rule("btree_index", template(
    "CREATE INDEX idx_{table}_{column} ON {table} ({column})"
))

grammar.rule("composite_index", template(
    "CREATE INDEX idx_{table}_composite ON {table} ({col1}, {col2})"
))

grammar.rule("partial_index", template(
    "CREATE INDEX idx_{table}_active ON {table} ({column}) WHERE status = 'active'"
))

grammar.rule("expression_index", template(
    "CREATE INDEX idx_{table}_lower_{column} ON {table} (LOWER({column}))"
))

grammar.rule("gin_index", template(
    "CREATE INDEX idx_{table}_search ON {table} USING GIN (to_tsvector('english', {column}))"
))

grammar.rule("jsonb_index", template(
    "CREATE INDEX idx_{table}_data ON {table} USING GIN (data jsonb_path_ops)"
))
```

## Performance Testing Patterns

### Heavy Queries

```python
grammar = Grammar("performance_testing")

# Large result set
grammar.rule("large_scan", template(
    "SELECT * FROM {large_table} WHERE {column} LIKE '%{pattern}%'"
))

# Complex aggregation
grammar.rule("heavy_aggregation", template("""
SELECT 
  {group_by_cols},
  COUNT(*) as cnt,
  COUNT(DISTINCT {distinct_col}) as unique_cnt,
  STRING_AGG(DISTINCT {string_col}, ', ') as values,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {numeric_col}) as median
FROM {table}
{joins}
WHERE {complex_condition}
GROUP BY {group_by_cols}
HAVING COUNT(*) > {min_count}
ORDER BY cnt DESC
"""))

# Expensive join
grammar.rule("expensive_join", template("""
SELECT *
FROM {table1} t1
CROSS JOIN {table2} t2
WHERE 
  LEVENSHTEIN(t1.{text_col}, t2.{text_col}) < 3
  OR t1.{col} = t2.{col}
"""))

# Recursive query
grammar.rule("recursive_stress", template("""
WITH RECURSIVE series(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM series WHERE n < {limit}
)
SELECT * FROM series CROSS JOIN {table}
"""))

grammar.rule("limit", choice("100", "1000", "10000"))
grammar.rule("large_table", choice("events", "logs", "metrics"))
grammar.rule("min_count", number(10, 100))
```

## Data Type Specific Patterns

### JSON/JSONB Operations

```python
grammar = Grammar("json_patterns")

# JSON selection
grammar.rule("json_select", choice(
    template("SELECT data->'{key}' FROM {table}"),
    template("SELECT data->>'{key}' FROM {table}"),
    template("SELECT data#>'{{{path}}}' FROM {table}"),
    template("SELECT data#>>'{{{path}}}' FROM {table}"),
    template("SELECT jsonb_path_query(data, '$.{path}') FROM {table}")
))

# JSON updates
grammar.rule("json_update", choice(
    template("UPDATE {table} SET data = data || '{new_data}'::jsonb"),
    template("UPDATE {table} SET data = jsonb_set(data, '{{{path}}}', '{value}')"),
    template("UPDATE {table} SET data = data - '{key}'"),
    template("UPDATE {table} SET data = jsonb_insert(data, '{{{path}}}', '{value}')")
))

# JSON conditions
grammar.rule("json_where", choice(
    template("data ? '{key}'"),
    template("data ?& array[{keys}]"),
    template("data ?| array[{keys}]"),
    template("data @> '{contains}'::jsonb"),
    template("data <@ '{contained_by}'::jsonb"),
    template("jsonb_path_exists(data, '$.{path}')")
))

grammar.rule("key", choice("name", "email", "settings", "metadata"))
grammar.rule("path", choice("user.name", "settings.theme", "items[0]"))
grammar.rule("keys", repeat(template("'{key}'"), min=2, max=4, separator=", "))
```

### Array Operations

```python
grammar = Grammar("array_patterns")

# Array operations
grammar.rule("array_ops", choice(
    template("SELECT * FROM {table} WHERE {array_col} && ARRAY[{values}]"),  # Overlap
    template("SELECT * FROM {table} WHERE {array_col} @> ARRAY[{values}]"),  # Contains
    template("SELECT * FROM {table} WHERE {array_col} <@ ARRAY[{values}]"),  # Contained by
    template("SELECT * FROM {table} WHERE {value} = ANY({array_col})"),
    template("SELECT * FROM {table} WHERE {value} = ALL({array_col})")
))

# Array functions
grammar.rule("array_functions", choice(
    template("SELECT array_length({col}, 1) FROM {table}"),
    template("SELECT array_upper({col}, 1) FROM {table}"),
    template("SELECT unnest({col}) FROM {table}"),
    template("SELECT array_agg(DISTINCT {col}) FROM {table}"),
    template("SELECT array_to_string({col}, ', ') FROM {table}")
))

# Array updates
grammar.rule("array_update", choice(
    template("UPDATE {table} SET {col} = array_append({col}, {value})"),
    template("UPDATE {table} SET {col} = array_prepend({value}, {col})"),
    template("UPDATE {table} SET {col} = array_remove({col}, {value})"),
    template("UPDATE {table} SET {col} = {col} || ARRAY[{values}]")
))
```

### Date/Time Operations

```python
grammar = Grammar("datetime_patterns")

# Date arithmetic
grammar.rule("date_math", choice(
    template("created_at + INTERVAL '{n} {unit}'"),
    template("created_at - INTERVAL '{n} {unit}'"),
    template("date_trunc('{unit}', created_at)"),
    template("extract({part} FROM created_at)")
))

# Date comparisons
grammar.rule("date_where", choice(
    template("{date_col} >= CURRENT_DATE - INTERVAL '{n} days'"),
    template("{date_col} BETWEEN '{start_date}' AND '{end_date}'"),
    template("extract(hour FROM {time_col}) BETWEEN {start_hour} AND {end_hour}"),
    template("date_part('dow', {date_col}) IN ({dow_list})")  # Day of week
))

# Date formatting
grammar.rule("date_format", choice(
    template("to_char({date_col}, 'YYYY-MM-DD')"),
    template("to_char({date_col}, 'Day, DD Mon YYYY')"),
    template("to_char({date_col}, 'HH24:MI:SS')")
))

grammar.rule("n", number(1, 30))
grammar.rule("unit", choice("day", "week", "month", "year", "hour", "minute"))
grammar.rule("part", choice("year", "month", "day", "hour", "minute", "dow", "doy"))
grammar.rule("dow_list", choice("0, 6", "1, 2, 3, 4, 5"))  # Weekend vs weekday
```

## Advanced Techniques

### Dynamic Column Generation

```python
grammar = Grammar("dynamic_columns")

# Generate column lists based on table
def table_columns(table_name):
    columns_map = {
        "users": ["id", "name", "email", "created_at"],
        "orders": ["id", "user_id", "total", "status", "created_at"],
        "products": ["id", "name", "price", "category", "stock"]
    }
    return columns_map.get(table_name, ["id"])

# Table-aware column selection
grammar.rule("smart_select", Lambda(lambda ctx: 
    f"SELECT {', '.join(ctx.rng.sample(table_columns(ctx.current_table), k=3))} "
    f"FROM {ctx.current_table}"
))

# Correlated column values
grammar.rule("correlated_insert", Lambda(lambda ctx: 
    f"INSERT INTO audit_log (table_name, record_id, action) "
    f"VALUES ('{ctx.current_table}', {ctx.rng.randint(1, 1000)}, 'INSERT')"
))
```

### Weighted Complexity

```python
grammar = Grammar("weighted_complexity")

# Increase complexity based on weight
def complexity_weight():
    return choice(
        ref("simple_query"),     # 70% - Simple queries
        ref("medium_query"),     # 25% - Medium complexity
        ref("complex_query"),    # 5%  - Complex queries
        weights=[70, 25, 5]
    )

grammar.rule("query", complexity_weight())

grammar.rule("simple_query", template(
    "SELECT * FROM {table} WHERE id = {n}"
))

grammar.rule("medium_query", template(
    "SELECT {cols} FROM {table1} "
    "JOIN {table2} ON {join_condition} "
    "WHERE {where_condition}"
))

grammar.rule("complex_query", template("""
WITH cte AS ({cte_query})
SELECT {cols} FROM cte
JOIN {table} ON {complex_join}
WHERE {complex_where}
GROUP BY {group_by}
HAVING {having}
ORDER BY {order_by}
"""))
```

### Query Chaining

```python
grammar = Grammar("query_chains")

# Generate related queries that build on each other
class QueryChain:
    def __init__(self):
        self.last_id = None
        self.last_table = None
    
    def generate_chain(self, ctx):
        if self.last_id is None:
            # First query - INSERT
            self.last_table = ctx.rng.choice(["users", "products"])
            self.last_id = ctx.rng.randint(1000, 9999)
            return f"INSERT INTO {self.last_table} (id, name) VALUES ({self.last_id}, 'Test') RETURNING id"
        else:
            # Follow-up queries use the last ID
            query_type = ctx.rng.choice(["select", "update", "related"])
            if query_type == "select":
                return f"SELECT * FROM {self.last_table} WHERE id = {self.last_id}"
            elif query_type == "update":
                return f"UPDATE {self.last_table} SET updated_at = CURRENT_TIMESTAMP WHERE id = {self.last_id}"
            else:
                # Insert related record
                return f"INSERT INTO {self.last_table}_history ({self.last_table}_id, action) VALUES ({self.last_id}, 'modified')"

chain = QueryChain()
grammar.rule("chained_query", Lambda(chain.generate_chain))
```

### Schema Evolution Patterns

```python
grammar = Grammar("schema_evolution")

# Generate migration-style queries
grammar.rule("add_column", template(
    "ALTER TABLE {table} ADD COLUMN {column_def}"
))

grammar.rule("column_def", choice(
    template("{name} {type} {constraints}"),
    template("{name} {type} DEFAULT {default} {constraints}")
))

grammar.rule("safe_migration", template("""
-- Add column with default
ALTER TABLE {table} ADD COLUMN {column} {type} DEFAULT {default};
-- Backfill data
UPDATE {table} SET {column} = {backfill_expr} WHERE {column} = {default};
-- Remove default if needed
ALTER TABLE {table} ALTER COLUMN {column} DROP DEFAULT;
"""))

grammar.rule("constraints", choice(
    "",
    "NOT NULL",
    "UNIQUE",
    "CHECK ({check_expr})"
))
```

## Summary

This cookbook provides practical patterns for generating diverse SQL queries using PyRQG's DSL. Key takeaways:

1. **Start Simple**: Begin with basic patterns and add complexity gradually
2. **Compose Elements**: Build complex queries from simple, reusable components
3. **Use Weights**: Model realistic query distributions with weighted choices
4. **Consider Context**: Use Lambda and custom elements for context-aware generation
5. **Test Patterns**: Validate generated queries against your target database

For more examples, explore the `grammars/` directory in the PyRQG repository. Each grammar demonstrates different techniques and patterns you can adapt for your needs.