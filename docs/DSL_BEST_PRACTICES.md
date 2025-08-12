# DSL Best Practices Guide

## Overview

This guide provides best practices for writing efficient, maintainable, and high-quality PyRQG grammars. Following these practices will help you create grammars that generate diverse, realistic queries while maintaining performance at scale.

## 1. Grammar Structure and Organization

### Use Hierarchical Rule Organization

**DO:** Organize rules from high-level to low-level
```python
grammar = Grammar("well_organized")

# Top-level rules
grammar.rule("query", choice(
    ref("select_query"),
    ref("insert_query"),
    ref("update_query"),
    ref("delete_query")
))

# Query-specific rules
grammar.rule("select_query", template(
    "SELECT {columns} FROM {table} {clauses}"
))

# Component rules
grammar.rule("columns", choice(
    "*",
    ref("column_list")
))

# Atomic rules
grammar.rule("column", choice("id", "name", "email"))
```

**DON'T:** Mix abstraction levels
```python
# Bad: Mixes high-level and low-level in one rule
grammar.rule("query", choice(
    template("SELECT {columns} FROM {table}"),
    "INSERT INTO users VALUES (1, 'test')",
    ref("complex_update"),
    template("DELETE FROM {table} WHERE id = {number}")
))
```

### Group Related Rules

**DO:** Keep related rules together with clear sections
```python
# ===== Table Definitions =====
grammar.rule("table", choice("users", "orders", "products"))
grammar.rule("system_table", choice("pg_stats", "pg_tables"))
grammar.rule("temp_table", template("temp_{table}"))

# ===== Column Definitions =====
grammar.rule("user_column", choice("id", "name", "email"))
grammar.rule("order_column", choice("id", "user_id", "total"))
grammar.rule("timestamp_column", choice("created_at", "updated_at"))

# ===== Value Generators =====
grammar.rule("string_value", template("'{value}'", 
    value=choice("test", "example", "demo")))
grammar.rule("numeric_value", number(1, 1000))
```

### Use Descriptive Rule Names

**DO:** Use clear, specific names
```python
grammar.rule("select_with_aggregation", template(
    "SELECT {agg_function}({column}) FROM {table} GROUP BY {group_column}"
))

grammar.rule("inner_join_on_foreign_key", template(
    "INNER JOIN {table2} ON {table1}.{fk} = {table2}.id"
))
```

**DON'T:** Use generic or cryptic names
```python
grammar.rule("q1", template("SELECT * FROM {t}"))  # What is q1?
grammar.rule("thing", choice("a", "b", "c"))      # What thing?
```

## 2. Performance Optimization

### Pre-compute Static Elements

**DO:** Calculate static values once
```python
# Pre-compute static choices
COMMON_TABLES = choice("users", "orders", "products", "customers")
INDEXED_COLUMNS = choice("id", "user_id", "order_id", "product_id")

grammar.rule("indexed_query", template(
    "SELECT * FROM {table} WHERE {column} = {value}",
    table=COMMON_TABLES,
    column=INDEXED_COLUMNS
))
```

**DON'T:** Recreate elements repeatedly
```python
# Bad: Creates new choice element each time
grammar.rule("query1", template(
    "SELECT * FROM {table}",
    table=choice("users", "orders", "products")  # Recreated each time
))

grammar.rule("query2", template(
    "UPDATE {table} SET ...",
    table=choice("users", "orders", "products")  # Recreated again
))
```

### Use Integer Weights

**DO:** Use integer weights for better performance
```python
query_type = choice(
    "SELECT",
    "INSERT", 
    "UPDATE",
    "DELETE",
    weights=[70, 20, 8, 2]  # Integers
)
```

**DON'T:** Use floating-point weights
```python
# Slower: Requires normalization
query_type = choice(
    "SELECT",
    "INSERT",
    "UPDATE", 
    "DELETE",
    weights=[0.7, 0.2, 0.08, 0.02]  # Floats
)
```

### Minimize Lambda Usage in Hot Paths

**DO:** Use simple elements for frequently generated values
```python
# Fast: Direct element
grammar.rule("common_value", number(1, 100))
```

**DON'T:** Use lambdas for simple operations
```python
# Slower: Lambda overhead
grammar.rule("common_value", Lambda(
    lambda ctx: str(ctx.rng.randint(1, 100))
))
```

## 3. Readability and Maintainability

### Use Template Formatting

**DO:** Format complex templates for readability
```python
grammar.rule("complex_select", template("""
    SELECT 
        {columns}
    FROM 
        {table}
    {joins}
    WHERE 
        {conditions}
    {group_by}
    {having}
    {order_by}
    {limit}
""".strip()))
```

**DON'T:** Use long single-line templates
```python
# Hard to read and maintain
grammar.rule("complex_select", template("SELECT {columns} FROM {table} {joins} WHERE {conditions} {group_by} {having} {order_by} {limit}"))
```

### Document Complex Logic

**DO:** Add comments explaining non-obvious choices
```python
# Weights based on production workload analysis:
# - 60% simple queries (single table, basic conditions)
# - 30% medium queries (joins, simple aggregations)  
# - 10% complex queries (subqueries, CTEs, window functions)
query_complexity = choice(
    ref("simple_query"),
    ref("medium_query"),
    ref("complex_query"),
    weights=[60, 30, 10]
)

# Higher probability of WHERE clause for UPDATE/DELETE
# to avoid accidental full-table operations
grammar.rule("update_where", maybe(
    ref("where_clause"),
    probability=0.95  # 95% chance of WHERE
))
```

### Use Helper Functions

**DO:** Create reusable helper functions
```python
def create_comparison(column_type="any"):
    """Create comparison condition for given column type"""
    if column_type == "numeric":
        return template("{column} {op} {value}",
            op=choice("=", ">", "<", ">=", "<=", "!="),
            value=number(1, 1000)
        )
    elif column_type == "string":
        return template("{column} {op} {value}",
            op=choice("=", "!=", "LIKE", "ILIKE"),
            value=template("'{string}'")
        )
    else:
        return template("{column} {op} {value}")

# Use in grammar
grammar.rule("numeric_condition", create_comparison("numeric"))
grammar.rule("string_condition", create_comparison("string"))
```

## 4. Query Realism

### Use Correlated Values

**DO:** Generate related values that make sense together
```python
# Correlated table and columns
grammar.rule("user_query", template(
    "SELECT {columns} FROM users",
    columns=choice("id, name, email", "id, created_at", "name, email")
))

grammar.rule("order_query", template(
    "SELECT {columns} FROM orders",
    columns=choice("id, user_id, total", "id, status", "user_id, created_at")
))
```

**DON'T:** Mix unrelated elements
```python
# Bad: Any column from any table
grammar.rule("query", template(
    "SELECT {any_column} FROM {any_table}"
))
```

### Use Realistic Value Distributions

**DO:** Model real-world distributions
```python
# Realistic status values with business logic
order_status = choice(
    "pending",    # 40% - new orders
    "processing", # 30% - being fulfilled
    "shipped",    # 20% - in transit
    "delivered",  # 8%  - completed
    "cancelled",  # 2%  - cancelled
    weights=[40, 30, 20, 8, 2]
)

# Realistic ID ranges
user_id = choice(
    number(1, 1000),      # 80% - active users
    number(1001, 10000),  # 15% - less active
    number(10001, 100000),# 5%  - dormant
    weights=[80, 15, 5]
)
```

### Include Edge Cases

**DO:** Test boundary conditions
```python
# Include edge cases with small probability
string_value = choice(
    "normal_value",           # 90% - normal case
    "",                      # 3%  - empty string
    "NULL",                  # 3%  - null value
    "'quoted''value'",       # 2%  - escaped quotes
    "very_" + "long_" * 50,  # 2%  - long string
    weights=[90, 3, 3, 2, 2]
)
```

## 5. Error Prevention

### Validate Table/Column Relationships

**DO:** Ensure valid column references
```python
# Define table-specific columns
USER_COLUMNS = ["id", "name", "email", "created_at"]
ORDER_COLUMNS = ["id", "user_id", "product_id", "quantity", "total"]

grammar.rule("user_select", template(
    "SELECT {columns} FROM users",
    columns=choice(*USER_COLUMNS)
))

grammar.rule("order_select", template(
    "SELECT {columns} FROM orders",
    columns=choice(*ORDER_COLUMNS)
))
```

### Handle NULL Values Properly

**DO:** Consider NULL handling in conditions
```python
# Proper NULL handling
null_safe_condition = choice(
    template("{column} = {value}"),
    template("{column} IS NULL"),
    template("{column} IS NOT NULL"),
    template("COALESCE({column}, {default}) = {value}")
)
```

### Escape Special Characters

**DO:** Handle special characters in strings
```python
def safe_string(text):
    """Escape single quotes in SQL strings"""
    return text.replace("'", "''")

grammar.rule("safe_string_value", Lambda(
    lambda ctx: f"'{safe_string(ctx.rng.choice(['test', 'O''Brien', 'data']))}"
))
```

## 6. Testing and Validation

### Test with Fixed Seeds

**DO:** Use deterministic generation for testing
```python
# Test grammar with fixed seed
def test_grammar(grammar):
    # Should always generate same output
    result1 = grammar.generate("query", seed=42)
    result2 = grammar.generate("query", seed=42)
    assert result1 == result2
    
    # Test different rules
    for rule in ["select", "insert", "update", "delete"]:
        try:
            query = grammar.generate(rule, seed=123)
            print(f"✓ {rule}: {query[:50]}...")
        except Exception as e:
            print(f"✗ {rule}: {e}")
```

### Validate Coverage

**DO:** Ensure all paths are reachable
```python
def validate_coverage(grammar, rule, iterations=1000):
    """Check that all choices are generated"""
    seen = set()
    for i in range(iterations):
        result = grammar.generate(rule, seed=i)
        seen.add(result)
    
    print(f"Rule '{rule}' generated {len(seen)} unique values")
    return seen
```

### Profile Performance

**DO:** Measure generation performance
```python
import time

def profile_grammar(grammar, iterations=10000):
    """Profile grammar performance"""
    start = time.time()
    
    for _ in range(iterations):
        grammar.generate("query")
    
    elapsed = time.time() - start
    qps = iterations / elapsed
    
    print(f"Generated {iterations} queries in {elapsed:.2f}s")
    print(f"Performance: {qps:.0f} queries/second")
```

## 7. Advanced Patterns

### Context-Aware Generation

**DO:** Use context for intelligent generation
```python
class TableAwareColumn(Element):
    """Generate columns based on current table"""
    def __init__(self, table_columns):
        self.table_columns = table_columns
    
    def generate(self, context):
        # Get current table from context
        current_table = context.current_table
        if current_table in self.table_columns:
            columns = self.table_columns[current_table]
            return context.rng.choice(columns)
        return "id"  # fallback

# Usage
TABLE_COLUMNS = {
    "users": ["id", "name", "email"],
    "orders": ["id", "user_id", "total"],
    "products": ["id", "name", "price"]
}

grammar.rule("smart_column", TableAwareColumn(TABLE_COLUMNS))
```

### Stateful Generation

**DO:** Track state when needed
```python
class UniqueTable(Element):
    """Generate each table at most once"""
    def __init__(self, tables):
        self.tables = tables
        self.used = set()
    
    def generate(self, context):
        available = [t for t in self.tables if t not in self.used]
        if not available:
            self.used.clear()  # Reset when exhausted
            available = self.tables
        
        table = context.rng.choice(available)
        self.used.add(table)
        return table
```

### Lazy Evaluation

**DO:** Defer expensive computations
```python
class LazyElement(Element):
    """Evaluate only when needed"""
    def __init__(self, generator_func):
        self.generator_func = generator_func
        self._cached = None
    
    def generate(self, context):
        if self._cached is None:
            self._cached = self.generator_func()
        return self._cached.generate(context)

# Use for expensive initialization
grammar.rule("expensive", LazyElement(
    lambda: load_complex_grammar()  # Only loads when first used
))
```

## Summary Checklist

✅ **Structure**
- [ ] Organize rules hierarchically
- [ ] Group related rules together
- [ ] Use descriptive rule names
- [ ] Add section comments

✅ **Performance**
- [ ] Pre-compute static elements
- [ ] Use integer weights
- [ ] Minimize lambda usage
- [ ] Cache common patterns

✅ **Readability**
- [ ] Format complex templates
- [ ] Document weight choices
- [ ] Create helper functions
- [ ] Use meaningful placeholders

✅ **Realism**
- [ ] Generate correlated values
- [ ] Model real distributions
- [ ] Include edge cases
- [ ] Handle NULLs properly

✅ **Testing**
- [ ] Test with fixed seeds
- [ ] Validate coverage
- [ ] Profile performance
- [ ] Check error cases

Following these best practices will help you create high-quality grammars that generate realistic, diverse queries while maintaining excellent performance at scale.