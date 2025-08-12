# PyRQG DSL Quick Reference

A concise reference guide for PyRQG's DSL components and usage patterns.

## Basic Elements

### Literal
```python
Literal("SELECT * FROM users")  # Fixed string
```

### Choice
```python
choice("A", "B", "C")                    # Equal probability
choice("A", "B", "C", weights=[60,30,10]) # Weighted selection
```

### Number
```python
number(1, 100)    # Random integer 1-100
number(-50, 50)   # Negative values supported
```

### Digit
```python
digit()  # Single digit 0-9
```

### Maybe (Optional)
```python
maybe("DISTINCT")              # 50% probability (default)
maybe("WHERE ...", 0.7)        # 70% probability
```

### Repeat
```python
repeat("?", min=1, max=5)              # "?" to "?, ?, ?, ?, ?"
repeat(ref("column"), 1, 3, ", ")      # Comma-separated columns
```

### Table & Field
```python
table()                        # Any table from context
table(min_rows=100)           # Tables with 100+ rows
field()                       # Any field from context
field(type="id")              # Fields containing "id"
```

### Template
```python
template("SELECT {cols} FROM {table}")                    # Basic
template("WHERE {col} = {val}", col="id", val=number())  # With values
```

### RuleRef
```python
ref("column_list")  # Reference another rule
```

### Lambda
```python
Lambda(lambda ctx: f"'{ctx.rng.choice(['a','b'])}'")  # Custom logic
```

## Grammar Definition

### Basic Grammar
```python
from pyrqg.dsl.core import Grammar, choice, template, repeat

grammar = Grammar("my_grammar")

# Define context
grammar.define_tables(users=1000, orders=5000, products=100)
grammar.define_fields("id", "name", "email", "price")

# Define rules
grammar.rule("query", choice(
    ref("select"),
    ref("insert")
))

grammar.rule("select", template(
    "SELECT {columns} FROM {table}"
))

grammar.rule("columns", choice("*", ref("column_list")))
grammar.rule("column_list", repeat(field(), 1, 5, ", "))

# Generate
query = grammar.generate("query")
```

## Common Patterns

### Optional Clauses
```python
template(
    "SELECT * FROM {table} {where} {order} {limit}",
    where=maybe(template("WHERE {condition}"), 0.8),
    order=maybe(template("ORDER BY {column}"), 0.3),
    limit=maybe(template("LIMIT {n}"), 0.2)
)
```

### Weighted Query Types
```python
choice(
    ref("select"),   # 70%
    ref("insert"),   # 20%
    ref("update"),   # 8%
    ref("delete"),   # 2%
    weights=[70, 20, 8, 2]
)
```

### Complex Conditions
```python
choice(
    template("{col} = {val}"),
    template("{col} IN ({vals})"),
    template("{col} BETWEEN {v1} AND {v2}"),
    template("({cond1} AND {cond2})"),
    template("({cond1} OR {cond2})")
)
```

### Multi-row INSERT
```python
template(
    "INSERT INTO {table} VALUES {rows}",
    rows=repeat(template("({values})"), 1, 10, ", ")
)
```

## Best Practices Checklist

✅ **Grammar Structure**
```python
# DO: Hierarchical organization
grammar.rule("query", ref("query_type"))
grammar.rule("query_type", choice(...))

# DON'T: Everything in one rule
grammar.rule("query", choice(template(...), template(...), ...))
```

✅ **Performance**
```python
# DO: Pre-compute static elements
COMMON_TABLES = choice("users", "orders", "products")

# DON'T: Recreate in each rule
grammar.rule("q1", template("... FROM {t}", t=choice("users", "orders")))
grammar.rule("q2", template("... FROM {t}", t=choice("users", "orders")))
```

✅ **Readability**
```python
# DO: Meaningful names
grammar.rule("user_columns", choice("id", "name", "email"))

# DON'T: Generic names
grammar.rule("cols1", choice("id", "name", "email"))
```

✅ **Realism**
```python
# DO: Correlated values
template("INSERT INTO orders (user_id, status) VALUES ({uid}, 'pending')")

# DON'T: Random combinations
template("INSERT INTO {any_table} ({any_cols}) VALUES ({any_vals})")
```

## Advanced Features

### Custom Elements
```python
class MyElement(Element):
    def generate(self, context: Context) -> str:
        return "generated value"
```

### Context Access
```python
Lambda(lambda ctx: ctx.rng.choice(list(ctx.tables.keys())))
```

### Dynamic Rules
```python
grammar.rule("dynamic", Lambda(
    lambda ctx: f"SELECT * FROM {ctx.current_table}" 
    if hasattr(ctx, 'current_table') else "SELECT 1"
))
```

### Stateful Generation
```python
class Counter(Element):
    def __init__(self):
        self.count = 0
    
    def generate(self, context):
        self.count += 1
        return str(self.count)
```

## Quick Examples

### Basic SELECT
```python
grammar.rule("select", template(
    "SELECT {what} FROM {table} WHERE {condition}",
    what=choice("*", "COUNT(*)", ref("columns")),
    table=table(),
    condition=template("{col} = {val}")
))
```

### JOIN Query
```python
grammar.rule("join", template(
    "SELECT * FROM {t1} JOIN {t2} ON {t1}.{fk} = {t2}.id",
    t1=table(),
    t2=table(),
    fk=field(type="id")
))
```

### INSERT with RETURNING
```python
grammar.rule("insert_returning", template(
    "INSERT INTO {table} ({cols}) VALUES ({vals}) RETURNING {ret}",
    ret=choice("id", "*", "id, created_at")
))
```

### CTE Pattern
```python
grammar.rule("with_cte", template("""
WITH {cte_name} AS (
    {cte_query}
)
SELECT * FROM {cte_name}
""".strip()))
```

## Generation Options

```python
# Single query
query = grammar.generate("query")

# With seed (deterministic)
query = grammar.generate("query", seed=42)

# Batch generation (for performance)
for i in range(1000):
    query = grammar.generate("query", seed=i)
```

## Debugging Tips

```python
# Test specific rule
print(grammar.generate("column"))  # Test just column generation

# Fixed seed for debugging
result1 = grammar.generate("query", seed=123)
result2 = grammar.generate("query", seed=123)
assert result1 == result2  # Should be identical

# List all rules
print(list(grammar.rules.keys()))
```

## Common Gotchas

1. **Undefined References**: Define all rules before generating
2. **Circular References**: Add base cases to prevent infinite recursion
3. **Missing Placeholders**: Ensure all template placeholders are defined
4. **Context Not Set**: Call define_tables() and define_fields()
5. **Type Mismatches**: String elements need quotes in SQL

---

For complete documentation, see:
- [Complete Guide](DSL_COMPLETE_GUIDE.md)
- [Best Practices](DSL_BEST_PRACTICES.md)
- [Cookbook](DSL_COOKBOOK.md)
- [Advanced Techniques](DSL_ADVANCED_TECHNIQUES.md)