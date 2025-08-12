# PyRQG DSL Complete Guide

## Table of Contents
1. [Introduction](#introduction)
2. [Core Concepts](#core-concepts)
3. [Basic Elements](#basic-elements)
4. [Advanced Elements](#advanced-elements)
5. [Grammar Definition](#grammar-definition)
6. [Template System](#template-system)
7. [Best Practices](#best-practices)
8. [Common Patterns](#common-patterns)
9. [Performance Tips](#performance-tips)
10. [Troubleshooting](#troubleshooting)

## Introduction

PyRQG's DSL (Domain Specific Language) is a Python-based framework for defining SQL query generation grammars. It provides a declarative, intuitive way to create complex query patterns while maintaining readability and maintainability.

### Key Features
- **Declarative Syntax**: Define what you want, not how to generate it
- **Composable Elements**: Build complex patterns from simple primitives
- **Type Safe**: Python's type system helps catch errors early
- **Performance Optimized**: Efficient generation even at billion-scale
- **Extensible**: Easy to add custom elements and patterns

## Core Concepts

### 1. Elements
Everything in the DSL is an `Element` - a component that can generate a string value.

```python
from pyrqg.dsl.core import Element, Context

class MyElement(Element):
    def generate(self, context: Context) -> str:
        return "generated value"
```

### 2. Context
The `Context` carries state during generation, including:
- Random number generator (for deterministic output)
- Table definitions and row counts
- Field definitions
- Custom state

```python
context = Context(
    tables={"users": 1000, "orders": 5000},
    fields=["id", "name", "email", "created_at"],
    seed=42  # For reproducible output
)
```

### 3. Grammar
A `Grammar` is a collection of named rules that define your query patterns.

```python
from pyrqg.dsl.core import Grammar

grammar = Grammar("my_grammar")
grammar.rule("query", template("SELECT * FROM {table}"))
```

## Basic Elements

### Literal
A fixed string value.

```python
from pyrqg.dsl.core import Literal

element = Literal("SELECT * FROM users")
# Always generates: "SELECT * FROM users"
```

### Choice
Random selection from options.

```python
from pyrqg.dsl.core import choice

# Equal probability
operation = choice("SELECT", "INSERT", "UPDATE", "DELETE")

# Weighted probability (60% SELECT, 20% INSERT, 15% UPDATE, 5% DELETE)
operation = choice("SELECT", "INSERT", "UPDATE", "DELETE", 
                  weights=[60, 20, 15, 5])

# Can mix strings and elements
column = choice("id", "name", number(1, 100))
```

### Number
Random integer in a range.

```python
from pyrqg.dsl.core import number

# Random number between 1 and 100
value = number(1, 100)

# Large ranges for IDs
user_id = number(1, 1000000)

# Negative numbers supported
temperature = number(-50, 50)
```

### Digit
Single digit (0-9).

```python
from pyrqg.dsl.core import digit

# Generates: "0" through "9"
d = digit()
```

### Maybe (Optional)
Include element with probability.

```python
from pyrqg.dsl.core import maybe

# 50% chance of including "DISTINCT"
query = template("SELECT {distinct} * FROM users")
distinct = maybe("DISTINCT")

# 30% chance of WHERE clause
where_clause = maybe(template("WHERE id > {number}"), probability=0.3)
```

### Repeat
Generate element multiple times.

```python
from pyrqg.dsl.core import repeat

# 1-5 column names
columns = repeat(choice("id", "name", "email"), min=1, max=5, separator=", ")
# Might generate: "id, email, name"

# Fixed repetition
placeholders = repeat("?", min=3, max=3, separator=", ")
# Always generates: "?, ?, ?"
```

## Advanced Elements

### Table
Reference to a table from context.

```python
from pyrqg.dsl.core import table

# Any table
t = table()

# Only tables with 100+ rows
large_table = table(min_rows=100)

# Tables with 10-1000 rows
medium_table = table(min_rows=10, max_rows=1000)
```

### Field
Reference to a field from context.

```python
from pyrqg.dsl.core import field

# Any field
f = field()

# Fields containing "id" in name
id_field = field(type="id")
```

### RuleRef
Reference to another rule in the grammar.

```python
from pyrqg.dsl.core import ref

# Reference the "where_clause" rule
where = ref("where_clause")
```

### Lambda
Custom generation logic.

```python
from pyrqg.dsl.core import Lambda

# Random timestamp
timestamp = Lambda(lambda ctx: f"'2024-01-{ctx.rng.randint(1, 31)}'")

# Context-aware generation
row_count = Lambda(lambda ctx: str(len(ctx.tables)))
```

## Grammar Definition

### Basic Grammar Structure

```python
from pyrqg.dsl.core import Grammar, choice, template, repeat

# Create grammar
grammar = Grammar("sql_grammar")

# Define tables and fields
grammar.define_tables(
    users=10000,
    orders=50000,
    products=1000
)

grammar.define_fields(
    "id", "user_id", "order_id", "product_id",
    "name", "email", "price", "quantity", "created_at"
)

# Define rules
grammar.rule("query", choice(
    ref("select"),
    ref("insert"),
    ref("update"),
    ref("delete")
))

grammar.rule("select", template(
    "SELECT {columns} FROM {table} {where_clause}"
))

grammar.rule("columns", choice(
    "*",
    repeat(field(), min=1, max=5, separator=", ")
))

grammar.rule("where_clause", maybe(
    template("WHERE {condition}"),
    probability=0.7
))

grammar.rule("condition", choice(
    template("{field} = {value}"),
    template("{field} > {value}"),
    template("{field} < {value}"),
    template("{field} BETWEEN {value} AND {value}")
))

grammar.rule("value", choice(
    number(1, 1000),
    "'test'",
    "NULL"
))

# Generate queries
query = grammar.generate("query")
```

### Rule Definition Methods

```python
# Method 1: String literal
grammar.rule("keyword", "SELECT")

# Method 2: Element
grammar.rule("table_name", table())

# Method 3: Lambda function
grammar.rule("timestamp", lambda ctx: f"'{datetime.now()}'")

# Method 4: Complex element
grammar.rule("insert", template(
    "INSERT INTO {table} ({columns}) VALUES ({values})"
))
```

## Template System

### Basic Templates

```python
from pyrqg.dsl.core import template

# Simple placeholder
select = template("SELECT * FROM {table}")

# Multiple placeholders
insert = template("INSERT INTO {table} ({columns}) VALUES ({values})")

# Inline values
query = template(
    "SELECT {columns} FROM users WHERE id = {user_id}",
    columns="id, name",
    user_id=number(1, 1000)
)
```

### Advanced Template Features

```python
# Nested templates
complex_query = template(
    "SELECT {columns} FROM {table} {joins} {where} {order_by}",
    joins=maybe(template("JOIN {table2} ON {condition}")),
    where=maybe(template("WHERE {predicate}")),
    order_by=maybe(template("ORDER BY {sort_columns}"))
)

# Template with rule references
query = template(
    "SELECT {columns:column_list} FROM {table:table_name}",
    # References grammar rules "column_list" and "table_name"
)
```

### Template Best Practices

```python
# DO: Use meaningful placeholder names
good = template("SELECT {column_list} FROM {table_name}")

# DON'T: Use generic names
bad = template("SELECT {a} FROM {b}")

# DO: Break complex templates into parts
subquery = template("SELECT {col} FROM {tbl} WHERE {cond}")
main_query = template(
    "SELECT * FROM ({subquery}) AS sub",
    subquery=subquery
)

# DON'T: Create overly complex single templates
bad = template(
    "SELECT {c1}, {c2}, {c3} FROM {t1} JOIN {t2} ON {j1} "
    "WHERE {w1} AND {w2} OR {w3} GROUP BY {g1}, {g2} "
    "HAVING {h1} ORDER BY {o1}, {o2} LIMIT {l}"
)
```

## Best Practices

### 1. Grammar Organization

```python
# Group related rules together
grammar = Grammar("organized")

# -- Table definitions --
grammar.rule("table", choice("users", "orders", "products"))
grammar.rule("indexed_table", choice("users", "orders"))  # Tables with indexes

# -- Column definitions --
grammar.rule("column", choice("id", "name", "email"))
grammar.rule("numeric_column", choice("id", "price", "quantity"))
grammar.rule("text_column", choice("name", "email", "description"))

# -- Value generators --
grammar.rule("string_value", template("'{value}'", value=choice("test", "example")))
grammar.rule("numeric_value", number(1, 1000))
grammar.rule("null_value", "NULL")

# -- Query patterns --
grammar.rule("simple_select", template("SELECT * FROM {table}"))
grammar.rule("complex_select", template(
    "SELECT {columns} FROM {table} WHERE {condition}"
))
```

### 2. Reusable Components

```python
# Create reusable elements
def identifier(name):
    """SQL identifier with optional schema"""
    return maybe(template("{schema}.", schema=choice("public", "app"))) + name

def string_literal(values):
    """Properly quoted string"""
    return template("'{value}'", value=choice(*values))

def comparison_operator():
    """Common comparison operators"""
    return choice("=", "!=", "<>", ">", "<", ">=", "<=")

# Use in grammar
grammar.rule("table_ref", identifier(table()))
grammar.rule("condition", template(
    "{column} {op} {value}",
    op=comparison_operator()
))
```

### 3. Weight Management

```python
# Use meaningful weight ratios
query_distribution = choice(
    "SELECT",  # 70% - read heavy
    "INSERT",  # 20% - moderate writes  
    "UPDATE",  # 8%  - occasional updates
    "DELETE",  # 2%  - rare deletes
    weights=[70, 20, 8, 2]
)

# Document weight reasoning
join_type = choice(
    "INNER JOIN",    # 60% - most common
    "LEFT JOIN",     # 30% - nullable relations
    "RIGHT JOIN",    # 5%  - less common
    "FULL OUTER",    # 5%  - rare but important
    weights=[60, 30, 5, 5]
)
```

### 4. Probability Guidelines

```python
# Common probability patterns
where_clause = maybe(template("WHERE {condition}"), 0.8)    # Usually filter
order_by = maybe(template("ORDER BY {column}"), 0.3)       # Sometimes sort
limit_clause = maybe(template("LIMIT {count}"), 0.2)       # Occasionally limit

# Cascading probabilities
query = template(
    "SELECT {columns} FROM {table} {where} {order} {limit}",
    where=maybe(ref("where_clause"), 0.8),
    order=maybe(ref("order_by"), 0.3 if where else 0.1),  # Less likely without WHERE
    limit=maybe(ref("limit_clause"), 0.5 if order else 0.1)  # More likely with ORDER BY
)
```

## Common Patterns

### 1. JOIN Patterns

```python
# Simple join
grammar.rule("join", template(
    "{join_type} {table} ON {t1}.{col1} = {t2}.{col2}",
    join_type=choice("INNER JOIN", "LEFT JOIN", "RIGHT JOIN"),
    t1=ref("table"),
    t2=ref("table"),
    col1=ref("column"),
    col2=ref("column")
))

# Multi-table joins
grammar.rule("multi_join", repeat(
    ref("join"),
    min=1,
    max=4,
    separator=" "
))

# Self-join pattern
grammar.rule("self_join", template(
    "JOIN {table} AS t2 ON t1.parent_id = t2.id"
))
```

### 2. Subquery Patterns

```python
# IN subquery
grammar.rule("in_subquery", template(
    "{column} IN (SELECT {column} FROM {table} WHERE {condition})"
))

# EXISTS subquery
grammar.rule("exists_subquery", template(
    "EXISTS (SELECT 1 FROM {table} WHERE {correlation})"
))

# Scalar subquery
grammar.rule("scalar_subquery", template(
    "(SELECT {agg}({column}) FROM {table})",
    agg=choice("COUNT", "MAX", "MIN", "AVG")
))
```

### 3. Complex WHERE Clauses

```python
# Nested conditions
grammar.rule("condition", choice(
    ref("simple_condition"),
    ref("complex_condition")
))

grammar.rule("simple_condition", template(
    "{column} {op} {value}"
))

grammar.rule("complex_condition", choice(
    template("({condition} AND {condition})"),
    template("({condition} OR {condition})"),
    template("NOT ({condition})")
))

# Range conditions
grammar.rule("range_condition", choice(
    template("{column} BETWEEN {low} AND {high}"),
    template("{column} >= {low} AND {column} <= {high}")
))
```

### 4. INSERT Patterns

```python
# Single row insert
grammar.rule("insert_single", template(
    "INSERT INTO {table} ({columns}) VALUES ({values})"
))

# Multi-row insert
grammar.rule("insert_multi", template(
    "INSERT INTO {table} ({columns}) VALUES {value_lists}",
    value_lists=repeat(
        template("({values})"),
        min=1,
        max=10,
        separator=", "
    )
))

# INSERT ... SELECT
grammar.rule("insert_select", template(
    "INSERT INTO {table} ({columns}) {select_query}"
))

# INSERT ON CONFLICT
grammar.rule("upsert", template(
    "INSERT INTO {table} ({columns}) VALUES ({values}) "
    "ON CONFLICT ({key}) DO UPDATE SET {updates}"
))
```

### 5. CTE Patterns

```python
# Simple CTE
grammar.rule("with_clause", template(
    "WITH {cte_name} AS ({query})"
))

# Multiple CTEs
grammar.rule("multi_cte", template(
    "WITH {cte1_name} AS ({query1}), {cte2_name} AS ({query2})"
))

# Recursive CTE
grammar.rule("recursive_cte", template(
    "WITH RECURSIVE {cte_name} AS ("
    "{base_query} UNION ALL {recursive_query}"
    ")"
))
```

## Performance Tips

### 1. Minimize String Operations

```python
# GOOD: Pre-compute static parts
static_prefix = "SELECT "
static_suffix = " FROM users"
grammar.rule("query", template(
    static_prefix + "{columns}" + static_suffix
))

# BAD: Concatenate in generation
grammar.rule("query", Lambda(
    lambda ctx: "SELECT " + generate_columns(ctx) + " FROM users"
))
```

### 2. Cache Common Patterns

```python
# Cache frequently used elements
COMMON_COLUMNS = choice("id", "name", "email", "created_at")
COMMON_TABLES = choice("users", "orders", "products")

grammar.rule("column", COMMON_COLUMNS)
grammar.rule("table", COMMON_TABLES)

# Reuse instead of recreating
grammar.rule("select1", template("SELECT {col} FROM {tbl}", 
    col=COMMON_COLUMNS, tbl=COMMON_TABLES))
grammar.rule("select2", template("SELECT {col} FROM {tbl} WHERE active = true",
    col=COMMON_COLUMNS, tbl=COMMON_TABLES))
```

### 3. Optimize Choice Weights

```python
# GOOD: Integer weights
fast_choice = choice("A", "B", "C", weights=[70, 20, 10])

# SLOWER: Float weights (requires normalization)
slow_choice = choice("A", "B", "C", weights=[0.7, 0.2, 0.1])
```

### 4. Batch Generation

```python
# Generate multiple queries efficiently
def generate_batch(grammar, count=1000):
    # Reuse context for better performance
    context = grammar.context
    return [grammar.rules["query"].generate(context) for _ in range(count)]
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Undefined Rule References

```python
# Problem: RuleRef can't find rule
grammar.rule("query", ref("undefined_rule"))  # Error!

# Solution: Define all rules before generation
grammar.rule("undefined_rule", "SELECT 1")
grammar.rule("query", ref("undefined_rule"))  # OK
```

#### 2. Circular References

```python
# Problem: Infinite recursion
grammar.rule("a", ref("b"))
grammar.rule("b", ref("a"))  # Circular!

# Solution: Add base case
grammar.rule("a", choice(ref("b"), "base_value"))
grammar.rule("b", choice(ref("a"), "other_value"))
```

#### 3. Template Placeholder Errors

```python
# Problem: Missing placeholder definition
grammar.rule("query", template("SELECT {columns} FROM {table}"))
# Error: 'columns' not defined

# Solution 1: Define in template
grammar.rule("query", template(
    "SELECT {columns} FROM {table}",
    columns="*",
    table="users"
))

# Solution 2: Define as rules
grammar.rule("columns", "*")
grammar.rule("table", "users")
grammar.rule("query", template("SELECT {columns} FROM {table}"))
```

#### 4. Context Not Propagating

```python
# Problem: Table/Field references fail
grammar.rule("t", table())  # Returns "table1" (default)

# Solution: Define context
grammar.define_tables(users=1000, orders=5000)
grammar.define_fields("id", "name", "email")
grammar.rule("t", table())  # Now works correctly
```

### Debugging Tips

```python
# 1. Test individual rules
grammar = Grammar("debug")
grammar.rule("test", number(1, 10))
print(grammar.generate("test"))  # Test just this rule

# 2. Use fixed seed for reproducibility
result1 = grammar.generate("query", seed=42)
result2 = grammar.generate("query", seed=42)
assert result1 == result2  # Should be identical

# 3. Add debug output
grammar.rule("debug_rule", Lambda(
    lambda ctx: f"[DEBUG: tables={list(ctx.tables.keys())}]"
))

# 4. Validate grammar structure
def validate_grammar(grammar):
    """Check all rule references exist"""
    for rule_name, rule in grammar.rules.items():
        # Attempt to generate to find missing refs
        try:
            grammar.generate(rule_name)
        except Exception as e:
            print(f"Rule '{rule_name}' has error: {e}")
```

## Advanced Techniques

### 1. Dynamic Rule Generation

```python
# Generate rules programmatically
def create_column_rules(grammar, columns):
    for col in columns:
        grammar.rule(f"{col}_value", 
            number(1, 1000) if "id" in col else f"'{col}_test'"
        )

# Apply to grammar
columns = ["user_id", "product_id", "name", "email"]
create_column_rules(grammar, columns)
```

### 2. Grammar Composition

```python
# Combine multiple grammars
base_grammar = Grammar("base")
base_grammar.rule("table", choice("users", "orders"))

extended_grammar = Grammar("extended")
# Copy rules from base
for name, rule in base_grammar.rules.items():
    extended_grammar.rule(name, rule.definition)
# Add new rules
extended_grammar.rule("table", choice(ref("table"), "products", "customers"))
```

### 3. Custom Elements

```python
from pyrqg.dsl.core import Element
import random
import string

class RandomString(Element):
    """Generate random string of given length"""
    def __init__(self, length=10, chars=string.ascii_letters):
        self.length = length
        self.chars = chars
    
    def generate(self, context: Context) -> str:
        return ''.join(context.rng.choice(self.chars) 
                      for _ in range(self.length))

# Use in grammar
grammar.rule("random_name", RandomString(8))
grammar.rule("random_code", RandomString(5, string.ascii_uppercase + string.digits))
```

### 4. Conditional Generation

```python
class ConditionalElement(Element):
    """Generate based on condition"""
    def __init__(self, condition, true_element, false_element):
        self.condition = condition
        self.true_element = true_element
        self.false_element = false_element
    
    def generate(self, context: Context) -> str:
        if self.condition(context):
            return self.true_element.generate(context)
        return self.false_element.generate(context)

# Use for context-aware generation
grammar.rule("table_specific", ConditionalElement(
    lambda ctx: len(ctx.tables) > 5,
    template("SELECT * FROM {table} SAMPLE 10 PERCENT"),  # Sample for many tables
    template("SELECT * FROM {table}")  # Full scan for few tables
))
```

## Conclusion

The PyRQG DSL provides a powerful, flexible framework for SQL query generation. By following these patterns and best practices, you can create maintainable, efficient grammars that generate diverse, realistic queries for comprehensive database testing.

Remember:
- Start simple and build complexity gradually
- Reuse components where possible
- Document weight and probability choices
- Test grammars with fixed seeds during development
- Profile and optimize for production use

For more examples, see the `grammars/` directory in the PyRQG repository.