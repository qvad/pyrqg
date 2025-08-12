# PyRQG Examples

This directory contains comprehensive examples demonstrating all aspects of PyRQG, from basic usage to advanced production scenarios.

## 📁 Directory Structure

```
examples/
├── basic/              # Getting started examples
├── advanced/           # Advanced techniques and patterns
├── grammars/          # Complete grammar examples
├── integration/       # Database integration examples
└── performance/       # Performance testing examples
```

## 🚀 Quick Start

### 1. Basic Query Generation

```bash
# Generate 10 simple queries
python examples/basic/01_simple_queries.py

# Generate queries with specific patterns
python examples/basic/02_query_patterns.py

# Use templates and placeholders
python examples/basic/03_templates.py
```

### 2. Grammar Development

```bash
# Create your first grammar
python examples/grammars/01_minimal_grammar.py

# E-commerce database grammar
python examples/grammars/02_ecommerce_grammar.py

# Analytics workload grammar
python examples/grammars/03_analytics_grammar.py
```

### 3. Production via Runner

```bash
# Development-scale generation
python -m pyrqg.runner grammar --grammar dml_unique --count 100

# Production-scale with predefined config
python -m pyrqg.runner production --config yugabyte --count 1000000 --threads 8

# Generate PostgreSQL-focused queries
python -m pyrqg.runner grammar --grammar dml_yugabyte --count 1000 --output queries.sql
```

## 📚 Example Categories

### Basic Examples (`basic/`)

| File | Description | Key Concepts |
|------|-------------|--------------|
| `01_simple_queries.py` | Generate basic SQL queries | Grammar, rules, elements |
| `02_query_patterns.py` | Common query patterns | Choice, weights, templates |
| `03_templates.py` | Template system usage | Placeholders, substitution |
| `04_random_elements.py` | Random value generation | Number, choice, maybe |
| `05_context_usage.py` | Using context effectively | Tables, fields, state |

### Advanced Examples (`advanced/`)

| File | Description | Key Concepts |
|------|-------------|--------------|
| `01_custom_elements.py` | Create custom DSL elements | Element class, generation |
| `02_stateful_generation.py` | Maintain state across queries | State management, sessions |
| `03_grammar_composition.py` | Compose multiple grammars | Inheritance, modularity |
| `04_weighted_distribution.py` | Statistical distributions | Probabilistic generation |
| `05_adaptive_grammar.py` | Self-improving grammars | Learning, adaptation |

### Grammar Examples (`grammars/`)

| File | Description | Use Case |
|------|-------------|----------|
| `01_minimal_grammar.py` | Simplest working grammar | Learning |
| `02_ecommerce_grammar.py` | E-commerce database queries | Web applications |
| `03_analytics_grammar.py` | OLAP and analytics queries | Data warehouses |
| `04_transaction_grammar.py` | Transaction patterns | OLTP systems |
| `05_ddl_grammar.py` | Schema modification queries | Database migrations |


## 🎯 Learning Path

### Beginner
1. Start with `basic/01_simple_queries.py`
2. Explore `basic/03_templates.py`
3. Try `grammars/01_minimal_grammar.py`

### Intermediate
1. Study `basic/04_random_elements.py`
2. Examine `grammars/02_ecommerce_grammar.py`
3. Try `advanced/01_custom_elements.py`
4. Run production via runner: `python -m pyrqg.runner production --config yugabyte --count 100000`

### Advanced
1. Master `advanced/02_stateful_generation.py`
2. Explore `advanced/04_weighted_distribution.py`
3. Study `integration/postgres_integration.py`

## 💡 Tips

1. **Start Simple**: Begin with basic examples and gradually increase complexity
2. **Read Comments**: Each example is thoroughly documented
3. **Experiment**: Modify examples to understand how changes affect output
4. **Check Output**: Examples print their output for easy verification
5. **Use Seeds**: Set seeds for reproducible results during development

## 🔧 Running Examples

### Prerequisites

```bash
# Ensure PyRQG is installed
pip install -e ..

# For database examples
pip install psycopg2-binary

# For performance examples
pip install matplotlib pandas
```

### Basic Execution

```bash
# Run any example directly
python examples/basic/01_simple_queries.py

# Generate 50 queries from a grammar via runner
python -m pyrqg.runner grammar --grammar dml_unique --count 50
```

### Advanced Usage

```bash
# Production run with checkpointing (via production system)
python -m pyrqg.runner production --config yugabyte --count 1000000 --threads 8 --checkpoint ./checkpoint.json --output queries.sql

# Generate 1,000 queries and save to a file
python -m pyrqg.runner grammar --grammar dml_unique --count 1000 --output queries.sql
```

## 📊 Example Output

Each example includes sample output to show what it generates:

```sql
-- From basic/01_simple_queries.py
SELECT * FROM users WHERE id = 42;
INSERT INTO orders (user_id, total) VALUES (123, 99.99);
UPDATE products SET price = price * 1.1 WHERE category = 'electronics';

-- From grammars/02_ecommerce_grammar.py
SELECT u.name, COUNT(o.id) as order_count, SUM(o.total) as total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE u.created_at > '2024-01-01'
GROUP BY u.id, u.name
HAVING COUNT(o.id) > 5
ORDER BY total_spent DESC
LIMIT 10;
```

## 🤝 Contributing

To add new examples:
1. Create a descriptive filename
2. Add comprehensive documentation
3. Include sample output
4. Update this README
5. Test with multiple Python versions

## 📖 Further Reading

- [DSL Complete Guide](../docs/DSL_COMPLETE_GUIDE.md)
- [DSL Best Practices](../docs/DSL_BEST_PRACTICES.md)
- [DSL Cookbook](../docs/DSL_COOKBOOK.md)
- [Production Guide](../docs/PRODUCTION_CONFIG.md)