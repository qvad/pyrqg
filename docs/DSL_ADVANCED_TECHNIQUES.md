# Advanced DSL Techniques

This guide covers advanced techniques for creating sophisticated grammars that push the boundaries of what's possible with PyRQG's DSL.

## Table of Contents

1. [Custom Element Development](#custom-element-development)
2. [State Management](#state-management)
3. [Grammar Composition and Inheritance](#grammar-composition-and-inheritance)
4. [Performance Optimization Techniques](#performance-optimization-techniques)
5. [Context-Aware Generation](#context-aware-generation)
6. [Dynamic Grammar Modification](#dynamic-grammar-modification)
7. [Probabilistic Models](#probabilistic-models)
8. [Grammar Testing and Validation](#grammar-testing-and-validation)
9. [Integration Patterns](#integration-patterns)
10. [Production-Scale Techniques](#production-scale-techniques)

## Custom Element Development

### Creating Reusable Elements

```python
from pyrqg.dsl.core import Element, Context
from typing import List, Optional, Dict, Any
import random
import string

class UniqueValueGenerator(Element):
    """Generate unique values within a session"""
    
    def __init__(self, prefix: str = "", length: int = 8):
        self.prefix = prefix
        self.length = length
        self.used_values = set()
    
    def generate(self, context: Context) -> str:
        max_attempts = 1000
        for _ in range(max_attempts):
            value = self.prefix + ''.join(
                context.rng.choices(string.ascii_letters + string.digits, k=self.length)
            )
            if value not in self.used_values:
                self.used_values.add(value)
                return f"'{value}'"
        
        # Fallback: add timestamp to ensure uniqueness
        import time
        value = f"{self.prefix}{int(time.time() * 1000000)}"
        self.used_values.add(value)
        return f"'{value}'"
    
    def reset(self):
        """Reset for new session"""
        self.used_values.clear()

# Usage
grammar = Grammar("unique_values")
email_generator = UniqueValueGenerator("user_", 12)
grammar.rule("unique_email", Lambda(
    lambda ctx: email_generator.generate(ctx) + "@example.com"
))
```

### Composite Elements

```python
class ConditionalSequence(Element):
    """Generate sequence based on conditions"""
    
    def __init__(self, conditions: List[Tuple[callable, Element]], default: Element):
        self.conditions = conditions
        self.default = default
    
    def generate(self, context: Context) -> str:
        for condition, element in self.conditions:
            if condition(context):
                return element.generate(context)
        return self.default.generate(context)

# Usage
grammar.rule("smart_query", ConditionalSequence(
    conditions=[
        (lambda ctx: len(ctx.tables) > 10, template("SELECT * FROM {table} TABLESAMPLE BERNOULLI (1)")),
        (lambda ctx: len(ctx.tables) > 5, template("SELECT * FROM {table} LIMIT 1000")),
        (lambda ctx: "users" in ctx.tables, template("SELECT * FROM users WHERE active = true"))
    ],
    default=template("SELECT * FROM {table}")
))
```

### Weighted Distribution Element

```python
class WeightedDistribution(Element):
    """Generate values based on statistical distribution"""
    
    def __init__(self, distribution_type: str = "normal", **params):
        self.distribution_type = distribution_type
        self.params = params
    
    def generate(self, context: Context) -> str:
        if self.distribution_type == "normal":
            mean = self.params.get("mean", 50)
            stddev = self.params.get("stddev", 15)
            value = int(context.rng.gauss(mean, stddev))
            # Clamp to reasonable range
            value = max(1, min(100, value))
        elif self.distribution_type == "exponential":
            lambd = self.params.get("lambda", 1.0)
            value = int(context.rng.expovariate(lambd) * 10)
        elif self.distribution_type == "zipf":
            # Zipf distribution for realistic ID selection
            a = self.params.get("a", 2.0)
            value = int(context.rng.paretovariate(a))
        else:
            value = context.rng.randint(1, 100)
        
        return str(value)

# Usage - realistic user ID distribution
grammar.rule("realistic_user_id", WeightedDistribution(
    "zipf", a=1.5  # Most queries hit a small set of "popular" users
))
```

## State Management

### Stateful Grammar Generation

```python
class StatefulGrammar(Grammar):
    """Grammar that maintains state between generations"""
    
    def __init__(self, name: str = "stateful"):
        super().__init__(name)
        self.state = {
            "transaction_depth": 0,
            "tables_created": set(),
            "current_schema": "public",
            "variables": {}
        }
    
    def begin_transaction(self):
        self.state["transaction_depth"] += 1
        return "BEGIN;" if self.state["transaction_depth"] == 1 else "SAVEPOINT sp{};".format(
            self.state["transaction_depth"]
        )
    
    def end_transaction(self):
        if self.state["transaction_depth"] > 0:
            self.state["transaction_depth"] -= 1
            return "COMMIT;" if self.state["transaction_depth"] == 0 else "RELEASE SAVEPOINT sp{};".format(
                self.state["transaction_depth"] + 1
            )
        return ""
    
    def create_table_if_not_exists(self, table_name: str) -> str:
        if table_name not in self.state["tables_created"]:
            self.state["tables_created"].add(table_name)
            return f"CREATE TABLE IF NOT EXISTS {table_name} (id SERIAL PRIMARY KEY, data TEXT);"
        return ""

# Usage
stateful = StatefulGrammar()
stateful.rule("transaction_aware", Lambda(lambda ctx: 
    stateful.begin_transaction() + "\n" +
    stateful.create_table_if_not_exists("test_table") + "\n" +
    "INSERT INTO test_table (data) VALUES ('test');\n" +
    stateful.end_transaction()
))
```

### Session Management

```python
class SessionAwareElement(Element):
    """Element that maintains session state"""
    
    def __init__(self):
        self.sessions = {}
    
    def get_session(self, context: Context) -> Dict[str, Any]:
        session_id = getattr(context, 'session_id', 'default')
        if session_id not in self.sessions:
            self.sessions[session_id] = {
                'query_count': 0,
                'last_table': None,
                'last_id': None,
                'temp_tables': []
            }
        return self.sessions[session_id]
    
    def generate(self, context: Context) -> str:
        session = self.get_session(context)
        session['query_count'] += 1
        
        if session['query_count'] % 100 == 0:
            # Every 100 queries, clean up temp tables
            cleanup = []
            for temp_table in session['temp_tables']:
                cleanup.append(f"DROP TABLE IF EXISTS {temp_table}")
            session['temp_tables'] = []
            return "; ".join(cleanup)
        
        # Regular query generation
        return self._generate_query(context, session)
```

## Grammar Composition and Inheritance

### Grammar Inheritance Pattern

```python
class BaseGrammar(Grammar):
    """Base grammar with common rules"""
    
    def __init__(self, name: str = "base"):
        super().__init__(name)
        self._define_base_rules()
    
    def _define_base_rules(self):
        self.rule("identifier", template("{prefix}_{suffix}",
            prefix=choice("tbl", "col", "idx", "fk"),
            suffix=Lambda(lambda ctx: ''.join(ctx.rng.choices(string.ascii_lowercase, k=8)))
        ))
        
        self.rule("basic_type", choice(
            "INTEGER", "VARCHAR(255)", "TEXT", "TIMESTAMP", "BOOLEAN"
        ))
        
        self.rule("constraint", choice(
            "NOT NULL",
            "UNIQUE",
            "DEFAULT NULL",
            template("DEFAULT {value}")
        ))

class ExtendedGrammar(BaseGrammar):
    """Extended grammar with additional features"""
    
    def __init__(self):
        super().__init__("extended")
        self._define_extended_rules()
    
    def _define_extended_rules(self):
        # Override base rule
        self.rule("basic_type", choice(
            ref("basic_type"),  # Include parent types
            "JSONB", "UUID", "ARRAY", "NUMERIC(10,2)"
        ))
        
        # Add new rules
        self.rule("advanced_constraint", choice(
            ref("constraint"),  # Include parent constraints
            template("CHECK ({condition})"),
            template("REFERENCES {table}({column})")
        ))
```

### Grammar Composition

```python
def compose_grammars(*grammars: Grammar) -> Grammar:
    """Compose multiple grammars into one"""
    composed = Grammar("composed")
    
    for grammar in grammars:
        # Copy all rules
        for rule_name, rule in grammar.rules.items():
            if rule_name not in composed.rules:
                composed.rules[rule_name] = rule
            else:
                # Merge rules with same name using choice
                existing = composed.rules[rule_name]
                composed.rule(rule_name, choice(
                    existing.definition,
                    rule.definition
                ))
    
    # Merge contexts
    for grammar in grammars:
        composed.context.tables.update(grammar.context.tables)
        composed.context.fields.extend(grammar.context.fields)
    
    return composed

# Usage
dml_grammar = Grammar("dml")
dml_grammar.rule("query", ref("select"))
dml_grammar.rule("select", template("SELECT * FROM {table}"))

ddl_grammar = Grammar("ddl")
ddl_grammar.rule("query", ref("create"))
ddl_grammar.rule("create", template("CREATE TABLE {table} (id INT)"))

combined = compose_grammars(dml_grammar, ddl_grammar)
# Now combined.generate("query") can produce both SELECT and CREATE
```

### Modular Grammar Design

```python
class ModularGrammar:
    """Grammar system with pluggable modules"""
    
    def __init__(self):
        self.modules = {}
        self.grammar = Grammar("modular")
    
    def register_module(self, name: str, module_func: callable):
        """Register a grammar module"""
        self.modules[name] = module_func
        module_func(self.grammar)
    
    def enable_modules(self, *module_names: str):
        """Enable specific modules"""
        enabled_grammar = Grammar("enabled")
        
        for name in module_names:
            if name in self.modules:
                self.modules[name](enabled_grammar)
        
        return enabled_grammar

# Define modules
def core_module(grammar: Grammar):
    """Core SQL functionality"""
    grammar.rule("table", choice("users", "orders", "products"))
    grammar.rule("column", choice("id", "name", "created_at"))

def json_module(grammar: Grammar):
    """JSON functionality"""
    grammar.rule("json_op", choice("->", "->>", "#>", "#>>"))
    grammar.rule("json_path", template("data{op}'{path}'",
        op=ref("json_op"),
        path=choice("$.name", "$.email", "$.settings")
    ))

def window_module(grammar: Grammar):
    """Window function functionality"""
    grammar.rule("window_func", choice(
        "ROW_NUMBER()", "RANK()", "DENSE_RANK()",
        template("{agg}({col})", agg=choice("SUM", "AVG"), col=ref("column"))
    ))
    grammar.rule("window_clause", template(
        "{func} OVER (PARTITION BY {partition} ORDER BY {order})",
        func=ref("window_func"),
        partition=ref("column"),
        order=ref("column")
    ))

# Usage
modular = ModularGrammar()
modular.register_module("core", core_module)
modular.register_module("json", json_module)
modular.register_module("window", window_module)

# Create grammar with specific features
json_enabled = modular.enable_modules("core", "json")
full_featured = modular.enable_modules("core", "json", "window")
```

## Performance Optimization Techniques

### Lazy Evaluation and Caching

```python
class CachedElement(Element):
    """Element that caches generated values"""
    
    def __init__(self, element: Element, cache_size: int = 1000):
        self.element = element
        self.cache_size = cache_size
        self.cache = []
        self.cache_hits = 0
        self.cache_misses = 0
    
    def generate(self, context: Context) -> str:
        if self.cache and context.rng.random() < 0.8:  # 80% cache hit rate
            self.cache_hits += 1
            return context.rng.choice(self.cache)
        
        self.cache_misses += 1
        value = self.element.generate(context)
        
        if len(self.cache) < self.cache_size:
            self.cache.append(value)
        else:
            # Replace random element
            idx = context.rng.randint(0, self.cache_size - 1)
            self.cache[idx] = value
        
        return value
    
    def stats(self):
        total = self.cache_hits + self.cache_misses
        hit_rate = self.cache_hits / total if total > 0 else 0
        return {
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "hit_rate": hit_rate,
            "cache_size": len(self.cache)
        }

# Usage - cache expensive computations
expensive_subquery = template(
    "(SELECT COUNT(*) FROM large_table WHERE complex_condition)"
)
grammar.rule("cached_subquery", CachedElement(expensive_subquery))
```

### Batch Generation Optimization

```python
class BatchOptimizedGrammar(Grammar):
    """Grammar optimized for batch generation"""
    
    def __init__(self, name: str = "batch_optimized"):
        super().__init__(name)
        self.batch_cache = {}
    
    def generate_batch(self, rule_name: str, count: int, seed: Optional[int] = None) -> List[str]:
        """Generate multiple queries efficiently"""
        if seed is not None:
            self.context.seed = seed
            self.context._rng = random.Random(seed)
        
        # Pre-generate commonly used values
        self._prebuild_cache(count)
        
        results = []
        rule = self.rules[rule_name]
        
        for i in range(count):
            # Add batch index to context for variation
            self.context.batch_index = i
            results.append(rule.generate(self.context))
        
        return results
    
    def _prebuild_cache(self, count: int):
        """Pre-generate frequently used values"""
        # Pre-generate table names
        if "tables" not in self.batch_cache:
            self.batch_cache["tables"] = [
                self.context.rng.choice(list(self.context.tables.keys()))
                for _ in range(min(count, 100))
            ]
        
        # Pre-generate common values
        if "values" not in self.batch_cache:
            self.batch_cache["values"] = [
                str(self.context.rng.randint(1, 10000))
                for _ in range(min(count, 1000))
            ]

class BatchAwareElement(Element):
    """Element aware of batch generation"""
    
    def generate(self, context: Context) -> str:
        if hasattr(context, 'batch_index'):
            # Use batch index for deterministic variation
            idx = context.batch_index
            
            # Different behavior based on position in batch
            if idx % 100 == 0:
                return "/* Checkpoint */ SELECT 1"
            elif idx % 10 == 0:
                return "/* Batch marker */ SELECT current_timestamp"
        
        return "SELECT * FROM users"
```

### Memory-Efficient Generation

```python
class StreamingGrammar(Grammar):
    """Grammar that generates queries in streaming fashion"""
    
    def generate_stream(self, rule_name: str, count: int = None):
        """Generate queries as a stream"""
        generated = 0
        rule = self.rules[rule_name]
        
        while count is None or generated < count:
            yield rule.generate(self.context)
            generated += 1
            
            # Periodic cleanup to prevent memory growth
            if generated % 10000 == 0:
                self._cleanup()
    
    def _cleanup(self):
        """Clean up temporary state"""
        import gc
        
        # Clear any accumulated state in elements
        for rule in self.rules.values():
            if hasattr(rule.definition, 'reset'):
                rule.definition.reset()
        
        # Force garbage collection
        gc.collect()

# Usage
streaming = StreamingGrammar()
streaming.rule("query", template("SELECT * FROM table_{n}", n=number(1, 1000)))

# Process queries without loading all into memory
for i, query in enumerate(streaming.generate_stream("query", 1000000)):
    process_query(query)
    if i % 10000 == 0:
        print(f"Processed {i} queries")
```

## Context-Aware Generation

### Advanced Context Usage

```python
class SmartContext(Context):
    """Enhanced context with additional state"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.query_history = []
        self.table_access_count = {}
        self.current_transaction = None
        self.variables = {}
    
    def track_query(self, query: str):
        self.query_history.append(query)
        
        # Track table access
        import re
        tables = re.findall(r'FROM\s+(\w+)', query, re.IGNORECASE)
        for table in tables:
            self.table_access_count[table] = self.table_access_count.get(table, 0) + 1
    
    def get_hot_tables(self, top_n: int = 5) -> List[str]:
        """Get most accessed tables"""
        sorted_tables = sorted(
            self.table_access_count.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return [table for table, _ in sorted_tables[:top_n]]

class ContextAwareElement(Element):
    """Element that adapts based on context"""
    
    def generate(self, context: SmartContext) -> str:
        # Prefer hot tables for better cache utilization
        hot_tables = context.get_hot_tables()
        
        if hot_tables and context.rng.random() < 0.7:  # 70% chance
            table = context.rng.choice(hot_tables)
        else:
            table = context.rng.choice(list(context.tables.keys()))
        
        query = f"SELECT * FROM {table}"
        context.track_query(query)
        
        return query
```

### Correlation-Aware Generation

```python
class CorrelatedValues(Element):
    """Generate correlated values"""
    
    def __init__(self):
        self.correlations = {
            "countries": {
                "USA": ["USD", "en_US", "America/New_York"],
                "UK": ["GBP", "en_GB", "Europe/London"],
                "Japan": ["JPY", "ja_JP", "Asia/Tokyo"],
                "Germany": ["EUR", "de_DE", "Europe/Berlin"]
            }
        }
    
    def generate(self, context: Context) -> str:
        country = context.rng.choice(list(self.correlations.keys()))
        currency, locale, timezone = self.correlations[country]
        
        # Store in context for correlated generation
        context.current_country = country
        context.current_currency = currency
        context.current_locale = locale
        context.current_timezone = timezone
        
        return f"('{country}', '{currency}', '{locale}', '{timezone}')"

class CorrelatedQuery(Element):
    """Generate query using correlated values"""
    
    def generate(self, context: Context) -> str:
        # Use correlated values if available
        if hasattr(context, 'current_country'):
            return f"""
                INSERT INTO users (country, currency, locale, timezone)
                VALUES ('{context.current_country}', '{context.current_currency}', 
                        '{context.current_locale}', '{context.current_timezone}')
            """.strip()
        
        # Fallback
        return "INSERT INTO users (country) VALUES ('Unknown')"
```

## Dynamic Grammar Modification

### Runtime Grammar Modification

```python
class DynamicGrammar(Grammar):
    """Grammar that can be modified at runtime"""
    
    def add_table(self, table_name: str, columns: List[str]):
        """Dynamically add a table to the grammar"""
        # Update context
        self.context.tables[table_name] = 1000  # Default row count
        
        # Add table-specific rules
        self.rule(f"{table_name}_columns", choice(*columns))
        self.rule(f"{table_name}_select", template(
            f"SELECT {{cols}} FROM {table_name}",
            cols=choice("*", ref(f"{table_name}_columns"))
        ))
        
        # Update main table rule if it exists
        if "table" in self.rules:
            current_tables = self.rules["table"]
            self.rule("table", choice(current_tables, table_name))
    
    def learn_from_schema(self, connection):
        """Learn grammar from database schema"""
        cursor = connection.cursor()
        
        # Get all tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        
        for (table,) in cursor.fetchall():
            # Get columns for each table
            cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table}'
            """)
            
            columns = [col for col, _ in cursor.fetchall()]
            self.add_table(table, columns)

# Usage
dynamic = DynamicGrammar()
dynamic.learn_from_schema(db_connection)
# Grammar now includes all tables from database
```

### Adaptive Grammar

```python
class AdaptiveGrammar(Grammar):
    """Grammar that adapts based on success/failure"""
    
    def __init__(self, name: str = "adaptive"):
        super().__init__(name)
        self.success_rates = {}
        self.rule_weights = {}
    
    def track_result(self, rule_name: str, query: str, success: bool):
        """Track query success/failure"""
        if rule_name not in self.success_rates:
            self.success_rates[rule_name] = {"success": 0, "total": 0}
        
        self.success_rates[rule_name]["total"] += 1
        if success:
            self.success_rates[rule_name]["success"] += 1
        
        # Adapt weights based on success rate
        self._update_weights()
    
    def _update_weights(self):
        """Update rule weights based on success rates"""
        for rule_name, stats in self.success_rates.items():
            if stats["total"] > 10:  # Enough samples
                success_rate = stats["success"] / stats["total"]
                
                # Increase weight for successful patterns
                if success_rate > 0.9:
                    self.rule_weights[rule_name] = 1.5
                elif success_rate < 0.5:
                    self.rule_weights[rule_name] = 0.5
                else:
                    self.rule_weights[rule_name] = 1.0
    
    def adaptive_choice(self, *rule_names):
        """Choose rule based on adaptive weights"""
        weights = [self.rule_weights.get(name, 1.0) for name in rule_names]
        return Lambda(lambda ctx: 
            self.rules[ctx.rng.choices(rule_names, weights=weights)[0]].generate(ctx)
        )
```

## Probabilistic Models

### Markov Chain Grammar

```python
class MarkovGrammar(Grammar):
    """Grammar based on Markov chains"""
    
    def __init__(self, name: str = "markov"):
        super().__init__(name)
        self.transitions = {}
    
    def add_transition(self, from_state: str, to_state: str, probability: float):
        """Add state transition"""
        if from_state not in self.transitions:
            self.transitions[from_state] = []
        self.transitions[from_state].append((to_state, probability))
    
    def build_query_chain(self):
        """Build Markov chain for query generation"""
        self.add_transition("START", "SELECT", 0.4)
        self.add_transition("START", "INSERT", 0.3)
        self.add_transition("START", "UPDATE", 0.2)
        self.add_transition("START", "DELETE", 0.1)
        
        self.add_transition("SELECT", "FROM", 1.0)
        self.add_transition("FROM", "WHERE", 0.7)
        self.add_transition("FROM", "JOIN", 0.2)
        self.add_transition("FROM", "END", 0.1)
        
        self.add_transition("WHERE", "AND", 0.3)
        self.add_transition("WHERE", "OR", 0.2)
        self.add_transition("WHERE", "END", 0.5)
        
        # Define rules for each state
        self.rule("SELECT", template("SELECT {columns}"))
        self.rule("FROM", template("FROM {table}"))
        self.rule("WHERE", template("WHERE {condition}"))
        self.rule("JOIN", template("JOIN {table} ON {condition}"))
    
    def generate_markov(self, start_state: str = "START") -> str:
        """Generate using Markov chain"""
        current_state = start_state
        parts = []
        
        while current_state != "END" and current_state in self.transitions:
            # Get possible transitions
            transitions = self.transitions[current_state]
            
            # Choose next state based on probabilities
            states, probs = zip(*transitions)
            next_state = self.context.rng.choices(states, weights=probs)[0]
            
            # Generate content for state
            if next_state in self.rules:
                parts.append(self.rules[next_state].generate(self.context))
            
            current_state = next_state
        
        return " ".join(parts)
```

### Bayesian Grammar Selection

```python
class BayesianGrammar(Grammar):
    """Grammar that uses Bayesian inference for rule selection"""
    
    def __init__(self, name: str = "bayesian"):
        super().__init__(name)
        self.prior_beliefs = {}
        self.observations = {}
    
    def set_prior(self, rule_name: str, belief: float):
        """Set prior belief about rule success"""
        self.prior_beliefs[rule_name] = belief
    
    def observe(self, rule_name: str, success: bool):
        """Update beliefs based on observation"""
        if rule_name not in self.observations:
            self.observations[rule_name] = {"success": 0, "failure": 0}
        
        if success:
            self.observations[rule_name]["success"] += 1
        else:
            self.observations[rule_name]["failure"] += 1
    
    def get_posterior(self, rule_name: str) -> float:
        """Calculate posterior probability"""
        prior = self.prior_beliefs.get(rule_name, 0.5)
        
        if rule_name not in self.observations:
            return prior
        
        obs = self.observations[rule_name]
        total = obs["success"] + obs["failure"]
        
        if total == 0:
            return prior
        
        # Simple Bayesian update
        likelihood = obs["success"] / total
        posterior = (likelihood * prior) / (
            likelihood * prior + (1 - likelihood) * (1 - prior)
        )
        
        return posterior
    
    def bayesian_choice(self, *rule_names):
        """Choose rule based on posterior probabilities"""
        posteriors = [self.get_posterior(name) for name in rule_names]
        
        return Lambda(lambda ctx:
            self.rules[ctx.rng.choices(rule_names, weights=posteriors)[0]].generate(ctx)
        )
```

## Grammar Testing and Validation

### Grammar Testing Framework

```python
class GrammarTester:
    """Comprehensive grammar testing framework"""
    
    def __init__(self, grammar: Grammar):
        self.grammar = grammar
        self.test_results = []
    
    def test_rule_coverage(self, rule_name: str, iterations: int = 1000) -> Dict[str, int]:
        """Test that all possible outputs are generated"""
        outputs = {}
        
        for i in range(iterations):
            result = self.grammar.generate(rule_name, seed=i)
            outputs[result] = outputs.get(result, 0) + 1
        
        return outputs
    
    def test_sql_validity(self, rule_name: str, iterations: int = 100) -> Dict[str, Any]:
        """Test SQL syntax validity"""
        import sqlparse
        
        valid = 0
        invalid = []
        
        for i in range(iterations):
            query = self.grammar.generate(rule_name, seed=i)
            
            try:
                parsed = sqlparse.parse(query)
                if parsed:
                    valid += 1
                else:
                    invalid.append((i, query, "Empty parse result"))
            except Exception as e:
                invalid.append((i, query, str(e)))
        
        return {
            "valid": valid,
            "invalid": len(invalid),
            "validity_rate": valid / iterations,
            "errors": invalid[:10]  # First 10 errors
        }
    
    def test_performance(self, rule_name: str, iterations: int = 10000) -> Dict[str, float]:
        """Test generation performance"""
        import time
        
        # Warmup
        for _ in range(100):
            self.grammar.generate(rule_name)
        
        # Actual test
        start = time.perf_counter()
        for i in range(iterations):
            self.grammar.generate(rule_name, seed=i)
        end = time.perf_counter()
        
        elapsed = end - start
        qps = iterations / elapsed
        
        return {
            "iterations": iterations,
            "elapsed_seconds": elapsed,
            "queries_per_second": qps,
            "avg_time_per_query_ms": (elapsed / iterations) * 1000
        }
    
    def test_determinism(self, rule_name: str, seed: int = 42) -> bool:
        """Test that same seed produces same output"""
        results = []
        
        for _ in range(5):
            result = self.grammar.generate(rule_name, seed=seed)
            results.append(result)
        
        return len(set(results)) == 1
    
    def run_all_tests(self) -> Dict[str, Any]:
        """Run comprehensive test suite"""
        results = {}
        
        for rule_name in self.grammar.rules:
            print(f"Testing rule: {rule_name}")
            
            results[rule_name] = {
                "coverage": len(self.test_rule_coverage(rule_name, 100)),
                "validity": self.test_sql_validity(rule_name, 50),
                "performance": self.test_performance(rule_name, 1000),
                "deterministic": self.test_determinism(rule_name)
            }
        
        return results

# Usage
tester = GrammarTester(my_grammar)
results = tester.run_all_tests()

for rule, metrics in results.items():
    print(f"\n{rule}:")
    print(f"  Coverage: {metrics['coverage']} unique outputs")
    print(f"  Validity: {metrics['validity']['validity_rate']:.1%}")
    print(f"  Performance: {metrics['performance']['queries_per_second']:.0f} q/s")
    print(f"  Deterministic: {metrics['deterministic']}")
```

### Property-Based Testing

```python
class PropertyBasedTester:
    """Property-based testing for grammars"""
    
    def __init__(self, grammar: Grammar):
        self.grammar = grammar
    
    def check_property(self, rule_name: str, property_func: callable, 
                      iterations: int = 100) -> Tuple[bool, List[str]]:
        """Check if generated queries satisfy a property"""
        failures = []
        
        for i in range(iterations):
            query = self.grammar.generate(rule_name, seed=i)
            
            if not property_func(query):
                failures.append(query)
        
        return len(failures) == 0, failures
    
    # Example properties
    @staticmethod
    def has_where_clause(query: str) -> bool:
        """Property: Query should have WHERE clause"""
        return "WHERE" in query.upper()
    
    @staticmethod
    def no_cartesian_product(query: str) -> bool:
        """Property: No unrestricted cross joins"""
        query_upper = query.upper()
        return not ("CROSS JOIN" in query_upper and "WHERE" not in query_upper)
    
    @staticmethod
    def safe_delete(query: str) -> bool:
        """Property: DELETE should have WHERE clause"""
        query_upper = query.upper()
        if query_upper.startswith("DELETE"):
            return "WHERE" in query_upper
        return True
    
    def test_properties(self, rule_name: str) -> Dict[str, Tuple[bool, int]]:
        """Test multiple properties"""
        properties = {
            "has_where_clause": self.has_where_clause,
            "no_cartesian_product": self.no_cartesian_product,
            "safe_delete": self.safe_delete
        }
        
        results = {}
        for prop_name, prop_func in properties.items():
            passed, failures = self.check_property(rule_name, prop_func)
            results[prop_name] = (passed, len(failures))
        
        return results
```

## Integration Patterns

### Database Integration

```python
class DatabaseIntegratedGrammar(Grammar):
    """Grammar that integrates with live database"""
    
    def __init__(self, connection):
        super().__init__("db_integrated")
        self.connection = connection
        self.schema_cache = {}
        self._load_schema()
    
    def _load_schema(self):
        """Load schema from database"""
        cursor = self.connection.cursor()
        
        # Get tables and columns
        cursor.execute("""
            SELECT 
                table_name, 
                column_name, 
                data_type,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
        """)
        
        for table, column, dtype, nullable, default in cursor.fetchall():
            if table not in self.schema_cache:
                self.schema_cache[table] = {
                    "columns": [],
                    "types": {},
                    "nullable": {},
                    "defaults": {}
                }
            
            self.schema_cache[table]["columns"].append(column)
            self.schema_cache[table]["types"][column] = dtype
            self.schema_cache[table]["nullable"][column] = nullable == 'YES'
            self.schema_cache[table]["defaults"][column] = default
    
    def create_table_specific_rules(self):
        """Create rules based on actual schema"""
        for table, info in self.schema_cache.items():
            # Table-specific column list
            self.rule(f"{table}_columns", choice(*info["columns"]))
            
            # Type-aware value generation
            for column, dtype in info["types"].items():
                self.rule(f"{table}_{column}_value", 
                    self._create_value_generator(dtype, info["nullable"][column])
                )
            
            # Valid INSERT for table
            self.rule(f"{table}_insert", Lambda(
                lambda ctx, t=table, i=info: self._generate_insert(t, i, ctx)
            ))
    
    def _create_value_generator(self, dtype: str, nullable: bool):
        """Create value generator based on data type"""
        if nullable and random.random() < 0.1:  # 10% NULL
            return Lambda(lambda ctx: "NULL")
        
        dtype_lower = dtype.lower()
        if 'int' in dtype_lower:
            return number(1, 10000)
        elif 'varchar' in dtype_lower or 'text' in dtype_lower:
            return template("'{value}'", value=choice("test", "example", "data"))
        elif 'timestamp' in dtype_lower:
            return choice("CURRENT_TIMESTAMP", "'2024-01-01'::timestamp")
        elif 'boolean' in dtype_lower:
            return choice("true", "false")
        elif 'json' in dtype_lower:
            return "'{}'::jsonb"
        else:
            return "'default'"
    
    def _generate_insert(self, table: str, info: Dict, context: Context) -> str:
        """Generate valid INSERT for specific table"""
        # Select non-default columns
        columns = [col for col in info["columns"] 
                  if info["defaults"].get(col) is None or context.rng.random() < 0.8]
        
        if not columns:
            columns = info["columns"][:1]  # At least one column
        
        # Generate values
        values = []
        for col in columns:
            rule_name = f"{table}_{col}_value"
            if rule_name in self.rules:
                values.append(self.rules[rule_name].generate(context))
            else:
                values.append("'default'")
        
        return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
```

### External Data Integration

```python
class ExternalDataGrammar(Grammar):
    """Grammar that uses external data sources"""
    
    def __init__(self, data_file: str):
        super().__init__("external_data")
        self.data = self._load_data(data_file)
        self._create_data_rules()
    
    def _load_data(self, filename: str) -> Dict[str, List[str]]:
        """Load data from CSV/JSON file"""
        import csv
        import json
        
        data = {}
        
        if filename.endswith('.csv'):
            with open(filename, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    for key, value in row.items():
                        if key not in data:
                            data[key] = []
                        data[key].append(value)
        elif filename.endswith('.json'):
            with open(filename, 'r') as f:
                data = json.load(f)
        
        return data
    
    def _create_data_rules(self):
        """Create rules from loaded data"""
        for key, values in self.data.items():
            # Direct values
            self.rule(f"real_{key}", choice(*values))
            
            # Transformed values
            if all(v.isdigit() for v in values[:10]):  # Numeric
                self.rule(f"range_{key}", Lambda(
                    lambda ctx, vals=values: str(ctx.rng.randint(
                        min(int(v) for v in vals),
                        max(int(v) for v in vals)
                    ))
                ))
            
            # Pattern-based generation
            if len(values) > 10:
                self.rule(f"pattern_{key}", Lambda(
                    lambda ctx, vals=values: self._generate_similar(ctx, vals)
                ))
    
    def _generate_similar(self, context: Context, examples: List[str]) -> str:
        """Generate value similar to examples"""
        example = context.rng.choice(examples)
        
        # Simple pattern matching
        if '@' in example:  # Email-like
            parts = example.split('@')
            username = ''.join(context.rng.choices(string.ascii_lowercase, k=8))
            return f"{username}@{parts[1]}"
        elif example.replace('-', '').isdigit():  # Phone-like
            return '-'.join([''.join(context.rng.choices(string.digits, k=len(part))) 
                           for part in example.split('-')])
        else:
            # Generic string mutation
            chars = list(example)
            if len(chars) > 3:
                idx = context.rng.randint(1, len(chars) - 2)
                chars[idx] = context.rng.choice(string.ascii_letters)
            return ''.join(chars)
```

## Production-Scale Techniques

### Distributed Grammar Generation

```python
class DistributedGrammar:
    """Grammar system for distributed generation"""
    
    def __init__(self, grammar: Grammar, worker_id: int, total_workers: int):
        self.grammar = grammar
        self.worker_id = worker_id
        self.total_workers = total_workers
        self.partition_seed = 12345
    
    def generate_partition(self, total_queries: int) -> Iterator[str]:
        """Generate this worker's partition of queries"""
        queries_per_worker = total_queries // self.total_workers
        start_idx = self.worker_id * queries_per_worker
        
        # Handle remainder
        if self.worker_id == self.total_workers - 1:
            queries_per_worker += total_queries % self.total_workers
        
        for i in range(queries_per_worker):
            # Deterministic seed based on global index
            seed = self.partition_seed + start_idx + i
            yield self.grammar.generate("query", seed=seed)
    
    def generate_with_checkpointing(self, total_queries: int, 
                                   checkpoint_file: str) -> Iterator[str]:
        """Generate with checkpoint support"""
        import json
        import os
        
        start_idx = 0
        
        # Resume from checkpoint if exists
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
                start_idx = checkpoint.get('last_index', 0) + 1
        
        queries_per_worker = total_queries // self.total_workers
        worker_start = self.worker_id * queries_per_worker
        
        for i in range(start_idx, queries_per_worker):
            global_idx = worker_start + i
            seed = self.partition_seed + global_idx
            
            yield self.grammar.generate("query", seed=seed)
            
            # Checkpoint every 10000 queries
            if i % 10000 == 0:
                with open(checkpoint_file, 'w') as f:
                    json.dump({'last_index': i, 'global_index': global_idx}, f)
```

### High-Performance Optimization

```python
class OptimizedGrammar(Grammar):
    """Grammar optimized for maximum performance"""
    
    def __init__(self, name: str = "optimized"):
        super().__init__(name)
        self._compiled_rules = {}
        self._jit_cache = {}
    
    def compile_rule(self, rule_name: str):
        """Pre-compile rule for faster execution"""
        rule = self.rules[rule_name]
        
        # Analyze rule structure
        if isinstance(rule.definition, Choice):
            # Pre-compute choice probabilities
            options = rule.definition.options
            weights = rule.definition.weights or [1] * len(options)
            total_weight = sum(weights)
            probabilities = [w / total_weight for w in weights]
            
            self._compiled_rules[rule_name] = {
                'type': 'choice',
                'options': options,
                'probabilities': probabilities,
                'cumulative': self._cumulative_probabilities(probabilities)
            }
        elif isinstance(rule.definition, Template):
            # Pre-parse template
            self._compiled_rules[rule_name] = {
                'type': 'template',
                'parsed': self._parse_template_fast(rule.definition.template),
                'elements': rule.definition.elements
            }
    
    def _cumulative_probabilities(self, probs: List[float]) -> List[float]:
        """Calculate cumulative probabilities for fast selection"""
        cumulative = []
        total = 0
        for p in probs:
            total += p
            cumulative.append(total)
        return cumulative
    
    def _parse_template_fast(self, template: str) -> List[Tuple[str, str]]:
        """Fast template parsing"""
        import re
        parts = []
        last_end = 0
        
        for match in re.finditer(r'{(\w+)}', template):
            if match.start() > last_end:
                parts.append(('literal', template[last_end:match.start()]))
            parts.append(('placeholder', match.group(1)))
            last_end = match.end()
        
        if last_end < len(template):
            parts.append(('literal', template[last_end:]))
        
        return parts
    
    def generate_optimized(self, rule_name: str, count: int) -> List[str]:
        """Optimized batch generation"""
        import numpy as np
        
        # Pre-compile if needed
        if rule_name not in self._compiled_rules:
            self.compile_rule(rule_name)
        
        compiled = self._compiled_rules[rule_name]
        results = []
        
        if compiled['type'] == 'choice':
            # Generate all random numbers at once
            randoms = np.random.random(count)
            cumulative = compiled['cumulative']
            
            for r in randoms:
                # Binary search for fast selection
                idx = np.searchsorted(cumulative, r)
                option = compiled['options'][idx]
                
                # Generate option
                if isinstance(option, str):
                    results.append(option)
                else:
                    results.append(option.generate(self.context))
        else:
            # Standard generation
            for _ in range(count):
                results.append(self.rules[rule_name].generate(self.context))
        
        return results
```

### Resource-Aware Generation

```python
class ResourceAwareGrammar(Grammar):
    """Grammar that adapts to available resources"""
    
    def __init__(self, name: str = "resource_aware"):
        super().__init__(name)
        self.resource_monitor = ResourceMonitor()
    
    def generate_adaptive(self, rule_name: str, target_qps: int = 1000):
        """Generate queries adapting to system resources"""
        import time
        
        batch_size = 100
        generated = 0
        start_time = time.time()
        
        while True:
            # Check resources
            resources = self.resource_monitor.get_current()
            
            # Adapt batch size based on CPU and memory
            if resources['cpu_percent'] > 80:
                batch_size = max(10, batch_size // 2)
            elif resources['cpu_percent'] < 50 and resources['memory_available_gb'] > 2:
                batch_size = min(1000, batch_size * 2)
            
            # Generate batch
            batch_start = time.time()
            for _ in range(batch_size):
                yield self.generate(rule_name)
                generated += 1
            
            # Rate limiting
            batch_time = time.time() - batch_start
            expected_time = batch_size / target_qps
            
            if batch_time < expected_time:
                time.sleep(expected_time - batch_time)
            
            # Progress reporting
            if generated % 10000 == 0:
                elapsed = time.time() - start_time
                actual_qps = generated / elapsed
                print(f"Generated: {generated}, QPS: {actual_qps:.1f}, "
                      f"Batch size: {batch_size}, CPU: {resources['cpu_percent']:.1f}%")

class ResourceMonitor:
    """Monitor system resources"""
    
    def get_current(self) -> Dict[str, float]:
        import psutil
        
        return {
            'cpu_percent': psutil.cpu_percent(interval=0.1),
            'memory_available_gb': psutil.virtual_memory().available / (1024**3),
            'memory_percent': psutil.virtual_memory().percent
        }
```

## Summary

These advanced techniques enable:

1. **Custom Elements**: Build specialized components for unique requirements
2. **State Management**: Maintain context across query generation
3. **Grammar Composition**: Combine and extend grammars modularly
4. **Performance Optimization**: Generate millions of queries efficiently
5. **Context Awareness**: Adapt to runtime conditions
6. **Dynamic Modification**: Evolve grammars based on feedback
7. **Probabilistic Models**: Use statistical models for realistic generation
8. **Testing Framework**: Validate grammar correctness and performance
9. **Integration**: Connect with databases and external data
10. **Production Scale**: Handle billion-scale generation requirements

The key to mastering PyRQG's DSL is understanding that it's not just about generating random queries, but about creating intelligent, adaptive systems that produce meaningful test data at scale.