#!/usr/bin/env python3
"""
05_context_usage.py - Context and State Management

This example demonstrates how to use PyRQG's Context system:
- Accessing table and field information
- Using context in custom elements
- State management across generations
- Context-aware query generation
- Advanced context patterns

Key concepts:
- Context object
- Table and field definitions
- Random number generator access
- State preservation
- Context-aware elements
"""

import sys
from pathlib import Path
from typing import Dict, List, Any

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, Element, Context, Lambda, template, choice, ref


def basic_context_usage():
    """Show basic context usage patterns."""
    
    print("Basic Context Usage")
    print("=" * 50)
    
    grammar = Grammar("basic_context")
    
    # Define tables with row counts
    grammar.define_tables(
        users=10000,
        products=500,
        orders=50000,
        reviews=100000
    )
    
    # Define available fields
    grammar.define_fields(
        "id", "user_id", "product_id", "order_id",
        "name", "email", "price", "quantity", "rating"
    )
    
    # Access tables from context
    grammar.rule("any_table", Lambda(
        lambda ctx: ctx.rng.choice(list(ctx.tables.keys()))
    ))
    
    # Access fields from context
    grammar.rule("any_field", Lambda(
        lambda ctx: ctx.rng.choice(ctx.fields) if ctx.fields else "id"
    ))
    
    # Table-aware query
    grammar.rule("table_info", Lambda(
        lambda ctx: f"-- Table '{ctx.rng.choice(list(ctx.tables.keys()))}' " +
                   f"has {ctx.tables[list(ctx.tables.keys())[0]]} rows"
    ))
    
    # Context-aware column selection
    grammar.rule("smart_columns", Lambda(lambda ctx:
        ', '.join(ctx.rng.sample(ctx.fields, min(3, len(ctx.fields))))
        if ctx.fields else "*"
    ))
    
    grammar.rule("context_query", template(
        "SELECT {columns} FROM {table} -- {info}",
        columns=ref("smart_columns"),
        table=ref("any_table"),
        info=ref("table_info")
    ))
    
    print("Context-aware queries:")
    for i in range(5):
        print(f"\n{grammar.generate('context_query', seed=i)}")


def table_aware_generation():
    """Generate queries aware of table characteristics."""
    
    print("\n\nTable-Aware Generation")
    print("=" * 50)
    
    # Custom element that considers table size
    class TableAwareQuery(Element):
        def generate(self, context: Context) -> str:
            # Find large and small tables
            large_tables = [t for t, rows in context.tables.items() if rows > 10000]
            small_tables = [t for t, rows in context.tables.items() if rows <= 1000]
            
            if large_tables and context.rng.random() < 0.7:
                # For large tables, use sampling or limits
                table = context.rng.choice(large_tables)
                rows = context.tables[table]
                
                if rows > 100000:
                    return f"SELECT * FROM {table} TABLESAMPLE SYSTEM (1)"
                else:
                    return f"SELECT * FROM {table} LIMIT 1000"
            
            elif small_tables:
                # For small tables, full scan is OK
                table = context.rng.choice(small_tables)
                return f"SELECT * FROM {table}"
            
            # Fallback
            table = context.rng.choice(list(context.tables.keys()))
            return f"SELECT * FROM {table} LIMIT 100"
    
    grammar = Grammar("table_aware")
    
    # Define tables with varying sizes
    grammar.define_tables(
        users=100000,        # Large
        products=500,        # Small  
        orders=500000,       # Very large
        categories=50,       # Tiny
        reviews=1000000      # Huge
    )
    
    grammar.rule("smart_query", TableAwareQuery())
    
    print("Queries adapted to table size:")
    for i in range(8):
        print(f"{i+1}. {grammar.generate('smart_query', seed=i)}")


def stateful_context():
    """Demonstrate stateful context usage."""
    
    print("\n\nStateful Context")
    print("=" * 50)
    
    # Extended context with state
    class StatefulContext(Context):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.query_count = 0
            self.used_tables = set()
            self.last_table = None
            self.transaction_active = False
    
    # Element that tracks state
    class StatefulQuery(Element):
        def generate(self, context: StatefulContext) -> str:
            context.query_count += 1
            
            # Start transaction every 5 queries
            if context.query_count % 5 == 1:
                context.transaction_active = True
                return "BEGIN;"
            
            # End transaction
            if context.query_count % 5 == 0 and context.transaction_active:
                context.transaction_active = False
                return "COMMIT;"
            
            # Prefer unused tables
            unused = [t for t in context.tables.keys() if t not in context.used_tables]
            if unused:
                table = context.rng.choice(unused)
            else:
                # Reset when all tables used
                context.used_tables.clear()
                table = context.rng.choice(list(context.tables.keys()))
            
            context.used_tables.add(table)
            context.last_table = table
            
            # Reference last table sometimes
            if context.last_table and context.rng.random() < 0.3:
                return f"SELECT COUNT(*) FROM {table} WHERE id IN (SELECT id FROM {context.last_table})"
            
            return f"SELECT * FROM {table}"
    
    # Create grammar with stateful context
    grammar = Grammar("stateful")
    grammar.context = StatefulContext(
        tables={"users": 1000, "products": 500, "orders": 5000}
    )
    
    grammar.rule("query", StatefulQuery())
    
    print("Stateful query generation:")
    for i in range(12):
        query = grammar.generate("query", seed=i)
        print(f"{i+1:2d}. {query}")


def context_aware_values():
    """Generate values based on context."""
    
    print("\n\nContext-Aware Value Generation")
    print("=" * 50)
    
    # Element that generates correlated values
    class ContextualValue(Element):
        def __init__(self, value_type: str):
            self.value_type = value_type
        
        def generate(self, context: Context) -> str:
            # Check if we have a current table in context
            current_table = getattr(context, 'current_table', None)
            
            if self.value_type == "id":
                if current_table == "users":
                    return str(context.rng.randint(1, 10000))
                elif current_table == "products":
                    return str(context.rng.randint(1, 500))
                else:
                    return str(context.rng.randint(1, 100000))
            
            elif self.value_type == "status":
                if current_table == "orders":
                    return context.rng.choice(["'pending'", "'shipped'", "'delivered'"])
                elif current_table == "users":
                    return context.rng.choice(["'active'", "'inactive'", "'suspended'"])
                else:
                    return "'unknown'"
            
            elif self.value_type == "date":
                # Recent dates for orders, older for users
                if current_table == "orders":
                    return "CURRENT_DATE - INTERVAL '{}' DAY".format(
                        context.rng.randint(1, 30)
                    )
                else:
                    return "CURRENT_DATE - INTERVAL '{}' DAY".format(
                        context.rng.randint(30, 365)
                    )
            
            return "'default'"
    
    # Grammar that sets current table
    class TableSettingGrammar(Grammar):
        def generate(self, rule_name: str = "query", seed=None) -> str:
            # Set current table in context
            tables = list(self.context.tables.keys())
            if seed is not None:
                self.context._rng = __import__('random').Random(seed)
            
            self.context.current_table = self.context.rng.choice(tables)
            return super().generate(rule_name, seed)
    
    grammar = TableSettingGrammar("contextual")
    grammar.define_tables(users=10000, products=500, orders=50000)
    
    grammar.rule("query", template(
        "SELECT * FROM {table} WHERE {condition}",
        table=Lambda(lambda ctx: ctx.current_table),
        condition=ref("smart_condition")
    ))
    
    grammar.rule("smart_condition", template(
        "{field} = {value}",
        field=Lambda(lambda ctx: 
            "status" if ctx.current_table in ["orders", "users"] else "id"
        ),
        value=Lambda(lambda ctx:
            ContextualValue("status").generate(ctx) 
            if ctx.current_table in ["orders", "users"]
            else ContextualValue("id").generate(ctx)
        )
    ))
    
    print("Context-aware value generation:")
    for i in range(8):
        query = grammar.generate("query", seed=i*10)
        print(f"{i+1}. {query}")


def advanced_context_patterns():
    """Advanced context usage patterns."""
    
    print("\n\nAdvanced Context Patterns")
    print("=" * 50)
    
    # Context with query history
    class HistoryContext(Context):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.query_history = []
            self.table_usage = {}
        
        def add_query(self, query: str):
            self.query_history.append(query)
            
            # Track table usage
            for table in self.tables.keys():
                if table in query:
                    self.table_usage[table] = self.table_usage.get(table, 0) + 1
        
        def get_hot_tables(self, n: int = 3) -> List[str]:
            """Get most used tables."""
            sorted_tables = sorted(
                self.table_usage.items(),
                key=lambda x: x[1],
                reverse=True
            )
            return [t[0] for t in sorted_tables[:n]]
        
        def get_cold_tables(self, n: int = 3) -> List[str]:
            """Get least used tables."""
            # Include tables with zero usage
            usage = {t: self.table_usage.get(t, 0) for t in self.tables.keys()}
            sorted_tables = sorted(usage.items(), key=lambda x: x[1])
            return [t[0] for t in sorted_tables[:n]]
    
    # Adaptive query generator
    class AdaptiveQuery(Element):
        def generate(self, context: HistoryContext) -> str:
            # After 10 queries, start being adaptive
            if len(context.query_history) > 10:
                if context.rng.random() < 0.3:
                    # Query cold tables to balance usage
                    cold_tables = context.get_cold_tables()
                    if cold_tables:
                        table = context.rng.choice(cold_tables)
                        query = f"-- Balancing load\nSELECT * FROM {table}"
                        context.add_query(query)
                        return query
                
                elif context.rng.random() < 0.6:
                    # Join hot tables (likely to be cached)
                    hot_tables = context.get_hot_tables(2)
                    if len(hot_tables) >= 2:
                        query = f"-- Using hot tables\nSELECT * FROM {hot_tables[0]} JOIN {hot_tables[1]} ON true LIMIT 10"
                        context.add_query(query)
                        return query
            
            # Default: random table
            table = context.rng.choice(list(context.tables.keys()))
            query = f"SELECT * FROM {table}"
            context.add_query(query)
            return query
    
    # Create grammar with history context
    grammar = Grammar("adaptive")
    grammar.context = HistoryContext(
        tables={
            "users": 10000,
            "products": 500,
            "orders": 50000,
            "reviews": 100000,
            "categories": 50
        }
    )
    
    grammar.rule("adaptive_query", AdaptiveQuery())
    
    print("Adaptive query generation based on history:")
    for i in range(20):
        query = grammar.generate("adaptive_query", seed=i)
        print(f"{i+1:2d}. {query}")
    
    # Show usage statistics
    print("\nTable usage statistics:")
    for table, count in sorted(grammar.context.table_usage.items(), 
                               key=lambda x: x[1], reverse=True):
        print(f"  {table}: {count} queries")


def context_validation():
    """Use context for validation and constraints."""
    
    print("\n\nContext-Based Validation")
    print("=" * 50)
    
    # Context with schema information
    class SchemaContext(Context):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.foreign_keys = {
                'orders': {'user_id': 'users', 'product_id': 'products'},
                'reviews': {'user_id': 'users', 'product_id': 'products', 'order_id': 'orders'}
            }
            self.primary_keys = {
                'users': 'id',
                'products': 'id', 
                'orders': 'id',
                'reviews': 'id'
            }
    
    # Valid foreign key generator
    class ValidForeignKey(Element):
        def __init__(self, from_table: str, to_table: str):
            self.from_table = from_table
            self.to_table = to_table
        
        def generate(self, context: SchemaContext) -> str:
            # Find the foreign key column
            fks = context.foreign_keys.get(self.from_table, {})
            fk_column = None
            
            for col, ref_table in fks.items():
                if ref_table == self.to_table:
                    fk_column = col
                    break
            
            if fk_column:
                pk = context.primary_keys.get(self.to_table, 'id')
                return f"{self.from_table}.{fk_column} = {self.to_table}.{pk}"
            
            # Fallback
            return "1 = 1"
    
    grammar = Grammar("validated")
    grammar.context = SchemaContext(
        tables={
            'users': 10000,
            'products': 500,
            'orders': 50000,
            'reviews': 100000
        }
    )
    
    # Valid JOIN query
    grammar.rule("valid_join", Lambda(lambda ctx:
        f"SELECT * FROM orders o JOIN users u ON " +
        ValidForeignKey('orders', 'users').generate(ctx)
    ))
    
    # Valid multi-way JOIN
    grammar.rule("three_way_join", Lambda(lambda ctx:
        f"SELECT * FROM reviews r\n" +
        f"  JOIN users u ON {ValidForeignKey('reviews', 'users').generate(ctx)}\n" +
        f"  JOIN products p ON {ValidForeignKey('reviews', 'products').generate(ctx)}"
    ))
    
    print("Schema-validated queries:")
    examples = [
        ("Valid foreign key JOIN", "valid_join"),
        ("Three-way JOIN", "three_way_join")
    ]
    
    for desc, rule in examples:
        print(f"\n{desc}:")
        print(grammar.generate(rule))


def main():
    """Run all context examples."""
    
    basic_context_usage()
    table_aware_generation()
    stateful_context()
    context_aware_values()
    advanced_context_patterns()
    context_validation()
    
    print("\n" + "=" * 50)
    print("Context Usage Summary:")
    print("- Context holds tables, fields, and RNG state")
    print("- Custom elements can access context for smart generation")
    print("- Extended contexts can maintain state across generations")
    print("- Context enables adaptive and validated query generation")
    print("- Use Lambda elements for context-aware logic")


if __name__ == "__main__":
    main()