#!/usr/bin/env python3
"""
02_stateful_generation.py - Maintaining State Across Queries

This example demonstrates stateful query generation:
- Transaction management
- Session tracking
- Temporary table lifecycle
- Progressive data generation
- State-aware query patterns

Key concepts:
- Stateful contexts
- Query dependencies
- Transaction boundaries
- Resource management
- State transitions
"""

import sys
from pathlib import Path
from typing import List, Dict, Set, Optional
from enum import Enum

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, Element, Context, template, choice, ref, Lambda


class TransactionState(Enum):
    """Transaction states."""
    NONE = "none"
    STARTED = "started"
    COMMITTED = "committed"
    ROLLED_BACK = "rolled_back"


class StatefulContext(Context):
    """Extended context for stateful generation."""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Transaction management
        self.transaction_state = TransactionState.NONE
        self.transaction_depth = 0
        self.savepoints = []
        
        # Session state
        self.session_id = None
        self.session_start = None
        self.query_count = 0
        
        # Temporary resources
        self.temp_tables = set()
        self.prepared_statements = {}
        self.cursors = set()
        
        # Data state
        self.inserted_ids = {}
        self.updated_tables = set()
        self.last_query_type = None


class TransactionManager(Element):
    """Manages transaction lifecycle."""
    
    def generate(self, context: StatefulContext) -> str:
        context.query_count += 1
        
        # Start transaction every 5 queries if not in transaction
        if context.query_count % 5 == 1 and context.transaction_state == TransactionState.NONE:
            context.transaction_state = TransactionState.STARTED
            context.transaction_depth = 1
            return "BEGIN;"
        
        # Create savepoint in nested transaction
        if context.query_count % 10 == 0 and context.transaction_state == TransactionState.STARTED:
            savepoint = f"sp_{context.query_count}"
            context.savepoints.append(savepoint)
            return f"SAVEPOINT {savepoint};"
        
        # Commit transaction
        if context.query_count % 5 == 0 and context.transaction_state == TransactionState.STARTED:
            context.transaction_state = TransactionState.NONE
            context.transaction_depth = 0
            context.savepoints.clear()
            return "COMMIT;"
        
        # Regular query within transaction
        return self._generate_stateful_query(context)
    
    def _generate_stateful_query(self, context: StatefulContext) -> str:
        """Generate query based on current state."""
        
        # If we have inserted data, reference it
        if context.inserted_ids and context.rng.random() < 0.3:
            table = context.rng.choice(list(context.inserted_ids.keys()))
            id_val = context.rng.choice(context.inserted_ids[table])
            return f"SELECT * FROM {table} WHERE id = {id_val}"
        
        # If in transaction, prefer DML
        if context.transaction_state == TransactionState.STARTED:
            query_type = context.rng.choice(['insert', 'update', 'delete'], p=[0.5, 0.3, 0.2])
            
            if query_type == 'insert':
                table = context.rng.choice(['users', 'orders', 'products'])
                new_id = context.rng.randint(1000, 9999)
                
                # Track inserted ID
                if table not in context.inserted_ids:
                    context.inserted_ids[table] = []
                context.inserted_ids[table].append(new_id)
                
                return f"INSERT INTO {table} (id, name) VALUES ({new_id}, 'test_{new_id}')"
            
            elif query_type == 'update':
                table = context.rng.choice(['users', 'orders', 'products'])
                context.updated_tables.add(table)
                return f"UPDATE {table} SET status = 'updated' WHERE id > 100"
            
            else:  # delete
                table = context.rng.choice(['users', 'orders', 'products'])
                return f"DELETE FROM {table} WHERE status = 'obsolete'"
        
        # Default: simple select
        return "SELECT COUNT(*) FROM users"


class TemporaryTableManager(Element):
    """Manages temporary table lifecycle."""
    
    def generate(self, context: StatefulContext) -> str:
        # Clean up old temp tables periodically
        if len(context.temp_tables) > 5 and context.rng.random() < 0.3:
            table = context.rng.choice(list(context.temp_tables))
            context.temp_tables.remove(table)
            return f"DROP TABLE IF EXISTS {table}"
        
        # Create new temp table
        if context.rng.random() < 0.4:
            table_name = f"temp_{context.query_count}_{context.rng.randint(100, 999)}"
            context.temp_tables.add(table_name)
            
            return f"""CREATE TEMP TABLE {table_name} AS
SELECT id, name, created_at
FROM users
WHERE created_at > CURRENT_DATE - INTERVAL '7 days'"""
        
        # Use existing temp table
        if context.temp_tables:
            table = context.rng.choice(list(context.temp_tables))
            operations = [
                f"SELECT COUNT(*) FROM {table}",
                f"INSERT INTO {table} SELECT * FROM {table} WHERE id < 100",
                f"UPDATE {table} SET name = name || '_updated' WHERE id > 50",
                f"DELETE FROM {table} WHERE id % 2 = 0"
            ]
            return context.rng.choice(operations)
        
        # Default: create first temp table
        context.temp_tables.add("temp_initial")
        return "CREATE TEMP TABLE temp_initial (id INT, data TEXT)"


class PreparedStatementManager(Element):
    """Manages prepared statements."""
    
    def generate(self, context: StatefulContext) -> str:
        # Prepare new statement
        if len(context.prepared_statements) < 3 and context.rng.random() < 0.3:
            stmt_name = f"stmt_{len(context.prepared_statements) + 1}"
            
            queries = [
                "SELECT * FROM users WHERE id = $1",
                "INSERT INTO orders (user_id, total) VALUES ($1, $2)",
                "UPDATE products SET price = $1 WHERE id = $2",
                "DELETE FROM old_data WHERE created_at < $1"
            ]
            
            query = context.rng.choice(queries)
            context.prepared_statements[stmt_name] = query
            
            return f"PREPARE {stmt_name} AS {query}"
        
        # Execute prepared statement
        if context.prepared_statements and context.rng.random() < 0.7:
            stmt_name = context.rng.choice(list(context.prepared_statements.keys()))
            
            # Generate appropriate parameters
            if "users" in context.prepared_statements[stmt_name]:
                params = f"({context.rng.randint(1, 1000)})"
            elif "orders" in context.prepared_statements[stmt_name]:
                params = f"({context.rng.randint(1, 1000)}, {context.rng.randint(10, 1000)})"
            elif "products" in context.prepared_statements[stmt_name]:
                params = f"({context.rng.randint(10, 100)}, {context.rng.randint(1, 100)})"
            else:
                params = "('2024-01-01')"
            
            return f"EXECUTE {stmt_name}{params}"
        
        # Deallocate old statement
        if len(context.prepared_statements) > 4:
            stmt_name = context.rng.choice(list(context.prepared_statements.keys()))
            del context.prepared_statements[stmt_name]
            return f"DEALLOCATE {stmt_name}"
        
        # Default: simple query
        return "SELECT version()"


class CursorManager(Element):
    """Manages database cursors."""
    
    def generate(self, context: StatefulContext) -> str:
        # Declare new cursor
        if len(context.cursors) < 2 and context.rng.random() < 0.3:
            cursor_name = f"cursor_{context.query_count}"
            context.cursors.add(cursor_name)
            
            return f"""DECLARE {cursor_name} CURSOR FOR
SELECT id, name, email FROM users ORDER BY id"""
        
        # Use existing cursor
        if context.cursors and context.rng.random() < 0.6:
            cursor = context.rng.choice(list(context.cursors))
            operations = [
                f"FETCH NEXT FROM {cursor}",
                f"FETCH 10 FROM {cursor}",
                f"FETCH PRIOR FROM {cursor}",
                f"MOVE 5 IN {cursor}"
            ]
            return context.rng.choice(operations)
        
        # Close cursor
        if context.cursors and context.rng.random() < 0.2:
            cursor = context.rng.choice(list(context.cursors))
            context.cursors.remove(cursor)
            return f"CLOSE {cursor}"
        
        # Default
        return "SELECT current_database()"


class ProgressiveDataGenerator(Element):
    """Generates data that builds on previous operations."""
    
    def __init__(self):
        self.phase = 0
        self.base_users = []
        self.base_products = []
        self.orders_created = 0
    
    def generate(self, context: StatefulContext) -> str:
        self.phase = (context.query_count // 10) % 4
        
        if self.phase == 0:
            # Phase 1: Create base data
            user_id = context.rng.randint(1000, 1999)
            self.base_users.append(user_id)
            return f"INSERT INTO users (id, name, email) VALUES ({user_id}, 'user_{user_id}', 'user_{user_id}@example.com')"
        
        elif self.phase == 1:
            # Phase 2: Create products
            product_id = context.rng.randint(100, 199)
            self.base_products.append(product_id)
            price = context.rng.randint(10, 1000)
            return f"INSERT INTO products (id, name, price) VALUES ({product_id}, 'product_{product_id}', {price})"
        
        elif self.phase == 2:
            # Phase 3: Create orders using existing data
            if self.base_users and self.base_products:
                user_id = context.rng.choice(self.base_users)
                product_id = context.rng.choice(self.base_products)
                quantity = context.rng.randint(1, 10)
                self.orders_created += 1
                
                return f"""INSERT INTO orders (user_id, product_id, quantity, order_date) 
VALUES ({user_id}, {product_id}, {quantity}, CURRENT_DATE)"""
            else:
                return "SELECT COUNT(*) FROM users"
        
        else:
            # Phase 4: Analytics on created data
            queries = [
                "SELECT u.name, COUNT(o.id) as order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name",
                "SELECT p.name, SUM(o.quantity) as total_sold FROM products p JOIN orders o ON p.id = o.product_id GROUP BY p.name",
                f"SELECT COUNT(*) as orders_created FROM orders -- Expected: {self.orders_created}",
                "SELECT AVG(quantity) as avg_order_size FROM orders"
            ]
            return context.rng.choice(queries)


def demonstrate_stateful_patterns():
    """Demonstrate various stateful patterns."""
    
    print("Stateful Generation Patterns")
    print("=" * 50)
    
    # 1. Transaction Management
    print("\n1. Transaction Management:")
    grammar = Grammar("transactions")
    grammar.context = StatefulContext()
    grammar.rule("query", TransactionManager())
    
    print("  Transaction flow:")
    for i in range(12):
        query = grammar.generate("query", seed=i)
        state = grammar.context.transaction_state.value
        print(f"  {i+1:2d}. [{state:8s}] {query}")
    
    # 2. Temporary Tables
    print("\n\n2. Temporary Table Lifecycle:")
    grammar = Grammar("temp_tables")
    grammar.context = StatefulContext()
    grammar.rule("query", TemporaryTableManager())
    
    for i in range(10):
        query = grammar.generate("query", seed=i*2)
        temp_count = len(grammar.context.temp_tables)
        print(f"  {i+1:2d}. [Temps: {temp_count}] {query[:80]}...")
    
    # 3. Prepared Statements
    print("\n\n3. Prepared Statement Management:")
    grammar = Grammar("prepared")
    grammar.context = StatefulContext()
    grammar.rule("query", PreparedStatementManager())
    
    for i in range(10):
        query = grammar.generate("query", seed=i*3)
        stmt_count = len(grammar.context.prepared_statements)
        print(f"  {i+1:2d}. [Stmts: {stmt_count}] {query}")
    
    # 4. Progressive Data Generation
    print("\n\n4. Progressive Data Generation:")
    grammar = Grammar("progressive")
    grammar.context = StatefulContext()
    generator = ProgressiveDataGenerator()
    grammar.rule("query", generator)
    
    print("  Building related data across phases:")
    for i in range(16):
        query = grammar.generate("query", seed=i)
        phase = generator.phase
        print(f"  {i+1:2d}. [Phase {phase+1}] {query[:70]}...")


def create_session_grammar():
    """Create grammar for session-based query generation."""
    
    print("\n\nSession-Based Query Generation")
    print("=" * 50)
    
    class SessionQuery(Element):
        """Generate queries for a user session."""
        
        def generate(self, context: StatefulContext) -> str:
            # Initialize session
            if context.session_id is None:
                context.session_id = context.rng.randint(10000, 99999)
                context.session_start = "CURRENT_TIMESTAMP"
                return f"-- Session {context.session_id} started"
            
            # Session phases
            query_num = context.query_count % 20
            
            if query_num < 3:
                # Login phase
                return self._login_queries(context)
            elif query_num < 10:
                # Browse phase
                return self._browse_queries(context)
            elif query_num < 15:
                # Transaction phase
                return self._transaction_queries(context)
            else:
                # Logout phase
                return self._logout_queries(context)
        
        def _login_queries(self, context: StatefulContext) -> str:
            queries = [
                f"SELECT * FROM users WHERE email = 'user_{context.session_id}@example.com'",
                f"UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE id = {context.session_id % 1000}",
                f"INSERT INTO user_sessions (user_id, session_id, ip_address) VALUES ({context.session_id % 1000}, '{context.session_id}', '192.168.1.1')"
            ]
            return context.rng.choice(queries)
        
        def _browse_queries(self, context: StatefulContext) -> str:
            queries = [
                "SELECT * FROM products WHERE category = 'electronics' LIMIT 20",
                f"SELECT * FROM products WHERE id = {context.rng.randint(1, 100)}",
                "SELECT p.*, r.rating FROM products p LEFT JOIN reviews r ON p.id = r.product_id",
                f"INSERT INTO user_activity (user_id, action, timestamp) VALUES ({context.session_id % 1000}, 'view_product', CURRENT_TIMESTAMP)"
            ]
            return context.rng.choice(queries)
        
        def _transaction_queries(self, context: StatefulContext) -> str:
            queries = [
                f"INSERT INTO cart_items (user_id, product_id, quantity) VALUES ({context.session_id % 1000}, {context.rng.randint(1, 100)}, {context.rng.randint(1, 5)})",
                f"SELECT SUM(p.price * c.quantity) FROM cart_items c JOIN products p ON c.product_id = p.id WHERE c.user_id = {context.session_id % 1000}",
                f"INSERT INTO orders (user_id, total, status) VALUES ({context.session_id % 1000}, {context.rng.randint(50, 500)}, 'pending')",
                f"DELETE FROM cart_items WHERE user_id = {context.session_id % 1000}"
            ]
            return context.rng.choice(queries)
        
        def _logout_queries(self, context: StatefulContext) -> str:
            queries = [
                f"UPDATE user_sessions SET end_time = CURRENT_TIMESTAMP WHERE session_id = '{context.session_id}'",
                f"INSERT INTO user_activity (user_id, action, timestamp) VALUES ({context.session_id % 1000}, 'logout', CURRENT_TIMESTAMP)",
                f"-- Session {context.session_id} ended"
            ]
            return context.rng.choice(queries)
    
    grammar = Grammar("session")
    grammar.context = StatefulContext()
    grammar.rule("query", SessionQuery())
    
    print("  User session simulation:")
    for i in range(22):
        query = grammar.generate("query", seed=i)
        print(f"  {i+1:2d}. {query[:80]}...")


def create_migration_grammar():
    """Create grammar for database migration scenarios."""
    
    print("\n\nDatabase Migration Patterns")
    print("=" * 50)
    
    class MigrationQuery(Element):
        """Generate migration queries."""
        
        def __init__(self):
            self.migration_phase = 0
            self.tables_created = []
            self.indexes_created = []
        
        def generate(self, context: StatefulContext) -> str:
            self.migration_phase = context.query_count // 5
            
            if self.migration_phase == 0:
                # Create new schema
                return self._create_schema(context)
            elif self.migration_phase == 1:
                # Copy data
                return self._copy_data(context)
            elif self.migration_phase == 2:
                # Create indexes
                return self._create_indexes(context)
            elif self.migration_phase == 3:
                # Verify migration
                return self._verify_migration(context)
            else:
                # Cleanup
                return self._cleanup(context)
        
        def _create_schema(self, context: Context) -> str:
            table_name = f"new_table_{context.rng.randint(1, 5)}"
            self.tables_created.append(table_name)
            
            return f"""CREATE TABLE IF NOT EXISTS {table_name} (
    id BIGSERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)"""
        
        def _copy_data(self, context: Context) -> str:
            if self.tables_created:
                table = context.rng.choice(self.tables_created)
                return f"""INSERT INTO {table} (data)
SELECT row_to_json(t) FROM (
    SELECT id, name, email FROM users LIMIT 1000
) t"""
            return "-- No tables to copy data to"
        
        def _create_indexes(self, context: Context) -> str:
            if self.tables_created:
                table = context.rng.choice(self.tables_created)
                index_name = f"idx_{table}_{context.rng.randint(1, 10)}"
                self.indexes_created.append(index_name)
                
                index_types = [
                    f"CREATE INDEX {index_name} ON {table} USING GIN (data)",
                    f"CREATE INDEX {index_name} ON {table} (created_at DESC)",
                    f"CREATE UNIQUE INDEX {index_name} ON {table} ((data->>'id'))"
                ]
                
                return context.rng.choice(index_types)
            return "-- No tables to index"
        
        def _verify_migration(self, context: Context) -> str:
            queries = []
            
            for table in self.tables_created:
                queries.append(f"SELECT COUNT(*) FROM {table}")
                queries.append(f"EXPLAIN SELECT * FROM {table} WHERE data @> '{{\"id\": 1}}'")
            
            if queries:
                return context.rng.choice(queries)
            return "SELECT 'No migration to verify'"
        
        def _cleanup(self, context: Context) -> str:
            if context.rng.random() < 0.5 and self.tables_created:
                # Drop old table
                return "DROP TABLE IF EXISTS old_users_backup"
            else:
                # Analyze new tables
                if self.tables_created:
                    table = context.rng.choice(self.tables_created)
                    return f"ANALYZE {table}"
                return "-- Migration complete"
    
    grammar = Grammar("migration")
    grammar.context = StatefulContext()
    migration = MigrationQuery()
    grammar.rule("query", migration)
    
    print("  Database migration simulation:")
    for i in range(20):
        query = grammar.generate("query", seed=i)
        phase = migration.migration_phase
        phase_names = ["Schema", "Copy", "Index", "Verify", "Cleanup"]
        phase_name = phase_names[min(phase, 4)]
        print(f"  {i+1:2d}. [{phase_name:8s}] {query[:60]}...")


def main():
    """Run all stateful generation examples."""
    
    demonstrate_stateful_patterns()
    create_session_grammar()
    create_migration_grammar()
    
    print("\n" + "=" * 50)
    print("Stateful Generation Summary:")
    print("- Use extended contexts to maintain state")
    print("- Track resources (transactions, temp tables, etc.)")
    print("- Build progressive data relationships")
    print("- Simulate realistic user sessions")
    print("- Model complex workflows (migrations, ETL, etc.)")


if __name__ == "__main__":
    main()