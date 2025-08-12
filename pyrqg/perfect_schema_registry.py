"""
Perfect schema registry that tracks exactly which columns exist in each table.
"""

import psycopg2
from typing import Dict, List, Set, Optional
import random


class PerfectSchemaRegistry:
    """Registry with perfect knowledge of database schema"""
    
    def __init__(self, connection_string="dbname=postgres"):
        self.conn = psycopg2.connect(connection_string)
        self.tables = {}
        self.column_types = {}
        self.column_map = {}  # Maps column names to tables that have them
        self._load_schema()
        
    def _load_schema(self):
        """Load complete schema information"""
        cur = self.conn.cursor()
        
        # Set search path
        cur.execute("SET search_path TO pyrqg, public")
        
        # Get all tables and columns with their types
        cur.execute("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'pyrqg'
            AND table_name NOT LIKE '%pkey%'
            ORDER BY table_name, ordinal_position
        """)
        
        for table, column, data_type in cur.fetchall():
            if table not in self.tables:
                self.tables[table] = []
            self.tables[table].append(column)
            
            # Store column type
            self.column_types[f"{table}.{column}"] = data_type
            
            # Build reverse mapping
            if column not in self.column_map:
                self.column_map[column] = []
            self.column_map[column].append(table)
        
        cur.close()
    
    def get_tables(self) -> List[str]:
        """Get list of all tables"""
        workload_tables = [
            'users', 'products', 'orders', 'inventory', 'transactions',
            'sessions', 'customers', 'logs', 'analytics', 'accounts'
        ]
        # Return tables with schema prefix
        return [f"pyrqg.{t}" for t in workload_tables if t in self.tables]
    
    def get_insertable_columns(self, table: str) -> List[str]:
        """Get columns suitable for INSERT (exclude id)"""
        # Strip schema prefix if present
        table_name = table.split('.')[-1] if '.' in table else table
        if table_name not in self.tables:
            return []
        return [c for c in self.tables[table_name] if c != 'id']
    
    def column_exists(self, table: str, column: str) -> bool:
        """Check if a column exists in a table"""
        # Strip schema prefix if present
        table_name = table.split('.')[-1] if '.' in table else table
        return table_name in self.tables and column in self.tables[table_name]
    
    def get_common_columns(self, table1: str, table2: str) -> List[str]:
        """Get columns that exist in both tables"""
        # Strip schema prefix if present
        table1_name = table1.split('.')[-1] if '.' in table1 else table1
        table2_name = table2.split('.')[-1] if '.' in table2 else table2
        if table1_name not in self.tables or table2_name not in self.tables:
            return []
        return list(set(self.tables[table1_name]) & set(self.tables[table2_name]))
    
    def get_column_value(self, column: str, rng=None, table=None) -> str:
        """Generate appropriate value for a column based on actual database type"""
        if rng is None:
            rng = random.Random()
        
        # Get actual column type from database
        data_type = None
        if table:
            # Strip schema prefix if present
            table_name = table.split('.')[-1] if '.' in table else table
            data_type = self.column_types.get(f"{table_name}.{column}")
        
        # Generate value based on actual data type
        if data_type:
            if data_type in ['integer', 'bigint', 'smallint']:
                return str(rng.randint(1, 1000))
            elif data_type in ['numeric', 'decimal', 'real', 'double precision']:
                return f"{rng.randint(1, 10000)}.{rng.randint(0, 99):02d}"
            elif data_type == 'boolean':
                return rng.choice(["true", "false"])
            elif data_type in ['timestamp', 'timestamp without time zone', 'date']:
                return "CURRENT_TIMESTAMP"
            elif data_type == 'jsonb':
                return "'{}'::jsonb"
            elif data_type == 'text[]':  # Array type
                return "ARRAY['item1', 'item2']"
        
        # Fallback to name-based heuristics
        if column.endswith('_id') or column in ['quantity', 'count', 'score', 
                                                'rating', 'age', 'level', 'priority',
                                                'version', 'visit_count', 'stock_quantity',
                                                'retry_count', 'notification_id', 'sale_id']:
            return str(rng.randint(1, 1000))
        
        elif column in ['price', 'amount', 'total', 'balance', 'fee', 'tax', 
                        'discount', 'cost', 'total_amount', 'shipping_cost',
                        'total_spent', 'unit_price', 'salary']:
            return f"{rng.randint(1, 10000)}.{rng.randint(0, 99):02d}"
        
        elif column == 'email':
            return rng.choice(["'user@example.com'", "'admin@example.com'", "'test@example.com'"])
        
        elif column in ['name', 'first_name', 'last_name', 'username', 'title',
                       'event_name', 'metric_name']:
            return rng.choice(["'Alice'", "'Bob'", "'Charlie'", "'David'", "'Eve'"])
        
        elif column in ['status', 'type', 'role', 'category', 'transaction_type',
                       'account_type', 'payment_method', 'log_level', 'event_type']:
            return rng.choice(["'active'", "'inactive'", "'pending'", "'completed'", "'cancelled'"])
        
        elif column.endswith('_at') or column in ['timestamp', 'date'] or 'date' in column:
            return "CURRENT_TIMESTAMP"
        
        elif column in ['data', 'metadata', 'properties', 'settings', 'items']:
            return "'{}'::jsonb"
        
        elif column in ['is_active', 'is_deleted', 'active', 'deleted', 'locked',
                       'is_verified']:
            return rng.choice(["true", "false"])
        
        elif column == 'tags':
            return "ARRAY['tag1', 'tag2']"
        
        elif column in ['message', 'description', 'notes', 'comments']:
            return "'Test message'"
        
        else:
            return rng.choice(["'Product X'", "'Item Y'", "'Service Z'", "'Test Data'"])
    
    def close(self):
        """Close database connection"""
        self.conn.close()


# Global instance
_registry = None

def get_perfect_registry():
    """Get or create perfect registry"""
    global _registry
    if _registry is None:
        _registry = PerfectSchemaRegistry()
    return _registry