"""
Schema-aware SQL generator that relies on a SchemaCatalog instance.

This generator no longer connects to a live database. Instead, callers provide
an instance of pyrqg.schema_support.SchemaCatalog (offline) or
pyrqg.online_catalog.OnlineSchemaCatalog (live) to supply table/column
information and value generation.
"""

from typing import List, Optional
import random

from .schema_support import SchemaCatalog


class SchemaAwareGenerator:
    """Generate queries that match a provided schema catalog"""

    def __init__(self, catalog: SchemaCatalog, seed: Optional[int] = None):
        self.catalog = catalog
        self.rng = random.Random(seed)
    
    def get_valid_columns(self, table: str, count: Optional[int] = None, for_insert: bool = True) -> List[str]:
        """Get valid columns for a table from the catalog."""
        if table not in self.catalog.tables:
            return []
        if for_insert:
            if count is None:
                return self.catalog.pick_insert_columns(table, self.rng)
            # Try to pick exactly `count` by clamping min/max
            return self.catalog.pick_insert_columns(table, self.rng, min_cols=count, max_cols=count)
        else:
            if count is None:
                return self.catalog.pick_update_columns(table, self.rng)
            return self.catalog.pick_update_columns(table, self.rng, min_cols=max(1, count), max_cols=count)
    
    def get_column_value(self, table: str, column: str) -> str:
        """Delegate value generation to the catalog"""
        return self.catalog.value_for(table, column, self.rng)
    
    def generate_insert(self, table: str) -> str:
        """Generate a valid INSERT statement with correct types"""
        if table not in self.catalog.tables:
            return None
            
        # Get valid columns for insert
        all_columns = self.get_valid_columns(table, for_insert=True)
        if not all_columns:
            return None
        
        # Pick 3-5 random columns
        num_cols = min(self.rng.randint(3, 5), len(all_columns))
        columns = random.sample(all_columns, num_cols)
        
        # Generate values with CORRECT TYPES
        values = []
        for col in columns:
            value = self.get_column_value(table, col)
            values.append(value)
                
        return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    
    def generate_update(self, table: str) -> str:
        """Generate valid UPDATE statement"""
        if table not in self.catalog.tables:
            return None
        
        # Get updateable columns (not id, created_at)
        columns = self.catalog.pick_update_columns(table, self.rng, min_cols=1, max_cols=5)
        
        if not columns:
            return None
        
        # Update 1-3 columns
        num_updates = min(self.rng.randint(1, 3), len(columns))
        update_cols = random.sample(columns, num_updates)
        
        set_parts = []
        for col in update_cols:
            value = self.get_column_value(table, col)
            set_parts.append(f"{col} = {value}")
        
        # Add WHERE clause
        where = self.generate_where_clause(table)
        
        return f"UPDATE {table} SET {', '.join(set_parts)} WHERE {where}"
    
    def generate_delete(self, table: str) -> str:
        """Generate valid DELETE statement"""
        if table not in self.catalog.tables:
            return None
        
        where = self.generate_where_clause(table)
        return f"DELETE FROM {table} WHERE {where}"
    
    def generate_select(self, table: str) -> str:
        """Generate valid SELECT statement"""
        if table not in self.catalog.tables:
            return None
        
        # Choose columns
        columns = self.get_valid_columns(table, for_insert=False)
        if self.rng.random() < 0.3:
            col_list = '*'
        else:
            num_cols = self.rng.randint(1, min(5, len(columns)))
            selected = random.sample(columns, num_cols)
            col_list = ', '.join(selected)
        
        query = f"SELECT {col_list} FROM {table}"
        
        # Add WHERE clause 70% of time
        if self.rng.random() < 0.7:
            where = self.generate_where_clause(table)
            query += f" WHERE {where}"
        
        # Add ORDER BY 30% of time
        if self.rng.random() < 0.3 and columns:
            order_col = random.choice(columns)
            order_dir = self.rng.choice(['ASC', 'DESC'])
            query += f" ORDER BY {order_col} {order_dir}"
        
        # Add LIMIT 40% of time
        if self.rng.random() < 0.4:
            limit = self.rng.randint(10, 100)
            query += f" LIMIT {limit}"
        
        return query
    
    def generate_where_clause(self, table: str) -> str:
        """Generate valid WHERE clause for table using the catalog helpers"""
        if table not in self.catalog.tables:
            return "1=1"
        return self.catalog.where_condition(table, self.rng)
    
    def fix_query(self, query: str) -> str:
        """Fix a query to use valid columns and types"""
        import re
        
        # Detect query type
        query_upper = query.upper().strip()
        
        if query_upper.startswith('INSERT'):
            # Extract table
            match = re.search(r'INSERT\s+INTO\s+(\w+)', query, re.IGNORECASE)
            if match:
                table = match.group(1).lower()
                return self.generate_insert(table) or query
        
        elif query_upper.startswith('UPDATE'):
            match = re.search(r'UPDATE\s+(\w+)', query, re.IGNORECASE)
            if match:
                table = match.group(1).lower()
                return self.generate_update(table) or query
        
        elif query_upper.startswith('DELETE'):
            match = re.search(r'DELETE\s+FROM\s+(\w+)', query, re.IGNORECASE)
            if match:
                table = match.group(1).lower()
                return self.generate_delete(table) or query
        
        elif query_upper.startswith('SELECT'):
            match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
            if match:
                table = match.group(1).lower()
                return self.generate_select(table) or query
        
        return query
    
    def close(self):
        """No-op retained for backward compatibility"""
        return None


# Global instance
_generator = None

def get_schema_aware_generator():
    """Get or create schema-aware generator with default offline catalog."""
    from .schema_support import get_schema_catalog
    global _generator
    if _generator is None:
        _generator = SchemaAwareGenerator(get_schema_catalog())
    return _generator