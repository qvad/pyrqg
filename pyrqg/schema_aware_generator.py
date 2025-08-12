"""
Schema-aware query generator that ACTUALLY WORKS.
This will generate queries with correct columns for each table.
"""

import psycopg2
from typing import Dict, List, Set, Tuple, Optional
import random


class SchemaAwareGenerator:
    """Generate queries that match the actual database schema"""
    
    def __init__(self, connection_string="dbname=postgres"):
        self.conn = psycopg2.connect(connection_string)
        self.schema = {}
        self.column_types = {}
        self._load_schema()
        
    def _load_schema(self):
        """Load actual table/column mappings AND TYPES from database"""
        cur = self.conn.cursor()
        
        # Set search path
        cur.execute("SET search_path TO pyrqg, public")
        
        # Get columns WITH THEIR DATA TYPES
        cur.execute("""
            SELECT table_name, column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_schema = 'pyrqg'
            AND table_name NOT LIKE '%pkey%'
            ORDER BY table_name, ordinal_position
        """)
        
        for table, column, data_type, is_nullable, default in cur.fetchall():
            if table not in self.schema:
                self.schema[table] = []
            self.schema[table].append(column)
            
            # Store column type for proper value generation
            self.column_types[f"{table}.{column}"] = {
                'type': data_type,
                'nullable': is_nullable == 'YES',
                'has_default': default is not None
            }
        
        cur.close()
    
    def get_valid_columns(self, table: str, count: int = None, for_insert: bool = True) -> List[str]:
        """Get valid columns for a table"""
        if table not in self.schema:
            return []
        
        columns = []
        required_columns = []
        
        for col in self.schema[table]:
            col_info = self.column_types.get(f"{table}.{col}")
            
            # Skip auto-generated columns for INSERT
            if for_insert:
                if col == 'id' or (col_info and col_info['has_default'] and 
                    col in ['created_at', 'updated_at', 'timestamp']):
                    continue
            
            columns.append(col)
            
            # Track required (NOT NULL without default) columns
            if for_insert and col_info and not col_info['nullable'] and not col_info['has_default']:
                required_columns.append(col)
        
        # For INSERT, always include required columns
        if for_insert and required_columns:
            # Start with required columns
            result = required_columns.copy()
            # Add optional columns if count allows
            optional = [c for c in columns if c not in required_columns]
            if count and len(result) < count and optional:
                remaining = count - len(result)
                result.extend(random.sample(optional, min(remaining, len(optional))))
            return result
        
        if count and count < len(columns):
            return random.sample(columns, count)
        return columns
    
    def get_column_value(self, table: str, column: str) -> str:
        """Generate appropriate value based on ACTUAL column type"""
        col_key = f"{table}.{column}"
        col_info = self.column_types.get(col_key, {})
        data_type = col_info.get('type', '').lower()
        
        # Check if this is a foreign key column (ends with _id)
        if column.endswith('_id') and data_type in ['integer', 'bigint', 'smallint']:
            # Try to get a valid foreign key value
            ref_table = column.replace('_id', 's')  # Simple heuristic: user_id -> users
            if ref_table in self.schema:
                cur = self.conn.cursor()
                try:
                    cur.execute(f"SELECT id FROM {ref_table} ORDER BY RANDOM() LIMIT 1")
                    result = cur.fetchone()
                    if result:
                        return str(result[0])
                except:
                    pass
                finally:
                    cur.close()
        
        # Generate based on ACTUAL PostgreSQL type
        if data_type in ['integer', 'bigint', 'smallint']:
            return str(random.randint(1, 100))
        elif data_type in ['numeric', 'decimal']:
            return f"{random.randint(1, 10000)}.{random.randint(0, 99):02d}"
        elif data_type in ['real', 'double precision']:
            return f"{random.uniform(0, 1000):.2f}"
        elif data_type == 'boolean':
            return random.choice(['true', 'false'])
        elif data_type in ['timestamp', 'timestamp without time zone']:
            # No quotes for timestamp!
            return 'CURRENT_TIMESTAMP'
        elif data_type == 'date':
            return 'CURRENT_DATE'
        elif data_type in ['jsonb', 'json']:
            return "'{}'::jsonb"
        elif data_type == 'array' or '[]' in data_type:
            # PostgreSQL array format
            return "'{item1,item2}'"
        else:
            # String types - use appropriate values based on column name
            if column == 'email':
                return "'user@example.com'"
            elif column in ['name', 'first_name', 'last_name', 'username']:
                return random.choice(["'Alice'", "'Bob'", "'Charlie'", "'David'"])
            elif column == 'status':
                return random.choice(["'active'", "'inactive'", "'pending'", "'completed'"])
            elif column in ['type', 'role', 'category']:
                return random.choice(["'typeA'", "'typeB'", "'typeC'"])
            else:
                return "'test_value'"
    
    def generate_insert(self, table: str) -> str:
        """Generate a valid INSERT statement with correct types"""
        if table not in self.schema:
            return None
            
        # Get valid columns for insert
        all_columns = self.get_valid_columns(table, for_insert=True)
        if not all_columns:
            return None
        
        # Pick 3-5 random columns
        num_cols = min(random.randint(3, 5), len(all_columns))
        columns = random.sample(all_columns, num_cols)
        
        # Generate values with CORRECT TYPES
        values = []
        for col in columns:
            value = self.get_column_value(table, col)
            values.append(value)
                
        return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)})"
    
    def generate_update(self, table: str) -> str:
        """Generate valid UPDATE statement"""
        if table not in self.schema:
            return None
        
        # Get updateable columns (not id, created_at)
        columns = [c for c in self.schema[table] 
                  if c not in ['id', 'created_at'] and not c.endswith('_at')]
        
        if not columns:
            return None
        
        # Update 1-3 columns
        num_updates = min(random.randint(1, 3), len(columns))
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
        if table not in self.schema:
            return None
        
        where = self.generate_where_clause(table)
        return f"DELETE FROM {table} WHERE {where}"
    
    def generate_select(self, table: str) -> str:
        """Generate valid SELECT statement"""
        if table not in self.schema:
            return None
        
        # Choose columns
        columns = self.get_valid_columns(table, for_insert=False)
        if random.random() < 0.3:
            col_list = '*'
        else:
            num_cols = random.randint(1, min(5, len(columns)))
            selected = random.sample(columns, num_cols)
            col_list = ', '.join(selected)
        
        query = f"SELECT {col_list} FROM {table}"
        
        # Add WHERE clause 70% of time
        if random.random() < 0.7:
            where = self.generate_where_clause(table)
            query += f" WHERE {where}"
        
        # Add ORDER BY 30% of time
        if random.random() < 0.3 and columns:
            order_col = random.choice(columns)
            order_dir = random.choice(['ASC', 'DESC'])
            query += f" ORDER BY {order_col} {order_dir}"
        
        # Add LIMIT 40% of time
        if random.random() < 0.4:
            limit = random.randint(10, 100)
            query += f" LIMIT {limit}"
        
        return query
    
    def generate_where_clause(self, table: str) -> str:
        """Generate valid WHERE clause for table"""
        if table not in self.schema:
            return "1=1"
        
        # Find numeric columns
        numeric_cols = []
        text_cols = []
        bool_cols = []
        
        for col in self.schema[table]:
            col_info = self.column_types.get(f"{table}.{col}", {})
            data_type = col_info.get('type', '').lower()
            
            if any(t in data_type for t in ['int', 'numeric', 'decimal', 'real', 'double']):
                numeric_cols.append(col)
            elif data_type == 'boolean':
                bool_cols.append(col)
            elif any(t in data_type for t in ['char', 'text']):
                text_cols.append(col)
        
        conditions = []
        
        # Add numeric condition
        if numeric_cols:
            col = random.choice(numeric_cols)
            op = random.choice(['=', '>', '<', '>=', '<=', '<>'])
            value = random.randint(1, 1000)
            conditions.append(f"{col} {op} {value}")
        
        # Add text condition sometimes
        if text_cols and random.random() < 0.3:
            col = random.choice(text_cols)
            value = self.get_column_value(table, col)
            conditions.append(f"{col} = {value}")
        
        # Add boolean condition sometimes
        if bool_cols and random.random() < 0.2:
            col = random.choice(bool_cols)
            value = random.choice(['true', 'false'])
            conditions.append(f"{col} = {value}")
        
        if not conditions:
            # Default condition
            if 'id' in self.schema[table]:
                return f"id > {random.randint(0, 100)}"
            return "1=1"
        
        # Combine conditions
        if len(conditions) == 1:
            return conditions[0]
        elif len(conditions) == 2 and random.random() < 0.5:
            return f"{conditions[0]} AND {conditions[1]}"
        else:
            return conditions[0]
    
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
        """Close database connection"""
        self.conn.close()


# Global instance
_generator = None

def get_schema_aware_generator():
    """Get or create schema-aware generator"""
    global _generator
    if _generator is None:
        _generator = SchemaAwareGenerator()
    return _generator