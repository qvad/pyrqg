"""
Schema-aware context for DSL generation that prevents schema and type errors
"""

import random
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
import psycopg2


@dataclass
class ColumnMetadata:
    """Complete metadata for a column"""
    name: str
    data_type: str
    is_nullable: bool = True
    is_primary_key: bool = False
    is_unique: bool = False
    has_default: bool = False
    foreign_key: Optional[str] = None  # table.column reference


@dataclass
class TableMetadata:
    """Complete metadata for a table"""
    name: str
    columns: Dict[str, ColumnMetadata]
    primary_key: Optional[str] = None
    unique_columns: List[str] = field(default_factory=list)
    foreign_keys: Dict[str, str] = field(default_factory=dict)  # column -> table.column
    row_count: int = 0


class SchemaAwareContext:
    """Context that ensures all generated queries are schema-compliant"""
    
    def __init__(self, connection_string: str = "dbname=postgres", seed: Optional[int] = None):
        self.conn = psycopg2.connect(connection_string)
        self.rng = random.Random(seed)
        self.tables: Dict[str, TableMetadata] = {}
        self.state: Dict[str, Any] = {}  # For compatibility with Grammar.generate
        self.fields: List[str] = []      # For compatibility with generic Context
        self.type_generators = self._init_type_generators()
        self._load_complete_schema()
    
    def _init_type_generators(self):
        """Initialize generators for each PostgreSQL type"""
        return {
            'integer': lambda: str(self.rng.randint(1, 1000)),
            'bigint': lambda: str(self.rng.randint(1, 100000)),
            'smallint': lambda: str(self.rng.randint(1, 100)),
            'serial': lambda: 'DEFAULT',
            'bigserial': lambda: 'DEFAULT',
            'numeric': lambda: f"{self.rng.randint(1, 10000)}.{self.rng.randint(0, 99):02d}",
            'decimal': lambda: f"{self.rng.randint(1, 10000)}.{self.rng.randint(0, 99):02d}",
            'real': lambda: f"{self.rng.uniform(0, 1000):.2f}",
            'double precision': lambda: f"{self.rng.uniform(0, 1000):.4f}",
            'boolean': lambda: self.rng.choice(['true', 'false']),
            'character varying': lambda: self._generate_varchar(),
            'varchar': lambda: self._generate_varchar(),
            'text': lambda: self._generate_text(),
            'character': lambda: "'A'",
            'char': lambda: "'B'",
            'timestamp': lambda: 'CURRENT_TIMESTAMP',
            'timestamp without time zone': lambda: 'CURRENT_TIMESTAMP',
            'date': lambda: 'CURRENT_DATE',
            'time': lambda: 'CURRENT_TIME',
            'jsonb': lambda: "'{}'::jsonb",
            'json': lambda: "'{}'::json",
            'ARRAY': lambda: "ARRAY['item1', 'item2']",
        }
    
    def _generate_varchar(self) -> str:
        """Generate contextual varchar value"""
        values = [
            "'Test User'", "'Product X'", "'Active Status'",
            "'user@example.com'", "'Description Text'",
            "'Category A'", "'Type B'", "'Role C'"
        ]
        return self.rng.choice(values)
    
    def _generate_text(self) -> str:
        """Generate text value"""
        return self.rng.choice([
            "'This is a test message'",
            "'Sample description text'",
            "'Notes about the record'",
            "'Additional information'"
        ])
    
    def _load_complete_schema(self):
        """Load complete schema with all metadata"""
        cur = self.conn.cursor()
        
        # Set search path
        cur.execute("SET search_path TO pyrqg, public")
        
        # Get all tables with row counts from pyrqg or public schemas
        cur.execute("""
            SELECT t.table_name, c.reltuples::bigint, t.table_schema
            FROM information_schema.tables t
            JOIN pg_class c ON c.relname = t.table_name
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
            WHERE t.table_schema IN ('pyrqg', 'public')
            AND t.table_type = 'BASE TABLE'
            AND t.table_name NOT LIKE '%pkey%'
        """)
        
        tables_info = cur.fetchall()
        
        for table_name, row_count, table_schema in tables_info:
            # Handle case where reltuples is -1 or None
            estimated_rows = row_count if row_count and row_count >= 0 else 0
            
            table_meta = TableMetadata(name=table_name, columns={}, row_count=estimated_rows)
            
            # Get columns with full metadata
            cur.execute("""
                SELECT 
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default IS NOT NULL as has_default,
                    CASE 
                        WHEN pk.column_name IS NOT NULL THEN true 
                        ELSE false 
                    END as is_primary_key,
                    CASE 
                        WHEN uc.column_name IS NOT NULL THEN true 
                        ELSE false 
                    END as is_unique
                FROM information_schema.columns c
                LEFT JOIN (
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = %s
                    AND tc.table_name = %s
                    AND tc.constraint_type = 'PRIMARY KEY'
                ) pk ON c.column_name = pk.column_name
                LEFT JOIN (
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                        ON tc.constraint_name = kcu.constraint_name
                    WHERE tc.table_schema = %s
                    AND tc.table_name = %s
                    AND tc.constraint_type = 'UNIQUE'
                ) uc ON c.column_name = uc.column_name
                WHERE c.table_schema = %s
                AND c.table_name = %s
                ORDER BY c.ordinal_position
            """, (table_schema, table_name, table_schema, table_name, table_schema, table_name))
            
            for row in cur.fetchall():
                col_meta = ColumnMetadata(
                    name=row[0],
                    data_type=row[1],
                    is_nullable=(row[2] == 'YES'),
                    has_default=row[3],
                    is_primary_key=row[4],
                    is_unique=row[5]
                )
                table_meta.columns[col_meta.name] = col_meta
                
                if col_meta.is_primary_key:
                    table_meta.primary_key = col_meta.name
                if col_meta.is_unique:
                    table_meta.unique_columns.append(col_meta.name)
            
            self.tables[table_name] = table_meta
        
        cur.close()
    
    def get_table(self, min_rows: Optional[int] = None, max_rows: Optional[int] = None, prefer_tables: Optional[List[str]] = None) -> str:
        """Get a random table, matching constraints and preferring certain tables"""
        candidate_tables = list(self.tables.keys())
        
        # Filter by row count if stats are available
        if min_rows is not None:
            candidate_tables = [t for t in candidate_tables if self.tables[t].row_count >= min_rows]
        if max_rows is not None:
            candidate_tables = [t for t in candidate_tables if self.tables[t].row_count <= max_rows]
            
        if not candidate_tables:
            # Relax constraints if no tables match
            candidate_tables = list(self.tables.keys())

        if prefer_tables:
            valid_preferred = [t for t in prefer_tables if t in candidate_tables]
            if valid_preferred:
                return self.rng.choice(valid_preferred)
        
        # Default to common workload tables if they are in the candidate list
        workload_tables = [
            'users', 'products', 'orders', 'inventory', 'transactions',
            'sessions', 'customers', 'employees', 'accounts', 'logs'
        ]
        valid_common = [t for t in workload_tables if t in candidate_tables]
        
        if valid_common:
            return self.rng.choice(valid_common)
        
        if candidate_tables:
            return self.rng.choice(candidate_tables)
            
        # Absolute fallback
        return "table1"

    def get_field(self, type: Optional[str] = None, table: Optional[str] = None) -> str:
        """Get a field name matching constraints"""
        if table:
            # If table is specified, look up columns in that table
            if table in self.tables:
                columns = list(self.tables[table].columns.values())
                if type:
                    columns = [c for c in columns if type in c.data_type.lower()]
                
                if columns:
                    return self.rng.choice([c.name for c in columns])
            # Fallback if table not found or no matching columns
            return "col1"
        
        # If no table specified, pick a random table and find a column
        # This is a bit inefficient but satisfies the interface
        if self.tables:
            random_table = self.rng.choice(list(self.tables.values()))
            columns = list(random_table.columns.values())
            if type:
                columns = [c for c in columns if type in c.data_type.lower()]
            
            if columns:
                return self.rng.choice([c.name for c in columns])
                
        return "col1"
    
    def get_columns_for_insert(self, table: str, count: Optional[int] = None) -> List[str]:
        """Get valid columns for INSERT (excluding auto-generated)"""
        if table not in self.tables:
            return []
        
        table_meta = self.tables[table]
        valid_columns = []
        
        for col_name, col_meta in table_meta.columns.items():
            # Skip auto-generated columns
            if col_meta.is_primary_key and col_meta.has_default:
                continue
            if col_name == 'id' and 'serial' in col_meta.data_type.lower():
                continue
            if col_name in ['created_at', 'updated_at'] and col_meta.has_default:
                continue
            
            valid_columns.append(col_name)
        
        if count and count < len(valid_columns):
            # Return a random subset
            return self.rng.sample(valid_columns, count)
        
        return valid_columns
    
    def get_columns_for_select(self, table: str, count: Optional[int] = None) -> List[str]:
        """Get columns for SELECT"""
        if table not in self.tables:
            return ['*']
        
        columns = list(self.tables[table].columns.keys())
        
        if count and count < len(columns):
            return self.rng.sample(columns, count)
        
        return columns
    
    def get_column_value(self, table: str, column: str) -> str:
        """Get appropriate value for a column based on its type"""
        if table not in self.tables or column not in self.tables[table].columns:
            return "'unknown'"
        
        col_meta = self.tables[table].columns[column]
        data_type = col_meta.data_type.lower()
        
        # Handle special cases first
        if col_meta.is_primary_key and col_meta.has_default:
            return 'DEFAULT'
        
        if column == 'email':
            return self.rng.choice([
                "'user@example.com'", "'admin@test.com'", 
                "'test@domain.com'", "'contact@company.com'"
            ])
        
        if column in ['name', 'first_name', 'last_name', 'username']:
            return self.rng.choice([
                "'Alice'", "'Bob'", "'Charlie'", "'David'", 
                "'Eve'", "'Frank'", "'Grace'", "'Henry'"
            ])
        
        if column == 'status':
            return self.rng.choice([
                "'active'", "'inactive'", "'pending'", 
                "'completed'", "'cancelled'"
            ])
        
        # Use type-based generator
        for type_key, generator in self.type_generators.items():
            if type_key in data_type:
                return generator()
        
        # Default to string
        return "'default_value'"
    
    def get_values_for_columns(self, table: str, columns: List[str]) -> List[str]:
        """Get values for a list of columns"""
        return [self.get_column_value(table, col) for col in columns]
    
    def get_join_columns(self, table1: str, table2: str) -> Optional[Tuple[str, str]]:
        """Get columns to join two tables"""
        if table1 not in self.tables or table2 not in self.tables:
            return None
        
        # Look for foreign key relationships
        # For now, use common column names
        common_columns = set(self.tables[table1].columns.keys()) & set(self.tables[table2].columns.keys())
        
        # Prefer _id columns
        id_columns = [col for col in common_columns if col.endswith('_id') or col == 'id']
        if id_columns:
            col = self.rng.choice(id_columns)
            return (col, col)
        
        # Any common column
        if common_columns:
            col = self.rng.choice(list(common_columns))
            return (col, col)
        
        return None
    
    def get_where_condition(self, table: str) -> str:
        """Generate valid WHERE condition for a table"""
        if table not in self.tables:
            return "1=1"
        
        table_meta = self.tables[table]
        
        # Choose a random column for condition
        numeric_columns = []
        text_columns = []
        bool_columns = []
        
        for col_name, col_meta in table_meta.columns.items():
            if any(t in col_meta.data_type.lower() for t in ['int', 'numeric', 'decimal', 'float', 'double']):
                numeric_columns.append(col_name)
            elif col_meta.data_type.lower() == 'boolean':
                bool_columns.append(col_name)
            elif any(t in col_meta.data_type.lower() for t in ['char', 'text']):
                text_columns.append(col_name)
        
        conditions = []
        
        if numeric_columns:
            col = self.rng.choice(numeric_columns)
            op = self.rng.choice(['>', '<', '>=', '<=', '=', '<>'])
            value = self.rng.randint(1, 1000)
            conditions.append(f"{col} {op} {value}")
        
        if text_columns and self.rng.random() < 0.3:
            col = self.rng.choice(text_columns)
            value = self.get_column_value(table, col)
            conditions.append(f"{col} = {value}")
        
        if bool_columns and self.rng.random() < 0.2:
            col = self.rng.choice(bool_columns)
            value = self.rng.choice(['true', 'false'])
            conditions.append(f"{col} = {value}")
        
        if not conditions:
            return "1=1"
        
        if len(conditions) == 1:
            return conditions[0]
        
        # Combine with AND/OR
        operator = self.rng.choice([' AND ', ' OR '])
        return operator.join(self.rng.sample(conditions, min(2, len(conditions))))
    
    def get_set_clause(self, table: str, exclude_columns: Optional[List[str]] = None) -> str:
        """Generate SET clause for UPDATE"""
        if table not in self.tables:
            return "status = 'updated'"
        
        table_meta = self.tables[table]
        exclude = exclude_columns or []
        
        # Don't update primary keys or system columns
        exclude.extend(['id', 'created_at'])
        if table_meta.primary_key:
            exclude.append(table_meta.primary_key)
        
        updateable_columns = [
            col for col in table_meta.columns.keys() 
            if col not in exclude
        ]
        
        if not updateable_columns:
            return "updated_at = CURRENT_TIMESTAMP"
        
        # Update 1-3 columns
        num_updates = min(self.rng.randint(1, 3), len(updateable_columns))
        columns_to_update = self.rng.sample(updateable_columns, num_updates)
        
        updates = []
        for col in columns_to_update:
            value = self.get_column_value(table, col)
            updates.append(f"{col} = {value}")
        
        return ', '.join(updates)
    
    def get_conflict_column(self, table: str) -> Optional[str]:
        """Get a column suitable for ON CONFLICT"""
        if table not in self.tables:
            return None
        
        table_meta = self.tables[table]
        
        # Prefer primary key
        if table_meta.primary_key:
            return table_meta.primary_key
        
        # Then unique columns
        if table_meta.unique_columns:
            return self.rng.choice(table_meta.unique_columns)
        
        return None
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()