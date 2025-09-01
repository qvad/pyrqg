"""
PyRQG Library API - Simple interface for query generation
"""

import random
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

# Use relative imports instead of path manipulation

from pyrqg.dsl.core import Grammar
from pyrqg.ddl_generator import DDLGenerator, TableDefinition

__all__ = [
    "TableMetadata",
    "GeneratedQuery",
    "QueryGenerator",
    "RQG",
    "create_rqg",
]

@dataclass
class TableMetadata:
    """Metadata for a database table"""
    name: str
    columns: List[Dict[str, Any]]
    primary_key: Optional[str] = None
    unique_keys: List[str] = field(default_factory=list)
    foreign_keys: Dict[str, str] = field(default_factory=dict)
    
    def get_column_names(self) -> List[str]:
        """Get list of column names"""
        return [col['name'] for col in self.columns]
    
    def get_numeric_columns(self) -> List[str]:
        """Get numeric column names"""
        return [col['name'] for col in self.columns 
                if col.get('type', '').lower() in ['integer', 'int', 'bigint', 'decimal', 'numeric', 'float', 'double']]
    
    def get_string_columns(self) -> List[str]:
        """Get string column names"""
        return [col['name'] for col in self.columns 
                if col.get('type', '').lower() in ['varchar', 'text', 'char', 'string']]

@dataclass
class GeneratedQuery:
    """A generated query with metadata"""
    sql: str
    query_type: str  # SELECT, INSERT, UPDATE, DELETE, etc.
    tables: List[str]
    complexity: str  # simple, medium, complex
    features: List[str] = field(default_factory=list)  # CTEs, subqueries, joins, etc.

class QueryGenerator:
    """Generate specific types of queries"""
    
    def __init__(self, tables: List[TableMetadata], seed: Optional[int] = None):
        self.tables = {t.name: t for t in tables}
        self.rng = random.Random(seed)
        self._query_id = 0
    
    def select(self, 
               tables: Optional[List[str]] = None,
               columns: Optional[List[str]] = None,
               where: bool = True,
               joins: bool = False,
               group_by: bool = False,
               order_by: bool = True,
               limit: bool = True) -> GeneratedQuery:
        """Generate a SELECT query"""
        tables = tables or [self.rng.choice(list(self.tables.keys()))]
        table = tables[0]
        table_meta = self.tables[table]
        
        # Precompute metadata lists to avoid repeated work
        all_col_names = table_meta.get_column_names()
        numeric_cols = table_meta.get_numeric_columns()

        # Select columns
        if not columns:
            columns = self.rng.sample(all_col_names, k=self.rng.randint(1, len(all_col_names)))
        
        query_parts = [f"SELECT {', '.join(columns)}", f"FROM {table}"]
        features = []
        
        # Add WHERE clause
        if where and numeric_cols:
            col = self.rng.choice(numeric_cols)
            value = self.rng.randint(1, 1000)
            query_parts.append(f"WHERE {col} > {value}")
        
        # Add GROUP BY
        if group_by and len(columns) > 1:
            group_cols = self.rng.sample(columns, k=self.rng.randint(1, len(columns)-1))
            query_parts.append(f"GROUP BY {', '.join(group_cols)}")
            features.append("GROUP BY")
        
        # Add ORDER BY
        if order_by:
            order_col = self.rng.choice(columns)
            order_dir = self.rng.choice(["ASC", "DESC"])
            query_parts.append(f"ORDER BY {order_col} {order_dir}")
        
        # Add LIMIT
        if limit:
            limit_val = self.rng.randint(10, 100)
            query_parts.append(f"LIMIT {limit_val}")
        
        sql = " ".join(query_parts)
        return GeneratedQuery(
            sql=sql,
            query_type="SELECT",
            tables=tables,
            complexity="simple" if not (group_by or joins) else "medium",
            features=features
        )
    
    def insert(self,
               table: Optional[str] = None,
               returning: bool = False,
               on_conflict: bool = False,
               multi_row: bool = False) -> GeneratedQuery:
        """Generate an INSERT query"""
        table = table or self.rng.choice(list(self.tables.keys()))
        table_meta = self.tables[table]
        
        # Precompute metadata lists
        all_col_names = table_meta.get_column_names()
        numeric_cols = set(table_meta.get_numeric_columns())

        # Select columns to insert (exclude auto-generated like 'id')
        cols = [c for c in all_col_names if c not in ['id', 'created_at', 'updated_at']]
        insert_cols = self.rng.sample(cols, k=self.rng.randint(1, len(cols)))
        
        # Generate values
        values = []
        for col in insert_cols:
            if col in numeric_cols:
                values.append(str(self.rng.randint(1, 1000)))
            else:
                self._query_id += 1
                values.append(f"'value_{self._query_id}'")
        
        query_parts = [f"INSERT INTO {table} ({', '.join(insert_cols)})"]
        features = []
        
        if multi_row:
            # Generate 2-5 rows
            all_values = []
            for _ in range(self.rng.randint(2, 5)):
                row_values = []
                for col in insert_cols:
                    if col in numeric_cols:
                        row_values.append(str(self.rng.randint(1, 1000)))
                    else:
                        self._query_id += 1
                        row_values.append(f"'value_{self._query_id}'")
                all_values.append(f"({', '.join(row_values)})")
            query_parts.append(f"VALUES {', '.join(all_values)}")
            features.append("multi-row")
        else:
            query_parts.append(f"VALUES ({', '.join(values)})")
        
        # Add ON CONFLICT
        if on_conflict and table_meta.unique_keys:
            conflict_col = self.rng.choice(table_meta.unique_keys)
            if self.rng.choice([True, False]):
                query_parts.append(f"ON CONFLICT ({conflict_col}) DO NOTHING")
            else:
                update_cols = [c for c in insert_cols if c != conflict_col]
                if update_cols:
                    updates = [f"{c} = EXCLUDED.{c}" for c in update_cols[:2]]
                    query_parts.append(f"ON CONFLICT ({conflict_col}) DO UPDATE SET {', '.join(updates)}")
            features.append("ON CONFLICT")
        
        # Add RETURNING
        if returning:
            query_parts.append("RETURNING *")
            features.append("RETURNING")
        
        sql = " ".join(query_parts)
        return GeneratedQuery(
            sql=sql,
            query_type="INSERT",
            tables=[table],
            complexity="simple" if not (on_conflict or multi_row) else "medium",
            features=features
        )
    
    def update(self,
               table: Optional[str] = None,
               where: bool = True,
               returning: bool = False,
               from_clause: bool = False) -> GeneratedQuery:
        """Generate an UPDATE query"""
        table = table or self.rng.choice(list(self.tables.keys()))
        table_meta = self.tables[table]
        
        # Precompute metadata
        all_col_names = table_meta.get_column_names()
        numeric_cols = set(table_meta.get_numeric_columns())

        # Select columns to update
        updateable_cols = [c for c in all_col_names if c not in ['id', 'created_at']]
        update_cols = self.rng.sample(updateable_cols, k=self.rng.randint(1, min(3, len(updateable_cols))))
        
        # Generate SET clause
        set_parts = []
        for col in update_cols:
            if col in numeric_cols:
                if self.rng.choice([True, False]):
                    set_parts.append(f"{col} = {col} + {self.rng.randint(1, 100)}")
                else:
                    set_parts.append(f"{col} = {self.rng.randint(1, 1000)}")
            else:
                self._query_id += 1
                set_parts.append(f"{col} = 'updated_{self._query_id}'")
        
        query_parts = [f"UPDATE {table}", f"SET {', '.join(set_parts)}"]
        features = []
        
        # Add WHERE clause
        if where:
            if table_meta.primary_key:
                query_parts.append(f"WHERE {table_meta.primary_key} = {self.rng.randint(1, 100)}")
            elif numeric_cols:
                col = self.rng.choice(list(numeric_cols))
                query_parts.append(f"WHERE {col} > {self.rng.randint(1, 500)}")
        
        # Add RETURNING
        if returning:
            query_parts.append("RETURNING *")
            features.append("RETURNING")
        
        sql = " ".join(query_parts)
        return GeneratedQuery(
            sql=sql,
            query_type="UPDATE",
            tables=[table],
            complexity="simple",
            features=features
        )
    
    def delete(self,
               table: Optional[str] = None,
               where: bool = True,
               returning: bool = False) -> GeneratedQuery:
        """Generate a DELETE query"""
        table = table or self.rng.choice(list(self.tables.keys()))
        table_meta = self.tables[table]
        
        query_parts = [f"DELETE FROM {table}"]
        features = []
        
        # Add WHERE clause (always recommended for DELETE)
        if where:
            numeric_cols = table_meta.get_numeric_columns()
            if numeric_cols:
                col = self.rng.choice(numeric_cols)
                query_parts.append(f"WHERE {col} < {self.rng.randint(1, 100)}")
            elif table_meta.primary_key:
                query_parts.append(f"WHERE {table_meta.primary_key} = {self.rng.randint(1, 100)}")
        
        # Add RETURNING
        if returning:
            query_parts.append("RETURNING *")
            features.append("RETURNING")
        
        sql = " ".join(query_parts)
        return GeneratedQuery(
            sql=sql,
            query_type="DELETE",
            tables=[table],
            complexity="simple",
            features=features
        )
    
    def generate_batch(self, count: int, 
                      query_types: Optional[List[str]] = None) -> List[GeneratedQuery]:
        """Generate a batch of random queries"""
        if not query_types:
            query_types = ["SELECT", "INSERT", "UPDATE", "DELETE"]
        
        queries = []
        for _ in range(count):
            query_type = self.rng.choice(query_types)
            
            if query_type == "SELECT":
                query = self.select(
                    where=self.rng.choice([True, False]),
                    group_by=self.rng.choice([True, False]),
                    order_by=self.rng.choice([True, False])
                )
            elif query_type == "INSERT":
                query = self.insert(
                    returning=self.rng.choice([True, False]),
                    on_conflict=self.rng.choice([True, False]),
                    multi_row=self.rng.choice([True, False])
                )
            elif query_type == "UPDATE":
                query = self.update(
                    returning=self.rng.choice([True, False])
                )
            else:  # DELETE
                query = self.delete(
                    returning=self.rng.choice([True, False])
                )
            
            queries.append(query)
        
        return queries

class RQG:
    """Main PyRQG API - Random Query Generator

    Improvements focused on UX and extensibility:
    - Built-in and optional plugin-based grammar loading
    - Simple wrapper methods for common generation patterns
    - Clear error messages listing available grammars
    """
    
    def __init__(self):
        self.grammars = {}
        self.tables = {}
        self.ddl_generator = DDLGenerator()
        self._load_builtin_grammars()
        self._load_plugin_grammars()
    
    def _load_builtin_grammars(self):
        """Load built-in grammars packaged with PyRQG."""
        
        def _add_alias(alias: str, gobj):
            if alias and alias not in self.grammars and gobj is not None:
                self.grammars[alias] = gobj
        
        # DML grammars
        try:
            from grammars.dml_unique import g as dml_unique
            self.grammars['dml_unique'] = dml_unique
            # Path-style alias (same at top level)
            _add_alias('dml_unique', dml_unique)
        except Exception:
            pass
        
        try:
            from grammars.dml_yugabyte import g as dml_yugabyte
            self.grammars['dml_yugabyte'] = dml_yugabyte
            _add_alias('dml_yugabyte', dml_yugabyte)
        except Exception:
            pass
        
        try:
            from grammars.dml_fixed import g as dml_fixed
            self.grammars['dml_fixed'] = dml_fixed
            _add_alias('dml_fixed', dml_fixed)
        except Exception:
            pass
        
        # YugabyteDB grammars
        try:
            from grammars.yugabyte.transactions_postgres import g as txn_grammar
            self.grammars['yugabyte_transactions'] = txn_grammar
            _add_alias('yugabyte/transactions_postgres', txn_grammar)
        except Exception:
            pass
        
        try:
            from grammars.yugabyte.optimizer_subquery_portable import g as subquery_grammar
            self.grammars['yugabyte_subquery'] = subquery_grammar
            _add_alias('yugabyte/optimizer_subquery_portable', subquery_grammar)
        except Exception:
            pass
        
        try:
            from grammars.yugabyte.outer_join_portable import g as outer_join_grammar
            self.grammars['yugabyte_outer_join'] = outer_join_grammar
            _add_alias('yugabyte/outer_join_portable', outer_join_grammar)
        except Exception:
            pass
        
        # Workload-specific grammars
        workload_grammars = [
            ('workload_insert', 'grammars.workload.insert_focused'),
            ('workload_update', 'grammars.workload.update_focused'),
            ('workload_delete', 'grammars.workload.delete_focused'),
            ('workload_upsert', 'grammars.workload.upsert_focused'),
            ('workload_select', 'grammars.workload.select_focused'),
            ('ddl_focused', 'grammars.ddl_focused'),
            ('functions_ddl', 'grammars.functions_ddl'),
            ('dml_with_functions', 'grammars.dml_with_functions'),
            # New comprehensive test grammars
            ('merge_statement', 'grammars.merge_statement'),
            ('security_testing', 'grammars.security_testing'),
            ('data_integrity_testing', 'grammars.data_integrity_testing'),
            ('concurrent_isolation_testing', 'grammars.concurrent_isolation_testing'),
            ('performance_edge_cases', 'grammars.performance_edge_cases')
        ]
        
        for name, module_path in workload_grammars:
            try:
                module = __import__(module_path, fromlist=['g'])
                self.grammars[name] = module.g
                # Add a path-style alias derived from the module path
                path_alias = module_path.replace('grammars.', '').replace('.', '/')
                _add_alias(path_alias, module.g)
            except Exception:
                pass

        # Unified simple aliases for common categories
        try:
            from grammars.workload.select_focused import g as _selects
            _add_alias('selects', _selects)
        except Exception:
            pass
        try:
            from grammars.workload.update_focused import g as _updates
            _add_alias('updates', _updates)
        except Exception:
            pass
        try:
            from grammars.workload.insert_focused import g as _inserts
            _add_alias('inserts', _inserts)
        except Exception:
            pass
        try:
            from grammars.functions_ddl import g as _functions
            _add_alias('functions', _functions)
        except Exception:
            pass
        try:
            from grammars.dml_with_functions import g as _functions_in_dml
            _add_alias('functions_in_dml', _functions_in_dml)
        except Exception:
            pass
        # New top-level separated grammars by syntax
        try:
            from grammars.update import g as _update_only
            _add_alias('update', _update_only)
        except Exception:
            pass
        try:
            from grammars.delete import g as _delete_only
            _add_alias('delete', _delete_only)
        except Exception:
            pass
        
        # Alias for backward compatibility
        if 'dml_unique' in self.grammars:
            self.grammars['dml'] = self.grammars['dml_unique']
        if 'yugabyte_transactions' in self.grammars:
            self.grammars['transactions'] = self.grammars['yugabyte_transactions']

    def _load_plugin_grammars(self):
        """Load user-provided grammars from environment variable PYRQG_GRAMMARS.
        
        Format: PYRQG_GRAMMARS="module.path1,module.path2,..."
        Each module must expose a top-level variable `g` (Grammar instance).
        Names are inferred from the module basename unless duplicated, where
        a suffix is added.
        """
        import os
        value = os.environ.get("PYRQG_GRAMMARS")
        if not value:
            return
        for module_path in [p.strip() for p in value.split(',') if p.strip()]:
            try:
                module = __import__(module_path, fromlist=['g'])
                if hasattr(module, 'g'):
                    base_name = module_path.split('.')[-1]
                    name = base_name
                    i = 2
                    while name in self.grammars:
                        name = f"{base_name}_{i}"
                        i += 1
                    self.grammars[name] = getattr(module, 'g')
                else:
                    # silently ignore modules without `g` to avoid breaking env-based runs
                    pass
            except Exception:
                # ignore plugin import errors to keep base functionality robust
                pass
    
    def add_table(self, table: TableMetadata):
        """Add a table definition"""
        self.tables[table.name] = table
    
    def add_tables(self, tables: List[TableMetadata]):
        """Add multiple table definitions"""
        for table in tables:
            self.add_table(table)
    
    def create_generator(self, seed: Optional[int] = None) -> QueryGenerator:
        """Create a query generator with registered tables"""
        if not self.tables:
            # Add default tables if none specified
            self._add_default_tables()
        return QueryGenerator(list(self.tables.values()), seed)
    
    def _add_default_tables(self):
        """Add default table definitions"""
        default_tables = [
            TableMetadata(
                name="users",
                columns=[
                    {"name": "id", "type": "integer"},
                    {"name": "email", "type": "varchar"},
                    {"name": "name", "type": "varchar"},
                    {"name": "age", "type": "integer"},
                    {"name": "status", "type": "varchar"},
                    {"name": "created_at", "type": "timestamp"}
                ],
                primary_key="id",
                unique_keys=["email"]
            ),
            TableMetadata(
                name="products",
                columns=[
                    {"name": "id", "type": "integer"},
                    {"name": "product_id", "type": "varchar"},
                    {"name": "name", "type": "varchar"},
                    {"name": "price", "type": "decimal"},
                    {"name": "quantity", "type": "integer"},
                    {"name": "category", "type": "varchar"}
                ],
                primary_key="id",
                unique_keys=["product_id"]
            ),
            TableMetadata(
                name="orders",
                columns=[
                    {"name": "id", "type": "integer"},
                    {"name": "order_id", "type": "varchar"},
                    {"name": "user_id", "type": "integer"},
                    {"name": "product_id", "type": "integer"},
                    {"name": "quantity", "type": "integer"},
                    {"name": "total", "type": "decimal"},
                    {"name": "status", "type": "varchar"}
                ],
                primary_key="id",
                unique_keys=["order_id"],
                foreign_keys={"user_id": "users.id", "product_id": "products.id"}
            )
        ]
        self.add_tables(default_tables)
    
    def generate_ddl(self, tables: Optional[List[str]] = None) -> List[str]:
        """Generate CREATE TABLE statements using simple DDL"""
        ddl_statements = []
        tables_to_generate = tables or list(self.tables.keys())
        
        for table_name in tables_to_generate:
            if table_name not in self.tables:
                continue
            
            table = self.tables[table_name]
            columns = []
            
            for col in table.columns:
                col_def = f"{col['name']} {col['type'].upper()}"
                if col['name'] == table.primary_key:
                    col_def += " PRIMARY KEY"
                elif col['name'] in table.unique_keys:
                    col_def += " UNIQUE"
                columns.append(col_def)
            
            # Add foreign keys
            for fk_col, fk_ref in table.foreign_keys.items():
                columns.append(f"FOREIGN KEY ({fk_col}) REFERENCES {fk_ref}")
            
            ddl = f"CREATE TABLE {table_name} (\n  " + ",\n  ".join(columns) + "\n)"
            ddl_statements.append(ddl)
        
        return ddl_statements
    
    def generate_complex_ddl(self, num_tables: int = 5, 
                           include_constraints: bool = True,
                           include_indexes: bool = True) -> List[str]:
        """Generate complex DDL with constraints and indexes"""
        return self.ddl_generator.generate_schema(num_tables)
    
    def generate_random_table_ddl(self, table_name: str,
                                num_columns: Optional[int] = None,
                                num_constraints: Optional[int] = None) -> str:
        """Generate DDL for a single random table with complex constraints"""
        table_def = self.ddl_generator.generate_random_table(
            table_name, num_columns, num_constraints
        )
        ddl_statements = [self.ddl_generator.generate_create_table(table_def)]
        
        # Add indexes
        for index in table_def.indexes:
            ddl_statements.append(
                self.ddl_generator.generate_create_index(table_name, index)
            )
        
        return ";\n".join(ddl_statements)
    
    def list_grammars(self) -> Dict[str, str]:
        """List all available grammars with descriptions"""
        descriptions = {
            # General DML grammars
            'dml_unique': 'Enhanced DML with 100% query uniqueness',
            'dml_yugabyte': 'YugabyteDB DML with ON CONFLICT, RETURNING, CTEs',
            'dml_fixed': 'Fixed DML grammar with basic features',
            
            # YugabyteDB specific
            'yugabyte_transactions': 'YugabyteDB transaction patterns',
            'yugabyte_subquery': 'Complex subqueries and optimizer tests',
            'yugabyte_outer_join': 'Outer join patterns for YugabyteDB',
            
            # Workload-focused grammars
            'workload_insert': 'INSERT-focused queries for workload testing',
            'workload_update': 'UPDATE-focused queries for workload testing',
            'workload_delete': 'DELETE-focused queries for workload testing',
            'workload_upsert': 'UPSERT/INSERT ON CONFLICT patterns',
            'workload_select': 'SELECT-focused queries with joins, subqueries',
            'ddl_focused': 'DDL-focused with complex constraints, indexes, views',
            'functions_ddl': 'Functions and procedures DDL operations',
            'dml_with_functions': 'DML queries using SQL functions',
            
            # Comprehensive test grammars
            'merge_statement': 'PostgreSQL 15+ MERGE statement testing',
            'security_testing': 'Security features: GRANT/REVOKE, roles, RLS',
            'data_integrity_testing': 'Constraint violations and data integrity edge cases',
            'concurrent_isolation_testing': 'Transaction isolation and concurrency testing',
            'performance_edge_cases': 'Query planner and performance boundary testing',
            
            # Aliases
            'dml': 'Alias for dml_unique',
            'transactions': 'Alias for yugabyte_transactions'
        }
        return {name: descriptions.get(name, 'Custom grammar') 
                for name in self.grammars.keys()}
    
    def add_grammar(self, name: str, grammar):
        """Add a custom grammar"""
        self.grammars[name] = grammar
    
    def load_grammar_file(self, name: str, file_path: str):
        """Load a grammar from a Python file"""
        import importlib.util
        spec = importlib.util.spec_from_file_location(f"grammar_{name}", file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        if hasattr(module, 'g'):
            self.grammars[name] = module.g
        else:
            raise ValueError(f"No grammar 'g' found in {file_path}")
    
    def generate_from_grammar(self, grammar_name: str, rule: str = "query", 
                            count: int = 1, seed: Optional[int] = None) -> List[str]:
        """Generate queries from a grammar.
        
        New: Supports path-style identifiers based on file/module path without .py,
        e.g., 'yugabyte/outer_join_portable' for grammars/yugabyte/outer_join_portable.py.
        If the grammar name is not preloaded, we attempt to import it dynamically
        from the 'grammars.' package using this mapping.
        Falls back to loading from the repository's grammars/ folder relative to this file
        when the import-based approach fails (useful in test environments where rootdir is tests/).
        """
        if grammar_name not in self.grammars:
            # Attempt dynamic import using path-style identifier mapping
            try:
                module_path = ("grammars." + grammar_name.replace('/', '.')).rstrip('.')
                module = __import__(module_path, fromlist=['g'])
                if hasattr(module, 'g'):
                    # Register under the provided identifier (path-style) for future use
                    self.grammars[grammar_name] = getattr(module, 'g')
                else:
                    # If no 'g' exposed, fall back to error
                    raise ImportError(f"Module '{module_path}' does not expose 'g'")
            except Exception:
                # Filesystem-based fallback: load from repo-root/grammars/<path>.py
                try:
                    import importlib.util
                    from pathlib import Path
                    base_dir = Path(__file__).parent.parent / "grammars"
                    rel_path = Path(*grammar_name.replace('.', '/').split('/'))
                    file_path = base_dir / (str(rel_path) + ".py")
                    if not file_path.exists():
                        # Also check for workload-style dotted names (e.g., workload.update_focused)
                        rel_path_alt = Path(*grammar_name.split('.'))
                        file_path_alt = base_dir / (str(rel_path_alt) + ".py")
                        if file_path_alt.exists():
                            file_path = file_path_alt
                    if file_path.exists():
                        spec = importlib.util.spec_from_file_location(f"grammar_{grammar_name.replace('/', '_').replace('.', '_')}", str(file_path))
                        module = importlib.util.module_from_spec(spec)
                        assert spec.loader is not None
                        spec.loader.exec_module(module)
                        if hasattr(module, 'g'):
                            self.grammars[grammar_name] = getattr(module, 'g')
                        else:
                            raise ImportError(f"File '{file_path}' does not expose 'g'")
                    else:
                        raise FileNotFoundError(str(file_path))
                except Exception:
                    available = ', '.join(sorted(self.grammars.keys()))
                    raise ValueError(
                        f"Grammar '{grammar_name}' not found. Available grammars: {available}. "
                        f"You can also specify a path-style identifier like 'yugabyte/outer_join_portable'."
                    )
        
        grammar = self.grammars[grammar_name]
        queries = []
        
        for i in range(count):
            query_seed = seed + i if seed is not None else None
            query = grammar.generate(rule, seed=query_seed)
            queries.append(query)
        
        return queries

    def generate(self, grammar: Optional[str] = None, rule: str = "query", count: int = 1,
                 seed: Optional[int] = None) -> List[str]:
        """Convenience wrapper to generate queries.
        
        - grammar: If None, uses 'dml_unique' when available, otherwise the first loaded.
        - rule: Grammar rule to generate from (default: 'query').
        - count: Number of queries.
        - seed: Optional base seed; each query increments seed deterministically.
        """
        if grammar is None:
            grammar = 'dml_unique' if 'dml_unique' in self.grammars else next(iter(self.grammars.keys()))
        return self.generate_from_grammar(grammar, rule=rule, count=count, seed=seed)

    # ================================
    # High-level integration API
    # ================================
    def generate_random_schema(self, num_tables: int = 5) -> List[str]:
        """Generate a random schema (tables with constraints and indexes).
        Uses the built-in DDLGenerator for rich schemas.
        """
        return self.generate_complex_ddl(num_tables=num_tables)

    def generate_random_constraints_and_functions(self, constraints: int = 10,
                                                  functions: int = 5,
                                                  include_procedures: bool = True,
                                                  seed: Optional[int] = None) -> List[str]:
        """Generate random ALTER TABLE constraints and function/procedure DDL.
        - constraints: number of ALTER/INDEX/DDL statements drawn from ddl_focused
        - functions: number of function/procedure statements from functions_ddl
        """
        out: List[str] = []
        base_seed = seed or random.randint(1, 1_000_000)
        # Constraints and miscellanous DDL
        if 'ddl_focused' in self.grammars:
            # Mix alter/index/view by just using the generic 'query' rule for variety
            out += self.generate_from_grammar('ddl_focused', rule='query', count=constraints, seed=base_seed)
        # Functions/procedures
        if 'functions_ddl' in self.grammars:
            func_seed = base_seed + 10_000
            # Alternate create_function and create_procedure when requested
            for i in range(functions):
                rule = 'create_procedure' if include_procedures and (i % 3 == 2) else 'create_function'
                out += self.generate_from_grammar('functions_ddl', rule=rule, count=1, seed=func_seed + i)
        return out

    def generate_random_data_inserts(self, rows_per_table: int = 10, seed: Optional[int] = None,
                                     multi_row: bool = False, on_conflict: bool = False,
                                     returning: bool = False) -> List[str]:
        """Generate INSERT statements for current tables using QueryGenerator.
        If no tables registered, default demo tables will be used.
        """
        gen = self.create_generator(seed=seed)
        inserts: List[str] = []
        table_names = list(self.tables.keys()) or []
        if not table_names:
            return []
        # Distribute rows per table
        for t in table_names:
            for _ in range(rows_per_table):
                q = gen.insert(table=t, multi_row=multi_row, on_conflict=on_conflict, returning=returning)
                inserts.append(q.sql)
        return inserts

    def run_mixed_workload(self, count: int = 100, seed: Optional[int] = None,
                           include_functions: bool = True,
                           include_selects: bool = True,
                           include_inserts: bool = True,
                           include_updates: bool = True,
                           include_deletes: bool = True) -> List[str]:
        """Generate a mixed workload of SELECT/INSERT/UPDATE/DELETE queries.
        - If include_functions is True, mixes in queries from dml_with_functions.
        - Otherwise, uses workload-focused grammars and QueryGenerator for diversity.
        """
        out: List[str] = []
        rng = random.Random(seed)
        base_seed = seed or rng.randint(1, 1_000_000)

        # Prefer grammar-driven generation for richer SQL
        sources: List[str] = []
        if include_functions and 'dml_with_functions' in self.grammars:
            sources.append('dml_with_functions')
        if include_selects and 'workload_select' in self.grammars:
            sources.append('workload_select')
        if include_inserts and 'workload_insert' in self.grammars:
            sources.append('workload_insert')
        if include_updates and 'workload_update' in self.grammars:
            sources.append('workload_update')
        if include_deletes and 'workload_delete' in self.grammars:
            sources.append('workload_delete')

        # Fallback: if no grammars, use simple QueryGenerator
        use_qg_fallback = len(sources) == 0
        qg = self.create_generator(seed=base_seed) if use_qg_fallback else None

        for i in range(count):
            if use_qg_fallback:
                choice = rng.choice(['select', 'insert', 'update', 'delete'])
                if choice == 'select' and include_selects:
                    out.append(qg.select().sql)
                elif choice == 'insert' and include_inserts:
                    out.append(qg.insert().sql)
                elif choice == 'update' and include_updates:
                    out.append(qg.update().sql)
                elif choice == 'delete' and include_deletes:
                    out.append(qg.delete().sql)
                else:
                    out.append(qg.select().sql)
            else:
                gname = rng.choice(sources)
                out += self.generate_from_grammar(gname, rule='query', count=1, seed=base_seed + i)
        return out

# Convenience function
def create_rqg() -> RQG:
    """Create a new RQG instance"""
    return RQG()