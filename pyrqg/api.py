"""
PyRQG Library API - Simple interface for query generation
"""

import os
import sys
import random
import importlib.util
from pathlib import Path
from typing import Dict, List, Optional, Any, Iterator
from dataclasses import dataclass, field

# Use relative imports instead of path manipulation
from pyrqg.dsl.core import Grammar
from pyrqg.ddl_generator import DDLGenerator, TableDefinition

# Optional Built-in Grammars (Loaded safely)
try:
    from grammars.ddl_focused import g as _builtin_ddl
except ImportError:
    _builtin_ddl = None

try:
    from grammars.real_workload import grammar as _builtin_real_workload
except ImportError:
    _builtin_real_workload = None


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
        if not cols:
            cols = list(all_col_names)
        if not cols:
            raise ValueError(f"Table '{table}' does not have any columns available for INSERT")
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
        if not updateable_cols:
            updateable_cols = list(all_col_names)
        if not updateable_cols:
            raise ValueError(f"Table '{table}' does not have any columns available for UPDATE")
        max_cols = min(3, len(updateable_cols))
        update_cols = self.rng.sample(updateable_cols, k=self.rng.randint(1, max_cols))
        
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
        if _builtin_ddl:
            self.grammars['ddl'] = _builtin_ddl
        else:
             print("[WARN] Builtin grammar 'ddl_focused' not found", file=sys.stderr)

        if _builtin_real_workload:
            self.grammars['real_workload'] = _builtin_real_workload
        else:
             print("[WARN] Builtin grammar 'real_workload' not found", file=sys.stderr)

    def _load_plugin_grammars(self):
        """Load user-provided grammars from environment variable PYRQG_GRAMMARS.
        
        Format: PYRQG_GRAMMARS="module.path1,module.path2,..."
        Each module must expose a top-level variable `g` (Grammar instance).
        """
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
                    print(f"[WARN] Grammar module '{module_path}' does not expose 'g'", file=sys.stderr)
            except Exception as e:
                print(f"[WARN] Failed to load plugin grammar module '{module_path}': {e}", file=sys.stderr)
    
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
        ddl_statements: List[str] = []

        # When no custom tables are registered, emit the rich sample schema that
        # powers the schema catalog so that CLI `--init-schema` creates every
        # table referenced by our grammars.
        if not self.tables:
            sample_tables = self.ddl_generator.generate_sample_tables()
            selected = sample_tables
            if tables:
                allowed = set(tables)
                selected = [table for table in sample_tables if table.name in allowed]
            for table_def in selected:
                ddl_statements.append(self.ddl_generator.generate_create_table(table_def))
                for index in table_def.indexes:
                    ddl_statements.append(
                        self.ddl_generator.generate_create_index(table_def.name, index)
                    )
            return ddl_statements

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
                                num_constraints: Optional[int] = None) -> List[str]:
        """Generate DDL statements for a single random table with constraints.

        Returns a list of individual SQL statements rather than a single
        concatenated string. This makes downstream execution more robust.
        """
        table_def = self.ddl_generator.generate_random_table(
            table_name, num_columns, num_constraints
        )
        ddl_statements = [self.ddl_generator.generate_create_table(table_def)]
        
        # Add indexes
        for index in table_def.indexes:
            ddl_statements.append(
                self.ddl_generator.generate_create_index(table_name, index)
            )
        return ddl_statements
    
    def list_grammars(self) -> Dict[str, str]:
        """List all available grammars with descriptions"""
        descriptions = {
            'ddl': 'Complex PostgreSQL DDL statements',
            'real_workload': 'Simplified real-world analytics workload',
        }
        return {name: descriptions.get(name, 'Custom grammar') 
                for name in self.grammars.keys()}
    
    def add_grammar(self, name: str, grammar):
        """Add a custom grammar"""
        self.grammars[name] = grammar
    
    def load_grammar_file(self, name: str, file_path: str):
        """Load a grammar from a Python file"""
        spec = importlib.util.spec_from_file_location(f"grammar_{name}", file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        if hasattr(module, 'g'):
            self.grammars[name] = module.g
        else:
            raise ValueError(f"No grammar 'g' found in {file_path}")
    
    def generate_from_grammar(self, grammar_name: str, rule: str = "query", 
                            count: int = 1, seed: Optional[int] = None, 
                            context: Any = None) -> Iterator[str]:
        """Generate queries from a grammar.
        
        Logic flows as follows:
        1. Check if grammar is already loaded/registered.
        2. If not, try dynamic import treating 'grammar_name' as a module path.
        3. If that fails, try looking for the file in the repository's 'grammars/' directory.
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
        
        for i in range(count):
            query_seed = seed + i if seed is not None else None
            yield grammar.generate(rule, seed=query_seed, context=context)

    def generate(self, grammar: Optional[str] = None, rule: str = "query", count: int = 1,
                 seed: Optional[int] = None, context: Any = None) -> List[str]:
        """Convenience wrapper to generate queries. 
        
        - grammar: If None, uses the first registered grammar.
        - rule: Grammar rule to generate from (default: 'query').
        - count: Number of queries.
        - seed: Optional base seed; each query increments seed deterministically.
        """
        if grammar is None:
            grammar = next(iter(self.grammars.keys()))
        return self.generate_from_grammar(grammar, rule=rule, count=count, seed=seed, context=context)

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
        """Generate random ALTER TABLE statements using the DDL grammar."""
        if 'ddl' not in self.grammars:
            return []
        base_seed = seed or random.randint(1, 1_000_000)
        return self.generate_from_grammar('ddl', rule='query', count=constraints + functions, seed=base_seed)

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
        """Generate a mixed workload using the simple QueryGenerator fallback."""
        out: List[str] = []
        rng = random.Random(seed)
        qg = self.create_generator(seed=seed)
        for _ in range(count):
            mode = rng.choice(['select', 'insert', 'update', 'delete'])
            if mode == 'select' and include_selects:
                out.append(qg.select().sql)
            elif mode == 'insert' and include_inserts:
                out.append(qg.insert().sql)
            elif mode == 'update' and include_updates:
                out.append(qg.update().sql)
            elif mode == 'delete' and include_deletes:
                out.append(qg.delete().sql)
            else:
                out.append(qg.select().sql)
        return out

# Convenience function
def create_rqg() -> RQG:
    """Create a new RQG instance"""
    return RQG()