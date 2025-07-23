"""
PyRQG Library API - Simple interface for query generation
"""

import logging
import random
from typing import Dict, List, Optional, Any, Iterator

logger = logging.getLogger(__name__)

from pyrqg.dsl.core import Grammar, Context
from pyrqg.ddl_generator import DDLGenerator
from pyrqg.core.schema import Table
from pyrqg.core.grammar_loader import GrammarLoader


__all__ = [
    "Table",
    "RQG",
    "create_rqg",
]


class RQG:
    """Main PyRQG API - Random Query Generator

    Improvements focused on UX and extensibility:
    - Built-in and optional plugin-based grammar loading
    - Simple wrapper methods for common generation patterns
    - Clear error messages listing available grammars
    """

    def __init__(self):
        self._loader = GrammarLoader()
        self.tables = {}
        self.ddl_generator = DDLGenerator()
        self._load_builtin_grammars()
        self._loader.load_from_env()

    @property
    def grammars(self) -> Dict[str, Any]:
        """Access loaded grammars."""
        return self._loader.grammars

    def _load_builtin_grammars(self):
        """Load built-in grammars packaged with PyRQG."""
        builtins = {
            'ddl': 'ddl_focused',
            'real_workload': 'real_workload',
            'basic_crud': 'basic_crud'
        }
        for name, module in builtins.items():
            if not self._loader.load_by_name(module):
                logger.warning("Builtin grammar '%s' could not be loaded", module)
            else:
                # Register under short name if different
                if name != module and module in self._loader.grammars:
                    self._loader.grammars[name] = self._loader.grammars[module]
    
    def add_table(self, table: Table):
        """Add a table definition"""
        self.tables[table.name] = table
    
    def add_tables(self, tables: List[Table]):
        """Add multiple table definitions"""
        for table in tables:
            self.add_table(table)
    
    def generate_ddl(self, tables: Optional[List[str]] = None) -> List[str]:
        """Generate CREATE TABLE statements using simple DDL"""
        ddl_statements: List[str] = []

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
            # Use DDLGenerator logic even for externally added tables if possible,
            # but they might lack full constraints info.
            # For now, stick to the simple logic for legacy support or upgrade to DDLGenerator if compatible.
            # Since we unified the models, we can use DDLGenerator!
            ddl_statements.append(self.ddl_generator.generate_create_table(table))
            for index in table.indexes:
                ddl_statements.append(self.ddl_generator.generate_create_index(table.name, index))

        return ddl_statements

    def generate_complex_ddl(self, num_tables: int = 5,
                           include_constraints: bool = True,
                           include_indexes: bool = True) -> List[str]:
        """Generate complex DDL with constraints and indexes"""
        return self.ddl_generator.generate_schema(num_tables)
    
    def generate_random_table_ddl(self, table_name: str,
                                num_columns: Optional[int] = None,
                                num_constraints: Optional[int] = None) -> List[str]:
        """Generate DDL statements for a single random table with constraints."""
        table_def = self.ddl_generator.generate_random_table(
            table_name, num_columns, num_constraints
        )
        ddl_statements = [self.ddl_generator.generate_create_table(table_def)]
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
            'basic_crud': 'Simple SELECT/INSERT/UPDATE/DELETE operations',
        }
        return {name: descriptions.get(name, 'Custom grammar') 
                for name in self.grammars.keys()}
    
    def add_grammar(self, name: str, grammar):
        """Add a custom grammar"""
        self._loader.grammars[name] = grammar

    def load_grammar_file(self, name: str, file_path: str):
        """Load a grammar from a Python file"""
        if not self._loader.load_from_file(name, file_path):
            raise ValueError(f"No grammar 'g' found in {file_path}")
    
    def generate_from_grammar(self, grammar_name: str, rule: str = "query",
                            count: int = 1, seed: Optional[int] = None,
                            context: Any = None) -> Iterator[str]:
        """Generate queries from a grammar."""
        if grammar_name not in self.grammars:
            if not self._loader.load_by_name(grammar_name):
                available = ', '.join(sorted(self.grammars.keys()))
                raise ValueError(
                    f"Grammar '{grammar_name}' not found. Available: {available}"
                )

        grammar = self.grammars[grammar_name]

        for i in range(count):
            query_seed = seed + i if seed is not None else None
            yield grammar.generate(rule, seed=query_seed, context=context)

    def generate(self, grammar: Optional[str] = None, rule: str = "query", count: int = 1,
                 seed: Optional[int] = None, context: Any = None) -> List[str]:
        """Convenience wrapper to generate queries."""
        if grammar is None:
            grammar = next(iter(self.grammars.keys()))
        return list(self.generate_from_grammar(grammar, rule=rule, count=count, seed=seed, context=context))

    def generate_random_schema(self, num_tables: int = 5) -> List[str]:
        """Generate a random schema (tables with constraints and indexes)."""
        return self.generate_complex_ddl(num_tables=num_tables)

    def generate_random_constraints_and_functions(self, constraints: int = 10,
                                                  functions: int = 5,
                                                  include_procedures: bool = True,
                                                  seed: Optional[int] = None) -> List[str]:
        """Generate random ALTER TABLE statements using the DDL grammar."""
        if 'ddl' not in self.grammars:
            return []
        base_seed = seed or random.randint(1, 1_000_000)
        return list(self.generate_from_grammar('ddl', rule='query', count=constraints + functions, seed=base_seed))

    def generate_random_data_inserts(self, rows_per_table: int = 10, seed: Optional[int] = None,
                                     multi_row: bool = False, on_conflict: bool = False,
                                     returning: bool = False) -> List[str]:
        """Generate INSERT statements for current tables using basic_crud grammar."""
        if 'basic_crud' not in self.grammars:
            logger.warning("basic_crud grammar not available for inserts")
            return []
            
        inserts: List[str] = []
        table_names = list(self.tables.keys())
        if not table_names:
            return []
            
        rng = random.Random(seed)
        
        for t in table_names:
            # Create a context scoped to just this table to force the grammar to use it
            single_table_ctx = Context()
            single_table_ctx.tables = {t: self.tables[t]}
            
            for _ in range(rows_per_table):
                # Basic CRUD doesn't support multi_row/on_conflict options yet, 
                # but this preserves the basic functionality of generating inserts
                q = next(self.generate_from_grammar('basic_crud', rule='insert', count=1, 
                                                  seed=rng.randint(0, 1000000), 
                                                  context=single_table_ctx))
                inserts.append(q)
        return inserts

    def run_mixed_workload(self, count: int = 100, seed: Optional[int] = None,
                           include_functions: bool = True,
                           include_selects: bool = True,
                           include_inserts: bool = True,
                           include_updates: bool = True,
                           include_deletes: bool = True) -> List[str]:
        """Generate a mixed workload using the basic_crud grammar."""
        if 'basic_crud' not in self.grammars:
            logger.warning("basic_crud grammar not available for mixed workload")
            return []

        out: List[str] = []
        rng = random.Random(seed)
        # Use full context with all tables
        ctx = Context()
        ctx.tables = self.tables
        
        for _ in range(count):
            mode = rng.choice(['select', 'insert', 'update', 'delete'])
            rule = mode
            
            should_run = False
            if mode == 'select' and include_selects: should_run = True
            elif mode == 'insert' and include_inserts: should_run = True
            elif mode == 'update' and include_updates: should_run = True
            elif mode == 'delete' and include_deletes: should_run = True
            
            if should_run:
                q = next(self.generate_from_grammar('basic_crud', rule=rule, count=1, 
                                                  seed=rng.randint(0, 1000000), context=ctx))
                out.append(q)
            else:
                # Fallback to select if chosen mode is disabled
                q = next(self.generate_from_grammar('basic_crud', rule='select', count=1, 
                                                  seed=rng.randint(0, 1000000), context=ctx))
                out.append(q)
        return out


def create_rqg() -> RQG:
    """Create a new RQG instance"""
    return RQG()
