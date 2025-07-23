#!/usr/bin/env python3
"""
Workload Generator - Generate database workloads using focused grammars
Supports creating tables and running specific query patterns
"""

import sys
import argparse
import json
import time
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass

sys.path.insert(0, str(Path(__file__).parent))

from pyrqg.api import RQG, TableMetadata, create_rqg

@dataclass
class WorkloadConfig:
    """Configuration for workload generation"""
    duration: int = 60  # seconds
    queries_per_second: int = 10
    query_distribution: Dict[str, float] = None
    table_count: int = 5
    seed: Optional[int] = None
    
    def __post_init__(self):
        if self.query_distribution is None:
            self.query_distribution = {
                'select': 0.5,
                'insert': 0.2,
                'update': 0.15,
                'delete': 0.05,
                'upsert': 0.1
            }

class WorkloadGenerator:
    """Generate database workloads with specific patterns"""
    
    def __init__(self, config: WorkloadConfig = None):
        self.config = config or WorkloadConfig()
        self.rqg = create_rqg()
        self._load_workload_grammars()
        self.tables = []
        self.generated_ddl = []
        self.generated_queries = []
    
    def _load_workload_grammars(self):
        """Load all workload-specific grammars"""
        grammar_dir = Path(__file__).parent / "grammars" / "workload"
        
        grammars = {
            'insert': 'insert_focused.py',
            'update': 'update_focused.py',
            'delete': 'delete_focused.py',
            'upsert': 'upsert_focused.py',
            'select': 'select_focused.py'
        }
        
        for name, filename in grammars.items():
            grammar_path = grammar_dir / filename
            if grammar_path.exists():
                try:
                    self.rqg.load_grammar_file(f'workload_{name}', str(grammar_path))
                except Exception as e:
                    print(f"Warning: Could not load {name} grammar: {e}")
    
    def generate_tables(self) -> List[TableMetadata]:
        """Generate random table definitions"""
        import random
        rng = random.Random(self.config.seed)
        
        table_templates = [
            # User-related tables
            {
                'name': 'users',
                'columns': [
                    {'name': 'id', 'type': 'SERIAL'},
                    {'name': 'email', 'type': 'VARCHAR(255)'},
                    {'name': 'username', 'type': 'VARCHAR(100)'},
                    {'name': 'name', 'type': 'VARCHAR(255)'},
                    {'name': 'status', 'type': 'VARCHAR(50)'},
                    {'name': 'created_at', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'},
                    {'name': 'updated_at', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'}
                ],
                'primary_key': 'id',
                'unique_keys': ['email', 'username']
            },
            # Product tables
            {
                'name': 'products',
                'columns': [
                    {'name': 'id', 'type': 'SERIAL'},
                    {'name': 'product_code', 'type': 'VARCHAR(50)'},
                    {'name': 'name', 'type': 'VARCHAR(255)'},
                    {'name': 'description', 'type': 'TEXT'},
                    {'name': 'price', 'type': 'DECIMAL(10,2)'},
                    {'name': 'quantity', 'type': 'INTEGER'},
                    {'name': 'status', 'type': 'VARCHAR(50)'},
                    {'name': 'created_at', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'}
                ],
                'primary_key': 'id',
                'unique_keys': ['product_code']
            },
            # Order tables
            {
                'name': 'orders',
                'columns': [
                    {'name': 'id', 'type': 'SERIAL'},
                    {'name': 'order_id', 'type': 'VARCHAR(50)'},
                    {'name': 'user_id', 'type': 'INTEGER'},
                    {'name': 'product_id', 'type': 'INTEGER'},
                    {'name': 'quantity', 'type': 'INTEGER'},
                    {'name': 'total_amount', 'type': 'DECIMAL(10,2)'},
                    {'name': 'status', 'type': 'VARCHAR(50)'},
                    {'name': 'order_date', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'}
                ],
                'primary_key': 'id',
                'unique_keys': ['order_id'],
                'foreign_keys': {
                    'user_id': 'users(id)',
                    'product_id': 'products(id)'
                }
            },
            # Inventory
            {
                'name': 'inventory',
                'columns': [
                    {'name': 'id', 'type': 'SERIAL'},
                    {'name': 'product_id', 'type': 'INTEGER'},
                    {'name': 'quantity', 'type': 'INTEGER'},
                    {'name': 'location', 'type': 'VARCHAR(100)'},
                    {'name': 'last_updated', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'}
                ],
                'primary_key': 'id',
                'foreign_keys': {'product_id': 'products(id)'}
            },
            # Transactions
            {
                'name': 'transactions',
                'columns': [
                    {'name': 'id', 'type': 'SERIAL'},
                    {'name': 'transaction_id', 'type': 'VARCHAR(50)'},
                    {'name': 'user_id', 'type': 'INTEGER'},
                    {'name': 'amount', 'type': 'DECIMAL(10,2)'},
                    {'name': 'transaction_type', 'type': 'VARCHAR(50)'},
                    {'name': 'status', 'type': 'VARCHAR(50)'},
                    {'name': 'timestamp', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'}
                ],
                'primary_key': 'id',
                'unique_keys': ['transaction_id']
            },
            # Sessions
            {
                'name': 'sessions',
                'columns': [
                    {'name': 'id', 'type': 'SERIAL'},
                    {'name': 'session_id', 'type': 'VARCHAR(100)'},
                    {'name': 'user_id', 'type': 'INTEGER'},
                    {'name': 'data', 'type': 'JSONB'},
                    {'name': 'last_accessed', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'}
                ],
                'primary_key': 'id',
                'unique_keys': ['session_id']
            },
            # Settings
            {
                'name': 'settings',
                'columns': [
                    {'name': 'id', 'type': 'SERIAL'},
                    {'name': 'user_id', 'type': 'INTEGER'},
                    {'name': 'setting_key', 'type': 'VARCHAR(100)'},
                    {'name': 'setting_value', 'type': 'TEXT'},
                    {'name': 'updated_at', 'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'}
                ],
                'primary_key': 'id',
                'unique_keys': ['(user_id, setting_key)']
            }
        ]
        
        # Select random tables
        selected = rng.sample(table_templates, min(self.config.table_count, len(table_templates)))
        
        # Convert to TableMetadata objects
        tables = []
        for template in selected:
            table = TableMetadata(
                name=template['name'],
                columns=template['columns'],
                primary_key=template.get('primary_key'),
                unique_keys=template.get('unique_keys', [])
            )
            if 'foreign_keys' in template:
                table.foreign_keys = template['foreign_keys']
            tables.append(table)
        
        self.tables = tables
        return tables
    
    def generate_ddl(self) -> List[str]:
        """Generate CREATE TABLE statements"""
        ddl_statements = []
        
        for table in self.tables:
            columns = []
            
            for col in table.columns:
                col_def = f"{col['name']} {col['type']}"
                if col['name'] == table.primary_key:
                    col_def += " PRIMARY KEY"
                columns.append(col_def)
            
            # Add unique constraints
            for unique_col in table.unique_keys:
                if unique_col.startswith('('):
                    columns.append(f"UNIQUE {unique_col}")
                else:
                    columns.append(f"UNIQUE ({unique_col})")
            
            # Add foreign keys
            if hasattr(table, 'foreign_keys'):
                for fk_col, fk_ref in table.foreign_keys.items():
                    columns.append(f"FOREIGN KEY ({fk_col}) REFERENCES {fk_ref}")
            
            ddl = f"CREATE TABLE IF NOT EXISTS {table.name} (\n  " + ",\n  ".join(columns) + "\n)"
            ddl_statements.append(ddl)
        
        # Add indexes
        for table in self.tables:
            # Index on foreign keys
            if hasattr(table, 'foreign_keys'):
                for fk_col in table.foreign_keys:
                    index = f"CREATE INDEX IF NOT EXISTS idx_{table.name}_{fk_col} ON {table.name}({fk_col})"
                    ddl_statements.append(index)
        
        self.generated_ddl = ddl_statements
        return ddl_statements
    
    def generate_workload(self) -> List[str]:
        """Generate workload queries based on distribution"""
        queries = []
        import random
        rng = random.Random(self.config.seed)
        
        # Calculate number of queries per type
        total_queries = self.config.duration * self.config.queries_per_second
        
        for query_type, percentage in self.config.query_distribution.items():
            count = int(total_queries * percentage)
            grammar_name = f'workload_{query_type}'
            
            if grammar_name in self.rqg.grammars:
                # Generate queries of this type
                for i in range(count):
                    seed = (self.config.seed or 0) + i + hash(query_type)
                    try:
                        query = self.rqg.generate_from_grammar(grammar_name, seed=seed)[0]
                        queries.append({
                            'type': query_type,
                            'sql': query,
                            'delay_ms': 1000 // self.config.queries_per_second
                        })
                    except Exception as e:
                        print(f"Error generating {query_type} query: {e}")
        
        # Shuffle queries for realistic workload
        rng.shuffle(queries)
        self.generated_queries = queries
        return queries
    
    def save_workload(self, filename: str):
        """Save workload to file"""
        workload = {
            'config': {
                'duration': self.config.duration,
                'queries_per_second': self.config.queries_per_second,
                'query_distribution': self.config.query_distribution,
                'table_count': self.config.table_count,
                'seed': self.config.seed
            },
            'ddl': self.generated_ddl,
            'queries': self.generated_queries,
            'tables': [
                {
                    'name': t.name,
                    'columns': t.columns,
                    'primary_key': t.primary_key,
                    'unique_keys': t.unique_keys
                }
                for t in self.tables
            ]
        }
        
        with open(filename, 'w') as f:
            json.dump(workload, f, indent=2)
    
    def print_summary(self):
        """Print workload summary"""
        print("\nWorkload Generation Summary")
        print("=" * 60)
        print(f"Tables: {len(self.tables)}")
        for table in self.tables:
            print(f"  - {table.name}: {len(table.columns)} columns")
        
        print(f"\nQueries: {len(self.generated_queries)}")
        query_counts = {}
        for q in self.generated_queries:
            query_counts[q['type']] = query_counts.get(q['type'], 0) + 1
        
        for qtype, count in sorted(query_counts.items()):
            percentage = count / len(self.generated_queries) * 100
            print(f"  - {qtype}: {count} ({percentage:.1f}%)")
        
        print(f"\nWorkload Duration: {self.config.duration} seconds")
        print(f"Queries per Second: {self.config.queries_per_second}")

def main():
    parser = argparse.ArgumentParser(
        description="Generate database workloads for testing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate a 60-second workload
  python workload_generator.py --duration 60
  
  # Custom query distribution
  python workload_generator.py --distribution select:0.7,insert:0.2,update:0.1
  
  # High-throughput workload
  python workload_generator.py --qps 100 --duration 300
  
  # Insert-heavy workload
  python workload_generator.py --distribution insert:0.6,select:0.3,upsert:0.1
  
  # Save workload to file
  python workload_generator.py --output workload.json
        """
    )
    
    parser.add_argument('--duration', type=int, default=60,
                       help='Workload duration in seconds')
    parser.add_argument('--qps', type=int, default=10,
                       help='Queries per second')
    parser.add_argument('--tables', type=int, default=5,
                       help='Number of tables to create')
    parser.add_argument('--distribution', type=str,
                       help='Query distribution (e.g., select:0.5,insert:0.3)')
    parser.add_argument('--seed', type=int,
                       help='Random seed for reproducibility')
    parser.add_argument('--output', type=str,
                       help='Save workload to JSON file')
    parser.add_argument('--print-ddl', action='store_true',
                       help='Print DDL statements')
    parser.add_argument('--print-queries', type=int, metavar='N',
                       help='Print first N queries')
    
    args = parser.parse_args()
    
    # Parse distribution if provided
    distribution = None
    if args.distribution:
        distribution = {}
        for part in args.distribution.split(','):
            qtype, weight = part.split(':')
            distribution[qtype] = float(weight)
    
    # Create config
    config = WorkloadConfig(
        duration=args.duration,
        queries_per_second=args.qps,
        table_count=args.tables,
        query_distribution=distribution,
        seed=args.seed
    )
    
    # Generate workload
    generator = WorkloadGenerator(config)
    
    print("Generating workload...")
    generator.generate_tables()
    generator.generate_ddl()
    generator.generate_workload()
    
    # Print DDL if requested
    if args.print_ddl:
        print("\nDDL Statements:")
        print("=" * 60)
        for ddl in generator.generated_ddl:
            print(ddl + ";")
            print()
    
    # Print sample queries if requested
    if args.print_queries:
        print(f"\nFirst {args.print_queries} queries:")
        print("=" * 60)
        for i, query in enumerate(generator.generated_queries[:args.print_queries]):
            print(f"{i+1}. [{query['type'].upper()}] {query['sql']}")
            print()
    
    # Save to file if requested
    if args.output:
        generator.save_workload(args.output)
        print(f"\nWorkload saved to: {args.output}")
    
    # Print summary
    generator.print_summary()

if __name__ == "__main__":
    main()