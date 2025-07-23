#!/usr/bin/env python3
"""
PyRQG Universal Launcher
Simple command-line interface and library usage examples
"""

import argparse
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrqg.api import RQG, TableMetadata, create_rqg

def print_queries(queries, format="sql"):
    """Print queries in specified format"""
    if format == "sql":
        for i, query in enumerate(queries, 1):
            if hasattr(query, 'sql'):
                print(f"-- Query {i} ({query.query_type})")
                print(f"{query.sql};")
                if query.features:
                    print(f"-- Features: {', '.join(query.features)}")
                print()
            else:
                print(f"-- Query {i}")
                print(f"{query};")
                print()
    elif format == "json":
        output = []
        for query in queries:
            if hasattr(query, 'sql'):
                output.append({
                    "sql": query.sql,
                    "type": query.query_type,
                    "tables": query.tables,
                    "features": query.features
                })
            else:
                output.append({"sql": query})
        print(json.dumps(output, indent=2))

def main():
    parser = argparse.ArgumentParser(
        description="PyRQG - Python Random Query Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate 10 random DML queries
  python rqg.py generate --count 10
  
  # Generate only SELECT queries
  python rqg.py generate --types SELECT --count 5
  
  # Generate queries from a specific grammar
  python rqg.py grammar --name dml --count 10
  
  # Generate DDL for tables
  python rqg.py ddl
  
  # Output in JSON format
  python rqg.py generate --count 5 --format json
  
  # Use specific seed for reproducibility
  python rqg.py generate --count 10 --seed 42
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Generate command
    gen_parser = subparsers.add_parser('generate', help='Generate random queries')
    gen_parser.add_argument('--count', type=int, default=10, help='Number of queries to generate')
    gen_parser.add_argument('--types', nargs='+', choices=['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
                           help='Query types to generate')
    gen_parser.add_argument('--seed', type=int, help='Random seed for reproducibility')
    gen_parser.add_argument('--format', choices=['sql', 'json'], default='sql',
                           help='Output format')
    
    # Grammar command
    gram_parser = subparsers.add_parser('grammar', help='Generate from grammar')
    gram_parser.add_argument('--name', required=True, help='Grammar name (dml, transactions)')
    gram_parser.add_argument('--count', type=int, default=10, help='Number of queries')
    gram_parser.add_argument('--rule', default='query', help='Grammar rule to use')
    gram_parser.add_argument('--seed', type=int, help='Random seed')
    gram_parser.add_argument('--format', choices=['sql', 'json'], default='sql',
                            help='Output format')
    
    # DDL command
    ddl_parser = subparsers.add_parser('ddl', help='Generate DDL statements')
    ddl_parser.add_argument('--tables', nargs='+', help='Specific tables to generate')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List available resources')
    list_parser.add_argument('what', choices=['grammars', 'tables'], 
                            help='What to list')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Initialize RQG
    rqg = create_rqg()
    
    if args.command == 'generate':
        # Generate random queries
        generator = rqg.create_generator(seed=args.seed)
        queries = generator.generate_batch(args.count, query_types=args.types)
        print_queries(queries, format=args.format)
    
    elif args.command == 'grammar':
        # Generate from grammar
        try:
            queries = rqg.generate_from_grammar(
                args.name, 
                rule=args.rule,
                count=args.count,
                seed=args.seed
            )
            print_queries(queries, format=args.format)
        except ValueError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
    
    elif args.command == 'ddl':
        # Generate DDL
        rqg._add_default_tables()  # Ensure default tables are loaded
        ddl_statements = rqg.generate_ddl(tables=args.tables)
        for ddl in ddl_statements:
            print(ddl + ";")
            print()
    
    elif args.command == 'list':
        if args.what == 'grammars':
            print("Available grammars:")
            grammars = rqg.list_grammars()
            for name, description in sorted(grammars.items()):
                print(f"  {name:.<25} {description}")
        elif args.what == 'tables':
            print("Default tables:")
            rqg._add_default_tables()
            for name, table in rqg.tables.items():
                print(f"  - {name}: {len(table.columns)} columns")

if __name__ == "__main__":
    main()