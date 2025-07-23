#!/usr/bin/env python3
"""
PyRQG CLI - Command Line Interface for Python Random Query Generator
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import List, Optional

from .core.engine import Engine, EngineConfig
from .core.validator import ValidatorRegistry
from .core.reporter import ReporterRegistry


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="PyRQG - Python Random Query Generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with a grammar file for 60 seconds
  pyrqg --grammar grammars/simple.py --duration 60
  
  # Run 1000 queries with specific validators
  pyrqg --grammar grammars/transactions.py --queries 1000 --validator transaction zero_sum
  
  # Connect to PostgreSQL and save report
  pyrqg --grammar grammars/yugabyte.py --dsn postgresql://user:pass@localhost/test --reporter file json
  
  # Dry run with verbose output
  pyrqg --grammar grammars/simple.py --duration 30 --debug
        """
    )
    
    # Required arguments
    parser.add_argument(
        '--grammar', '-g',
        required=True,
        help='Path to grammar file (Python DSL)'
    )
    
    # Execution mode
    execution_group = parser.add_mutually_exclusive_group()
    execution_group.add_argument(
        '--duration', '-d',
        type=int,
        default=300,
        help='Test duration in seconds (default: 300)'
    )
    execution_group.add_argument(
        '--queries', '-q',
        type=int,
        help='Number of queries to execute (overrides duration)'
    )
    
    # Database connection
    parser.add_argument(
        '--dsn',
        help='Database connection string (e.g., postgresql://user:pass@host/db)'
    )
    parser.add_argument(
        '--database',
        default='postgres',
        choices=['postgres', 'mysql', 'yugabyte'],
        help='Database type (default: postgres)'
    )
    
    # Test configuration
    parser.add_argument(
        '--threads', '-t',
        type=int,
        default=1,
        help='Number of threads (default: 1)'
    )
    parser.add_argument(
        '--seed', '-s',
        type=int,
        help='Random seed for reproducibility'
    )
    
    # Validators
    parser.add_argument(
        '--validator', '-v',
        nargs='+',
        choices=ValidatorRegistry.list(),
        default=['error_message'],
        help='Validators to use (default: error_message)'
    )
    parser.add_argument(
        '--list-validators',
        action='store_true',
        help='List available validators and exit'
    )
    
    # Reporters
    parser.add_argument(
        '--reporter', '-r',
        nargs='+',
        choices=ReporterRegistry.list(),
        default=['console'],
        help='Reporters to use (default: console)'
    )
    parser.add_argument(
        '--list-reporters',
        action='store_true',
        help='List available reporters and exit'
    )
    
    # Output options
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Minimize output'
    )
    
    return parser.parse_args(args)


def list_validators():
    """List available validators"""
    print("\nAvailable Validators:")
    print("-" * 40)
    validators = {
        'error_message': 'Check for unexpected error messages',
        'result_set': 'Validate result set consistency',
        'performance': 'Check for slow queries',
        'transaction': 'Validate transaction semantics',
        'replication': 'Check replication consistency',
        'zero_sum': 'Validate zero-sum operations'
    }
    
    for name in ValidatorRegistry.list():
        desc = validators.get(name, 'No description available')
        print(f"  {name:<20} {desc}")
    print()


def list_reporters():
    """List available reporters"""
    print("\nAvailable Reporters:")
    print("-" * 40)
    reporters = {
        'console': 'Output to console (stdout)',
        'file': 'Write detailed report to file',
        'json': 'Generate JSON report',
        'errors': 'Report only errors and issues'
    }
    
    for name in ReporterRegistry.list():
        desc = reporters.get(name, 'No description available')
        print(f"  {name:<20} {desc}")
    print()


def main(args: Optional[List[str]] = None) -> int:
    """Main CLI entry point"""
    parsed_args = parse_args(args)
    
    # Handle list commands
    if parsed_args.list_validators:
        list_validators()
        return 0
    
    if parsed_args.list_reporters:
        list_reporters()
        return 0
    
    # Validate grammar file
    grammar_path = Path(parsed_args.grammar)
    if not grammar_path.exists():
        print(f"Error: Grammar file not found: {grammar_path}")
        return 1
    
    # Configure logging
    if parsed_args.quiet:
        logging.basicConfig(level=logging.ERROR)
    elif parsed_args.debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format='%(levelname)s: %(message)s'
        )
    
    # Create engine configuration
    config = EngineConfig(
        duration=parsed_args.duration,
        queries=parsed_args.queries,
        threads=parsed_args.threads,
        seed=parsed_args.seed,
        dsn=parsed_args.dsn,
        database=parsed_args.database,
        validators=parsed_args.validator,
        reporters=parsed_args.reporter,
        grammar_file=str(grammar_path),
        debug=parsed_args.debug
    )
    
    # Create and run engine
    try:
        engine = Engine(config)
        stats = engine.run()
        
        # Return non-zero if there were failures
        if stats.queries_failed > 0:
            return 1
        return 0
        
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        return 130
    except Exception as e:
        if parsed_args.debug:
            logging.exception("Fatal error")
        else:
            print(f"Error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())