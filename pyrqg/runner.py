"""
PyRQG Runner

Purpose:
- Provide a simple CLI to run grammars against a local PostgreSQL/YugabyteDB cluster
- Key feature: run ALL grammars and execute generated SQL against a target DSN
- Supports both YSQL (PostgreSQL-compatible) and YCQL (Cassandra-compatible) APIs
- Uses pluggable runner architecture for different database backends

Usage examples:
  # List grammars
  python -m pyrqg.runner list

  # List available database runners
  python -m pyrqg.runner runners

  # Run all grammars against local cluster (defaults to postgres on 5432)
  python -m pyrqg.runner all --count 10 --seed 42 --dsn "postgresql://postgres:postgres@localhost:5432/postgres"

  # Initialize a simple default schema before running
  python -m pyrqg.runner all --init-schema --count 5

  # Run a single grammar
  python -m pyrqg.runner grammar --grammar real_workload --count 100 --seed 123

  # Run YCQL grammar against YugabyteDB YCQL API
  python -m pyrqg.runner grammar --grammar yugabyte_ycql --count 100 --ycql-host localhost --ycql-port 9042

  # Specify a custom runner
  python -m pyrqg.runner grammar --grammar basic_crud --runner postgresql --count 50

Notes:
- Default DSN can be provided via env var PYRQG_DSN; CLI --dsn overrides it.
- YugabyteDB YSQL typically listens on 5433; YCQL on 9042.
- YCQL grammars require the cassandra-driver package.
"""
from __future__ import annotations

import os
import sys
import argparse
import logging
import time
from typing import List, Optional

logger = logging.getLogger(__name__)

try:
    import psycopg2  # type: ignore
    from psycopg2.extensions import connection as PGConnection
except ImportError:  # pragma: no cover - optional at import time for non-exec commands
    psycopg2 = None  # type: ignore
    PGConnection = None  # type: ignore

from pyrqg.dsl.core import Context
from pyrqg.api import RQG, create_rqg
from pyrqg.core.runners import (
    RunnerRegistry, RunnerConfig,
    PostgreSQLRunner, YSQLRunner, YCQL_AVAILABLE
)

if YCQL_AVAILABLE:
    from pyrqg.core.runners import YCQLRunner


def _connect(dsn: Optional[str]) -> PGConnection:
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed. Please install requirements.")
    dsn_effective = dsn or os.environ.get("PYRQG_DSN") or "postgresql://postgres:postgres@localhost:5432/postgres"
    return psycopg2.connect(dsn_effective)  # type: ignore


def _init_context(dsn: Optional[str], seed: Optional[int]) -> Optional[Context]:
    """Initialize a schema context for introspection.

    This is a helper to avoid duplicate context initialization code.
    Returns None if context cannot be initialized.
    """
    if not dsn:
        return None
    try:
        return Context(dsn=dsn, seed=seed)
    except Exception as e:
        logger.warning("Could not initialize schema context: %s", e)
        return None


def _get_runner_config(args: argparse.Namespace) -> RunnerConfig:
    """Build RunnerConfig from command line arguments."""
    dsn = args.dsn or os.environ.get("PYRQG_DSN")

    return RunnerConfig(
        dsn=dsn,
        host=getattr(args, 'ycql_host', None) or os.environ.get("YCQL_HOST", "localhost"),
        port=int(getattr(args, 'ycql_port', None) or os.environ.get("YCQL_PORT", "9042")),
        keyspace=getattr(args, 'ycql_keyspace', None) or os.environ.get("YCQL_KEYSPACE", "test_keyspace"),
        threads=getattr(args, 'threads', 10),
        continue_on_error=getattr(args, 'continue_on_error', True),
    )


def _get_grammar_target_api(rqg: RQG, grammar_name: str) -> str:
    """Get the target API for a grammar (ysql, ycql, or postgres)."""
    # Try to load the grammar if not already loaded
    if grammar_name not in rqg.grammars:
        rqg._loader.load_by_name(grammar_name)

    grammar = rqg.grammars.get(grammar_name)
    if grammar and hasattr(grammar, 'target_api'):
        return grammar.target_api

    # Try loading common YCQL grammar names by file
    ycql_grammars = ['sqlancer_ycql', 'yugabyte_ycql']
    if grammar_name in ycql_grammars:
        # Try loading yugabyte_ycql.py which exports as sqlancer_ycql
        try:
            from pathlib import Path
            grammars_dir = Path(__file__).parent.parent / "grammars"
            for filename in ['yugabyte_ycql.py', f'{grammar_name}.py']:
                file_path = grammars_dir.parent / "grammars" / filename
                if file_path.exists():
                    if rqg._loader.load_from_file(grammar_name, str(file_path)):
                        grammar = rqg.grammars.get(grammar_name)
                        if grammar and hasattr(grammar, 'target_api'):
                            return grammar.target_api
        except Exception:
            pass

    return 'ysql'  # Default to YSQL


def action_list(rqg: RQG, _args: argparse.Namespace) -> int:
    grammars = rqg.list_grammars()
    print("Available grammars (name: description):")
    for name in sorted(grammars.keys()):
        print(f"- {name}: {grammars[name]}")
    return 0


def action_runners(_rqg: RQG, _args: argparse.Namespace) -> int:
    """List available database runners."""
    runners = RunnerRegistry.list_runners()
    print("Available database runners:")
    for name in sorted(runners.keys()):
        print(f"- {name}: {runners[name]}")
    return 0


def action_grammar(rqg: RQG, args: argparse.Namespace) -> int:
    dsn = args.dsn or os.environ.get("PYRQG_DSN")

    # Check if this is a YCQL grammar
    target_api = _get_grammar_target_api(rqg, args.grammar)

    # Determine which runner to use
    runner_name = getattr(args, 'runner', None)
    if runner_name is None:
        # Auto-detect based on grammar target API
        if target_api == 'ycql':
            runner_name = 'ycql'
        elif target_api == 'ysql':
            runner_name = 'ysql'
        else:
            runner_name = 'postgresql'

    # Check if runner is available
    if runner_name == 'ycql' and not YCQL_AVAILABLE:
        logger.error(
            "YCQL runner requires cassandra-driver. "
            "Install it with: pip install cassandra-driver"
        )
        return 2

    # Build runner config
    config = _get_runner_config(args)

    # Initialize context for YSQL/PostgreSQL
    context = None
    if target_api != 'ycql':
        context = _init_context(dsn, args.seed)

    # Dry run / Output to file mode
    if not (args.execute or dsn):
        queries = rqg.generate_from_grammar(args.grammar, rule="query", count=args.count, seed=args.seed, context=context)

        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                for q in queries:
                    f.write(q.rstrip(";\n") + ";\n")
            print(f"Saved {args.count} queries to {args.output}")
        else:
            for q in queries:
                print(q.rstrip(";\n") + ";")
        return 0

    # Execution mode
    if runner_name in ('ysql', 'postgresql') and not dsn:
        logger.error("DSN is required for execution (use --dsn or PYRQG_DSN env var)")
        return 2

    print(f"--- Starting Fuzz Test on {args.grammar} with {args.count} queries ---")
    print(f"--- Using runner: {runner_name} ---")

    # Get runner and execute
    runner = RunnerRegistry.get(runner_name, config=config)

    # Generate queries
    queries = rqg.generate_from_grammar(
        args.grammar, rule="query", count=args.count, seed=args.seed, context=context
    )

    stats = runner.execute_queries(queries)

    return 0 if stats.failed == 0 or args.continue_on_error else 2


def _ensure_default_schema(conn: PGConnection, rqg: RQG) -> None:
    ddl = rqg.generate_ddl()
    with conn.cursor() as cur:
        for stmt in ddl:
            try:
                cur.execute(stmt)
            except Exception as e:
                logger.warning("Schema init error: %s", e)


def action_all(rqg: RQG, args: argparse.Namespace) -> int:
    start = time.time()
    grammars = sorted(rqg.list_grammars().keys())
    if not grammars:
        print("No grammars found.")
        return 0

    dsn = args.dsn or os.environ.get("PYRQG_DSN")
    if not dsn:
        logger.error("DSN is required for 'all' mode execution")
        return 2

    # Connect once for schema init
    conn = _connect(dsn)
    try:
        if args.init_schema:
            logger.info("Initializing default schema...")
            _ensure_default_schema(conn, rqg)
    finally:
        conn.close()

    # Initialize schema context for generation
    logger.info("Introspecting schema from %s...", dsn)
    context = _init_context(dsn, args.seed)
    if context:
        logger.info("Loaded %d tables.", len(context.tables))

    total_stats = {'ok': 0, 'err': 0}

    # Build base config
    config = _get_runner_config(args)

    for name in grammars:
        print(f"\n=== Running Grammar: {name} ===")
        target_api = _get_grammar_target_api(rqg, name)

        # Skip YCQL grammars in 'all' mode if YCQL not available
        if target_api == 'ycql' and not YCQL_AVAILABLE:
            print(f"Skipping {name} (YCQL runner not available)")
            continue

        try:
            # Get appropriate runner
            runner_name = 'ycql' if target_api == 'ycql' else 'ysql'
            runner = RunnerRegistry.get(runner_name, config=config)

            # Generate queries
            gen_context = context if target_api != 'ycql' else None
            queries = rqg.generate_from_grammar(
                name, count=args.count, seed=args.seed, context=gen_context
            )

            stats = runner.execute_queries(queries)
            total_stats['ok'] += stats.success
            total_stats['err'] += stats.failed
        except Exception as e:
            logger.error("Grammar '%s' failed to run: %s", name, e)
            if not args.continue_on_error:
                return 2

    dur = time.time() - start
    print(f"\nAll grammars done: total_ok={total_stats['ok']} total_err={total_stats['err']} time={dur:.2f}s")
    return 0 if total_stats['err'] == 0 or args.continue_on_error else 2


def action_ddl(rqg: RQG, args: argparse.Namespace) -> int:
    conn: Optional[PGConnection] = None
    try:
        if args.table:
            ddls = rqg.generate_random_table_ddl(args.table, args.num_columns, args.num_constraints)
        else:
            ddls = rqg.generate_complex_ddl(args.num_tables)

        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                for stmt in ddls:
                    f.write(stmt.rstrip(";\n") + ";\n")
            print(f"Saved DDL to {args.output}")
            return 0

        if args.dsn or os.environ.get("PYRQG_DSN") or args.execute:
            conn = _connect(args.dsn)
            conn.autocommit = True
            ok, err = 0, 0
            with conn.cursor() as cur:
                for stmt in ddls:
                    try:
                        cur.execute(stmt)
                        ok += 1
                    except Exception as e:
                        err += 1
                        print(f"[ERROR] {e}\nQuery: {stmt}", file=sys.stderr)
            print(f"Executed DDL: ok={ok} errors={err}")
            return 0 if err == 0 else 2

        for stmt in ddls:
            print(stmt.rstrip(";\n") + ";")
        return 0
    finally:
        if conn is not None:
            conn.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="pyrqg.runner", description="PyRQG Runner")

    # Parent parser for shared arguments
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("--dsn", dest="dsn", default=None, help="PostgreSQL/Yugabyte DSN (env PYRQG_DSN also supported)")
    parent_parser.add_argument("--continue-on-error", action="store_true", help="Do not stop on first error")
    parent_parser.add_argument("--verbose", action="store_true", help="Trace every executed query to stdout")
    parent_parser.add_argument("--log-errors", action="store_true", help="Trace only failing SQL statements")
    parent_parser.add_argument("--execute", action="store_true", help="Execute statements even if --dsn not provided (uses env/default)")
    parent_parser.add_argument("--error-log", default=None, help="Optional file to append failing SQL statements")
    parent_parser.add_argument("--seed", type=int, default=None, help="Base seed for deterministic generation")
    parent_parser.add_argument("--threads", type=int, default=10, help="Number of concurrent threads for execution")

    subparsers = p.add_subparsers(dest="mode", required=True)

    # list
    parser_list = subparsers.add_parser("list", help="List available grammars", parents=[parent_parser])

    # runners
    parser_runners = subparsers.add_parser("runners", help="List available database runners", parents=[parent_parser])

    # grammar
    parser_grammar = subparsers.add_parser("grammar", help="Generate queries from a specific grammar", parents=[parent_parser])
    parser_grammar.add_argument("--grammar", required=True, help="Grammar name to run")
    parser_grammar.add_argument("--count", type=int, default=100, help="Number of queries to generate/execute")
    parser_grammar.add_argument("--output", default=None, help="Output file to write queries instead of executing/printing")
    parser_grammar.add_argument("--runner", default=None, help="Database runner to use (postgresql, ysql, ycql)")
    # YCQL-specific arguments
    parser_grammar.add_argument("--ycql-host", default=None, help="YCQL host (env YCQL_HOST, default: localhost)")
    parser_grammar.add_argument("--ycql-port", type=int, default=None, help="YCQL port (env YCQL_PORT, default: 9042)")
    parser_grammar.add_argument("--ycql-keyspace", default=None, help="YCQL keyspace (env YCQL_KEYSPACE, default: test_keyspace)")

    # all
    parser_all = subparsers.add_parser("all", help="Run all grammars", parents=[parent_parser])
    parser_all.add_argument("--count", type=int, default=100, help="Number of queries per grammar")
    parser_all.add_argument("--init-schema", action="store_true", help="Initialize a basic default schema before running")

    # ddl
    parser_ddl = subparsers.add_parser("ddl", help="Generate or execute DDL", parents=[parent_parser])
    parser_ddl.add_argument("--num-tables", type=int, default=5, help="Number of tables for complex DDL mode")
    parser_ddl.add_argument("--table", type=str, default=None, help="Generate a single random table DDL with this name")
    parser_ddl.add_argument("--num-columns", type=int, default=None, help="Columns for single-table DDL")
    parser_ddl.add_argument("--num-constraints", type=int, default=None, help="Constraints for single-table DDL")
    parser_ddl.add_argument("--output", type=str, default=None, help="Write DDL to file instead of executing/printing")

    return p


def main(argv: Optional[List[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)

    rqg = create_rqg()

    if args.mode == "list":
        return action_list(rqg, args)
    elif args.mode == "runners":
        return action_runners(rqg, args)
    elif args.mode == "grammar":
        if not getattr(args, "grammar", None):
            logger.error("--grammar is required for 'grammar' mode")
            return 2
        return action_grammar(rqg, args)
    elif args.mode == "all":
        return action_all(rqg, args)
    elif args.mode == "ddl":
        return action_ddl(rqg, args)
    else:  # pragma: no cover
        logger.error("Unknown mode: %s", args.mode)
        return 2


if __name__ == "__main__":
    sys.exit(main())
