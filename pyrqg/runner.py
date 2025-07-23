"""
PyRQG Runner

Purpose:
- Provide a simple CLI to run grammars against a local PostgreSQL/YugabyteDB cluster
- Key feature: run ALL grammars and execute generated SQL against a target DSN

Usage examples:
  # List grammars
  python -m pyrqg.runner list

  # Run all grammars against local cluster (defaults to postgres on 5432)
  python -m pyrqg.runner all --count 10 --seed 42 --dsn "postgresql://postgres:postgres@localhost:5432/postgres"

  # Initialize a simple default schema before running
  python -m pyrqg.runner all --init-schema --count 5

  # Run a single grammar
  python -m pyrqg.runner grammar --grammar real_workload --count 100 --seed 123

Notes:
- Default DSN can be provided via env var PYRQG_DSN; CLI --dsn overrides it.
- YugabyteDB typically listens on 5433; just pass the correct DSN.
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
from pyrqg.core.execution import WorkloadExecutor


def _connect(dsn: Optional[str]) -> PGConnection:
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed. Please install requirements.")
    dsn_effective = dsn or os.environ.get("PYRQG_DSN") or "postgresql://postgres:postgres@localhost:5432/postgres"
    return psycopg2.connect(dsn_effective)  # type: ignore


def action_list(rqg: RQG, _args: argparse.Namespace) -> int:
    grammars = rqg.list_grammars()
    print("Available grammars (name: description):")
    for name in sorted(grammars.keys()):
        print(f"- {name}: {grammars[name]}")
    return 0


def action_grammar(rqg: RQG, args: argparse.Namespace) -> int:
    dsn = args.dsn or os.environ.get("PYRQG_DSN")
    context = None
    
    if dsn:
        try:
            # Introspection connection
            context = Context(dsn=dsn, seed=args.seed)
        except Exception as e:
            logger.warning("Could not initialize schema context: %s", e)

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
    if not dsn:
        logger.error("DSN is required for execution (use --dsn or PYRQG_DSN env var)")
        return 2

    print(f"--- Starting Fuzz Test on {args.grammar} with {args.count} queries ---")
    executor = WorkloadExecutor(dsn, threads=args.threads)
    stats = executor.run(rqg, args.grammar, count=args.count, seed=args.seed, context=context)
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
    context = None
    try:
        logger.info("Introspecting schema from %s...", dsn)
        context = Context(dsn=dsn, seed=args.seed)
        logger.info("Loaded %d tables.", len(context.tables))
    except Exception as e:
        logger.warning("Could not initialize schema context: %s", e)

    total_stats = {'ok': 0, 'err': 0}
    
    executor = WorkloadExecutor(dsn, threads=args.threads)

    for name in grammars:
        print(f"\n=== Running Grammar: {name} ===")
        try:
            stats = executor.run(rqg, name, count=args.count, seed=args.seed, context=context)
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
    # ... (existing DDL logic is fine, keeping it simple for now)
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

    # grammar
    parser_grammar = subparsers.add_parser("grammar", help="Generate queries from a specific grammar", parents=[parent_parser])
    parser_grammar.add_argument("--grammar", required=True, help="Grammar name to run")
    parser_grammar.add_argument("--count", type=int, default=100, help="Number of queries to generate/execute")
    parser_grammar.add_argument("--output", default=None, help="Output file to write queries instead of executing/printing")

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