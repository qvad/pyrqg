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
  python -m pyrqg.runner grammar --grammar snowflake --count 100 --seed 123

Notes:
- Default DSN can be provided via env var PYRQG_DSN; CLI --dsn overrides it.
- YugabyteDB typically listens on 5433; just pass the correct DSN.
"""
from __future__ import annotations

import os
import sys
import argparse
import time
from pathlib import Path
from typing import List, Optional, Tuple, TextIO

try:
    import psycopg2  # type: ignore
    from psycopg2.extensions import connection as PGConnection
except Exception:  # pragma: no cover - optional at import time for non-exec commands
    psycopg2 = None  # type: ignore
    PGConnection = None  # type: ignore

from pyrqg.api import RQG, create_rqg


def _connect(dsn: Optional[str]) -> PGConnection:
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is not installed. Please install requirements.")
    dsn_effective = dsn or os.environ.get("PYRQG_DSN") or "postgresql://postgres:postgres@localhost:5432/postgres"
    return psycopg2.connect(dsn_effective)  # type: ignore


def _print_error(message: str, query: str, *, stream: TextIO) -> None:
    print(f"[ERROR] {message}\nWhile executing: {query}", file=stream)


def _exec_statements(conn: PGConnection, statements: List[str], continue_on_error: bool = True,
                     errors_only: bool = False, error_file: Optional[TextIO] = None) -> Tuple[int, int]:
    ok, err = 0, 0
    conn.autocommit = True
    with conn.cursor() as cur:
        for stmt in statements:
            sql = stmt.strip()
            if not sql:
                continue
            # Split on semicolons to avoid batching errors
            parts = [p.strip() for p in sql.split(';') if p.strip()]
            for p in parts:
                try:
                    cur.execute(p)
                    ok += 1
                except Exception as e:
                    err += 1
                    error_stream = sys.stdout if errors_only else sys.stderr
                    _print_error(str(e), p, stream=error_stream)
                    if error_file is not None:
                        _print_error(str(e), p, stream=error_file)
                    if not continue_on_error:
                        raise
    return ok, err


def action_list(rqg: RQG, _args: argparse.Namespace) -> int:
    grammars = rqg.list_grammars()
    print("Available grammars (name: description):")
    for name in sorted(grammars.keys()):
        print(f"- {name}: {grammars[name]}")
    return 0


def _generate_queries_for_grammar(rqg: RQG, grammar: str, count: int, seed: Optional[int]) -> List[str]:
    # Prefer 'query' rule; api uses 'query' by default as well
    return rqg.generate_from_grammar(grammar, rule="query", count=count, seed=seed)


def action_grammar(rqg: RQG, args: argparse.Namespace) -> int:
    queries = _generate_queries_for_grammar(rqg, args.grammar, args.count, args.seed)
    unique_queries = len({q.strip() for q in queries if q.strip()})
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            for q in queries:
                f.write(q.rstrip(";\n") + ";\n")
        print(f"Saved {len(queries)} queries to {args.output}")
        return 0

    if args.dsn or os.environ.get("PYRQG_DSN") or args.execute:
        # Execute generated queries
        error_handle: Optional[TextIO] = None
        conn = _connect(args.dsn)
        try:
            if args.error_log:
                log_path = Path(args.error_log)
                log_path.parent.mkdir(parents=True, exist_ok=True)
                error_handle = log_path.open('a', encoding='utf-8')
            ok, err = _exec_statements(
                conn,
                queries,
                continue_on_error=args.continue_on_error,
                errors_only=args.errors_only,
                error_file=error_handle,
            )
            print(
                "Executed grammar '{grammar}': ok={ok}, errors={err}, unique_queries={unique}".format(
                    grammar=args.grammar,
                    ok=ok,
                    err=err,
                    unique=unique_queries,
                )
            )
        finally:
            if error_handle is not None:
                error_handle.close()
            conn.close()
        return 0 if err == 0 or args.continue_on_error else 2

    # Default: print to stdout
    for q in queries:
        print(q.rstrip(";\n") + ";")
    return 0


def _ensure_default_schema(conn: PGConnection, rqg: RQG) -> None:
    ddl = rqg.generate_ddl()
    _exec_statements(conn, ddl, continue_on_error=True)


def action_all(rqg: RQG, args: argparse.Namespace) -> int:
    start = time.time()
    grammars = sorted(rqg.list_grammars().keys())
    if not grammars:
        print("No grammars found.")
        return 0

    # Connect once
    conn = _connect(args.dsn)
    try:
        if args.init_schema:
            _ensure_default_schema(conn, rqg)

        total_ok = 0
        total_err = 0
        error_handle: Optional[TextIO] = None
        if args.error_log:
            log_path = Path(args.error_log)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            error_handle = log_path.open('a', encoding='utf-8')
        try:
            for name in grammars:
                try:
                    queries = _generate_queries_for_grammar(rqg, name, args.count, args.seed)
                except Exception as e:
                    total_err += 1
                    if args.errors_only:
                        print(f"[GEN-ERROR] Grammar '{name}': {e}", file=sys.stdout)
                    else:
                        print(f"[GEN-ERROR] Grammar '{name}': {e}", file=sys.stderr)
                    if not args.continue_on_error:
                        raise
                    continue

                try:
                    ok, err = _exec_statements(
                        conn,
                        queries,
                        continue_on_error=True,
                        errors_only=args.errors_only,
                        error_file=error_handle,
                    )
                    total_ok += ok
                    total_err += err
                    if args.verbose:
                        print(f"[{name}] ok={ok} err={err}")
                except Exception as e:
                    total_err += 1
                    if args.errors_only:
                        print(f"[EXEC-ERROR] Grammar '{name}': {e}", file=sys.stdout)
                    else:
                        print(f"[EXEC-ERROR] Grammar '{name}': {e}", file=sys.stderr)
                    if not args.continue_on_error:
                        raise

            dur = time.time() - start
        finally:
            if error_handle is not None:
                error_handle.close()
        print(f"All grammars done: grammars={len(grammars)} total_ok={total_ok} total_err={total_err} time={dur:.2f}s")
        return 0 if total_err == 0 or args.continue_on_error else 2
    finally:
        conn.close()


def action_ddl(rqg: RQG, args: argparse.Namespace) -> int:
    conn: Optional[PGConnection] = None
    try:
        if args.table:
            ddl_sql = rqg.generate_random_table_ddl(args.table, args.num_columns, args.num_constraints)
            ddls = [ddl_sql]
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
            ok, err = _exec_statements(conn, ddls, continue_on_error=True)
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
    p.add_argument("mode", nargs="?", default="list", choices=["list", "grammar", "all", "ddl"], help="Runner mode")
    p.add_argument("--dsn", dest="dsn", default=None, help="PostgreSQL/Yugabyte DSN (env PYRQG_DSN also supported)")
    p.add_argument("--continue-on-error", action="store_true", help="Do not stop on first error")
    p.add_argument("--errors-only", action="store_true", help="Print only failing SQL statements (to stdout)")
    p.add_argument("--execute", action="store_true", help="Execute statements even if --dsn not provided (uses env/default)")
    p.add_argument("--error-log", default=None, help="Optional file to append failing SQL statements")
    p.add_argument("--seed", type=int, default=None, help="Base seed for deterministic generation")

    sub = {}

    # grammar
    sp = argparse.ArgumentParser(add_help=False)
    sp.add_argument("--grammar", required=False, help="Grammar name to run")
    sp.add_argument("--count", type=int, default=100, help="Number of queries to generate/execute")
    sp.add_argument("--output", default=None, help="Output file to write queries instead of executing/printing")
    sub["grammar"] = sp

    # all
    sp = argparse.ArgumentParser(add_help=False)
    sp.add_argument("--count", type=int, default=100, help="Number of queries per grammar")
    sp.add_argument("--init-schema", action="store_true", help="Initialize a basic default schema before running")
    sp.add_argument("--verbose", action="store_true", help="Per-grammar summary")
    sub["all"] = sp

    # ddl
    sp = argparse.ArgumentParser(add_help=False)
    sp.add_argument("--num-tables", type=int, default=5, help="Number of tables for complex DDL mode")
    sp.add_argument("--table", type=str, default=None, help="Generate a single random table DDL with this name")
    sp.add_argument("--num-columns", type=int, default=None, help="Columns for single-table DDL")
    sp.add_argument("--num-constraints", type=int, default=None, help="Constraints for single-table DDL")
    sp.add_argument("--output", type=str, default=None, help="Write DDL to file instead of executing/printing")
    sub["ddl"] = sp

    p.set_defaults(_subparsers=sub)
    return p


def main(argv: Optional[List[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = build_parser()
    args, unknown = parser.parse_known_args(argv)

    # Merge in subcommand specific args
    sub = args._subparsers.get(args.mode)
    if sub is not None:
        sub_args, _ = sub.parse_known_args(argv)
        for k, v in vars(sub_args).items():
            setattr(args, k, v)

    rqg = create_rqg()

    if args.mode == "list":
        return action_list(rqg, args)
    elif args.mode == "grammar":
        if not getattr(args, "grammar", None):
            print("--grammar is required for 'grammar' mode", file=sys.stderr)
            return 2
        return action_grammar(rqg, args)
    elif args.mode == "all":
        return action_all(rqg, args)
    elif args.mode == "ddl":
        return action_ddl(rqg, args)
    else:  # pragma: no cover
        print(f"Unknown mode: {args.mode}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
