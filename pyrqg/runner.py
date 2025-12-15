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
import time
import re
from pathlib import Path
from typing import List, Optional, Tuple, TextIO, Iterator, Any

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


def _error_stream(log_errors: bool) -> TextIO:
    return sys.stdout if log_errors else sys.stderr


def _print_error(message: str, query: str, *, stream: TextIO, include_query: bool = True) -> None:
    if include_query and query:
        print(f"[ERROR] {message}\nWhile executing: {query}", file=stream)
    else:
        print(f"[ERROR] {message}", file=stream)


def _exec_statements(
    conn: PGConnection,
    statements: List[str],
    continue_on_error: bool = True,
    trace_all: bool = False,
    log_errors: bool = False,
    trace_stream: Optional[TextIO] = None,
    error_file: Optional[TextIO] = None,
    track_rows: bool = False,
) -> Tuple[int, int, int]:
    ok, err, total_rows = 0, 0, 0
    conn.autocommit = True
    trace_stream = trace_stream or sys.stdout
    with conn.cursor() as cur:
        for stmt in statements:
            sql = stmt.strip()
            if not sql:
                continue
            if trace_all:
                print(f"[SQL] {sql}", file=trace_stream)
            try:
                cur.execute(sql)
                if track_rows and cur.description is not None:
                    rows = cur.fetchall()
                    total_rows += len(rows)
                ok += 1
            except Exception as e:
                err += 1
                include_query = trace_all or log_errors
                error_stream = trace_stream if include_query else sys.stderr
                _print_error(str(e), sql, stream=error_stream, include_query=include_query)
                if error_file is not None:
                    _print_error(str(e), sql, stream=error_file, include_query=True)
                if not continue_on_error:
                    raise
    return ok, err, total_rows


_STRING_LITERAL_RE = re.compile(r"'(?:''|[^'])*'")
_NUMERIC_LITERAL_RE = re.compile(r"\b\d+(?:\.\d+)?\b")


def _query_shape(query: str) -> str:
    """Normalize a query so literal changes don't produce new shapes."""
    q = query.strip()
    q = _STRING_LITERAL_RE.sub("'?'", q)
    q = _NUMERIC_LITERAL_RE.sub('?', q)
    q = " ".join(q.split())
    return q


def action_list(rqg: RQG, _args: argparse.Namespace) -> int:
    grammars = rqg.list_grammars()
    print("Available grammars (name: description):")
    for name in sorted(grammars.keys()):
        print(f"- {name}: {grammars[name]}")
    return 0


def _generate_queries_for_grammar(rqg: RQG, grammar: str, count: int, seed: Optional[int], context: Any = None) -> Iterator[str]:
    # Prefer 'query' rule; api uses 'query' by default as well
    return rqg.generate_from_grammar(grammar, rule="query", count=count, seed=seed, context=context)


def action_grammar(rqg: RQG, args: argparse.Namespace) -> int:
    context = None
    dsn = args.dsn or os.environ.get("PYRQG_DSN")
    
    if dsn:
        try:
            from pyrqg.dsl.schema_aware_context import SchemaAwareContext
            # Introspection connection
            context = SchemaAwareContext(dsn, seed=args.seed)
        except ImportError:
            pass
        except Exception as e:
            # Don't fail hard if introspection fails, just warn and use default context
            print(f"[WARN] Could not initialize schema context: {e}", file=sys.stderr)

    try:
        # Consume generator into list for CLI compatibility
        queries = list(_generate_queries_for_grammar(rqg, args.grammar, args.count, args.seed, context=context))
        shapes = {_query_shape(q) for q in queries if q.strip()}
        unique_shapes = len(shapes)
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
                ok, err, total_rows = _exec_statements(
                    conn,
                    queries,
                    continue_on_error=args.continue_on_error,
                    trace_all=args.verbose,
                    log_errors=args.log_errors,
                    error_file=error_handle,
                    track_rows=True,
                )
                print(
                    "Executed grammar '{grammar}': count={count}, ok={ok}, errors={err}, rows={rows}, unique_shapes={unique}".format(
                        grammar=args.grammar,
                        count=len(queries),
                        ok=ok,
                        err=err,
                        rows=total_rows,
                        unique=unique_shapes,
                    )
                )
                dup_shapes = len(queries) - unique_shapes
                if dup_shapes > 0:
                    print(
                        f"[WARN] Detected {dup_shapes} repeated query shapes out of {len(queries)}",
                        file=sys.stderr,
                    )
            finally:
                if error_handle is not None:
                    error_handle.close()
                conn.close()
            return 0 if err == 0 or args.continue_on_error else 2

        # Default: print to stdout
        for q in queries:
            print(q.rstrip(";\n") + ";")
        if unique_shapes != len(queries):
            print(
                f"-- unique query shapes: {unique_shapes}/{len(queries)}",
                file=sys.stderr,
            )
        return 0
    finally:
        if context:
            context.close()


def _ensure_default_schema(conn: PGConnection, rqg: RQG) -> None:
    ddl = rqg.generate_ddl()
    _exec_statements(conn, ddl, continue_on_error=True)


def action_all(rqg: RQG, args: argparse.Namespace) -> int:
    start = time.time()
    grammars = sorted(rqg.list_grammars().keys())
    if not grammars:
        print("No grammars found.")
        return 0

    # Connect once for execution
    conn = _connect(args.dsn)
    
    try:
        if args.init_schema:
            print("Initializing default schema...", file=sys.stderr)
            _ensure_default_schema(conn, rqg)

        # Initialize schema context for generation (after schema init)
        context = None
        dsn = args.dsn or os.environ.get("PYRQG_DSN")
        if dsn:
            try:
                from pyrqg.dsl.schema_aware_context import SchemaAwareContext
                print(f"Introspecting schema from {dsn}...", file=sys.stderr)
                context = SchemaAwareContext(dsn, seed=args.seed)
                print(f"Loaded {len(context.tables)} tables.", file=sys.stderr)
            except ImportError:
                pass
            except Exception as e:
                print(f"[WARN] Could not initialize schema context: {e}", file=sys.stderr)

        total_ok = 0
        total_err = 0
        total_rows = 0
        error_handle: Optional[TextIO] = None
        if args.error_log:
            log_path = Path(args.error_log)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            error_handle = log_path.open('a', encoding='utf-8')
        try:
            for name in grammars:
                try:
                    # Consume generator into list for stats calculation
                    queries = list(_generate_queries_for_grammar(rqg, name, args.count, args.seed, context=context))
                    shapes = {_query_shape(q) for q in queries if q.strip()}
                    dup_shapes = len(queries) - len(shapes)
                    if dup_shapes > 0:
                        print(
                            f"[WARN] Grammar '{name}' repeated {dup_shapes} query shapes",
                            file=sys.stderr,
                        )
                except Exception as e:
                    total_err += 1
                    print(
                        f"[GEN-ERROR] Grammar '{name}': {e}",
                        file=_error_stream(args.log_errors),
                    )
                    if not args.continue_on_error:
                        raise
                    continue

                try:
                    ok, err, rows = _exec_statements(
                        conn,
                        queries,
                        continue_on_error=True,
                        trace_all=args.verbose,
                        log_errors=args.log_errors,
                        error_file=error_handle,
                        track_rows=True,
                    )
                    total_ok += ok
                    total_err += err
                    total_rows += rows
                except Exception as e:
                    total_err += 1
                    print(
                        f"[EXEC-ERROR] Grammar '{name}': {e}",
                        file=_error_stream(args.log_errors),
                    )
                    if not args.continue_on_error:
                        raise

            dur = time.time() - start
        finally:
            if error_handle is not None:
                error_handle.close()
        print(f"All grammars done: grammars={len(grammars)} total_ok={total_ok} total_err={total_err} total_rows={total_rows} time={dur:.2f}s")
        return 0 if total_err == 0 or args.continue_on_error else 2
    finally:
        if context:
            context.close()
        conn.close()


def action_ddl(rqg: RQG, args: argparse.Namespace) -> int:
    conn: Optional[PGConnection] = None
    try:
        if args.table:
            ddl_sqls = rqg.generate_random_table_ddl(args.table, args.num_columns, args.num_constraints)
            ddls = ddl_sqls
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
            ok, err, _ = _exec_statements(conn, ddls, continue_on_error=True)
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
