"""
Runner for PyRQG
Unifies DDL generation, grammar-based query generation, and production workloads.
Default database focus: YugabyteDB (PostgreSQL-compatible).
"""
from __future__ import annotations

import argparse
import sys
import time
import logging
from pathlib import Path
from typing import List, Optional, Tuple

from pyrqg.api import RQG, TableMetadata
from pyrqg.ddl_generator import DDLGenerator, TableDefinition

# Optional: production runner (kept intact, we just call into it)
try:
    from pyrqg.production.production_rqg import main as production_main
    HAS_PRODUCTION = True
except Exception:
    HAS_PRODUCTION = False


def run_ddl(args) -> int:
    """Generate random DDL schema and optionally save to a file."""
    seed = args.seed
    dialect = args.dialect or ("yugabyte" if args.db == "yugabyte" else "postgresql")
    gen = DDLGenerator(dialect=dialect, seed=seed)

    if args.table:
        ddl = gen.generate_random_table(args.table, args.num_columns, args.num_constraints)
        stmts = [gen.generate_create_table(ddl)] + [gen.generate_create_index(ddl.name, idx) for idx in ddl.indexes]
        sql = ";\n".join(stmts) + ";\n"
    else:
        stmts = gen.generate_schema(args.num_tables)
        sql = ";\n".join(stmts) + ";\n"

    if args.output:
        Path(args.output).write_text(sql, encoding="utf-8")
        print(f"DDL written to {args.output}")
    else:
        print(sql)
    return 0


def run_grammar(args) -> int:
    """Generate queries from a specified grammar using the high-level API."""
    rqg = RQG()
    grammar = args.grammar
    count = args.count
    seed = args.seed
    queries = rqg.generate_from_grammar(grammar, count=count, seed=seed)

    if args.output:
        Path(args.output).write_text(";\n".join(queries) + ";\n", encoding="utf-8")
        print(f"Queries written to {args.output}")
    else:
        print("\n".join(queries))
    return len(queries)


def run_production(args, forwarded: Optional[List[str]] = None) -> int:
    """Production entry. If --production-scenario is provided, run scenario flow; otherwise delegate to production CLI.

    Fallback behavior: if the external production module is unavailable but the caller
    requested --custom, handle a lightweight generation pipeline here supporting
    --queries/--count, --grammars and --threads (ignored).
    """
    if getattr(args, 'production_scenario', None):
        return run_production_scenario(args)

    # If production package is available, delegate
    if HAS_PRODUCTION:
        orig_argv = sys.argv[:]  # copy
        try:
            # Forward unknown extras plus selected global flags consumed by this runner
            fw = list(forwarded or [])
            # Forward DSN and execution/visibility options so production CLI can execute in real time
            if getattr(args, 'dsn', None):
                fw += ['--dsn', args.dsn]
            if getattr(args, 'use_filter', False):
                fw += ['--use-filter']
            if getattr(args, 'print_errors', False):
                fw += ['--print-errors']
            if getattr(args, 'error_samples', None) is not None:
                fw += ['--error-samples', str(args.error_samples)]
            if getattr(args, 'echo_queries', False):
                fw += ['--echo-queries']
            if getattr(args, 'progress_every', None) is not None:
                fw += ['--progress-every', str(args.progress_every)]
            if getattr(args, 'duration', None):
                fw += ['--duration', str(args.duration)]
            if getattr(args, 'print_duplicates', False):
                fw += ['--print-duplicates']
            if getattr(args, 'duplicates_output', None):
                fw += ['--duplicates-output', args.duplicates_output]
            if getattr(args, 'verbose', False):
                fw += ['--verbose']
            if getattr(args, 'debug', False):
                fw += ['--debug']
            sys.argv = [orig_argv[0]] + fw
            return production_main()
        finally:
            sys.argv = orig_argv

    # Fallback: lightweight "custom" production mode implemented locally
    fw = forwarded or []
    if "--custom" in fw:
        return _run_production_custom_fallback(args, fw)

    # Otherwise, inform the user with actionable guidance
    raise RuntimeError(
        "Production runner is not available in this build. "
        "Either install the production extras, use `--custom` (supported in this build), "
        "or use other modes: ddl/grammar/scenario/exec."
    )


def _parse_production_custom_args(fw: List[str]) -> Tuple[int, List[str], Optional[int]]:
    """Parse a minimal subset of production --custom args from forwarded list.
    Supports: --queries N (or --count N), --grammars a,b,c, --threads N.
    Returns (count, grammars, threads).
    """
    count: Optional[int] = None
    grammars: Optional[List[str]] = None
    threads: Optional[int] = None

    i = 0
    while i < len(fw):
        tok = fw[i]
        if tok == "--queries" and i + 1 < len(fw):
            try:
                count = int(fw[i + 1])
            except Exception:
                pass
            i += 2
            continue
        if tok == "--count" and i + 1 < len(fw):
            try:
                count = int(fw[i + 1])
            except Exception:
                pass
            i += 2
            continue
        if tok == "--grammars" and i + 1 < len(fw):
            grammars = [x.strip() for x in fw[i + 1].split(',') if x.strip()]
            i += 2
            continue
        if tok == "--threads" and i + 1 < len(fw):
            try:
                threads = int(fw[i + 1])
            except Exception:
                pass
            i += 2
            continue
        # skip unknown tokens and their value if it looks like an option value
        i += 1

    if grammars is None:
        grammars = ["dml_unique"]
    if count is None:
        count = 1000

    return count, grammars, threads


def _run_production_custom_fallback(args, fw: List[str]) -> int:
    """Lightweight custom production pipeline: generates queries from listed grammars.
    Streams output to stdout or a file, without external production package.
    """
    total, grammars, threads = _parse_production_custom_args(fw)

    rqg = RQG()
    remaining = int(total)
    seed = args.seed

    # Prepare output
    if args.output:
        out_path = Path(args.output)
        out = out_path.open('w', encoding='utf-8')
        close_out = True
        print(f"[PyRQG] Production(custom) generating {remaining} queries from {','.join(grammars)} -> {args.output}")
    else:
        out = sys.stdout
        close_out = False

    try:
        gi = 0
        batch_base = 0
        CHUNK = 1000
        while remaining > 0:
            gname = grammars[gi % len(grammars)]
            to_gen = min(CHUNK, remaining)
            # Vary seed slightly per batch to improve diversity if provided
            cur_seed = (seed + batch_base) if (seed is not None) else None
            queries = rqg.generate_from_grammar(gname, count=to_gen, seed=cur_seed)
            for q in queries:
                out.write(q.rstrip(';') + ';\n')
            remaining -= to_gen
            gi += 1
            batch_base += 1
        if out is not sys.stdout:
            print(f"[PyRQG] Done. Wrote {total} queries.")
    finally:
        if close_out:
            out.close()

    return total


def _resolve_scenario_files(keyword: str) -> Tuple[Optional[Path], Optional[Path]]:
    """Resolve schema and workload files for a production scenario by keyword.
    Performs case-insensitive substring matching. Returns (schema_path, workload_path).
    """
    root = Path(__file__).resolve().parent.parent
    schemas_dir = root / 'production_scenarios' / 'schemas'
    workloads_dir = root / 'production_scenarios' / 'workloads'
    key = keyword.lower()

    schema_match = None
    workload_match = None

    if schemas_dir.exists():
        for p in sorted(schemas_dir.glob('*.sql')):
            if key in p.stem.lower():
                schema_match = p
                break
    if workloads_dir.exists():
        for p in sorted(workloads_dir.glob('*.py')):
            if key in p.stem.lower():
                workload_match = p
                break

    return schema_match, workload_match


def run_production_scenario(args) -> int:
    """Generate and optionally execute a production scenario (DDL + mixed workload).

    Behavior:
    - Always generates the scenario DDL and N mixed queries (70% scenario, 30% general by default).
    - If --dsn is provided, executes the DDL first and then executes the generated queries against the database.
    - If --output is provided, also writes the combined SQL to a file for inspection/replay.
    - If no --dsn is provided, emits the SQL to stdout or --output (legacy behavior).
    """
    if not args.production_scenario:
        raise ValueError("--production-scenario is required for production scenario mode")

    # Resolve schema and workload
    schema_path, workload_path = _resolve_scenario_files(args.production_scenario)
    if schema_path is None:
        raise FileNotFoundError(f"No schema found for scenario keyword '{args.production_scenario}' in production_scenarios/schemas")

    ddl_sql = Path(schema_path).read_text(encoding='utf-8').strip()

    rqg = RQG()
    total = args.count or 1000
    seed = args.seed

    # Load scenario grammar if present
    scenario_queries: List[str] = []
    if workload_path is not None:
        from importlib.util import spec_from_file_location, module_from_spec
        spec = spec_from_file_location("scenario_grammar", str(workload_path))
        module = module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        grammar = getattr(module, 'grammar', getattr(module, 'g', None))
        if grammar is not None:
            name = workload_path.stem
            rqg.add_grammar(name, grammar)
            scenario_ratio = 0.7
            scenario_count = int(total * scenario_ratio)
            scenario_queries = rqg.generate_from_grammar(name, count=scenario_count, seed=seed)

    # Fill the rest with general workload
    remaining = total - len(scenario_queries)
    general_grammar = 'dml_yugabyte' if 'dml_yugabyte' in rqg.grammars else ('dml_unique' if 'dml_unique' in rqg.grammars else next(iter(rqg.grammars)))
    general_queries = rqg.generate_from_grammar(general_grammar, count=max(0, remaining), seed=(seed + len(scenario_queries) if seed is not None else None))

    # Compose output (for optional write/print)
    parts: List[str] = []
    parts.append(ddl_sql.rstrip(';') + ';')
    all_queries = scenario_queries + general_queries
    if all_queries:
        parts.append(';' + "\n".join(q.rstrip(';') + ';' for q in all_queries))

    out_text = "\n\n".join(parts) + ("\n" if not parts[-1].endswith("\n") else "")

    # If DSN provided: execute scenario directly
    if getattr(args, 'dsn', None):
        # Choose executor (optionally with filter)
        from pyrqg.core.executor import create_executor
        try:
            from pyrqg.core.filtered_executor import create_filtered_executor
        except Exception:
            create_filtered_executor = None  # type: ignore

        use_filter = getattr(args, 'use_filter', False)
        executor = (create_filtered_executor(args.dsn) if (use_filter and create_filtered_executor is not None)
                    else create_executor(args.dsn))

        # Execute DDL as a single batch to preserve function bodies/dollar-quoting
        ddl_to_run = ddl_sql if ddl_sql.endswith(';') else (ddl_sql + ';')
        executor.execute(ddl_to_run)

        # Execute workload queries one by one, track errors
        from pyrqg.core.constants import Status
        syntax_errors = 0
        executed = 0
        error_samples = []
        max_samples = getattr(args, 'error_samples', 10)
        print_errors = getattr(args, 'print_errors', False)

        start_time = time.time()
        progress_every = getattr(args, 'progress_every', 0) or 0
        echo_queries = getattr(args, 'echo_queries', False)

        for q in all_queries:
            if echo_queries:
                print(f"[{executed + 1}] {q}")
            res = executor.execute(q)
            executed += 1
            if res.status == Status.SYNTAX_ERROR:
                syntax_errors += 1
                if print_errors and len(error_samples) < max_samples:
                    error_samples.append((q, res.errstr))
            if progress_every and executed % progress_every == 0:
                elapsed = max(1e-6, time.time() - start_time)
                qps = executed / elapsed
                print(f"Progress: executed={executed}, syntax_errors={syntax_errors}, qps={qps:.1f}", flush=True)

        print(f"Scenario '{args.production_scenario}': Executed {executed} queries. Syntax errors: {syntax_errors}")
        if print_errors and error_samples:
            print("\nSample syntax errors (showing up to", len(error_samples), "):")
            for i, (q, err) in enumerate(error_samples, 1):
                print(f"[{i}] Error: {err}")
                print(f"    Query: {q}")

        # Optionally write SQL for audit
        if args.output:
            Path(args.output).write_text(out_text, encoding='utf-8')
            print(f"Scenario '{args.production_scenario}' DDL + {len(all_queries)} queries also written to {args.output}")
        return executed

    # Legacy behavior: no DSN, just print/write SQL
    if args.output:
        Path(args.output).write_text(out_text, encoding='utf-8')
        print(f"Scenario '{args.production_scenario}' DDL + {len(all_queries)} queries written to {args.output}")
    else:
        print(out_text)
    return len(all_queries)


def run_exec(args) -> int:
    """Execute a full flow against a local PostgreSQL database:
    - Create random tables (DDL)
    - Apply random ALTER TABLE statements
    - Generate and execute random DML queries, capturing syntax errors
    Requires --dsn. Uses optional PostgreSQL filter.
    """
    if not args.dsn:
        raise ValueError("--dsn is required for exec mode")

    # Choose executor
    from pyrqg.core.executor import create_executor
    from pyrqg.core.filtered_executor import create_filtered_executor

    dialect = args.dialect or ("postgresql")
    gen = DDLGenerator(dialect=dialect, seed=args.seed)

    # Generate a small schema of random tables
    num_tables = args.num_tables
    tables: List[TableDefinition] = []
    for i in range(num_tables):
        tname = f"rt_{i+1}"
        tdef = gen.generate_random_table(tname)
        tables.append(tdef)

    # Connect executor
    executor = create_filtered_executor(args.dsn) if getattr(args, 'use_filter', False) else create_executor(args.dsn)

    # Create tables and indexes
    for t in tables:
        ddl_sql = gen.generate_create_table(t)
        executor.execute(ddl_sql)
        for idx in t.indexes:
            executor.execute(gen.generate_create_index(t.name, idx))
        # Apply ALTER statements
        for stmt in gen.generate_alter_table_statements(t, max_alters=getattr(args, 'alters_per_table', 2)):
            executor.execute(stmt)

    # Build TableMetadata for DML generator (after alters)
    metas: List[TableMetadata] = []
    for t in tables:
        cols = [{"name": c.name, "type": c.data_type.split()[0].lower()} for c in t.columns]
        # Find simple PK if any
        pk = None
        for con in t.constraints:
            if con.constraint_type == "PRIMARY KEY" and len(con.columns) == 1:
                pk = con.columns[0]
                break
        metas.append(TableMetadata(name=t.name, columns=cols, primary_key=pk))

    # Generate and execute queries
    rqg = RQG()
    rqg.add_tables(metas)
    qgen = rqg.create_generator(seed=args.seed)

    total = args.count
    from pyrqg.core.constants import Status
    syntax_errors = 0
    executed = 0

    error_samples = []
    max_samples = getattr(args, 'error_samples', 10)

    start_time = time.time()
    progress_every = getattr(args, 'progress_every', 0) or 0
    echo_queries = getattr(args, 'echo_queries', False)

    for _ in range(total):
        q = qgen.generate_batch(1)[0].sql
        if echo_queries:
            print(f"[{executed + 1}] {q}", flush=True)
        res = executor.execute(q)
        executed += 1
        if res.status == Status.SYNTAX_ERROR:
            syntax_errors += 1
            if getattr(args, 'print_errors', False) and len(error_samples) < max_samples:
                error_samples.append((q, res.errstr))
        if progress_every and executed % progress_every == 0:
            elapsed = max(1e-6, time.time() - start_time)
            qps = executed / elapsed
            print(f"Progress: executed={executed}, syntax_errors={syntax_errors}, qps={qps:.1f}", flush=True)

    print(f"Executed {executed} queries. Syntax errors: {syntax_errors}")
    if getattr(args, 'print_errors', False) and error_samples:
        print("\nSample syntax errors (showing up to", len(error_samples), "):")
        for i, (q, err) in enumerate(error_samples, 1):
            print(f"[{i}] Error: {err}")
            print(f"    Query: {q}")
    return 0


def run_scenario(args) -> int:
    """Run a production scenario workload (grammars under production_scenarios)."""
    # For now, we simply load the grammar file and generate queries
    from importlib.util import spec_from_file_location, module_from_spec

    scenario_path = Path(args.file)
    if not scenario_path.exists():
        raise FileNotFoundError(f"Scenario file not found: {scenario_path}")

    spec = spec_from_file_location("scenario_grammar", str(scenario_path))
    module = module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    if not hasattr(module, "grammar") and not hasattr(module, "g"):
        raise ValueError("Scenario module must expose 'grammar' or 'g'")

    grammar = getattr(module, "grammar", getattr(module, "g"))
    rqg = RQG()
    rqg.add_grammar(args.name or scenario_path.stem, grammar)

    queries = rqg.generate_from_grammar(args.name or scenario_path.stem, count=args.count, seed=args.seed)

    if args.output:
        Path(args.output).write_text(";\n".join(queries) + ";\n", encoding="utf-8")
        print(f"Scenario queries written to {args.output}")
    else:
        print("\n".join(queries))
    return len(queries)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="PyRQG Runner - yugabyte-focused",
    )

    parser.add_argument("mode", nargs='?', default='list', choices=["ddl", "grammar", "production", "scenario", "list", "exec"], help="Runner mode (default: list)")
    parser.add_argument("--db", default="yugabyte", choices=["yugabyte", "postgresql"], help="Target database")
    parser.add_argument("--dialect", default=None, help="SQL dialect override")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    parser.add_argument("--output", help="Output file path")

    # DDL/exec mode options
    parser.add_argument("--num-tables", dest="num_tables", type=int, default=5, help="Number of tables for schema generation (or exec mode setup)")
    parser.add_argument("--table", help="Generate a single random table with this name instead of a full schema")
    parser.add_argument("--num-columns", dest="num_columns", type=int, help="Columns for single-table generation")
    parser.add_argument("--num-constraints", dest="num_constraints", type=int, help="Constraints for single-table generation")

    # Grammar mode options
    parser.add_argument("--grammar", help="Grammar name to generate from (see RQG.list_grammars())")
    parser.add_argument("--count", type=int, default=10, help="How many queries to generate")

    # Scenario mode options
    parser.add_argument("--file", help="Path to a production scenario grammar file")
    parser.add_argument("--name", help="Name for the loaded scenario grammar")

    # Production scenario options
    parser.add_argument("--production-scenario", dest="production_scenario", help="Name keyword of production scenario (e.g., bank, ecommerce). With mode=production, will emit scenario DDL and mixed workload queries.")

    # Exec mode options (execute against a live PostgreSQL/YugabyteDB database)
    parser.add_argument("--dsn", help="PostgreSQL-compatible DSN for exec mode, e.g. postgresql://user:pass@localhost:5433/postgres (YugabyteDB)")
    parser.add_argument("--use-filter", dest="use_filter", action="store_true", help="Use PostgreSQL compatibility filter before executing queries")
    parser.add_argument("--alters-per-table", dest="alters_per_table", type=int, default=2, help="Max ALTERs per table in exec mode")
    parser.add_argument("--print-errors", dest="print_errors", action="store_true", help="Print sample SQL syntax errors encountered during execution")
    parser.add_argument("--error-samples", dest="error_samples", type=int, default=10, help="Max number of error samples to print with --print-errors")
    parser.add_argument("--echo-queries", dest="echo_queries", action="store_true", help="Echo each executed query to stdout")
    parser.add_argument("--progress-every", dest="progress_every", type=int, default=0, help="Print a progress line every N executed queries (0=disable)")
    parser.add_argument("--duration", type=int, default=0, help="Run for N seconds instead of a fixed count in production mode (forwarded)")
    parser.add_argument("--print-duplicates", dest="print_duplicates", action="store_true", help="Collect/print duplicates when high duplicate rate is detected (production; forwarded)")
    parser.add_argument("--duplicates-output", dest="duplicates_output", help="Optional file to write duplicates (production; forwarded)")

    # Convenience: list grammars regardless of mode
    parser.add_argument("--list-grammars", action="store_true", help="List all available grammars and exit")

    # Verbosity
    parser.add_argument("--verbose", action="store_true", help="Enable INFO level logging with timestamps")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG level logging (very verbose)")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    # Parse known args and forward the rest to production CLI if needed
    args, extras = parser.parse_known_args(argv)

    # Configure root logging early
    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    elif args.verbose:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')

    # Global quick action: list grammars and exit
    if getattr(args, 'list_grammars', False):
        rqg = RQG()
        grams = rqg.list_grammars()
        for name, desc in sorted(grams.items()):
            print(f"{name}: {desc}")
        return 0

    if args.mode == "ddl": 
        return run_ddl(args)
    elif args.mode == "grammar":
        if not args.grammar:
            parser.error("--grammar is required for grammar mode")
        return run_grammar(args)
    elif args.mode == "production":
        # forward extras (e.g., --config, --count, --threads, etc.) to production CLI
        return run_production(args, forwarded=extras)
    elif args.mode == "scenario":
        if not args.file:
            parser.error("--file is required for scenario mode")
        return run_scenario(args)
    elif args.mode == "list":
        rqg = RQG()
        grams = rqg.list_grammars()
        for name, desc in sorted(grams.items()):
            print(f"{name}: {desc}")
        return 0
    elif args.mode == "exec":
        return run_exec(args)

    parser.error("Unknown mode")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
