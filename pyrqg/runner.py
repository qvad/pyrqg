"""
Runner for PyRQG
Unifies DDL generation, grammar-based query generation, and production workloads.
Default database focus: YugabyteDB (PostgreSQL-compatible).
"""
from __future__ import annotations

import argparse
import sys
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
    """Production entry. If --production-scenario is provided, run scenario flow; otherwise delegate to production CLI."""
    if getattr(args, 'production_scenario', None):
        return run_production_scenario(args)
    if not HAS_PRODUCTION:
        raise RuntimeError("Production runner is not available in this build.")
    # Delegate to existing production CLI, forwarding remaining CLI args
    orig_argv = sys.argv[:]  # copy
    try:
        sys.argv = [orig_argv[0]] + (forwarded or [])
        return production_main()
    finally:
        sys.argv = orig_argv


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
    """Generate DDL and mixed workload for a named production scenario.

    - Writes the scenario schema DDL first.
    - Generates N mixed queries (70% scenario workload if available, 30% general workload).
    - Outputs to --output if provided, otherwise stdout.
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

    # Compose output
    parts: List[str] = []
    parts.append(ddl_sql.rstrip(';') + ';')
    all_queries = scenario_queries + general_queries
    if all_queries:
        parts.append(';' + "\n".join(q.rstrip(';') + ';' for q in all_queries))

    out_text = "\n\n".join(parts) + ("\n" if not parts[-1].endswith("\n") else "")
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

    for _ in range(total):
        q = qgen.generate_batch(1)[0].sql
        res = executor.execute(q)
        executed += 1
        if res.status == Status.SYNTAX_ERROR:
            syntax_errors += 1

    print(f"Executed {executed} queries. Syntax errors: {syntax_errors}")
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

    parser.add_argument("mode", choices=["ddl", "grammar", "production", "scenario", "list", "exec"], help="Runner mode")
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

    # Exec mode options (execute against a live PostgreSQL database)
    parser.add_argument("--dsn", help="PostgreSQL DSN for exec mode, e.g. postgresql://user:pass@localhost:5432/db")
    parser.add_argument("--use-filter", dest="use_filter", action="store_true", help="Use PostgreSQL compatibility filter before executing queries")
    parser.add_argument("--alters-per-table", dest="alters_per_table", type=int, default=2, help="Max ALTERs per table in exec mode")

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    # Parse known args and forward the rest to production CLI if needed
    args, extras = parser.parse_known_args(argv)

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
