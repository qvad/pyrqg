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
    rule = getattr(args, 'grammar_rule', 'query') or 'query'
    queries = rqg.generate_from_grammar(grammar, rule=rule, count=count, seed=seed)

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


def _parse_production_custom_args(
    fw: List[str],
) -> Tuple[
    int,                # count
    List[str],          # grammars
    Optional[int],      # threads
    Optional[int],      # duration
    Optional[str],      # grammar_rule
    int,                # schema_num_tables
    int,                # schema_num_functions
    int,                # schema_num_views
    Optional[str],      # schema_name
    Optional[str],      # schema_profile
    Optional[float],    # fk_ratio
    Optional[float],    # index_ratio
    Optional[float],    # composite_index_ratio
    Optional[float],    # partial_index_ratio
    Optional[List[str]],# schema_files
]:
    """Parse a minimal subset of production --custom args from forwarded list.
    Supports:
      - --queries / --count
      - --grammars a,b,c (or --workload-grammars a,b,c)
      - --threads, --duration, --grammar-rule
      - Schema controls: --schema-num-tables N, --schema-num-functions N, --schema-num-views N
    """
    count: Optional[int] = None
    grammars: Optional[List[str]] = None
    threads: Optional[int] = None
    duration: Optional[int] = None
    grammar_rule: Optional[str] = None
    schema_num_tables = 0
    schema_num_functions = 0
    schema_num_views = 0
    schema_name: Optional[str] = None
    schema_profile: Optional[str] = None
    fk_ratio: Optional[float] = None
    index_ratio: Optional[float] = None
    composite_index_ratio: Optional[float] = None
    partial_index_ratio: Optional[float] = None
    schema_files: Optional[List[str]] = None

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
        if tok in ("--grammars", "--workload-grammars") and i + 1 < len(fw):
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
        if tok == "--duration" and i + 1 < len(fw):
            try:
                duration = int(fw[i + 1])
            except Exception:
                pass
            i += 2
            continue
        if tok == "--grammar-rule" and i + 1 < len(fw):
            grammar_rule = fw[i + 1]
            i += 2
            continue
        # Schema controls
        if tok in ("--schema-num-tables", "--num-tables") and i + 1 < len(fw):
            try:
                schema_num_tables = int(fw[i + 1])
            except Exception:
                pass
            i += 2
            continue
        if tok in ("--schema-num-functions", "--num-functions") and i + 1 < len(fw):
            try:
                schema_num_functions = int(fw[i + 1])
            except Exception:
                pass
            i += 2
            continue
        if tok in ("--schema-num-views", "--num-views") and i + 1 < len(fw):
            try:
                schema_num_views = int(fw[i + 1])
            except Exception:
                pass
            i += 2
            continue
        if tok == "--schema-name" and i + 1 < len(fw):
            schema_name = fw[i + 1]
            i += 2
            continue
        if tok == "--schema-profile" and i + 1 < len(fw):
            schema_profile = fw[i + 1]
            i += 2
            continue
        if tok == "--fk-ratio" and i + 1 < len(fw):
            try:
                fk_ratio = float(fw[i + 1])
            except Exception:
                pass
            i += 2
            continue
        if tok == "--index-ratio" and i + 1 < len(fw):
            try:
                index_ratio = float(fw[i + 1])
            except Exception:
                pass
            i += 2
            continue
        if tok == "--composite-index-ratio" and i + 1 < len(fw):
            try:
                composite_index_ratio = float(fw[i + 1])
            except Exception:
                pass
            i += 2
            continue
        if tok == "--partial-index-ratio" and i + 1 < len(fw):
            try:
                partial_index_ratio = float(fw[i + 1])
            except Exception:
                pass
            i += 2
            continue
        if tok == "--schema-file" and i + 1 < len(fw):
            raw = fw[i + 1]
            parts = [p.strip() for p in raw.split(',') if p.strip()]
            schema_files = (schema_files or []) + parts
            i += 2
            continue
        # skip unknown tokens and their value if it looks like an option value
        i += 1

    if grammars is None:
        grammars = ["dml_unique"]
    if count is None:
        count = 1000

    return (
        count,
        grammars,
        threads,
        duration,
        grammar_rule,
        schema_num_tables,
        schema_num_functions,
        schema_num_views,
        schema_name,
        schema_profile,
        fk_ratio,
        index_ratio,
        composite_index_ratio,
        partial_index_ratio,
        schema_files,
    )


def _run_production_custom_fallback(args, fw: List[str]) -> int:
    """Lightweight custom production pipeline: generates queries from listed grammars.
    If --dsn is provided, executes the generated queries against the database.
    Otherwise, streams output to stdout or a file.
    """
    from pyrqg.core.constants import Status

    (
        total,
        grammars,
        threads,
        parsed_duration,
        grammar_rule,
        schema_num_tables,
        schema_num_functions,
        schema_num_views,
        schema_name,
        schema_profile,
        fk_ratio,
        index_ratio,
        composite_index_ratio,
        partial_index_ratio,
        schema_files,
    ) = _parse_production_custom_args(fw)

    # If argparse consumed --count, prefer it
    if getattr(args, 'count', None) is not None:
        try:
            total = int(args.count)
        except Exception:
            pass

    # If argparse consumed schema size flags, prefer them as well
    if (not schema_num_tables) and getattr(args, 'num_tables', None):
        try:
            schema_num_tables = int(args.num_tables)
        except Exception:
            pass
    if (not schema_num_functions) and getattr(args, 'num_functions', None):
        try:
            schema_num_functions = int(args.num_functions)
        except Exception:
            pass
    if (not schema_num_views) and getattr(args, 'num_views', None):
        try:
            schema_num_views = int(args.num_views)
        except Exception:
            pass

    rqg = RQG()
    remaining = int(total)
    seed = args.seed
    # Duration precedence: forwarded value overrides args.duration if present
    duration = parsed_duration if parsed_duration is not None else getattr(args, 'duration', 0) or 0
    time_mode = duration > 0

    dsn = getattr(args, 'dsn', None)
    # Hint schema-aware components with DSN ahead of time
    if dsn:
        try:
            import os as _os
            _os.environ["PYRQG_DSN"] = dsn
            # Yugabyte-safe toggle for grammars (suppress PG-only features)
            dsn_lc = dsn.lower()
            if "yugabyte" in dsn_lc or ":5433" in dsn_lc:
                _os.environ["PYRQG_YB"] = "1"
        except Exception:
            pass
    # Schema name
    schema_name = schema_name or getattr(args, 'schema_name', None) or 'pyrqg'
    try:
        import os as _os
        _os.environ["PYRQG_SCHEMA"] = schema_name
    except Exception:
        pass
    use_filter = getattr(args, 'use_filter', False)
    echo_queries = getattr(args, 'echo_queries', False)
    progress_every = getattr(args, 'progress_every', 0) or 0
    print_errors = getattr(args, 'print_errors', False)
    max_samples = getattr(args, 'error_samples', 10)

    # Optional: apply user-provided schema files, then generate and emit schema bundle
    schema_text = ""
    # Read provided schema files if any
    file_sql_parts: List[str] = []
    if schema_files:
        for p in schema_files:
            try:
                file_sql_parts.append(Path(p).read_text(encoding='utf-8').strip())
            except Exception as e:
                print(f"[PyRQG] Warning: failed to read schema file {p}: {e}")
    if (schema_num_tables or schema_num_functions or schema_num_views):
        try:
            from pyrqg.dsl.schema_primitives import schema_bundle_element
            from pyrqg.dsl.core import Context as _DSLContext
            _ctx = _DSLContext(seed=seed)
            schema_text = schema_bundle_element(
                num_tables=max(0, schema_num_tables or 0),
                functions=max(0, schema_num_functions or 0),
                views=max(0, schema_num_views or 0),
                dialect="postgresql",
                profile=(schema_profile or 'core'),
                fk_ratio=(fk_ratio if fk_ratio is not None else 0.3),
                index_ratio=(index_ratio if index_ratio is not None else 0.7),
                composite_index_ratio=(composite_index_ratio if composite_index_ratio is not None else 0.3),
                partial_index_ratio=(partial_index_ratio if partial_index_ratio is not None else 0.2),
            ).generate(_ctx)
        except Exception:
            # Fallback to DDLGenerator only
            try:
                ddls = rqg.generate_complex_ddl(num_tables=max(1, schema_num_tables or 1))
                schema_text = "\n".join(s.rstrip(';') + ';' for s in ddls)
            except Exception:
                schema_text = "-- failed to generate schema bundle"
    # Merge file-based schema and generated schema
    combined_schema_sql = "\n\n".join([s for s in file_sql_parts + [schema_text] if s])

    # If no DSN provided: keep legacy behavior (print/write queries)
    if not dsn:
        # Prepare output
        if args.output:
            out_path = Path(args.output)
            out = out_path.open('w', encoding='utf-8')
            close_out = True
            if combined_schema_sql:
                out.write(combined_schema_sql.rstrip(';') + ';;\n\n')
            if time_mode:
                print(f"[PyRQG] Production(custom) generating for {duration}s from {','.join(grammars)} -> {args.output}")
            else:
                print(f"[PyRQG] Production(custom) generating {remaining} queries from {','.join(grammars)} -> {args.output}")
        else:
            out = sys.stdout
            close_out = False

        try:
            gi = 0
            batch_base = 0
            CHUNK = 1000
            start_time = time.time()
            generated = 0
            if not args.output and combined_schema_sql:
                print(combined_schema_sql.rstrip(';') + ';;\n')
            while (remaining > 0) or time_mode:
                if time_mode and (time.time() - start_time) >= duration:
                    break
                gname = grammars[gi % len(grammars)]
                to_gen = CHUNK if time_mode else min(CHUNK, remaining)
                # Vary seed slightly per batch to improve diversity if provided
                cur_seed = (seed + batch_base) if (seed is not None) else None
                queries = rqg.generate_from_grammar(gname, rule=(grammar_rule or "query"), count=to_gen, seed=cur_seed)
                for q in queries:
                    if time_mode and (time.time() - start_time) >= duration:
                        break
                    out.write(q.rstrip(';') + ';\n')
                    generated += 1
                    if not time_mode:
                        remaining -= 1
                if not time_mode:
                    remaining -= max(0, to_gen - min(to_gen, 0))  # no-op; left for clarity
                gi += 1
                batch_base += 1
            if out is not sys.stdout:
                if time_mode:
                    print(f"[PyRQG] Done. Generated {generated} queries in ~{int(time.time()-start_time)}s.")
                else:
                    print(f"[PyRQG] Done. Wrote {total} queries.")
        finally:
            if close_out:
                out.close()
        return generated if time_mode else total

    # DSN provided: execute queries. Optionally use filter and threads
    if time_mode:
        print(f"[PyRQG] Production(custom) starting execution: duration={duration}s, grammars={','.join(grammars)}, threads={threads or 1}, dsn={'***' if not dsn else dsn}")
    else:
        print(f"[PyRQG] Production(custom) starting execution: total={total}, grammars={','.join(grammars)}, threads={threads or 1}, dsn={'***' if not dsn else dsn}")
    # If DSN provided and schema generation requested: execute schema first
    if combined_schema_sql:
        try:
            from pyrqg.core.executor import create_executor
            executor = create_executor(dsn)
            # Ensure schema exists and search_path targets pyrqg first
            prelude = f"CREATE SCHEMA IF NOT EXISTS {schema_name}; SET search_path TO {schema_name}, public;"
            executor.execute(prelude)
            ddl_to_run = combined_schema_sql if combined_schema_sql.endswith(';') else (combined_schema_sql + ';')
            executor.execute(ddl_to_run)
            # Verify presence of tables in pyrqg
            verify = executor.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='{schema_name}'")
            try:
                n_tables = int(verify.data[0][0]) if verify.data else 0
            except Exception:
                n_tables = 0
            print(f"[PyRQG] Applied random schema bundle (tables={schema_num_tables}, functions={schema_num_functions}, views={schema_num_views}); found {n_tables} tables in schema '{schema_name}'.")
            if n_tables == 0:
                print(f"[PyRQG] Hint: In psql, run `SET search_path TO {schema_name}, public;` then `\\dt` to list tables in the '{schema_name}' schema.")
        except Exception as e:
            print(f"[PyRQG] Warning: failed to apply generated schema: {e}")

    # Create executor factory so each thread gets its own connection
    def _make_executor():
        try:
            from pyrqg.core.filtered_executor import create_filtered_executor  # type: ignore
            if use_filter:
                return create_filtered_executor(dsn)
        except Exception:
            pass
        from pyrqg.core.executor import create_executor
        return create_executor(dsn)

    # Optional: prepare schema for selected grammars by introspecting Grammar metadata
    def _infer_sql_type(col_name: str) -> str:
        n = col_name.lower()
        if n == 'pk' or n.endswith('_pk'):
            return 'BIGINT'
        if 'bigint' in n:
            return 'BIGINT'
        if 'int' in n:
            return 'INT'
        if 'decimal' in n or 'numeric' in n:
            return 'DECIMAL(18,4)'
        if 'double' in n:
            return 'DOUBLE PRECISION'
        if 'float' in n:
            return 'REAL'
        if 'char_10' in n:
            return 'CHAR(10)'
        if 'char_255' in n:
            return 'CHAR(255)'
        if 'varchar_10' in n:
            return 'VARCHAR(10)'
        if 'varchar_255' in n:
            return 'VARCHAR(255)'
        if 'text' in n:
            return 'TEXT'
        # default fallback
        return 'TEXT'

    def _generate_schema_ddls_for_grammar(grammar_obj) -> List[str]:
        ddls: List[str] = []
        # Try to get defined tables/fields from DSL Grammar context
        try:
            ctx = getattr(grammar_obj, 'context', None)
            table_names = []
            fields = []
            if ctx is not None:
                table_names = list(getattr(ctx, 'tables', {}).keys())
                fields = list(getattr(ctx, 'fields', []) or [])
            if not table_names:
                return ddls
            # Ensure at least a primary key column exists if known
            cols_per_table = []
            if fields:
                # Build column definitions
                col_defs = []
                for f in fields:
                    sql_type = _infer_sql_type(f)
                    if f.lower() == 'pk':
                        col_defs.append(f"{f} {sql_type} PRIMARY KEY")
                    else:
                        col_defs.append(f"{f} {sql_type}")
                cols_per_table = col_defs
            else:
                # Minimal default
                cols_per_table = [
                    "pk BIGINT PRIMARY KEY",
                    "col_int INT",
                    "col_bigint BIGINT",
                    "col_char_10 CHAR(10)",
                    "col_varchar_10 VARCHAR(10)"
                ]
            for t in table_names:
                ddl = f"CREATE TABLE IF NOT EXISTS {t} (\n  " + ",\n  ".join(cols_per_table) + "\n)"
                ddls.append(ddl)
        except Exception:
            # Best-effort only
            pass
        return ddls

    def _maybe_prepare_schema():
        # Auto-prepare schema by default unless explicitly disabled.
        # Recognize these flags:
        #   --no-prepare-schema           -> disable auto-preparation
        #   --prepare-schema [target]     -> enable and optionally filter by target substring
        #   --init-schema / --create-schema -> aliases for --prepare-schema
        prepare_flags = {"--prepare-schema", "--init-schema", "--create-schema"}
        no_prepare_flag = "--no-prepare-schema"

        prepare = True  # default: prepare schema
        prepare_value = None

        i = 0
        while i < len(fw):
            tok = fw[i]
            if tok == no_prepare_flag:
                prepare = False
            elif tok in prepare_flags:
                prepare = True
                # Support optional value: --prepare-schema <name>
                if i + 1 < len(fw) and not fw[i + 1].startswith("--"):
                    prepare_value = fw[i + 1]
                    i += 1
            elif tok.startswith("--prepare-schema="):
                prepare = True
                prepare_value = tok.split("=", 1)[1]
            i += 1

        if not prepare:
            print("[PyRQG] Schema auto-preparation disabled (--no-prepare-schema).")
            return

        target = (prepare_value or "").strip().lower()
        # Collect ddls for all selected grammars; if target specified, filter by it
        selected = [g for g in grammars if (not target or target in g.lower())]
        if not selected:
            selected = grammars

        all_ddls: List[str] = []
        for gname in selected:
            gobj = rqg.grammars.get(gname)
            if not gobj:
                continue
            ddls = _generate_schema_ddls_for_grammar(gobj)
            if ddls:
                print(f"[PyRQG] Preparing schema for grammar '{gname}' ({len(ddls)} tables)")
                all_ddls.extend(ddls)
            else:
                print(f"[PyRQG] Grammar '{gname}' does not expose tables via DSL metadata; skipping schema prep.")

        if not all_ddls:
            print("[PyRQG] No schema could be inferred for the selected grammars. Skipping schema preparation.")
            return

        # Use a dedicated executor for setup and ensure schema context
        setup_exec = _make_executor()
        try:
            setup_exec.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}; SET search_path TO {schema_name}, public;")
        except Exception:
            pass
        try:
            for ddl in all_ddls:
                setup_exec.execute(ddl + ";")
        except Exception as e:
            # Non-fatal: continue even if some tables already exist or errors occur
            print(f"[PyRQG] Warning: schema preparation encountered an error: {e}")
        else:
            print("[PyRQG] Schema preparation completed.")

    # Execution counters
    executed = 0
    syntax_errors = 0
    error_samples = []  # list of (query, err)
    unique_queries = set()  # track unique queries submitted for execution

    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Thread-local storage for executors
    _tls = threading.local()

    def _get_executor():
        ex = getattr(_tls, 'executor', None)
        if ex is None:
            _tls.executor = _make_executor()
            ex = _tls.executor
        return ex

    # Possibly prepare schema before starting workload
    _maybe_prepare_schema()

    def _run_query(qi_pair):
        i, q = qi_pair
        ex = _get_executor()
        res = ex.execute(q)
        return (i, res)

    start_time = time.time()
    last_heartbeat = start_time

    # Decide threading
    workers = max(1, threads or 1)

    # Generate and dispatch in chunks to bound memory
    gi = 0
    batch_base = 0
    CHUNK = 1000

    # Backpressure: max outstanding futures
    MAX_OUTSTANDING = max(workers * 4, 32)
    futures = []

    def _drain_done(futs):
        nonlocal executed, syntax_errors, error_samples
        for f in list(futs):
            if f.done():
                futs.remove(f)
                try:
                    _, res = f.result()
                    executed += 1
                    if res.status == Status.SYNTAX_ERROR:
                        syntax_errors += 1
                        # Collect a sample of syntax errors regardless of --print-errors to aid debugging
                        if len(error_samples) < max_samples:
                            error_samples.append((res.query, res.errstr))
                except Exception as e:
                    executed += 1
                    # Count unexpected exceptions as unknown errors
                    if print_errors and len(error_samples) < max_samples:
                        error_samples.append(("<internal>", str(e)))
                if progress_every and executed % progress_every == 0:
                    elapsed = max(1e-6, time.time() - start_time)
                    qps = executed / elapsed
                    print(f"Progress: executed={executed}, syntax_errors={syntax_errors}, qps={qps:.1f}", flush=True)

    # Watchdog to report long-running queries (full formatted SQL)
    from pyrqg.core.watchdog import QueryWatchdog
    watch_threshold = float(getattr(args, 'watch_threshold', 300) or 300)
    watch_interval = max(0.5, float(getattr(args, 'watch_interval', 5) or 5))
    watchdog = QueryWatchdog(threshold_s=watch_threshold, interval_s=watch_interval)
    watchdog.start()

    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            stop_submitting = False
            while (remaining > 0 or (time_mode and not stop_submitting)) or futures:
                # Generate next batch if needed
                if not stop_submitting and (remaining > 0 or time_mode):
                    if time_mode and (time.time() - start_time) >= duration:
                        stop_submitting = True
                    else:
                        gname = grammars[gi % len(grammars)]
                        to_gen = CHUNK if time_mode else min(CHUNK, remaining)
                        cur_seed = (seed + batch_base) if (seed is not None) else None
                        queries = rqg.generate_from_grammar(gname, rule=(grammar_rule or "query"), count=to_gen, seed=cur_seed)
                        # Submit
                        for q in queries:
                            if time_mode and (time.time() - start_time) >= duration:
                                stop_submitting = True
                                break
                            q_sql = q.rstrip(';') + ';'
                            if echo_queries:
                                print(f"[{executed + 1}] {q_sql}")
                            # Control outstanding futures
                            while len(futures) >= MAX_OUTSTANDING:
                                _drain_done(futures)
                                if len(futures) >= MAX_OUTSTANDING:
                                    time.sleep(0.001)
                            unique_queries.add(q_sql)
                            fut = pool.submit(_run_query, (executed + len(futures) + 1, q_sql))
                            watchdog.register(fut, q_sql)
                            futures.append(fut)
                        if not time_mode:
                            remaining -= to_gen
                        gi += 1
                        batch_base += 1
                # Drain any completed tasks
                _drain_done(futures)

                # Time-based heartbeat when --progress-every is 0
                if not progress_every:
                    now = time.time()
                    if now - last_heartbeat >= 5.0:
                        elapsed = max(1e-6, now - start_time)
                        qps = executed / elapsed
                        print(f"Heartbeat: executed={executed}, syntax_errors={syntax_errors}, qps={qps:.1f}, outstanding={len(futures)}, remaining~={remaining}", flush=True)
                        last_heartbeat = now

            # Final drain
            while futures:
                _drain_done(futures)
                if futures:
                    time.sleep(0.001)
    except KeyboardInterrupt:
        print("\n[PyRQG] Interrupt received. Stopping...", flush=True)
    finally:
        # Stop watchdog
        watchdog.stop()
    
    elapsed = max(1e-6, time.time() - start_time)
    qps = executed / elapsed
    uniq = len(unique_queries)
    if time_mode:
        print(f"[PyRQG] Production(custom) executed {executed} queries in ~{int(elapsed)}s with {syntax_errors} syntax errors at {qps:.1f} qps, unique={uniq}.")
    else:
        print(f"[PyRQG] Production(custom) executed {executed} queries with {syntax_errors} syntax errors at {qps:.1f} qps, unique={uniq}.")
    if error_samples:
        print("\nSyntax error samples (showing", len(error_samples), "of up to", max_samples, "):")
        for i, (q, err) in enumerate(error_samples, 1):
            print(f"[{i}] Error: {err}")
            print(f"    Query: {q}")
        if not print_errors and syntax_errors > len(error_samples):
            print(f"\nTip: use --print-errors to enable sampling during execution and --error-samples N to control how many errors to show (default {max_samples}).")

    # If output is set, also write the generated SQL to file for audit
    if args.output:
        # Re-generate deterministically using the same approach for a small header sample only (avoid doubling cost)
        # Here we just note that execution was performed.
        Path(args.output).write_text(f"-- Executed {executed} queries from grammars {','.join(grammars)} in custom production mode.\n", encoding='utf-8')
        print(f"[PyRQG] Note: Wrote execution summary to {args.output}")

    return executed


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

    # Two modes supported:
    # 1) Single-file scenario module (preferred): a .py file exposing `schema_sql` (str)
    #    or `schema_files` (list[str]) and `grammar` (Grammar) or `g`.
    # 2) Legacy keyword resolution: find schema under production_scenarios/schemas and workload under .../workloads.

    ddl_sql: str = ""
    scenario_queries: List[str] = []
    rqg = RQG()
    total = args.count or 1000
    seed = args.seed

    scenario_arg = args.production_scenario
    as_path = Path(scenario_arg)

    if as_path.suffix == ".py" and as_path.exists():
        # Load single-file scenario module
        from importlib.util import spec_from_file_location, module_from_spec
        spec = spec_from_file_location("single_file_scenario", str(as_path))
        module = module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)

        # Schema: either inline `schema_sql` or list `schema_files` under ./schem.files
        schema_sql = getattr(module, 'schema_sql', None)
        if isinstance(schema_sql, str) and schema_sql.strip():
            ddl_sql = schema_sql.strip()
        else:
            files = getattr(module, 'schema_files', None)
            if not files:
                raise FileNotFoundError("Single-file scenario must define `schema_sql` or `schema_files`.")
            base = Path.cwd() / 'schem.files'
            parts = []
            for f in files:
                p = base / f
                if not p.exists():
                    raise FileNotFoundError(f"Schema file not found: {p}")
                parts.append(p.read_text(encoding='utf-8').strip())
            ddl_sql = "\n\n".join(parts)

        # Grammar
        grammar = getattr(module, 'grammar', getattr(module, 'g', None))
        if grammar is None:
            raise ValueError("Single-file scenario must export `grammar` (or `g`).")
        name = as_path.stem
        rqg.add_grammar(name, grammar)
        scenario_count = total
        scenario_queries = rqg.generate_from_grammar(name, count=scenario_count, seed=seed)
    else:
        # Legacy keyword: resolve schema and workload files by substring match
        schema_path, workload_path = _resolve_scenario_files(scenario_arg)
        if schema_path is None:
            raise FileNotFoundError(
                f"No schema found for scenario keyword '{scenario_arg}' in production_scenarios/schemas or single-file path not found"
            )
        ddl_sql = Path(schema_path).read_text(encoding='utf-8').strip()

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

    # Optional restriction to specific query types (e.g., INSERT only)
    query_types = None
    if getattr(args, 'query_types', None):
        # Normalize and validate
        raw = [x.strip().upper() for x in (args.query_types or '').split(',') if x.strip()]
        allowed = {"SELECT", "INSERT", "UPDATE", "DELETE"}
        query_types = [x for x in raw if x in allowed] or None

    for _ in range(total):
        qobj = qgen.generate_batch(1, query_types=query_types)[0]
        q = qobj.sql
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


def run_random(args) -> int:
    """One-shot random generator that emits schema, constraints/functions, data, and workload."""
    rqg = RQG()
    parts: List[str] = []

    # 1) Random schema
    schema_sql = rqg.generate_random_schema(num_tables=getattr(args, 'num_tables', 5))
    parts.append("-- Schema\n" + ";\n".join(schema_sql) + ";")

    # 2) Constraints and functions
    constraints = getattr(args, 'constraints', 10) or 0
    functions = getattr(args, 'functions', 5) or 0
    if constraints or functions:
        cf_sql = rqg.generate_random_constraints_and_functions(constraints=constraints, functions=functions, seed=args.seed)
        parts.append("-- Constraints & Functions\n" + ";\n".join(cf_sql) + ";")

    # 3) Random data inserts
    rows_per_table = getattr(args, 'rows_per_table', 10)
    data_sql = rqg.generate_random_data_inserts(rows_per_table=rows_per_table, seed=args.seed, multi_row=False, on_conflict=True)
    parts.append("-- Data Inserts\n" + ";\n".join(data_sql) + ";")

    # 4) Mixed workload with function calls
    workload_count = getattr(args, 'workload_count', getattr(args, 'count', 50))
    workload_sql = rqg.run_mixed_workload(count=workload_count, seed=args.seed, include_functions=True)
    parts.append("-- Workload\n" + ";\n".join(workload_sql) + ";")

    output_text = "\n\n".join(parts) + "\n"
    if args.output:
        Path(args.output).write_text(output_text, encoding="utf-8")
        print(f"Random generation written to {args.output}")
    else:
        print(output_text)
    return len(output_text)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="PyRQG Runner - yugabyte-focused",
    )

    parser.add_argument("mode", nargs='?', default='list', choices=["ddl", "grammar", "production", "scenario", "list", "exec", "random"], help="Runner mode (default: list)")
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
    parser.add_argument("--grammar-rule", dest="grammar_rule", default="query", help="Grammar rule to generate from (default: query)")
    parser.add_argument("--count", type=int, default=10, help="How many queries to generate")

    # Random mode options
    parser.add_argument("--constraints", type=int, default=10, help="Number of constraint DDL statements to generate (random mode)")
    parser.add_argument("--functions", type=int, default=5, help="Number of function/procedure DDL statements (random mode)")
    parser.add_argument("--rows-per-table", dest="rows_per_table", type=int, default=10, help="Rows per table for data inserts (random mode)")
    parser.add_argument("--workload-count", dest="workload_count", type=int, default=50, help="Number of workload queries (random mode)")

    # Scenario mode options
    parser.add_argument("--file", help="Path to a production scenario grammar file")
    parser.add_argument("--name", help="Name for the loaded scenario grammar")

    # Production scenario options
    parser.add_argument("--production-scenario", dest="production_scenario", help="Name keyword of production scenario (e.g., bank, ecommerce). With mode=production, will emit scenario DDL and mixed workload queries.")

    # Exec mode options (execute against a live PostgreSQL/YugabyteDB database)
    parser.add_argument("--dsn", help="PostgreSQL-compatible DSN for exec mode, e.g. postgresql://user:pass@localhost:5433/postgres (YugabyteDB)")
    parser.add_argument("--use-filter", dest="use_filter", action="store_true", help="Use PostgreSQL compatibility filter before executing queries")
    parser.add_argument("--alters-per-table", dest="alters_per_table", type=int, default=2, help="Max ALTERs per table in exec mode")
    parser.add_argument("--query-types", dest="query_types", help="Comma-separated query types to execute in exec mode (e.g., INSERT or SELECT,INSERT). Default mixed.")
    parser.add_argument("--print-errors", dest="print_errors", action="store_true", help="Print sample SQL syntax errors encountered during execution")
    parser.add_argument("--error-samples", dest="error_samples", type=int, default=10, help="Max number of error samples to print with --print-errors")
    parser.add_argument("--echo-queries", dest="echo_queries", action="store_true", help="Echo each executed query to stdout")
    parser.add_argument("--progress-every", dest="progress_every", type=int, default=0, help="Print a progress line every N executed queries (0=disable)")
    parser.add_argument("--duration", type=int, default=0, help="Run for N seconds instead of a fixed count in production mode (forwarded)")
    parser.add_argument("--print-duplicates", dest="print_duplicates", action="store_true", help="Collect/print duplicates when high duplicate rate is detected (production; forwarded)")
    parser.add_argument("--duplicates-output", dest="duplicates_output", help="Optional file to write duplicates (production; forwarded)")
    # Schema generation knobs (also parsed from --custom extras)
    parser.add_argument("--schema-name", dest="schema_name", default="pyrqg", help="Target schema name for production/scenario (default: pyrqg)")
    parser.add_argument("--schema-profile", dest="schema_profile", help="Schema type profile: core, json_heavy, time_series, network_heavy, wide_range")
    parser.add_argument("--fk-ratio", dest="fk_ratio", type=float, help="Cross-table FK density (0..1)")
    parser.add_argument("--index-ratio", dest="index_ratio", type=float, help="Index density per table (0..1)")
    parser.add_argument("--composite-index-ratio", dest="composite_index_ratio", type=float, help="Probability of composite indexes (0..1)")
    parser.add_argument("--partial-index-ratio", dest="partial_index_ratio", type=float, help="Probability of partial indexes (0..1)")
    # Watchdog for long-running queries
    parser.add_argument("--watch-threshold", dest="watch_threshold", type=float, default=300, help="Seconds before reporting a long-running query (default: 300)")
    parser.add_argument("--watch-interval", dest="watch_interval", type=float, default=5, help="Watchdog poll interval in seconds (default: 5)")

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
    elif args.mode == "random":
        return run_random(args)

    parser.error("Unknown mode")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
