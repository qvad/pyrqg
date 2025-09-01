import time
import random
"""
Concurrent Isolation Testing Grammar for PostgreSQL
Tests transaction isolation levels, locking, MVCC, deadlocks, and concurrency edge cases
"""

import sys
from pathlib import Path

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, Lambda
from pyrqg.perfect_schema_registry import get_perfect_registry

# Uniqueness helpers
def random_suffix():
    """Generate unique suffix"""
    return f"_{int(time.time() * 1000) % 1000000}_{random.randint(1000, 9999)}"

def random_id():
    """Generate high-entropy ID"""
    return random.randint(1, 10000000)

try:
    import psycopg2  # type: ignore
except Exception:  # psycopg2 optional at import-time; functions fallback safely
    psycopg2 = None

# Get real database schema for isolation testing
def get_test_schema():
    """Fetch tables suitable for concurrency testing"""
    try:
        conn = psycopg2.connect("dbname=postgres")
        cur = conn.cursor()
        
        # Get tables with numeric columns (for increment operations)
        cur.execute("""
            SELECT DISTINCT 
                t.table_name,
                array_agg(c.column_name) FILTER (WHERE c.data_type IN ('integer', 'bigint', 'numeric')) as numeric_cols,
                array_agg(c.column_name) FILTER (WHERE c.data_type IN ('character varying', 'text')) as text_cols
            FROM information_schema.tables t
            JOIN information_schema.columns c ON t.table_name = c.table_name
            WHERE t.table_schema = 'public' 
            AND t.table_type = 'BASE TABLE'
            AND t.table_name NOT LIKE 'table_%'
            GROUP BY t.table_name
            HAVING COUNT(c.column_name) > 2
            LIMIT 10
        """)
        
        tables_info = []
        for table, num_cols, text_cols in cur.fetchall():
            tables_info.append({
                'table': table,
                'numeric_columns': num_cols or ['id'],
                'text_columns': text_cols or ['name']
            })
        
        # Get tables with foreign key relationships
        cur.execute("""
            SELECT DISTINCT
                tc.table_name as child_table,
                kcu.column_name as fk_column,
                ccu.table_name as parent_table,
                ccu.column_name as parent_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu 
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
            LIMIT 5
        """)
        
        fk_relationships = [row for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return tables_info, fk_relationships
    except:
        # Fallback
        return (
            [
                {'table': 'users', 'numeric_columns': ['id'], 'text_columns': ['name']},
                {'table': 'products', 'numeric_columns': ['id', 'price'], 'text_columns': ['name']},
                {'table': 'orders', 'numeric_columns': ['id', 'quantity'], 'text_columns': []}
            ],
            []
        )

tables_info, fk_relationships = get_test_schema()

# Extract useful values
tables = [t['table'] for t in tables_info]
numeric_columns = []
for t in tables_info:
    numeric_columns.extend(t['numeric_columns'])
numeric_columns = list(set(numeric_columns))

# Ensure we have some values
if not tables:
    tables = ["users", "products", "orders"]
if not numeric_columns:
    numeric_columns = ["id", "quantity", "price"]

g = Grammar("concurrent_isolation_testing")

# ============ Schema-aware safe generators ============
def _pick_table(ctx):
    reg = get_perfect_registry()
    ts = reg.get_tables()
    return ctx.rng.choice(ts) if ts else "public"

def _pick_numeric(ctx, t):
    reg = get_perfect_registry()
    cols = reg.get_insertable_columns(t)
    tname = t.split('.')[-1] if '.' in t else t
    nums = []
    for c in cols:
        dt = reg.column_types.get(f"{tname}.{c}")
        if dt in ('integer','bigint','numeric','decimal','real','double precision'):
            nums.append(c)
    return ctx.rng.choice(nums) if nums else (cols[0] if cols else 'id')

def _pick_pk(ctx, t):
    reg = get_perfect_registry()
    for c in ('id','pk','row_id','pk_id','record_id'):
        if reg.column_exists(t, c):
            return c
    cols = reg.get_insertable_columns(t)
    return cols[0] if cols else 'id'

def _safe_iso(ctx):
    t = _pick_table(ctx)
    num = _pick_numeric(ctx, t)
    pk = _pick_pk(ctx, t)
    lvl = choice("READ COMMITTED","REPEATABLE READ","SERIALIZABLE").generate(ctx)
    return f"BEGIN ISOLATION LEVEL {lvl};\nSELECT SUM({num}) FROM {t};\nUPDATE {t} SET {num} = {num} + 1 WHERE {pk} = 1;\nCOMMIT;"

def _safe_nowait(ctx):
    t = _pick_table(ctx)
    num = _pick_numeric(ctx, t)
    pk = _pick_pk(ctx, t)
    return f"BEGIN;\nSELECT * FROM {t} WHERE {pk} = 1 FOR UPDATE NOWAIT;\nUPDATE {t} SET {num} = {num} * 2 WHERE {pk} = 1;\nCOMMIT;"

def _safe_lost_update(ctx):
    t = _pick_table(ctx)
    num = _pick_numeric(ctx, t)
    pk = _pick_pk(ctx, t)
    return (f"-- Lost update test\nSELECT {num} INTO TEMP lost_update_test FROM {t} WHERE {pk} = 1;\n"
            f"UPDATE {t} SET {num} = (SELECT {num} FROM lost_update_test) + 1 WHERE {pk} = 1;")

def _safe_predicate(ctx):
    t = _pick_table(ctx)
    num = _pick_numeric(ctx, t)
    a = ctx.rng.randint(10,20)
    b = a + ctx.rng.randint(1,10)
    ins = ctx.rng.randint(a, b)
    return (f"BEGIN ISOLATION LEVEL SERIALIZABLE;\nSELECT * FROM {t} WHERE {num} BETWEEN {a} AND {b};\n"
            f"INSERT INTO {t} ({num}) VALUES ({ins});\nCOMMIT;")

g.rule("safe_iso", Lambda(_safe_iso))
g.rule("safe_nowait", Lambda(_safe_nowait))
g.rule("safe_lost_update", Lambda(_safe_lost_update))
g.rule("safe_predicate", Lambda(_safe_predicate))

# Override main rule with safe schema-aware variants
g.rule("query", choice(ref("safe_iso"), ref("safe_nowait"), ref("safe_lost_update"), ref("safe_predicate"), weights=[30,30,20,20]))

# Transaction isolation level tests
g.rule("isolation_level_tests",
    choice(
        # Read uncommitted phantom reads
        template("""-- READ UNCOMMITTED phantom read test
BEGIN ISOLATION LEVEL READ UNCOMMITTED;
SELECT COUNT(*) FROM {table} WHERE {numeric_column} > 0;
-- Another transaction would INSERT here
SELECT COUNT(*) FROM {table} WHERE {numeric_column} > 0;
COMMIT;"""),
        
        # Read committed non-repeatable read
        template("""-- READ COMMITTED non-repeatable read test
BEGIN ISOLATION LEVEL READ COMMITTED;
SELECT {numeric_column} FROM {table} WHERE {pk_column} = 1;
-- Another transaction would UPDATE here
SELECT {numeric_column} FROM {table} WHERE {pk_column} = 1;
COMMIT;"""),
        
        # Repeatable read phantom
        template("""-- REPEATABLE READ phantom read test
BEGIN ISOLATION LEVEL REPEATABLE READ;
-- Snapshot taken here
SELECT COUNT(*) FROM {table} WHERE {numeric_column} BETWEEN 1 AND 100;
-- Another transaction INSERTs in range
SELECT COUNT(*) FROM {table} WHERE {numeric_column} BETWEEN 1 AND 100;
COMMIT;"""),
        
        # Serializable test
        template("""-- SERIALIZABLE isolation test
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT SUM({numeric_column}) FROM {table};
UPDATE {table} SET {numeric_column} = {numeric_column} + 1 WHERE {pk_column} = 1;
COMMIT;""")
    )
)

# Locking mechanism tests
g.rule("locking_tests",
    choice(
        # Row-level locking
        template("""-- Row-level lock test
BEGIN;
SELECT * FROM {table} WHERE {pk_column} = 1 FOR UPDATE;
UPDATE {table} SET {numeric_column} = {numeric_column} + 1 WHERE {pk_column} = 1;
COMMIT;"""),
        
        # Shared locks
        template("""-- Shared lock test
BEGIN;
SELECT * FROM {table} WHERE {numeric_column} > 0 FOR SHARE;
-- Other transactions can read but not write
COMMIT;"""),
        
        # Nowait locking
        template("""-- NOWAIT lock test
BEGIN;
SELECT * FROM {table} WHERE {pk_column} = 1 FOR UPDATE NOWAIT;
UPDATE {table} SET {numeric_column} = {numeric_column} * 2 WHERE {pk_column} = 1;
COMMIT;"""),
        
        # Skip locked
        template("""-- SKIP LOCKED test
BEGIN;
SELECT * FROM {table} 
WHERE {numeric_column} > 0 
ORDER BY {pk_column}
FOR UPDATE SKIP LOCKED
LIMIT 1;
COMMIT;"""),
        
        # Advisory locks
        template("""-- Advisory lock test
SELECT pg_advisory_lock({lock_id});
-- Do work while holding lock
UPDATE {table} SET {numeric_column} = {numeric_column} + 1 WHERE {pk_column} = 1;
SELECT pg_advisory_unlock({lock_id});""")
    )
)

# MVCC behavior tests
g.rule("mvcc_tests",
    choice(
        # Snapshot visibility
        template("""-- MVCC snapshot test
BEGIN ISOLATION LEVEL REPEATABLE READ;
-- Take snapshot
SELECT txid_current(), COUNT(*) FROM {table};
-- Changes by other transactions not visible
SELECT COUNT(*) FROM {table};
COMMIT;"""),
        
        # Update chains
        template("""-- HOT (Heap Only Tuple) update test
-- Update non-indexed column to trigger HOT
UPDATE {table} 
SET {text_column} = {text_column} || '_updated' 
WHERE {pk_column} = 1;"""),
        
        # Vacuum visibility
        template("""-- Dead tuple visibility test
DELETE FROM {table} WHERE {pk_column} > 1000000;
-- Dead tuples still visible to older transactions
SELECT COUNT(*) FROM {table};""")
    )
)

# Deadlock scenarios
g.rule("deadlock_scenarios",
    choice(
        # Classic A-B B-A deadlock
        template("""-- Classic deadlock pattern (Transaction 1)
BEGIN;
UPDATE {table1} SET {numeric_column} = {numeric_column} + 1 WHERE {pk_column} = 1;
-- Transaction 2 would update table2 then table1
UPDATE {table2} SET {numeric_column} = {numeric_column} + 1 WHERE {pk_column} = 1;
COMMIT;"""),
        
        # Foreign key deadlock
        template("""-- Foreign key induced deadlock
BEGIN;
-- Lock parent row
SELECT * FROM {parent_table} WHERE {pk_column} = 1 FOR UPDATE;
-- Try to insert child that references locked parent
INSERT INTO {child_table} ({fk_column}) VALUES (1);
COMMIT;"""),
        
        # Index order deadlock
        template("""-- Index-order contention pattern (PostgreSQL-compatible)
BEGIN;
UPDATE {table} SET {numeric_column} = {numeric_column} + 1 
WHERE {pk_column} IN (1, 2, 3);
COMMIT;""")
    )
)

# Race condition tests
g.rule("race_condition_tests",
    choice(
        # Lost update
        template("""-- Lost update test
-- Read
SELECT {numeric_column} INTO TEMP lost_update_test FROM {table} WHERE {pk_column} = 1;
-- Modify (another transaction could update between)
UPDATE {table} 
SET {numeric_column} = (SELECT * FROM lost_update_test) + 1 
WHERE {pk_column} = 1;"""),
        
        # Check-then-act
        template("""-- Check-then-act race condition
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM {table} WHERE {pk_column} = 999) THEN
        -- Race: another transaction could insert between check and act
        INSERT INTO {table} ({pk_column}, {numeric_column}) VALUES (999, 1);
    END IF;
END $$;"""),
        
        # Read-modify-write
        template("""-- Read-modify-write with SELECT FOR UPDATE
BEGIN;
SELECT * FROM {table} WHERE {pk_column} = 1 FOR UPDATE;
UPDATE {table} 
SET {numeric_column} = {numeric_column} + 1 
WHERE {pk_column} = 1;
COMMIT;""")
    )
)

# Serialization anomaly tests
g.rule("serialization_anomalies",
    choice(
        # Write skew
        template("""-- Write skew anomaly test
BEGIN ISOLATION LEVEL SERIALIZABLE;
-- Two accounts should sum to 100
SELECT SUM({numeric_column}) FROM {table} WHERE {pk_column} IN (1, 2);
-- Withdraw from account 1
UPDATE {table} SET {numeric_column} = {numeric_column} - 10 WHERE {pk_column} = 1;
COMMIT;"""),
        
        # Read-only anomaly
        template("""-- Read-only serialization anomaly
BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY;
SELECT COUNT(*) as before FROM {table} WHERE {numeric_column} > 50;
-- Anomaly: result depends on concurrent transactions
SELECT COUNT(*) as after FROM {table} WHERE {numeric_column} > 50;
COMMIT;"""),
        
        # Predicate lock test
        template("""-- Predicate lock test
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT * FROM {table} WHERE {numeric_column} BETWEEN 10 AND 20;
INSERT INTO {table} ({numeric_column}) VALUES (15);
COMMIT;""")
    )
)

# Table and column rules
g.rule("table", choice(*tables) if tables else "users")
g.rule("table1", choice(*tables[:len(tables)//2]) if len(tables) > 1 else tables[0])
g.rule("table2", choice(*tables[len(tables)//2:]) if len(tables) > 1 else tables[0])

# Handle FK relationships
if fk_relationships:
    g.rule("parent_table", choice(*[fk[2] for fk in fk_relationships]))
    g.rule("child_table", choice(*[fk[0] for fk in fk_relationships]))
    g.rule("fk_column", choice(*[fk[1] for fk in fk_relationships]))
else:
    g.rule("parent_table", tables[0])
    g.rule("child_table", tables[1] if len(tables) > 1 else tables[0])
    g.rule("fk_column", "id")

# Column rules
g.rule("pk_column", choice("id", "pk_id", "row_id", "primary_key", "record_id"))
g.rule("numeric_column", choice(*numeric_columns))
g.rule("text_column", choice("name", "description", "status", "title", "content", "data", "info", "details", "notes", "comment", "value", "text_data"))

# Lock IDs for advisory locks
g.rule("lock_id", number(1, 1000000))


# Add more randomization through template parameters
g.rule("random_num", lambda: random.randint(1, 1000000))
g.rule("random_suffix", lambda: f"_{random.randint(1000, 9999)}")
g.rule("timestamp", lambda: int(time.time() * 1000))

# Export grammar
grammar = g
