#!/usr/bin/env python3
"""
YugabyteDB Transaction Grammar using Simple Python DSL

This is a complete rewrite of transactions_postgres.yy using our simple DSL.
Notice how much cleaner and easier to understand this is!
"""

import sys
from pathlib import Path

from pyrqg.dsl.core import (
    Grammar, choice, maybe, repeat, template, table, field, 
    number, digit, ref, Lambda
)

# ============================================================================
# Create and Configure Grammar
# ============================================================================

g = Grammar("yugabyte_transactions")

# Define tables with their row counts (from original .zz file)
g.define_tables(
    A=10, B=20, C=100, D=100, E=0,
    AA=50, BB=20, CC=300, DD=10,
    AAA=10, BBB=10, CCC=100, DDD=1000
)

# Define fields
g.define_fields(
    'pk',           # Primary key
    'col_int',      # Integer column
    'col_int_key',  # Indexed integer column
)

# ============================================================================
# Grammar Rules - Declarative and Simple!
# ============================================================================

# Main query structure
def _build_txn(ctx):
    stmts = []
    if ctx.rng.random() < 0.5:
        stmts.append("START TRANSACTION;")
    if ctx.rng.random() < 0.2:
        stmts.append("SAVEPOINT SP1;")
    stmts.append(g.rules['body'].generate(ctx) + ";")
    if ctx.rng.random() < 0.2:
        stmts.append("SAVEPOINT SP1;")
    if ctx.rng.random() < 0.3:
        stmts.append(g.rules['body'].generate(ctx) + ";")
    if ctx.rng.random() < 0.25:
        stmts.append("ROLLBACK TO SAVEPOINT SP1;")
    if ctx.rng.random() < 0.5:
        stmts.append(ctx.rng.choice(["COMMIT;", "ROLLBACK; "]))
    return "\n".join(stmts)

g.rule("query", Lambda(_build_txn))

# Transaction control
g.rule("start_transaction", "START TRANSACTION ; ")
g.rule("commit_rollback", choice("COMMIT", "ROLLBACK"))
g.rule("savepoint", template("SAVEPOINT SP{n} ; ", n=digit()))
g.rule("rollback_to_savepoint", template("ROLLBACK TO SAVEPOINT SP{n} ; ", n=digit()))

# Body - main operations
g.rule("body",
    choice(
        ref("update_all"),
        ref("update_multi"),
        ref("update_one"),
        ref("update_between"),
        ref("update_in"),
        ref("insert_one"),
        ref("insert_multi"),
        ref("insert_select"),
        ref("insert_delete"),
        ref("upsert"),
        ref("delete_one"),
        ref("delete_multi"),
        weights=[1, 2, 1, 1, 1, 2, 1, 1, 1, 2, 1, 1]
    )
)

# ============================================================================
# UPDATE Operations
# ============================================================================

# Update all rows - zero sum operation
g.rule("update_all",
    template("UPDATE {table} SET col_int_key = col_int_key - 20, col_int = col_int + 20",
        table=table()
    )
)

# Update multiple rows
g.rule("update_multi",
    template("UPDATE {table} SET col_int = col_int + 30, col_int_key = col_int_key - 30 WHERE {field} > {n}",
        table=table(),
        field=choice("col_int_key", "col_int", "pk"),
        n=digit()
    )
)

# Update single row
g.rule("update_one",
    template("UPDATE {table} SET col_int_key = col_int_key - 20, col_int = col_int + 20 WHERE pk = {n}",
        table=table(),
        n=digit()
    )
)

# Update with BETWEEN
g.rule("update_between",
    template("UPDATE {table} SET col_int = col_int + 30, col_int_key = col_int_key - 30 WHERE pk BETWEEN {n1} AND {n2}",
        table=table(),
        n1=number(1, 8),
        n2=Lambda(lambda ctx: str(int(ctx.rng.randint(1, 8)) + 1))
    )
)

# Update with IN and CASE
g.rule("update_in",
    choice(
        # Dynamic digits
        template(
            "UPDATE {table} SET col_int_key = col_int_key + CASE WHEN pk % 2 = 1 THEN {d1} ELSE -{d2} END WHERE pk IN ( {n1} , {n2} )",
            table=table(),
            d1=digit(),
            d2=digit(),
            n1=digit(),
            n2=digit()
        ),
        # Even/odd pattern
        Lambda(lambda ctx: 
            f"UPDATE {ctx.rng.choice(list(ctx.tables.keys()))} SET col_int_key = col_int_key + " +
            f"CASE WHEN pk % 2 = 1 THEN 30 ELSE -30 END WHERE pk IN ( {ctx.rng.randint(0,4)*2} , {ctx.rng.randint(0,4)*2+1} )"
        )
    )
)

# ============================================================================
# INSERT Operations  
# ============================================================================

# Simple insert
g.rule("insert_one",
    choice(
        template("INSERT INTO {table} ( pk , col_int_key , col_int) VALUES ( DEFAULT , 100 , 100 )",
            table=table()
        ),
        template("INSERT INTO {table} ( pk ) VALUES ( DEFAULT ) ; {action}",
            table=table(),
            action=choice("COMMIT", "ROLLBACK")
        ),
        weights=[4, 1]
    )
)

# Multi-row insert
g.rule("insert_multi",
    template("INSERT INTO {table} ( pk , col_int_key , col_int) VALUES ( DEFAULT , 100 , 100 ) , ( DEFAULT , 100 , 100 )",
        table=table()
    )
)

# INSERT ... SELECT
g.rule("insert_select",
    template("INSERT INTO {t1} ( col_int_key , col_int ) SELECT col_int , col_int_key FROM {t2} WHERE pk > 10",
        t1=table(),
        t2=table()
    )
)

# Insert then delete - tests lastval()
g.rule("insert_delete",
    Lambda(lambda ctx:
        f"INSERT INTO {ctx.rng.choice(list(ctx.tables.keys()))} ( pk , col_int_key , col_int ) VALUES ( DEFAULT , 50 , 60 ) ; " +
        f"DELETE FROM {ctx.rng.choice(list(ctx.tables.keys()))} WHERE pk = lastval()"
    )
)

# PostgreSQL UPSERT
g.rule("upsert",
    choice(
        template(
            "INSERT INTO {table} ( pk , col_int_key , col_int ) VALUES ( DEFAULT, 100 , 100 ) " +
            "ON CONFLICT ( pk ) DO UPDATE SET (col_int_key , col_int) = (100, 100)",
            table=table()
        ),
        template(
            "INSERT INTO {table} ( pk ) VALUES ( {n} ) ON CONFLICT ( pk ) DO UPDATE SET pk = {n} ; {action}",
            table=table(),
            n=digit(),
            action=choice("COMMIT", "ROLLBACK")
        ),
        weights=[3, 1]
    )
)

# ============================================================================
# DELETE Operations
# ============================================================================

g.rule("delete_one",
    template("DELETE FROM {table} WHERE pk = {n} AND pk > 10",
        table=table(),
        n=number(0, 255)
    )
)

g.rule("delete_multi",
    template("DELETE FROM {table} WHERE pk > {n} AND pk > 10",
        table=table(),
        n=number(0, 255)
    )
)

# ============================================================================
# Usage
# ============================================================================

if __name__ == "__main__":
    print("=== YugabyteDB Transactions Grammar - Python DSL Version ===\n")
    
    # Generate some queries
    for i in range(10):
        query = g.generate("query", seed=42 + i)
        print(f"Query {i+1}:")
        print(f"{query}\n")
    
    print("\n=== DSL Benefits ===")
    print("• Declarative syntax - define WHAT, not HOW")
    print("• No string parsing or escaping issues")  
    print("• IDE support - autocomplete, refactoring, debugging")
    print("• Type safety with Python type hints")
    print("• Easy to extend - just add new Element classes")
    print("• Mix declarative and imperative (Lambda) as needed")
    print("• Built-in weighted choices")
    print("• Clean template syntax")
