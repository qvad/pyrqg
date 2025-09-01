#!/usr/bin/env python3
"""
YugabyteDB Transactions Grammar for PostgreSQL
Converted from transactions_postgres.yy

This grammar tests zero-sum transactions with PostgreSQL-specific features.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, maybe, template, table, field, number, digit, ref, Lambda

# Create grammar
g = Grammar("transactions_postgres")

# Define tables from transactions_postgres.zz
g.define_tables(
    A=10,    # 10 rows
    B=20,    # 20 rows  
    C=100,   # 100 rows
    D=100,   # 100 rows
    E=0,     # 0 rows
    AA=50,   # 10-100 rows
    BB=20,   # 10-100 rows
    CC=300,  # 10-100 rows
    DD=10,   # 10-100 rows
    AAA=10,  # 10-100 rows
    BBB=10,  # 10-100 rows
    CCC=100, # 10-100 rows
    DDD=1000 # 10-100 rows
)

# Define fields (from .zz file)
g.define_fields('pk', 'col_int_key', 'col_int')

# ============================================================================
# Main Query Structure
# ============================================================================

# Store state for transaction counting
class TransactionState:
    def __init__(self):
        self.transactions = 0
        self.savepoints = 0
        
state = TransactionState()

def _build_query(ctx):
    stmts = []
    if ctx.rng.choice([True, False]):
        stmts.append("START TRANSACTION;")
    state.savepoints = 0
    if ctx.rng.random() < 0.2:
        stmts.append("SAVEPOINT SP1;")
    stmts.append(g.rules['body'].generate(ctx) + ";")
    if ctx.rng.random() < 0.2:
        stmts.append("SAVEPOINT SP1;")
    if ctx.rng.random() < 0.3:
        stmts.append(g.rules['body'].generate(ctx) + ";")
    if ctx.rng.random() < 0.25:
        stmts.append("ROLLBACK TO SAVEPOINT SP1;")
    if ctx.rng.choice([True, False]):
        stmts.append(ctx.rng.choice(["COMMIT;", "ROLLBACK;"]))
    return "\n".join(stmts)

g.rule("query", Lambda(_build_query))

# ============================================================================
# Transaction Control
# ============================================================================

g.rule("start_txn",
    choice("START TRANSACTION", "", weights=[1, 1])
)

g.rule("savepoint", choice("", "", "", "", "SAVEPOINT SP1;", weights=[4, 0, 0, 0, 1]))
g.rule("rollback_to_savepoint", choice("", "", "", "ROLLBACK TO SAVEPOINT SP1;", weights=[3, 0, 0, 1]))
g.rule("commit_rollback", choice("COMMIT;", "ROLLBACK;", "", weights=[1, 1, 1]))

# ============================================================================
# Body Operations
# ============================================================================

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
        ref("replace"),
        ref("delete_one"),
        ref("delete_multi")
    )
)

# ============================================================================
# UPDATE Operations - Zero Sum
# ============================================================================

g.rule("update_all",
    template("UPDATE {table} SET col_int_key = col_int_key - 20, col_int = col_int + 20",
        table=ref("_table")
    )
)

g.rule("update_multi", 
    template("UPDATE {table} SET col_int = col_int + 30, col_int_key = col_int_key - 30 WHERE {field} > {digit}",
        table=ref("_table"),
        field=ref("key_nokey_pk"),
        digit=ref("_digit")
    )
)

g.rule("update_one",
    template("UPDATE {table} SET col_int_key = col_int_key - 20, col_int = col_int + 20 WHERE pk = {digit}",
        table=ref("_table"),
        digit=ref("_digit")
    )
)

g.rule("update_between",
    template("UPDATE {table} SET col_int = col_int + 30, col_int_key = col_int_key - 30 WHERE pk BETWEEN {half} AND {half} + 1",
        table=ref("_table"),
        half=ref("half_digit")
    )
)

g.rule("update_in",
    choice(
        template("UPDATE {table} SET col_int_key = col_int_key + CASE WHEN pk % 2 = 1 THEN {d1} ELSE - {d2} END WHERE pk IN ( {d3} , {d4} )",
            table=ref("_table"),
            d1=ref("_digit"),
            d2=ref("_digit"),
            d3=ref("_digit"),
            d4=ref("_digit")
        ),
        Lambda(lambda ctx: (
            f"UPDATE {g.generate('_table', ctx.seed)} SET col_int_key = col_int_key + " +
            f"CASE WHEN pk % 2 = 1 THEN 30 ELSE -30 END WHERE pk IN ( " +
            f"{ctx.rng.choice(['1 , 2', '2 , 1', '3 , 4', '4 , 3'])} )"
        ))
    )
)

# ============================================================================
# INSERT Operations
# ============================================================================

g.rule("insert_one",
    choice(
        template("INSERT INTO {table} ( pk , col_int_key , col_int) VALUES ( DEFAULT , 100 , 100 )",
            table=ref("_table")
        ),
        template("INSERT INTO {table} ( pk ) VALUES ( DEFAULT ) ; {action}",
            table=ref("_table"),
            action=ref("rollback")
        ),
        weights=[4, 1]
    )
)

g.rule("insert_multi",
    template("INSERT INTO {table} ( pk , col_int_key , col_int) VALUES ( DEFAULT , 100 , 100 ) , ( DEFAULT , 100 , 100 )",
        table=ref("_table")
    )
)

g.rule("insert_select",
    template("INSERT INTO {table} ( col_int_key , col_int ) SELECT col_int , col_int_key FROM {table} WHERE pk > 10",
        table=ref("_table")
    )
)

g.rule("insert_delete",
    Lambda(lambda ctx: (
        f"INSERT INTO {g.generate('_table', ctx.seed)} ( pk , col_int_key , col_int ) VALUES ( DEFAULT , 50 , 60 ) ; " +
        f"DELETE FROM {g.generate('_table', ctx.seed)} WHERE pk = lastval()"
    ))
)

# PostgreSQL UPSERT
g.rule("replace",
    choice(
        template("INSERT INTO {table} ( pk , col_int_key , col_int ) VALUES ( DEFAULT, 100 , 100 ) ON CONFLICT ( pk ) DO UPDATE SET (col_int_key , col_int) = (100, 100)",
            table=ref("_table")
        ),
        template("INSERT INTO {table} ( pk ) VALUES ( {digit} ) ON CONFLICT ( pk ) DO UPDATE SET pk = {digit} ; {action}",
            table=ref("_table"),
            digit=ref("_digit"),
            action=ref("rollback")
        ),
        weights=[3, 1]
    )
)

# ============================================================================
# DELETE Operations
# ============================================================================

g.rule("delete_one",
    template("DELETE FROM {table} WHERE pk = {tinyint} AND pk > 10",
        table=ref("_table"),
        tinyint=ref("_tinyint_unsigned")
    )
)

g.rule("delete_multi",
    template("DELETE FROM {table} WHERE pk > {tinyint} AND pk > 10",
        table=ref("_table"),
        tinyint=ref("_tinyint_unsigned")
    )
)

# ============================================================================
# Helper Rules
# ============================================================================

g.rule("_table",
    choice('A', 'B', 'C', 'D', 'E', 'AA', 'BB', 'CC', 'DD', 'AAA', 'BBB', 'CCC', 'DDD')
)

g.rule("_digit", digit())

g.rule("_tinyint_unsigned", number(0, 255))

g.rule("half_digit", choice('1', '2', '3', '4', '5', '6', '7', '8'))

g.rule("key_nokey_pk", choice('col_int_key', 'col_int', 'pk'))

# ============================================================================
# Test
# ============================================================================

if __name__ == "__main__":
    print("YugabyteDB Transactions Grammar (PostgreSQL)")
    print("=" * 60)
    
    for i in range(10):
        query = g.generate("query", seed=42 + i)
        print(f"\nQuery {i+1}:")
        print(query)
    
    print(f"\n\nTotal transactions generated: {state.transactions}")
