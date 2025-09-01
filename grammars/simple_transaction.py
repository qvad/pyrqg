#!/usr/bin/env python3
"""
Simple Transaction Grammar using Python DSL

This shows how simple it is to define grammars with the new DSL.
Clean and readable Python DSL approach!
"""

import sys
from pathlib import Path

from pyrqg.dsl.core import (
    Grammar, choice, maybe, repeat, template, table, field, 
    number, digit, ref, Choice, Template, Repeat
)

# Create grammar
g = Grammar("transactions")

# Define tables and their sizes
g.define_tables(
    A=10,
    B=20, 
    C=100,
    D=100,
    E=0,
    AA=50,
    BB=20,
    CC=300
)

# Define available fields
g.define_fields(
    'pk',
    'col_int',
    'col_int_key',
    'col_varchar',
    'col_varchar_key'
)

# ============================================================================
# Define Grammar Rules - Look how simple this is!
# ============================================================================

# Main query rule
g.rule("query", 
    template("{maybe_txn}{body}{maybe_commit}",
        # Ensure valid statement separators when a transaction is used
        maybe_txn=maybe("START TRANSACTION ; ", 0.5),
        body=repeat(ref("operation"), min=1, max=3, sep=" ; "),
        # If present, prepend a separator and terminate properly
        maybe_commit=maybe(choice(" ; COMMIT", " ; ROLLBACK"), 0.5)
    )
)

# Operations
g.rule("operation",
    choice(
        ref("simple_insert"),
        ref("simple_update"),
        ref("simple_delete"),
        ref("upsert"),
        weights=[3, 3, 2, 2]  # Weighted probability
    )
)

# Simple INSERT
g.rule("simple_insert",
    template("INSERT INTO {table} ({field1}, {field2}) VALUES ({val1}, {val2})",
        table=table(),
        field1=field(),
        field2=field(),
        val1=number(1, 100),
        val2=number(1, 100)
    )
)

# Simple UPDATE  
g.rule("simple_update",
    choice(
        # Update all rows
        template("UPDATE {table} SET {field} = {value}",
            table=table(),
            field=field(),
            value=number()
        ),
        
        # Update with WHERE
        template("UPDATE {table} SET {field} = {field} + {delta} WHERE pk > {min_pk}",
            table=table(),
            field=field("int"),  # Prefer integer fields
            delta=number(-50, 50),
            min_pk=number(1, 10)
        ),
        
        # Update with CASE
        template("UPDATE {table} SET {field} = CASE WHEN pk % 2 = 0 THEN {even_val} ELSE {odd_val} END",
            table=table(),
            field=field(),
            even_val=number(0, 50),
            odd_val=number(51, 100)
        )
    )
)

# Simple DELETE
g.rule("simple_delete",
    template("DELETE FROM {table} WHERE {field} = {value}",
        table=table(),
        field=field(),
        value=number()
    )
)

# PostgreSQL UPSERT
g.rule("upsert",
    template(
        "INSERT INTO {table} (pk, {field}) VALUES ({pk}, {value}) "
        "ON CONFLICT (pk) DO UPDATE SET {field} = EXCLUDED.{field}",
        table=table(),
        field=field(),
        pk=number(1, 20),
        value=number(1, 100)
    )
)

# ============================================================================
# Even Simpler: One-liner rules!
# ============================================================================

# Savepoint operations
g.rule("savepoint", template("SAVEPOINT sp{n}", n=digit()))
g.rule("rollback_savepoint", template("ROLLBACK TO SAVEPOINT sp{n}", n=digit()))

# Transaction control
g.rule("begin", "START TRANSACTION")
g.rule("commit", "COMMIT")
g.rule("rollback", "ROLLBACK")

# ============================================================================
# Complex example with lambda
# ============================================================================

g.rule("complex_update",
    lambda ctx: f"UPDATE {ctx.rng.choice(list(ctx.tables.keys()))} SET " +
                f"{ctx.rng.choice(ctx.fields)} = {ctx.rng.choice(ctx.fields)} + " +
                f"CASE WHEN pk % 2 = 1 THEN {ctx.rng.randint(1, 50)} " +
                f"ELSE -{ctx.rng.randint(1, 50)} END " +
                f"WHERE pk IN ({ctx.rng.randint(1, 10)}, {ctx.rng.randint(11, 20)})"
)

# ============================================================================
# Usage
# ============================================================================

if __name__ == "__main__":
    print("=== Simple Python DSL Grammar Example ===\n")
    
    # Generate queries
    for i in range(5):
        query = g.generate("query", seed=i)
        print(f"Query {i+1}:")
        print(f"{query}\n")
    
    print("\n=== Individual Rules ===\n")
    
    # Generate individual operations
    print(f"Insert: {g.generate('simple_insert')}")
    print(f"Update: {g.generate('simple_update')}")
    print(f"Delete: {g.generate('simple_delete')}")
    print(f"Upsert: {g.generate('upsert')}")
    print(f"Complex: {g.generate('complex_update')}")
    
    print("\n=== Benefits ===")
    print("• Declarative - just say what you want")
    print("• No string parsing or complex syntax")
    print("• Type-safe with IDE support")
    print("• Easy to read and maintain")
    print("• Powerful when needed (lambdas, custom logic)")
