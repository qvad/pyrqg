#!/usr/bin/env python3
"""
YugabyteDB Outer Join Grammar (Portable)
Converted from outer_join_portable.yy

This grammar generates complex multi-table JOIN queries (6-10 tables).
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyrqg.dsl.core import Grammar, choice, maybe, template, table, field, number, digit, ref, Lambda, repeat

# Create grammar
g = Grammar("outer_join_portable")

# Define tables from outer_join.zz - 32 tables A through PP
tables = {}
# First set: A-H
for i, letter in enumerate(['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']):
    tables[letter] = [0, 1, 8, 100, 128, 210, 220, 255][i]

# Second set: I-P (all empty)
for letter in ['I', 'J', 'K', 'L', 'M', 'N', 'O', 'P']:
    tables[letter] = 0

# Third set: AA-HH  
for i, letters in enumerate(['AA', 'BB', 'CC', 'DD', 'EE', 'FF', 'GG', 'HH']):
    tables[letters] = [8, 100, 128, 210, 220, 255, 0, 1][i]

# Fourth set: II-PP (all empty)
for letters in ['II', 'JJ', 'KK', 'LL', 'MM', 'NN', 'OO', 'PP']:
    tables[letters] = 0

g.define_tables(**tables)

# Define fields from outer_join.zz
g.define_fields(
    'pk', 'col_int', 'col_int_key',
    'col_bigint', 'col_bigint_key',
    'col_decimal', 'col_decimal_key', 
    'col_float', 'col_float_key',
    'col_double', 'col_double_key',
    'col_char_255', 'col_char_255_key',
    'col_char_10', 'col_char_10_key',
    'col_varchar_10', 'col_varchar_10_key',
    'col_text', 'col_text_key',
    'col_varchar_255', 'col_varchar_255_key'
)

# ============================================================================
# Main Query
# ============================================================================

# State for tracking tables
class JoinState:
    def __init__(self):
        self.max_table_id = 0
        
join_state = JoinState()

g.rule("query",
    Lambda(lambda ctx: (
        # Reset table counter
        (setattr(join_state, 'max_table_id', 0) or "") +
        g.generate("select", ctx.seed)
    ))
)

g.rule("select",
    template("SELECT {select_list} FROM {base_from} {join_chain}",
        select_list=ref("select_list"),
        base_from=ref("base_from"),
        join_chain=ref("join_missing_table_items")
    )
)

# ============================================================================
# SELECT List
# ============================================================================

g.rule("select_list",
    choice(
        ref("loose_index_hints"),
        ref("field_list")
    )
)

g.rule("loose_index_hints",
    choice(
        template("MIN( {table_alias}.col_int_key ) AS {alias}, MAX( {table_alias}.col_int_key ) AS {alias}",
            table_alias=ref("current_table_alias"),
            alias=ref("field_alias")
        ),
        ref("field_list")
    )
)

g.rule("field_list",
    choice(
        "*",
        repeat(ref("select_field"), min=2, max=10, sep=", ")
    )
)

g.rule("select_field",
    choice(
        template("{alias}.{field}", 
            alias=ref("current_table_alias"),
            field=ref("field_name")
        ),
        template("{alias}.*",
            alias=ref("current_table_alias")
        )
    )
)

# ============================================================================
# JOIN List - The Core Logic
# ============================================================================

# Base FROM table (emitted once)
#gives the initial table in FROM clause as table0
g.rule("base_from",
    template("{table} AS table0",
        table=ref("table_name")
    )
)

# This generates joins until we have 6-10 tables
g.rule("join_missing_table_items",
    Lambda(lambda ctx: 
        # Keep joining until we have enough tables
        g.generate("join", ctx.seed) + " " + 
        g.generate("join_missing_table_items", ctx.seed)
        if join_state.max_table_id < ctx.rng.randint(6, 10)
        else ""
    )
)

g.rule("join",
    choice(
        ref("join_inner"),
        ref("join_left"),
        ref("join_right")
    )
)

g.rule("join_inner",
    template("INNER JOIN {new_table} AS {alias2} ON {condition}",
        new_table=ref("new_table_item"),
        alias2=ref("new_table_alias"),
        condition=ref("join_condition")
    )
)

g.rule("join_left", 
    template("LEFT OUTER JOIN {new_table} AS {alias2} ON {condition}",
        new_table=ref("new_table_item"),
        alias2=ref("new_table_alias"),
        condition=ref("join_condition")
    )
)

g.rule("join_right",
    template("RIGHT OUTER JOIN {new_table} AS {alias2} ON {condition}",
        new_table=ref("new_table_item"),
        alias2=ref("new_table_alias"),
        condition=ref("join_condition")
    )
)

# ============================================================================
# Join Conditions
# ============================================================================

g.rule("join_condition",
    choice(
        ref("join_condition_item"),
        ref("join_condition_list")
    )
)

g.rule("join_condition_list",
    repeat(ref("join_condition_item"), min=2, max=3, sep=" AND ")
)

g.rule("join_condition_item",
    choice(
        # Simple equality
        template("{alias1}.{field1} = {alias2}.{field2}",
            alias1=ref("existing_table_alias"),
            field1=ref("index_field"),
            alias2=ref("new_table_alias"),
            field2=ref("index_field")
        ),
        # Comparison
        template("{alias1}.{field1} {op} {alias2}.{field2}",
            alias1=ref("existing_table_alias"),
            field1=ref("field_name"),
            op=choice("<", ">", "<=", ">="),
            alias2=ref("new_table_alias"),
            field2=ref("field_name")
        ),
        # BETWEEN
        template("{alias}.{field} BETWEEN {value1} AND {value2}",
            alias=ref("existing_table_alias"),
            field=ref("field_name"),
            value1=ref("value"),
            value2=ref("value")
        ),
        # IN list
        template("{alias}.{field} IN ({values})",
            alias=ref("existing_table_alias"),
            field=ref("field_name"),
            values=repeat(ref("value"), min=2, max=5, sep=", ")
        ),
        # Row value comparison
        template("( {alias1}.{f1} , {alias1}.{f2} ) = ( {alias2}.{f1} , {alias2}.{f2} )",
            alias1=ref("existing_table_alias"),
            alias2=ref("new_table_alias"),
            f1=ref("field_name"),
            f2=ref("field_name")
        )
    )
)

# ============================================================================
# Table Management
# ============================================================================

g.rule("existing_table_item",
    Lambda(lambda ctx: 
        g.generate("table_name", ctx.seed) if join_state.max_table_id == 0
        else g.generate("table_name", ctx.seed)
    )
)

g.rule("new_table_item",
    Lambda(lambda ctx: (
        # Increment table counter
        setattr(join_state, 'max_table_id', join_state.max_table_id + 1) or
        g.generate("table_name", ctx.seed)
    ))
)

g.rule("existing_table_alias",
    Lambda(lambda ctx: f"table{ctx.rng.randint(0, max(0, join_state.max_table_id - 1))}")
)

g.rule("new_table_alias",
    Lambda(lambda ctx: f"table{join_state.max_table_id}")
)

g.rule("current_table_alias",
    Lambda(lambda ctx: f"table{ctx.rng.randint(0, max(0, join_state.max_table_id))}")
)

# ============================================================================
# Fields and Values
# ============================================================================

g.rule("table_name",
    choice(*list(tables.keys()))
)

g.rule("field_name",
    choice(
        'pk', 'col_int', 'col_int_key',
        'col_bigint', 'col_bigint_key',
        'col_char_10', 'col_char_10_key',
        'col_varchar_10', 'col_varchar_10_key'
    )
)

g.rule("index_field",
    choice('pk', 'col_int_key', 'col_bigint_key', 'col_char_10_key')
)

g.rule("field_alias",
    Lambda(lambda ctx: f"field{ctx.rng.randint(1, 20)}")
)

g.rule("value",
    choice(
        number(0, 1000),
        Lambda(lambda ctx: f"'{ctx.rng.choice(['a', 'b', 'c', 'd', 'e'])}'"),
        "NULL"
    )
)

# ============================================================================
# Additional Complex Patterns
# ============================================================================

# GROUP BY support
g.rule("select_with_group_by",
    template("SELECT {group_fields}, {aggregates} FROM {base_from} {join_chain} GROUP BY {group_fields} {having}",
        group_fields=ref("group_by_fields"),
        aggregates=ref("aggregate_list"),
        base_from=ref("base_from"),
        join_chain=ref("join_missing_table_items"),
        having=maybe(ref("having_clause"))
    )
)

g.rule("group_by_fields",
    repeat(ref("select_field"), min=1, max=3, sep=", ")
)

g.rule("aggregate_list",
    repeat(
        template("{func}({field}) AS {alias}",
            func=choice("COUNT", "SUM", "MIN", "MAX", "AVG"),
            field=ref("select_field"),
            alias=ref("field_alias")
        ),
        min=1, max=3, sep=", "
    )
)

g.rule("having_clause",
    template("HAVING {condition}",
        condition=choice(
            template("COUNT(*) > {n}", n=number(1, 10)),
            template("SUM({field}) > {n}",
                field=ref("select_field"),
                n=number(100, 1000)
            )
        )
    )
)

# ============================================================================
# Test
# ============================================================================

if __name__ == "__main__":
    print("YugabyteDB Outer Join Grammar")
    print("=" * 60)
    
    for i in range(5):
        # Reset state for each query
        join_state.max_table_id = 0
        
        query = g.generate("query", seed=42 + i)
        print(f"\nQuery {i+1} (tables: {join_state.max_table_id + 1}):")
        print(query)
    
    # Test GROUP BY variant
    print("\n\nGROUP BY queries:")
    for i in range(2):
        join_state = JoinState()
        query = g.generate("select_with_group_by", seed=100 + i)
        print(f"\n{i+1}. {query}")