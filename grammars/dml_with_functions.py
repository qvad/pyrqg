#!/usr/bin/env python3
"""
DML with Function Calls Grammar

Extends DML operations to include function calls in various contexts:
- Functions in SELECT clauses
- Functions in WHERE conditions
- Functions in VALUES
- User-defined function calls
- Built-in PostgreSQL functions
"""

import sys
from pathlib import Path

# Add parent directory to path for imports

from pyrqg.dsl.core import Grammar, ref, choice, template, Literal, number

# Initialize grammar
g = Grammar("dml_with_functions")

# ============================================================================
# Function Call Patterns
# ============================================================================

g.rule("user_function_call",
    choice(
        # Simple function calls
        template("calculate_total({param})", param=ref("function_parameter")),
        template("get_user_count({param})", param=ref("function_parameter")),
        template("validate_email({param})", param=ref("function_parameter")),
        template("process_order({param1}, {param2})", 
                param1=ref("function_parameter"),
                param2=ref("function_parameter")),
        
        # Schema-qualified function calls
        template("public.calculate_discount({amount}, {rate})",
                amount=ref("numeric_value"),
                rate=ref("numeric_value")),
        template("utils.format_name({first}, {last})",
                first=ref("string_value"),
                last=ref("string_value")),
        template("reporting.generate_summary({start_date}, {end_date})",
                start_date=ref("date_value"),
                end_date=ref("date_value"))
    )
)

g.rule("builtin_function_call",
    choice(
        # String functions
        template("LENGTH({str})", str=ref("string_parameter")),
        template("UPPER({str})", str=ref("string_parameter")),
        template("LOWER({str})", str=ref("string_parameter")),
        template("SUBSTRING({str} FROM {start} FOR {length})", 
                str=ref("string_parameter"),
                start=number(1, 10),
                length=number(1, 20)),
        template("CONCAT({str1}, {str2})", 
                str1=ref("string_parameter"),
                str2=ref("string_parameter")),
        template("TRIM({str})", str=ref("string_parameter")),
        
        # Numeric functions
        template("ABS({num})", num=ref("numeric_parameter")),
        template("ROUND({num}, {precision})", 
                num=ref("numeric_parameter"),
                precision=number(0, 4)),
        template("CEIL({num})", num=ref("numeric_parameter")),
        template("FLOOR({num})", num=ref("numeric_parameter")),
        template("GREATEST({num1}, {num2})", 
                num1=ref("numeric_parameter"),
                num2=ref("numeric_parameter")),
        template("LEAST({num1}, {num2})", 
                num1=ref("numeric_parameter"),
                num2=ref("numeric_parameter")),
        
        # Date functions
        template("NOW()", ),
        template("CURRENT_DATE", ),
        template("CURRENT_TIMESTAMP", ),
        template("DATE_TRUNC({interval}, {date})", 
                interval=choice(Literal("'day'"), Literal("'month'"), Literal("'year'")),
                date=ref("date_parameter")),
        template("EXTRACT({part} FROM {date})", 
                part=choice(Literal("YEAR"), Literal("MONTH"), Literal("DAY")),
                date=ref("date_parameter")),
        template("AGE({date})", date=ref("date_parameter")),
        
        # JSON functions
        template("json_extract_path_text({json}, {path})",
                json=ref("json_parameter"),
                path=ref("string_value")),
        template("{json} ->> {key}",
                json=ref("json_parameter"),
                key=ref("string_value")),
        template("{json} -> {key}",
                json=ref("json_parameter"),
                key=ref("string_value")),
        
        # Array functions
        template("array_length({array}, {dim})",
                array=ref("array_parameter"),
                dim=number(1, 3)),
        template("unnest({array})", array=ref("array_parameter")),
        template("array_agg({value})", value=ref("column_reference")),
        
        # Conditional functions
        template("COALESCE({val1}, {val2})", 
                val1=ref("nullable_parameter"),
                val2=ref("function_parameter")),
        template("NULLIF({val1}, {val2})", 
                val1=ref("function_parameter"),
                val2=ref("function_parameter")),
        template("CASE WHEN {condition} THEN {val1} ELSE {val2} END",
                condition=ref("condition_expression"),
                val1=ref("function_parameter"),
                val2=ref("function_parameter"))
    )
)

g.rule("aggregate_function_call",
    choice(
        template("COUNT({param})", param=choice(Literal("*"), ref("column_reference"))),
        template("SUM({param})", param=ref("numeric_parameter")),
        template("AVG({param})", param=ref("numeric_parameter")),
        template("MIN({param})", param=ref("function_parameter")),
        template("MAX({param})", param=ref("function_parameter")),
        template("STRING_AGG({param}, {delimiter})", 
                param=ref("string_parameter"),
                delimiter=ref("string_value")),
        template("ARRAY_AGG({param})", param=ref("column_reference")),
        template("JSON_AGG({param})", param=ref("column_reference")),
        template("BOOL_AND({param})", param=ref("boolean_parameter")),
        template("BOOL_OR({param})", param=ref("boolean_parameter"))
    )
)

g.rule("window_function_call",
    choice(
        template("ROW_NUMBER() OVER ({window_spec})", window_spec=ref("window_specification")),
        template("RANK() OVER ({window_spec})", window_spec=ref("window_specification")),
        template("DENSE_RANK() OVER ({window_spec})", window_spec=ref("window_specification")),
        template("LAG({param}, {offset}) OVER ({window_spec})", 
                param=ref("column_reference"),
                offset=number(1, 5),
                window_spec=ref("window_specification")),
        template("LEAD({param}, {offset}) OVER ({window_spec})", 
                param=ref("column_reference"),
                offset=number(1, 5),
                window_spec=ref("window_specification")),
        template("FIRST_VALUE({param}) OVER ({window_spec})", 
                param=ref("column_reference"),
                window_spec=ref("window_specification")),
        template("LAST_VALUE({param}) OVER ({window_spec})", 
                param=ref("column_reference"),
                window_spec=ref("window_specification"))
    )
)

g.rule("window_specification",
    choice(
        template("ORDER BY {col}", col=ref("column_reference")),
        template("PARTITION BY {col} ORDER BY {order_col}", 
                col=ref("column_reference"),
                order_col=ref("column_reference")),
        template("ORDER BY {col} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
                col=ref("column_reference"))
    )
)

# ============================================================================
# Function Parameters
# ============================================================================

g.rule("function_parameter",
    choice(
        ref("column_reference"),
        ref("numeric_value"),
        ref("string_value"),
        ref("date_value"),
        ref("boolean_value"),
        Literal("NULL")
    )
)

g.rule("string_parameter",
    choice(
        ref("column_reference"),
        ref("string_value")
    )
)

g.rule("numeric_parameter",
    choice(
        ref("column_reference"),
        ref("numeric_value")
    )
)

g.rule("date_parameter",
    choice(
        ref("column_reference"),
        ref("date_value"),
        Literal("CURRENT_DATE"),
        Literal("CURRENT_TIMESTAMP")
    )
)

g.rule("json_parameter",
    choice(
        ref("column_reference"),
        template("'{json}'::JSONB", json=ref("json_string"))
    )
)

g.rule("array_parameter",
    choice(
        ref("column_reference"),
        template("ARRAY[{values}]", values=ref("array_values"))
    )
)

g.rule("boolean_parameter",
    choice(
        ref("column_reference"),
        ref("boolean_value")
    )
)

g.rule("nullable_parameter",
    choice(
        ref("column_reference"),
        Literal("NULL")
    )
)

# ============================================================================
# DML with Function Calls
# ============================================================================

g.rule("select_with_functions",
    choice(
        # Functions in SELECT clause
        template("SELECT {func1}, {func2} FROM {table}",
                func1=ref("any_function_call"),
                func2=ref("any_function_call"),
                table=ref("table_name")),
        
        template("SELECT {col}, {func} FROM {table} WHERE {condition}",
                col=ref("column_reference"),
                func=ref("any_function_call"),
                table=ref("table_name"),
                condition=ref("function_condition")),
        
        # Aggregate functions with GROUP BY
        template("SELECT {col}, {agg_func} FROM {table} GROUP BY {col}",
                col=ref("column_reference"),
                agg_func=ref("aggregate_function_call"),
                table=ref("table_name")),
        
        # Window functions
        template("SELECT {col}, {window_func} FROM {table}",
                col=ref("column_reference"),
                window_func=ref("window_function_call"),
                table=ref("table_name")),
        
        # Subquery with functions
        template("SELECT * FROM {table} WHERE {col} IN (SELECT {func} FROM {other_table})",
                table=ref("table_name"),
                col=ref("column_reference"),
                func=ref("any_function_call"),
                other_table=ref("table_name"))
    )
)

g.rule("insert_with_functions",
    choice(
        # Functions in VALUES
        template("INSERT INTO {table} ({cols}) VALUES ({func_values})",
                table=ref("table_name"),
                cols=ref("column_list"),
                func_values=ref("function_value_list")),
        
        # INSERT with function in subquery
        template("INSERT INTO {table} ({cols}) SELECT {func_cols} FROM {source_table}",
                table=ref("table_name"),
                cols=ref("column_list"),
                func_cols=ref("function_column_list"),
                source_table=ref("table_name")),
        
        # INSERT with RETURNING function
        template("INSERT INTO {table} ({cols}) VALUES ({values}) RETURNING {func}",
                table=ref("table_name"),
                cols=ref("column_list"),
                values=ref("value_list"),
                func=ref("any_function_call"))
    )
)

g.rule("update_with_functions",
    choice(
        # Function in SET clause
        template("UPDATE {table} SET {col} = {func} WHERE {condition}",
                table=ref("table_name"),
                col=ref("column_reference"),
                func=ref("any_function_call"),
                condition=ref("function_condition")),
        
        # Multiple functions in SET
        template("UPDATE {table} SET {col1} = {func1}, {col2} = {func2} WHERE {condition}",
                table=ref("table_name"),
                col1=ref("column_reference"),
                func1=ref("any_function_call"),
                col2=ref("column_reference"),
                func2=ref("any_function_call"),
                condition=ref("function_condition")),
        
        # UPDATE with function in FROM clause
        template("UPDATE {table} SET {col} = {func} FROM {other_table} WHERE {join_condition}",
                table=ref("table_name"),
                col=ref("column_reference"),
                func=ref("any_function_call"),
                other_table=ref("table_name"),
                join_condition=ref("join_condition")),
        
        # UPDATE with RETURNING function
        template("UPDATE {table} SET {col} = {value} WHERE {condition} RETURNING {func}",
                table=ref("table_name"),
                col=ref("column_reference"),
                value=ref("function_parameter"),
                condition=ref("function_condition"),
                func=ref("any_function_call"))
    )
)

g.rule("delete_with_functions",
    choice(
        # Function in WHERE clause
        template("DELETE FROM {table} WHERE {func_condition}",
                table=ref("table_name"),
                func_condition=ref("function_condition")),
        
        # DELETE with function in USING
        template("DELETE FROM {table} USING {other_table} WHERE {func_condition}",
                table=ref("table_name"),
                other_table=ref("table_name"),
                func_condition=ref("function_condition")),
        
        # DELETE with RETURNING function
        template("DELETE FROM {table} WHERE {condition} RETURNING {func}",
                table=ref("table_name"),
                condition=ref("simple_condition"),
                func=ref("any_function_call"))
    )
)

# ============================================================================
# Helper Rules
# ============================================================================

g.rule("any_function_call",
    choice(
        ref("user_function_call"),
        ref("builtin_function_call"),
        ref("aggregate_function_call"),
        ref("window_function_call")
    )
)

g.rule("function_condition",
    choice(
        template("{func} > {value}", 
                func=ref("any_function_call"),
                value=ref("numeric_value")),
        template("{func} = {value}", 
                func=ref("any_function_call"),
                value=ref("function_parameter")),
        template("{func} IS NOT NULL", func=ref("any_function_call")),
        template("{func1} < {func2}", 
                func1=ref("any_function_call"),
                func2=ref("any_function_call"))
    )
)

g.rule("function_value_list",
    choice(
        ref("any_function_call"),
        template("{func1}, {func2}", 
                func1=ref("any_function_call"),
                func2=ref("any_function_call")),
        template("{value}, {func}", 
                value=ref("function_parameter"),
                func=ref("any_function_call"))
    )
)

g.rule("function_column_list",
    choice(
        ref("any_function_call"),
        template("{col}, {func}", 
                col=ref("column_reference"),
                func=ref("any_function_call")),
        template("{func1}, {func2}", 
                func1=ref("any_function_call"),
                func2=ref("any_function_call"))
    )
)

g.rule("condition_expression",
    choice(
        template("{col} > {value}", col=ref("column_reference"), value=ref("numeric_value")),
        template("{col} = {value}", col=ref("column_reference"), value=ref("function_parameter")),
        template("{col} IS NOT NULL", col=ref("column_reference"))
    )
)

g.rule("simple_condition",
    choice(
        template("{col} = {value}", col=ref("column_reference"), value=ref("function_parameter")),
        template("{col} > {value}", col=ref("column_reference"), value=ref("numeric_value"))
    )
)

g.rule("join_condition",
    template("{table1}.{col1} = {table2}.{col2}",
            table1=ref("table_name"),
            col1=ref("column_reference"),
            table2=ref("table_name"), 
            col2=ref("column_reference"))
)

# Basic value rules
g.rule("column_reference",
    choice(
        Literal("id"), Literal("name"), Literal("email"), Literal("amount"),
        Literal("created_at"), Literal("status"), Literal("user_id"), Literal("total")
    )
)

g.rule("table_name",
    choice(
        Literal("users"), Literal("orders"), Literal("products"), Literal("transactions")
    )
)

g.rule("column_list",
    choice(
        ref("column_reference"),
        template("{col1}, {col2}", col1=ref("column_reference"), col2=ref("column_reference"))
    )
)

g.rule("value_list",
    choice(
        ref("function_parameter"),
        template("{val1}, {val2}", val1=ref("function_parameter"), val2=ref("function_parameter"))
    )
)

g.rule("numeric_value", number(1, 1000))
g.rule("string_value", choice(Literal("test"), Literal("example"), Literal("data")))
g.rule("date_value", choice(Literal("'2024-01-01'"), Literal("'2024-12-31'")))
g.rule("boolean_value", choice(Literal("TRUE"), Literal("FALSE")))
g.rule("json_string", choice(Literal('{"key": "value"}'), Literal('{"id": 123}')))
g.rule("array_values", choice(Literal("1,2,3"), Literal("'a','b','c'")))

# ============================================================================
# Main Query Rules
# ============================================================================

g.rule("query",
    choice(
        ref("select_with_functions"),
        ref("insert_with_functions"),
        ref("update_with_functions"),
        ref("delete_with_functions")
    )
)

# Entry point for grammar
g.rule("start", ref("query"))

if __name__ == "__main__":
    print("DML with Functions Grammar - Sample Generation")
    print("=" * 60)
    
    # Generate sample queries
    samples = [
        ("SELECT with User Functions", "select_with_functions"),
        ("INSERT with Functions", "insert_with_functions"),
        ("UPDATE with Functions", "update_with_functions"),
        ("DELETE with Functions", "delete_with_functions"),
        ("Built-in Function Call", "builtin_function_call"),
        ("Window Function", "window_function_call")
    ]
    
    for i, (desc, rule) in enumerate(samples):
        print(f"\n{i+1}. {desc}:")
        print("-" * 40)
        query = g.generate(rule, seed=42 + i)
        print(query)
    
    print(f"\n{'='*60}")
    print("Grammar covers:")
    print("• User-defined function calls in DML")
    print("• Built-in PostgreSQL functions (string, numeric, date, JSON, array)")
    print("• Aggregate and window functions")
    print("• Functions in SELECT, INSERT, UPDATE, DELETE")
    print("• Functions in WHERE, SET, VALUES clauses")
    print("• Complex function combinations and nesting")