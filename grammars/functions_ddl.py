#!/usr/bin/env python3
"""
PostgreSQL Functions and Stored Procedures Grammar

Comprehensive coverage of PostgreSQL function creation, including:
- SQL functions
- PL/pgSQL functions  
- Stored procedures
- Function calls and invocations
- Function management (DROP, ALTER)
"""

import sys
from pathlib import Path

# Add parent directory to path for imports

from pyrqg.dsl.core import Grammar, ref, choice, template, Literal, number

# Initialize grammar
g = Grammar("functions_ddl")

# ============================================================================
# Function Parameters and Return Types
# ============================================================================

g.rule("function_parameter_list",
    choice(
        Literal(""),  # No parameters
        ref("function_parameters")
    )
)

g.rule("function_parameters",
    choice(
        ref("function_parameter"),
        template("{param1}, {param2}", 
                param1=ref("function_parameter"),
                param2=ref("function_parameter")),
        template("{param1}, {param2}, {param3}", 
                param1=ref("function_parameter"),
                param2=ref("function_parameter"),
                param3=ref("function_parameter"))
    )
)

g.rule("function_parameter",
    choice(
        # Named parameters
        template("{name} {type}", 
                name=ref("parameter_name"),
                type=ref("data_type")),
        # Parameters with default values
        template("{name} {type} DEFAULT {default}", 
                name=ref("parameter_name"),
                type=ref("data_type"),
                default=ref("default_value")),
        # INOUT/OUT parameters
        template("INOUT {name} {type}", 
                name=ref("parameter_name"),
                type=ref("data_type")),
        template("OUT {name} {type}", 
                name=ref("parameter_name"),
                type=ref("data_type"))
    )
)

g.rule("parameter_name",
    choice(
        Literal("p_id"), Literal("p_name"), Literal("p_value"), Literal("p_count"),
        Literal("input_val"), Literal("user_id"), Literal("amount"), Literal("status"),
        Literal("start_date"), Literal("end_date"), Literal("category"), Literal("limit_val")
    )
)

g.rule("data_type",
    choice(
        Literal("INTEGER"), Literal("BIGINT"), Literal("DECIMAL(10,2)"), 
        Literal("VARCHAR(255)"), Literal("TEXT"), Literal("BOOLEAN"),
        Literal("DATE"), Literal("TIMESTAMP"), Literal("TIMESTAMPTZ"),
        Literal("JSON"), Literal("JSONB"), Literal("UUID"), Literal("BYTEA"),
        Literal("INTEGER[]"), Literal("TEXT[]"), Literal("NUMERIC[]")
    )
)

g.rule("return_type",
    choice(
        ref("data_type"),
        Literal("VOID"),
        # Table return types
        template("TABLE({columns})", columns=ref("table_return_columns")),
        # Set returning functions
        template("SETOF {type}", type=ref("data_type")),
        template("/* q_{query_id} */ SETOF RECORD", query_id=number(100000, 999999))
    )
)

g.rule("table_return_columns",
    choice(
        template("{col} {type}", col=ref("column_name"), type=ref("data_type")),
        template("{col1} {type1}, {col2} {type2}", 
                col1=ref("column_name"), type1=ref("data_type"),
                col2=ref("column_name"), type2=ref("data_type"))
    )
)

g.rule("default_value",
    choice(
        number(1, 1000000),
        Literal("'default_text'"),
        Literal("CURRENT_TIMESTAMP"),
        Literal("gen_random_uuid()"),
        Literal("NULL"),
        Literal("TRUE"), Literal("FALSE")
    )
)

# ============================================================================
# Function Body Types
# ============================================================================

g.rule("function_language",
    choice(
        Literal("SQL"),
        Literal("PLPGSQL"),
        Literal("PYTHON"),  # PL/Python
        Literal("PERL"),    # PL/Perl
        Literal("C")        # C functions
    )
)

g.rule("function_volatility",
    choice(
        Literal("IMMUTABLE"),
        Literal("STABLE"), 
        Literal("VOLATILE")
    )
)

g.rule("function_security",
    choice(
        Literal("SECURITY DEFINER"),
        Literal("SECURITY INVOKER")
    )
)

# ============================================================================
# SQL Function Bodies
# ============================================================================

g.rule("sql_function_body",
    choice(
        # Simple SELECT functions
        template("SELECT {expr}", expr=ref("sql_expression")),
        template("SELECT {col} FROM {table} WHERE {condition}", 
                col=ref("column_name"),
                table=ref("table_name"),
                condition=ref("where_condition")),
        
        # Aggregate functions
        template("SELECT COUNT(*) FROM {table} WHERE {condition}",
                table=ref("table_name"),
                condition=ref("where_condition")),
        template("SELECT SUM({col}) FROM {table} WHERE {condition}",
                col=ref("column_name"),
                table=ref("table_name"), 
                condition=ref("where_condition")),
        
        # Mathematical functions
        template("SELECT {param} * {param}", param=ref("parameter_name")),
        template("SELECT CASE WHEN {param1} > {param2} THEN {param1} ELSE {param2} END",
                param1=ref("parameter_name"),
                param2=ref("parameter_name"))
    )
)

g.rule("sql_expression",
    choice(
        ref("column_name"),
        template("{param} + {value}", param=ref("parameter_name"), value=number(1, 1000000)),
        template("COALESCE({param}, {default})", 
                param=ref("parameter_name"), 
                default=ref("default_value")),
        template("GREATEST({param1}, {param2})", 
                param1=ref("parameter_name"),
                param2=ref("parameter_name"))
    )
)

# ============================================================================
# PL/pgSQL Function Bodies
# ============================================================================

g.rule("plpgsql_function_body",
    choice(
        ref("plpgsql_simple_function"),
        ref("plpgsql_conditional_function"), 
        ref("plpgsql_loop_function"),
        ref("plpgsql_exception_function")
    )
)

g.rule("plpgsql_simple_function",
    template("""
DECLARE
    {declarations}
BEGIN
    {statements}
    RETURN {return_expr};
END;
""", 
    declarations=ref("plpgsql_declarations"),
    statements=ref("plpgsql_statements"),
    return_expr=ref("plpgsql_return_expression")
    )
)

g.rule("plpgsql_conditional_function",
    template("""
DECLARE
    result {type};
BEGIN
    IF {condition} THEN
        result := {expr1};
    ELSE
        result := {expr2};
    END IF;
    RETURN result;
END;
""",
    type=ref("data_type"),
    condition=ref("plpgsql_condition"),
    expr1=ref("plpgsql_expression"),
    expr2=ref("plpgsql_expression")
    )
)

g.rule("plpgsql_loop_function",
    template("""
DECLARE
    i INTEGER;
    total INTEGER := 0;
BEGIN
    FOR i IN 1..{param} LOOP
        total := total + i;
    END LOOP;
    RETURN total;
END;
""", param=ref("parameter_name"))
)

g.rule("plpgsql_exception_function",
    template("""
DECLARE
    result {type};
BEGIN
    {statement};
    RETURN result;
EXCEPTION
    WHEN {exception_type} THEN
        RETURN {default_value};
END;
""",
    type=ref("data_type"),
    statement=ref("plpgsql_risky_statement"),
    exception_type=ref("exception_type"),
    default_value=ref("default_value")
    )
)

g.rule("plpgsql_declarations",
    choice(
        template("temp_var {type};", type=ref("data_type")),
        template("/* q_{query_id} */ counter INTEGER := 0;", query_id=number(100000, 999999)),
        template("/* q_{query_id} */ result_text TEXT;", query_id=number(100000, 999999)),
        template("/* q_{query_id} */ found_record RECORD;", query_id=number(100000, 999999))
    )
)

g.rule("plpgsql_statements",
    choice(
        template("SELECT {col} INTO temp_var FROM {table} WHERE {condition};",
                col=ref("column_name"),
                table=ref("table_name"),
                condition=ref("where_condition")),
        template("/* q_{query_id} */ counter := counter + 1;", query_id=number(100000, 999999)),
        template("/* q_{query_id} */ PERFORM pg_sleep(0.1);", query_id=number(100000, 999999)),
        template("RAISE NOTICE 'Processing: %', {param};", param=ref("parameter_name"))
    )
)

g.rule("plpgsql_condition",
    choice(
        template("{param} > {value}", param=ref("parameter_name"), value=number(1, 1000000)),
        template("{param} IS NOT NULL", param=ref("parameter_name")),
        template("EXISTS(SELECT 1 FROM {table} WHERE {condition})",
                table=ref("table_name"),
                condition=ref("where_condition"))
    )
)

g.rule("plpgsql_expression",
    choice(
        ref("parameter_name"),
        template("{param} * 2", param=ref("parameter_name")),
        template("'{text}'::TEXT", text=ref("string_value")),
        number(1, 1000000)
    )
)

g.rule("plpgsql_return_expression",
    choice(
        ref("parameter_name"),
        Literal("temp_var"),
        Literal("counter"),
        Literal("result_text")
    )
)

g.rule("plpgsql_risky_statement",
    choice(
        template("SELECT {col} INTO STRICT result FROM {table} WHERE {condition}",
                col=ref("column_name"),
                table=ref("table_name"),
                condition=ref("where_condition")),
        template("result := {param1} / {param2}",
                param1=ref("parameter_name"),
                param2=ref("parameter_name"))
    )
)

g.rule("exception_type",
    choice(
        Literal("NO_DATA_FOUND"),
        Literal("TOO_MANY_ROWS"),
        Literal("DIVISION_BY_ZERO"),
        Literal("INVALID_TEXT_REPRESENTATION"),
        Literal("OTHERS")
    )
)

# ============================================================================
# Main Function Creation Rules
# ============================================================================

# Create function variants coupled with language/body to avoid mismatches
# SQL-language function
g.rule("create_function_sql",
    template("""CREATE OR REPLACE FUNCTION {schema}.{name}({params})
RETURNS {return_type}
LANGUAGE SQL
{volatility}
{security}
AS $function$
{body}
$function$;""",
        schema=ref("schema_name"),
        name=ref("function_name"),
        params=ref("function_parameter_list"),
        return_type=ref("return_type"),
        volatility=ref("function_volatility"),
        security=ref("function_security"),
        body=ref("sql_function_body")
    )
)

# PL/pgSQL-language function
g.rule("create_function_plpgsql",
    template("""CREATE OR REPLACE FUNCTION {schema}.{name}({params})
RETURNS {return_type}
LANGUAGE PLPGSQL
{volatility}
{security}
AS $function$
{body}
$function$;""",
        schema=ref("schema_name"),
        name=ref("function_name"),
        params=ref("function_parameter_list"),
        return_type=ref("return_type"),
        volatility=ref("function_volatility"),
        security=ref("function_security"),
        body=ref("plpgsql_function_body")
    )
)

# Top-level create_function chooses between the two valid variants
g.rule("create_function",
    choice(
        ref("create_function_sql"),
        ref("create_function_plpgsql")
    )
)

# Restrict procedures to PL/pgSQL since the body uses DECLARE/BEGIN/END
g.rule("create_procedure",
    template("""CREATE OR REPLACE PROCEDURE {schema}.{name}({params})
LANGUAGE PLPGSQL
{security}
AS $procedure$
{body}
$procedure$;""",
        schema=ref("schema_name"),
        name=ref("function_name"),
        params=ref("function_parameter_list"),
        security=ref("function_security"),
        body=ref("procedure_body")
    )
)

g.rule("procedure_body",
    template("""
DECLARE
    {declarations}
BEGIN
    {statements}
    COMMIT;
END;
""",
    declarations=ref("plpgsql_declarations"),
    statements=ref("procedure_statements")
    )
)

g.rule("procedure_statements",
    choice(
        template("INSERT INTO {table} ({cols}) VALUES ({values});",
                table=ref("table_name"),
                cols=ref("column_list"),
                values=ref("value_list")),
        template("UPDATE {table} SET {col} = {value} WHERE {condition};",
                table=ref("table_name"),
                col=ref("column_name"),
                value=ref("parameter_name"),
                condition=ref("where_condition")),
        template("DELETE FROM {table} WHERE {condition};",
                table=ref("table_name"),
                condition=ref("where_condition"))
    )
)

# ============================================================================
# Function Management (DROP, ALTER)
# ============================================================================

g.rule("drop_function",
    choice(
        template("DROP FUNCTION {schema}.{name}({params});",
                schema=ref("schema_name"),
                name=ref("function_name"),
                params=ref("function_parameter_types")),
        template("DROP FUNCTION IF EXISTS {schema}.{name}({params});",
                schema=ref("schema_name"),
                name=ref("function_name"),
                params=ref("function_parameter_types"))
    )
)

g.rule("function_parameter_types",
    choice(
        Literal(""),
        ref("data_type"),
        template("{type1}, {type2}", type1=ref("data_type"), type2=ref("data_type"))
    )
)

g.rule("alter_function",
    choice(
        template("ALTER FUNCTION {schema}.{name}({params}) RENAME TO {new_name};",
                schema=ref("schema_name"),
                name=ref("function_name"),
                params=ref("function_parameter_types"),
                new_name=ref("function_name")),
        template("ALTER FUNCTION {schema}.{name}({params}) {volatility};",
                schema=ref("schema_name"),
                name=ref("function_name"),
                params=ref("function_parameter_types"),
                volatility=ref("function_volatility"))
    )
)

# ============================================================================
# Function Calls and Invocations
# ============================================================================

g.rule("function_call",
    choice(
        template("{schema}.{name}({args})",
                schema=ref("schema_name"),
                name=ref("function_name"),
                args=ref("function_arguments")),
        template("{name}({args})",
                name=ref("function_name"),
                args=ref("function_arguments"))
    )
)

g.rule("function_arguments",
    choice(
        Literal(""),
        ref("function_argument"),
        template("{arg1}, {arg2}", 
                arg1=ref("function_argument"),
                arg2=ref("function_argument"))
    )
)

g.rule("function_argument",
    choice(
        number(1, 1000000),
        Literal("'test_value'"),
        ref("column_name"),
        Literal("NULL"),
        Literal("CURRENT_TIMESTAMP")
    )
)

g.rule("procedure_call",
    template("CALL {schema}.{name}({args});",
            schema=ref("schema_name"),
            name=ref("function_name"),
            args=ref("function_arguments"))
)

# ============================================================================
# Helper Rules
# ============================================================================

g.rule("function_name",
    choice(
        Literal("calculate_total"), Literal("get_user_count"), Literal("process_order"),
        Literal("validate_email"), Literal("generate_report"), Literal("cleanup_data"),
        Literal("calculate_discount"), Literal("format_name"), Literal("check_permissions"),
        Literal("update_statistics"), Literal("calculate_age"), Literal("normalize_phone")
    )
)

g.rule("schema_name",
    choice(
        Literal("public"), Literal("functions"), Literal("utils"), Literal("reporting")
    )
)

g.rule("table_name",
    choice(
        Literal("users"), Literal("orders"), Literal("products"), Literal("customers"),
        Literal("transactions"), Literal("logs"), Literal("settings")
    )
)

g.rule("column_name",
    choice(
        Literal("id"), Literal("name"), Literal("email"), Literal("created_at"),
        Literal("amount"), Literal("status"), Literal("user_id"), Literal("total")
    )
)

g.rule("column_list",
    choice(
        ref("column_name"),
        template("{col1}, {col2}", col1=ref("column_name"), col2=ref("column_name"))
    )
)

g.rule("value_list",
    choice(
        ref("parameter_name"),
        template("{val1}, {val2}", val1=ref("parameter_name"), val2=ref("parameter_name"))
    )
)

g.rule("where_condition",
    choice(
        template("{col} = {param}", col=ref("column_name"), param=ref("parameter_name")),
        template("{col} > {value}", col=ref("column_name"), value=number(1, 1000000)),
        template("{col} IS NOT NULL", col=ref("column_name"))
    )
)

g.rule("string_value",
    choice(
        Literal("example"), Literal("test"), Literal("data"), Literal("value")
    )
)

# ============================================================================
# Main Query Rules
# ============================================================================

g.rule("query",
    choice(
        ref("create_function"),
        ref("create_procedure"),
        ref("drop_function"),
        ref("alter_function"),
        ref("procedure_call")
    )
)

# Entry point for grammar
g.rule("start", ref("query"))

if __name__ == "__main__":
    print("PostgreSQL Functions Grammar - Sample Generation")
    print("=" * 60)
    
    # Generate sample functions
    samples = [
        ("SQL Function", "create_function"),
        ("PL/pgSQL Function", "create_function"),
        ("Stored Procedure", "create_procedure"),
        ("Function Call", "function_call"),
        ("Procedure Call", "procedure_call"),
        ("Drop Function", "drop_function")
    ]
    
    for i, (desc, rule) in enumerate(samples):
        print(f"\n{i+1}. {desc}:")
        print("-" * 40)
        query = g.generate(rule, seed=42 + i)
        print(query)
    
    print(f"\n{'='*60}")
    print("Grammar covers:")
    print("• SQL and PL/pgSQL functions")
    print("• Stored procedures")
    print("• Function parameters and return types")
    print("• Exception handling in PL/pgSQL")
    print("• Function management (CREATE, DROP, ALTER)")
    print("• Function and procedure calls")
    print("• Various PostgreSQL-specific features")