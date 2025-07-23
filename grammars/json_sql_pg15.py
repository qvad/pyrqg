"""
PostgreSQL 15 JSON/SQL Features Grammar
Implements JSON_TABLE, JSON_EXISTS, JSON_QUERY, and enhanced JSON functions
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda

g = Grammar("json_sql_pg15")

# ============================================================================
# Main JSON/SQL Query Types
# ============================================================================

g.rule("query",
    choice(
        ref("json_table_query"),
        ref("json_exists_query"), 
        ref("json_query_function"),
        ref("json_value_function"),
        ref("json_path_query"),
        ref("json_aggregate_query"),
        weights=[30, 20, 20, 15, 10, 5]
    )
)

# ============================================================================
# JSON_TABLE Implementation
# ============================================================================

g.rule("json_table_query",
    template("""SELECT {select_columns} FROM JSON_TABLE(
        {json_data},
        {path_expression}
        COLUMNS (
            {column_definitions}
        )
    ) AS {table_alias} {where_clause} {order_clause}""",
        select_columns=ref("json_table_select"),
        json_data=ref("json_source"),
        path_expression=ref("json_path"),
        column_definitions=repeat(ref("json_column_def"), min=1, max=5, sep=",\n            "),
        table_alias=ref("table_alias"),
        where_clause=maybe(template("WHERE {condition}", condition=ref("json_where_condition"))),
        order_clause=maybe(template("ORDER BY {col}", col=ref("json_column_name")))
    )
)

g.rule("json_column_def",
    choice(
        # Basic column with PATH
        template("{col_name} {data_type} PATH {json_path}",
            col_name=ref("json_column_name"),
            data_type=ref("json_column_type"),
            json_path=ref("json_path")
        ),
        
        # Column with EXISTS PATH
        template("{col_name} {data_type} EXISTS PATH {json_path}",
            col_name=ref("json_column_name"), 
            data_type="BOOLEAN",
            json_path=ref("json_path")
        ),
        
        # Column with DEFAULT and ERROR handling
        template("{col_name} {data_type} PATH {json_path} {default_clause} {error_clause}",
            col_name=ref("json_column_name"),
            data_type=ref("json_column_type"),
            json_path=ref("json_path"),
            default_clause=maybe(ref("json_default_clause")),
            error_clause=maybe(ref("json_error_clause"))
        ),
        
        # Nested PATH column
        template("{col_name} {data_type} PATH {json_path} NESTED PATH {nested_path} COLUMNS ({nested_cols})",
            col_name=ref("json_column_name"),
            data_type="JSON",
            json_path=ref("json_path"),
            nested_path=ref("json_nested_path"),
            nested_cols=repeat(ref("json_simple_column"), min=1, max=3, sep=", ")
        )
    )
)

g.rule("json_simple_column",
    template("{name} {type} PATH {path}",
        name=ref("json_column_name"),
        type=choice("TEXT", "INTEGER", "NUMERIC"),
        path=ref("json_simple_path")
    )
)

# ============================================================================
# JSON Path Expressions
# ============================================================================

g.rule("json_path",
    choice(
        # Basic paths
        "'$'",
        "'$.name'",
        "'$.age'", 
        "'$.email'",
        "'$.address'",
        "'$.metadata'",
        
        # Array paths
        "'$.items[*]'",
        "'$.users[*]'",
        "'$.orders[*]'",
        "'$.products[*].name'",
        "'$.users[*].profile'",
        
        # Nested object paths
        "'$.address.city'",
        "'$.address.country'",
        "'$.profile.settings'",
        "'$.metadata.created_at'",
        "'$.order.items[*]'",
        
        # Array indexing
        "'$.users[0]'",
        "'$.items[1]'",
        "'$.orders[0].total'",
        "'$.products[0].price'",
        
        # Complex expressions
        "'$.items[*].price'",
        "'$.users[*].orders[*]'",
        "'$.metadata.tags[*]'",
        
        # Conditional paths (PostgreSQL 15)
        "'$.items[*] ? (@.price > 100)'",
        "'$.users[*] ? (@.age >= 18)'",
        "'$.products[*] ? (@.category == \"electronics\")'"
    )
)

g.rule("json_nested_path",
    choice(
        "'$.items[*]'",
        "'$.addresses[*]'", 
        "'$.contacts[*]'",
        "'$.orders[*].items[*]'"
    )
)

g.rule("json_simple_path",
    choice(
        "'$.name'",
        "'$.value'",
        "'$.id'",
        "'$.amount'",
        "'$.status'"
    )
)

# ============================================================================
# JSON Functions (PostgreSQL 15)
# ============================================================================

g.rule("json_exists_query",
    template("SELECT {columns} FROM {table} WHERE JSON_EXISTS({json_column}, {path_expression} {passing_clause})",
        columns=ref("select_columns"),
        table=ref("table_name"),
        json_column=ref("json_column_ref"),
        path_expression=ref("json_path"),
        passing_clause=maybe(ref("json_passing_clause"))
    )
)

g.rule("json_query_function",
    choice(
        # Basic JSON_QUERY
        template("SELECT JSON_QUERY({json_column}, {path_expression} {returning_clause}) FROM {table}",
            json_column=ref("json_column_ref"),
            path_expression=ref("json_path"),
            returning_clause=ref("json_returning_clause"),
            table=ref("table_name")
        ),
        
        # JSON_QUERY with error handling
        template("SELECT JSON_QUERY({json_column}, {path_expression} {returning_clause} {wrapper_clause} {error_clause}) AS result FROM {table}",
            json_column=ref("json_column_ref"),
            path_expression=ref("json_path"),
            returning_clause=ref("json_returning_clause"),
            wrapper_clause=maybe(ref("json_wrapper_clause")),
            error_clause=maybe(ref("json_error_clause")),
            table=ref("table_name")
        )
    )
)

g.rule("json_value_function", 
    template("SELECT JSON_VALUE({json_column}, {path_expression} RETURNING {data_type} {error_clause}) FROM {table}",
        json_column=ref("json_column_ref"),
        path_expression=ref("json_path"),
        data_type=choice("TEXT", "INTEGER", "NUMERIC", "BOOLEAN"),
        error_clause=maybe(ref("json_error_clause")),
        table=ref("table_name")
    )
)

# ============================================================================
# JSON Path Queries and Aggregation
# ============================================================================

g.rule("json_path_query",
    choice(
        # IS JSON predicate
        template("SELECT * FROM {table} WHERE {json_column} IS JSON {json_type}",
            table=ref("table_name"),
            json_column=ref("json_column_ref"),
            json_type=choice("", "OBJECT", "ARRAY", "SCALAR")
        ),
        
        # JSON path predicate with filter
        template("SELECT * FROM {table} WHERE JSON_EXISTS({json_column}, {filter_path})",
            table=ref("table_name"),
            json_column=ref("json_column_ref"),
            filter_path=choice(
                "'$.age ? (@ > 18)'",
                "'$.price ? (@ > 100)'", 
                "'$.status ? (@ == \"active\")'",
                "'$.items[*] ? (@.quantity > 0)'"
            )
        ),
        
        # Complex path expressions
        template("SELECT JSON_VALUE({json_column}, {path}) as extracted FROM {table} WHERE JSON_EXISTS({json_column}, {exists_path})",
            json_column=ref("json_column_ref"),
            path=ref("json_path"),
            table=ref("table_name"),
            exists_path=ref("json_path")
        )
    )
)

g.rule("json_aggregate_query",
    choice(
        # JSON_ARRAYAGG with JSON_TABLE
        template("""SELECT JSON_ARRAYAGG(jt.name ORDER BY jt.age) 
FROM {table}, JSON_TABLE({json_column}, '$.users[*]' COLUMNS (
    name TEXT PATH '$.name',
    age INTEGER PATH '$.age'
)) AS jt""",
            table=ref("table_name"),
            json_column=ref("json_column_ref")
        ),
        
        # JSON_OBJECTAGG with extracted values
        template("SELECT JSON_OBJECTAGG(JSON_VALUE({json_column}, '$.name'), JSON_VALUE({json_column}, '$.value')) FROM {table}",
            json_column=ref("json_column_ref"),
            table=ref("table_name")
        )
    )
)

# ============================================================================
# Helper Rules and Data Sources
# ============================================================================

g.rule("json_source",
    choice(
        # Column reference
        ref("json_column_ref"),
        
        # Literal JSON data
        """'[{"name": "John", "age": 30, "email": "john@example.com"}, 
           {"name": "Jane", "age": 25, "email": "jane@example.com"}]'""",
        
        """'{"users": [
           {"id": 1, "name": "Alice", "profile": {"age": 28, "city": "NYC"}},
           {"id": 2, "name": "Bob", "profile": {"age": 32, "city": "LA"}}
           ]}'""",
        
        """'{"products": [
           {"id": 1, "name": "Laptop", "price": 999, "category": "electronics"},
           {"id": 2, "name": "Book", "price": 29, "category": "books"}
           ]}'""",
        
        # Parameter or function result
        "$1",
        "data_column",
        "metadata_json"
    )
)

g.rule("json_column_ref",
    choice(
        "data", "metadata", "config", "attributes", 
        "user_data", "order_details", "product_info",
        "settings", "preferences", "profile_data"
    )
)

g.rule("json_column_type",
    choice(
        "TEXT", "INTEGER", "BIGINT", "NUMERIC", "REAL", 
        "BOOLEAN", "DATE", "TIMESTAMP", "UUID", "JSON", "JSONB"
    )
)

g.rule("json_column_name",
    choice(
        "id", "name", "email", "age", "price", "quantity",
        "status", "category", "description", "created_at",
        "user_name", "product_name", "order_total", "item_count"
    )
)

g.rule("json_table_select",
    choice(
        "*",
        Lambda(lambda ctx: f"{ref('json_column_name').generate(ctx)}, {ref('json_column_name').generate(ctx)}"),
        Lambda(lambda ctx: f"COUNT(*), {ref('json_column_name').generate(ctx)}"),
        Lambda(lambda ctx: f"{ref('json_column_name').generate(ctx)} as extracted_value")
    )
)

# ============================================================================
# JSON Clauses and Options
# ============================================================================

g.rule("json_returning_clause",
    choice(
        "RETURNING JSON",
        "RETURNING JSONB", 
        "RETURNING TEXT",
        ""
    )
)

g.rule("json_wrapper_clause",
    choice(
        "WITH WRAPPER",
        "WITHOUT WRAPPER",
        "WITH CONDITIONAL WRAPPER"
    )
)

g.rule("json_error_clause",
    choice(
        "ERROR ON ERROR",
        "NULL ON ERROR",
        "EMPTY ON ERROR",
        "EMPTY ARRAY ON ERROR",
        "EMPTY OBJECT ON ERROR"
    )
)

g.rule("json_default_clause",
    choice(
        "DEFAULT NULL ON EMPTY",
        "DEFAULT '{}' ON EMPTY", 
        "DEFAULT '[]' ON EMPTY",
        "DEFAULT 'unknown' ON EMPTY"
    )
)

g.rule("json_passing_clause",
    template("PASSING {param} AS {var}",
        param=choice("$1", "$2", "current_user", "current_timestamp"),
        var=choice("user_id", "timestamp", "param")
    )
)

# ============================================================================
# Basic Helper Rules
# ============================================================================

g.rule("table_name",
    choice(
        "users", "orders", "products", "customers", "documents",
        "events", "transactions", "logs", "metadata_table"
    )
)

g.rule("table_alias", 
    choice("jt", "json_data", "extracted", "parsed", "source_data"))

g.rule("select_columns",
    choice(
        "*",
        "id, name",
        "name, email, age", 
        "product_id, name, price",
        "user_id, extracted_name, extracted_age"
    )
)

g.rule("json_where_condition",
    choice(
        Lambda(lambda ctx: f"{ref('json_column_name').generate(ctx)} IS NOT NULL"),
        Lambda(lambda ctx: f"{ref('json_column_name').generate(ctx)} > {ctx.rng.randint(1, 100)}"),
        Lambda(lambda ctx: f"{ref('json_column_name').generate(ctx)} = '{ctx.rng.choice(['active', 'pending', 'completed'])}'")
    )
)

if __name__ == "__main__":
    print("PostgreSQL 15 JSON/SQL Grammar Test")
    print("=" * 60)
    
    # Test different query types
    query_types = ["json_table_query", "json_exists_query", "json_query_function", "json_value_function", "json_path_query"]
    
    for i, query_type in enumerate(query_types):
        print(f"\n{i+1}. {query_type.upper()}:")
        query = g.generate(query_type, seed=i)
        print(query)
        print("-" * 40)
    
    # Test general query generation
    print("\nRANDOM QUERIES:")
    for i in range(5):
        query = g.generate("query", seed=i + 100)
        print(f"\n{i+1}. {query}")