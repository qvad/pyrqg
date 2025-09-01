"""
PostgreSQL 15 Extended Data Types Grammar
Covers range types, multirange types, geometric types, and specialized types
"""

import sys
from pathlib import Path

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda

g = Grammar("postgresql15_types")

# ============================================================================
# Main Query Types with Extended Data Types
# ============================================================================

g.rule("query",
    choice(
        ref("create_table_extended"),
        ref("insert_extended_types"),
        ref("select_extended_types"),
        ref("range_operations"),
        ref("multirange_operations"),
        ref("geometric_operations"),
        ref("network_operations"),
        ref("fulltext_operations"),
        weights=[20, 15, 15, 15, 15, 10, 5, 5]
    )
)

# ============================================================================
# Extended CREATE TABLE with PostgreSQL 15 Types
# ============================================================================

g.rule("create_table_extended",
    template("""CREATE TABLE {table_name} (
    id SERIAL PRIMARY KEY,
    {column_definitions}
    {constraints}
)""",
        table_name=ref("table_name"),
        column_definitions=repeat(ref("extended_column_def"), min=3, max=8, sep=",\n    "),
        constraints=maybe(template(",\n    {constraint}", constraint=ref("table_constraint")))
    )
)

g.rule("extended_column_def",
    choice(
        # Range types
        template("{col_name} {range_type} {default_clause}",
            col_name=ref("column_name"),
            range_type=ref("range_data_type"),
            default_clause=maybe(template("DEFAULT {default}", default=ref("range_default")))
        ),
        
        # Multirange types (PostgreSQL 14+)
        template("{col_name} {multirange_type} {default_clause}",
            col_name=ref("column_name"),
            multirange_type=ref("multirange_data_type"),
            default_clause=maybe(template("DEFAULT {default}", default=ref("multirange_default")))
        ),
        
        # Geometric types
        template("{col_name} {geometric_type} {constraint_clause}",
            col_name=ref("column_name"),
            geometric_type=ref("geometric_data_type"),
            constraint_clause=maybe(ref("geometric_constraint"))
        ),
        
        # Network types
        template("{col_name} {network_type} {not_null}",
            col_name=ref("column_name"),
            network_type=ref("network_data_type"),
            not_null=maybe("NOT NULL")
        ),
        
        # Binary and specialized types
        template("{col_name} {specialized_type} {default_clause}",
            col_name=ref("column_name"),
            specialized_type=ref("specialized_data_type"),
            default_clause=maybe(template("DEFAULT {default}", default=ref("specialized_default")))
        ),
        
        # Array types
        template("{col_name} {array_type} {array_constraint}",
            col_name=ref("column_name"),
            array_type=ref("array_data_type"),
            array_constraint=maybe(ref("array_constraint"))
        ),
        
        # Full-text search types
        template("{col_name} {fulltext_type} {index_hint}",
            col_name=ref("column_name"),
            fulltext_type=ref("fulltext_data_type"),
            index_hint=maybe("-- GIN index recommended")
        )
    )
)

# ============================================================================
# PostgreSQL 15 Data Types
# ============================================================================

g.rule("range_data_type",
    choice(
        "INT4RANGE",
        "INT8RANGE", 
        "NUMRANGE",
        "TSRANGE",
        "TSTZRANGE",
        "DATERANGE"
    )
)

g.rule("multirange_data_type",
    choice(
        "INT4MULTIRANGE",
        "INT8MULTIRANGE",
        "NUMMULTIRANGE", 
        "TSMULTIRANGE",
        "TSTZMULTIRANGE", 
        "DATEMULTIRANGE"
    )
)

g.rule("geometric_data_type",
    choice(
        "POINT",
        "LINE", 
        "LSEG",
        "BOX",
        "PATH",
        "POLYGON",
        "CIRCLE"
    )
)

g.rule("network_data_type",
    choice(
        "INET",
        "CIDR",
        "MACADDR",
        "MACADDR8"
    )
)

g.rule("specialized_data_type",
    choice(
        "BYTEA",
        "BIT(8)",
        "BIT VARYING(32)",
        "XML",
        "MONEY",
        "UUID",
        "JSON",
        "JSONB"
    )
)

g.rule("array_data_type",
    choice(
        "INTEGER[]",
        "TEXT[]",
        "NUMERIC[]",
        "TIMESTAMP[]",
        "UUID[]",
        "JSONB[]",
        "INET[]"
    )
)

g.rule("fulltext_data_type",
    choice(
        "TSVECTOR",
        "TSQUERY"
    )
)

# ============================================================================
# Default Value Generation for Extended Types
# ============================================================================

g.rule("range_default",
    choice(
        # Integer ranges
        Lambda(lambda ctx: f"'[{ctx.rng.randint(1, 100)},{ctx.rng.randint(101, 200)})'"),
        Lambda(lambda ctx: f"'({ctx.rng.randint(1, 50)},{ctx.rng.randint(51, 100)}]'"),
        
        # Date ranges
        "'[2023-01-01,2023-12-31)'",
        "'[2024-01-01,2024-06-30]'",
        
        # Timestamp ranges
        "'[2023-01-01 00:00:00,2023-12-31 23:59:59)'",
        
        # Numeric ranges
        "'[0.0,100.0)'",
        "'(50.5,150.75]'"
    )
)

g.rule("multirange_default",
    choice(
        # Integer multiranges
        "'{[1,10),[20,30),[40,50)}'",
        "'{[100,200),[300,400)}'",
        
        # Date multiranges  
        "'{[2023-01-01,2023-03-31),[2023-07-01,2023-09-30)}'",
        
        # Numeric multiranges
        "'{[0.0,25.0),[50.0,75.0),[100.0,125.0)}'"
    )
)

g.rule("specialized_default",
    choice(
        # BYTEA
        "'\\x48656c6c6f'",  # "Hello" in hex
        "'\\x576f726c64'",  # "World" in hex
        
        # BIT
        "B'10101010'",
        "B'11110000'",
        
        # XML
        "'<root><item>value</item></root>'",
        "'<data><name>test</name><value>123</value></data>'",
        
        # MONEY
        "'$100.50'",
        "'$1,234.99'",
        
        # UUID
        "gen_random_uuid()",
        "'550e8400-e29b-41d4-a716-446655440000'",
        
        # JSON/JSONB
        "'{\"key\": \"value\", \"number\": 42}'",
        "'[1, 2, 3, {\"nested\": true}]'"
    )
)

# ============================================================================
# Extended INSERT Operations
# ============================================================================

g.rule("insert_extended_types",
    template("INSERT INTO {table} ({columns}) VALUES ({values})",
        table=ref("table_name"),
        columns=repeat(ref("column_name"), min=2, max=5, sep=", "),
        values=repeat(ref("extended_type_value"), min=2, max=5, sep=", ")
    )
)

g.rule("extended_type_value",
    choice(
        # Range literals
        Lambda(lambda ctx: f"'[{ctx.rng.randint(1, 100)},{ctx.rng.randint(101, 200)})'"),
        
        # Multirange literals
        "'{[1,10),[20,30)}'",
        "'{[2023-01-01,2023-06-30),[2023-07-01,2023-12-31)}'",
        
        # Geometric literals
        Lambda(lambda ctx: f"'({ctx.rng.randint(0, 100)},{ctx.rng.randint(0, 100)})'"),  # POINT
        "'((0,0),(100,100))'",  # BOX
        "'<(50,50),25>'",  # CIRCLE
        
        # Network literals
        "'192.168.1.0/24'",  # CIDR
        "'192.168.1.100'",   # INET
        "'08:00:2b:01:02:03'",  # MACADDR
        
        # Array literals
        "'{1,2,3,4,5}'",
        "'{\"apple\",\"banana\",\"cherry\"}'",
        "'{\"2023-01-01\",\"2023-02-01\",\"2023-03-01\"}'",
        
        # Full-text search
        "to_tsvector('english', 'The quick brown fox')",
        "to_tsquery('english', 'quick & fox')",
        
        # Binary data
        "'\\x48656c6c6f576f726c64'",  # BYTEA
        "B'10101010'",  # BIT
        
        # JSON data
        "'{\"type\": \"extended\", \"value\": 42}'",
        "'[{\"id\": 1, \"active\": true}, {\"id\": 2, \"active\": false}]'"
    )
)

# ============================================================================
# Extended SELECT Operations
# ============================================================================

g.rule("select_extended_types",
    choice(
        # Range queries
        template("SELECT * FROM {table} WHERE {range_col} && {range_literal}",
            table=ref("table_name"),
            range_col=ref("column_name"),
            range_literal=ref("range_default")
        ),
        
        # Array operations
        template("SELECT {col}, array_length({array_col}, 1) FROM {table} WHERE {array_col} && ARRAY{array_literal}",
            col=ref("column_name"),
            array_col=ref("column_name"),
            table=ref("table_name"),
            array_literal=choice("['value1','value2']", "[1,2,3]", "['2023-01-01']")
        ),
        
        # Geometric calculations
        template("SELECT {point_col}, {point_col} <-> point(0,0) as distance FROM {table} ORDER BY distance",
            point_col=ref("column_name"),
            table=ref("table_name")
        ),
        
        # JSON extraction with types
        template("SELECT {json_col}, {json_col}->'{key}' as extracted FROM {table}",
            json_col=ref("column_name"),
            key=choice("name", "value", "id", "status"),
            table=ref("table_name")
        )
    )
)

# ============================================================================
# Specialized Operations by Type Category
# ============================================================================

g.rule("range_operations",
    choice(
        # Range containment
        template("SELECT * FROM {table} WHERE {range_col} @> {value}",
            table=ref("table_name"),
            range_col=ref("column_name"),
            value=Lambda(lambda ctx: str(ctx.rng.randint(1, 100)))
        ),
        
        # Range overlap
        template("SELECT * FROM {table} WHERE {range_col1} && {range_col2}",
            table=ref("table_name"),
            range_col1=ref("column_name"),
            range_col2=ref("column_name")
        ),
        
        # Range operators
        template("SELECT lower({range_col}), upper({range_col}), isempty({range_col}) FROM {table}",
            range_col=ref("column_name"),
            table=ref("table_name")
        ),
        
        # Range aggregation
        template("SELECT range_merge({range_col}) FROM {table} GROUP BY {group_col}",
            range_col=ref("column_name"),
            table=ref("table_name"),
            group_col=ref("column_name")
        )
    )
)

g.rule("multirange_operations",
    choice(
        # Multirange containment  
        template("SELECT * FROM {table} WHERE {multirange_col} @> {range_literal}",
            table=ref("table_name"),
            multirange_col=ref("column_name"),
            range_literal=ref("range_default")
        ),
        
        # Multirange functions
        template("SELECT range_intersect_agg({multirange_col}) FROM {table}",
            multirange_col=ref("column_name"),
            table=ref("table_name")
        ),
        
        # Multirange unnesting
        template("SELECT unnest({multirange_col}) as individual_range FROM {table}",
            multirange_col=ref("column_name"),
            table=ref("table_name")
        )
    )
)

g.rule("geometric_operations",
    choice(
        # Distance calculations
        template("SELECT {point_col1} <-> {point_col2} as distance FROM {table}",
            point_col1=ref("column_name"),
            point_col2=ref("column_name"), 
            table=ref("table_name")
        ),
        
        # Area calculations
        template("SELECT area({polygon_col}), center({polygon_col}) FROM {table}",
            polygon_col=ref("column_name"),
            table=ref("table_name")
        ),
        
        # Geometric containment
        template("SELECT * FROM {table} WHERE {polygon_col} @> {point_col}",
            table=ref("table_name"),
            polygon_col=ref("column_name"),
            point_col=ref("column_name")
        ),
        
        # Bounding box operations
        template("SELECT * FROM {table} WHERE {box_col} && box(point(0,0), point(100,100))",
            table=ref("table_name"),
            box_col=ref("column_name")
        )
    )
)

g.rule("network_operations",
    choice(
        # Network containment
        template("SELECT * FROM {table} WHERE {cidr_col} >> {inet_col}",
            table=ref("table_name"),
            cidr_col=ref("column_name"),
            inet_col=ref("column_name")
        ),
        
        # Network functions
        template("SELECT network({inet_col}), broadcast({inet_col}), masklen({inet_col}) FROM {table}",
            inet_col=ref("column_name"),
            table=ref("table_name")
        ),
        
        # MAC address operations  
        template("SELECT {mac_col}, macaddr8_set7bit({mac_col}) FROM {table}",
            mac_col=ref("column_name"),
            table=ref("table_name")
        )
    )
)

g.rule("fulltext_operations",
    choice(
        # Full-text search
        template("SELECT * FROM {table} WHERE {tsvector_col} @@ to_tsquery('{search_term}')",
            table=ref("table_name"),
            tsvector_col=ref("column_name"),
            search_term=choice("quick & brown", "fox | wolf", "jump & !lazy")
        ),
        
        # Text search ranking
        template("SELECT *, ts_rank({tsvector_col}, to_tsquery('{query}')) as rank FROM {table} WHERE {tsvector_col} @@ to_tsquery('{query}') ORDER BY rank DESC",
            table=ref("table_name"),
            tsvector_col=ref("column_name"),
            query=choice("search", "text & query", "important | relevant")
        ),
        
        # Text search highlighting
        template("SELECT ts_headline({text_col}, to_tsquery('{query}'), 'MaxWords=35') FROM {table}",
            table=ref("table_name"),
            text_col=ref("column_name"),
            query=choice("keyword", "search & term")
        )
    )
)

# ============================================================================
# Helper Rules and Constraints
# ============================================================================

g.rule("table_name",
    choice(
        "spatial_data", "network_config", "search_index", "range_data",
        "geometric_shapes", "time_ranges", "binary_storage", "text_search"
    )
)

g.rule("column_name",
    choice(
        "id", "name", "location", "boundary", "network", "address", 
        "time_range", "date_range", "coordinates", "search_text",
        "binary_data", "config_data", "measurements", "intervals"
    )
)

g.rule("geometric_constraint",
    choice(
        "CHECK (location IS NOT NULL)",
        "CHECK (coordinates <@ box(point(0,0), point(1000,1000)))",
        "EXCLUDE USING gist (boundary WITH &&)"
    )
)

g.rule("array_constraint", 
    choice(
        "CHECK (array_length(name, 1) > 0)",
        "CHECK (cardinality(name) BETWEEN 1 AND 10)"
    )
)

g.rule("table_constraint",
    choice(
        "CONSTRAINT unique_location UNIQUE (location)",
        "CONSTRAINT check_range_valid CHECK (time_range IS NOT NULL)",
        "CONSTRAINT exclude_overlap EXCLUDE USING gist (date_range WITH &&)"
    )
)

if __name__ == "__main__":
    print("PostgreSQL 15 Extended Types Grammar Test")
    print("=" * 60)
    
    # Test different operation types
    operation_types = [
        "create_table_extended", 
        "insert_extended_types",
        "range_operations", 
        "multirange_operations",
        "geometric_operations"
    ]
    
    for i, op_type in enumerate(operation_types):
        print(f"\n{i+1}. {op_type.upper()}:")
        query = g.generate(op_type, seed=i)
        print(query)
        print("-" * 50)
    
    # Test random queries
    print("\nRANDOM QUERIES:")
    for i in range(3):
        query = g.generate("query", seed=i + 100)
        print(f"\n{i+1}. {query}")