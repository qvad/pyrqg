#!/usr/bin/env python3
"""
postgres_features.py - PostgreSQL-Specific Features Grammar

This grammar generates queries using PostgreSQL-specific features:
- Array operations and functions
- JSON/JSONB operations
- Full-text search
- Range types and operations
- UUID operations
- PostGIS spatial queries
- LISTEN/NOTIFY
- Advisory locks

Demonstrates PostgreSQL's advanced capabilities.
"""

from pyrqg.dsl.core import Grammar, choice, template, maybe, repeat, ref, Lambda

# Create the grammar
grammar = Grammar("postgres_features")

# ==================== Array Operations ====================

grammar.rule("array_query", choice(
    ref("array_creation"),
    ref("array_operations"),
    ref("array_aggregation"),
    ref("array_unnest")
))

grammar.rule("array_creation", choice(
    # Array literals
    template("SELECT ARRAY[{array_elements}] as arr"),
    template("SELECT '{{{array_elements}}}'::integer[] as arr"),
    template("SELECT ARRAY(SELECT {column} FROM {table} WHERE {condition})"),
    
    # Array construction
    template("SELECT array_agg({column}) FROM {table} GROUP BY {group_column}"),
    template("SELECT array_agg(DISTINCT {column} ORDER BY {column}) FROM {table}")
))

grammar.rule("array_elements", choice(
    "1, 2, 3, 4, 5",
    "'a', 'b', 'c'",
    "10, 20, 30, 40",
    "true, false, true"
))

grammar.rule("array_operations", choice(
    # Array operators
    template("SELECT * FROM {table} WHERE {array_column} @> ARRAY[{value}]"),
    template("SELECT * FROM {table} WHERE {array_column} <@ ARRAY[{array_elements}]"),
    template("SELECT * FROM {table} WHERE {array_column} && ARRAY[{array_elements}]"),
    
    # Array functions
    template("SELECT array_length({array_column}, 1) FROM {table}"),
    template("SELECT array_upper({array_column}, 1) FROM {table}"),
    template("SELECT array_position({array_column}, {value}) FROM {table}"),
    template("SELECT array_remove({array_column}, {value}) FROM {table}"),
    template("SELECT array_append({array_column}, {value}) FROM {table}"),
    template("SELECT array_cat({array_column}, ARRAY[{array_elements}]) FROM {table}")
))

grammar.rule("array_aggregation", template(
    """SELECT 
  {group_column},
  array_agg({agg_column} ORDER BY {order_column}) as grouped_array,
  array_agg(DISTINCT {agg_column}) as unique_values
FROM {table}
GROUP BY {group_column}
HAVING array_length(array_agg(DISTINCT {agg_column}), 1) > {min_length}"""
))

grammar.rule("array_unnest", choice(
    template("SELECT unnest({array_column}) as value FROM {table}"),
    template("""SELECT 
  t.*,
  unnested.value,
  unnested.ordinality
FROM {table} t,
LATERAL unnest({array_column}) WITH ORDINALITY as unnested(value, ordinality)""")
))

grammar.rule("array_column", choice(
    "tags", "categories", "permissions", "roles", "features"
))

grammar.rule("column", choice(
    "id", "name", "email", "status", "created_at"
))

grammar.rule("table", choice(
    "users", "products", "posts", "documents", "configurations"
))

grammar.rule("group_column", choice(
    "category_id", "user_id", "status", "DATE_TRUNC('day', created_at)"
))

grammar.rule("agg_column", ref("column"))
grammar.rule("order_column", ref("column"))
grammar.rule("value", choice("'admin'", "'active'", "123", "'tag1'"))
grammar.rule("condition", template("{column} = {value}"))
grammar.rule("min_length", choice(2, 3, 5))

# ==================== JSON/JSONB Operations ====================

grammar.rule("json_query", choice(
    ref("json_creation"),
    ref("json_extraction"),
    ref("json_modification"),
    ref("json_aggregation"),
    ref("json_indexing")
))

grammar.rule("json_creation", choice(
    # JSON object creation
    template("SELECT json_build_object({json_pairs}) as json_data"),
    template("SELECT jsonb_build_object({json_pairs}) as jsonb_data"),
    
    # JSON array creation
    template("SELECT json_build_array({json_array_elements}) as json_array"),
    
    # Row to JSON
    template("SELECT row_to_json(t) FROM {table} t WHERE {condition}"),
    template("SELECT json_agg(t) FROM {table} t WHERE {condition}"),
    
    # JSON from subquery
    template("""SELECT json_build_object(
  'user', (SELECT row_to_json(u) FROM users u WHERE u.id = o.user_id),
  'items', (SELECT json_agg(i) FROM order_items i WHERE i.order_id = o.id)
) as order_json
FROM orders o""")
))

grammar.rule("json_pairs", choice(
    "'id', id, 'name', name",
    "'status', status, 'count', COUNT(*)",
    "'data', data, 'metadata', metadata"
))

grammar.rule("json_array_elements", choice(
    "1, 2, 3, 4",
    "'a', 'b', 'c'",
    "id, name, email"
))

grammar.rule("json_extraction", choice(
    # Operators
    template("SELECT {json_column}->'{key}' FROM {table}"),
    template("SELECT {json_column}->>'{key}' FROM {table}"),
    template("SELECT {json_column}#>'{{{path}}}' FROM {table}"),
    template("SELECT {json_column}#>>'{{{path}}}' FROM {table}"),
    
    # Path queries
    template("SELECT jsonb_path_query({json_column}, '$.{json_path}') FROM {table}"),
    template("SELECT * FROM {table} WHERE {json_column} @> '{json_contains}'::jsonb"),
    template("SELECT * FROM {table} WHERE {json_column} ? '{key}'"),
    template("SELECT * FROM {table} WHERE {json_column} ?& array[{key_array}]"),
    
    # Functions
    template("SELECT jsonb_array_elements({json_column}) FROM {table}"),
    template("SELECT jsonb_object_keys({json_column}) FROM {table}"),
    template("SELECT jsonb_typeof({json_column}->'{key}') FROM {table}")
))

grammar.rule("json_modification", choice(
    # Update operations
    template("UPDATE {table} SET {json_column} = jsonb_set({json_column}, '{{{path}}}', '{new_value}'::jsonb)"),
    template("UPDATE {table} SET {json_column} = {json_column} || '{json_merge}'::jsonb"),
    template("UPDATE {table} SET {json_column} = {json_column} - '{key}'"),
    template("UPDATE {table} SET {json_column} = jsonb_insert({json_column}, '{{{path}}}', '{new_value}'::jsonb)"),
    
    # Complex modifications
    template("""UPDATE {table} 
SET {json_column} = jsonb_set(
  {json_column},
  '{{{nested_path}}}',
  (COALESCE({json_column}#>'{{{nested_path}}}', '0')::int + 1)::text::jsonb
)
WHERE {json_column} ? '{key}'""")
))

grammar.rule("json_aggregation", template(
    """SELECT 
  {group_column},
  jsonb_agg({json_column}) as aggregated_json,
  jsonb_object_agg({key_column}, {value_column}) as json_object
FROM {table}
GROUP BY {group_column}"""
))

grammar.rule("json_indexing", choice(
    # GIN indexes for JSONB
    template("CREATE INDEX idx_{table}_{json_column}_gin ON {table} USING gin ({json_column})"),
    template("CREATE INDEX idx_{table}_{json_column}_path ON {table} USING gin ({json_column} jsonb_path_ops)"),
    
    # Expression indexes
    template("CREATE INDEX idx_{table}_{key} ON {table} (({json_column}->>'id'))"),
    template("CREATE INDEX idx_{table}_{key}_lower ON {table} (lower({json_column}->>'name'))")
))

grammar.rule("json_column", choice("data", "metadata", "settings", "attributes"))
grammar.rule("key", choice("id", "name", "status", "type", "value"))
grammar.rule("key_array", "'status', 'active', 'verified'")
grammar.rule("path", choice("user,name", "settings,notifications", "data,items,0"))
grammar.rule("json_path", choice("$.user.name", "$.items[*].price", "$.settings.notifications"))
grammar.rule("json_contains", choice('{"status": "active"}', '{"verified": true}'))
grammar.rule("new_value", choice('"updated"', '123', 'true', '{"nested": "value"}'))
grammar.rule("json_merge", choice('{"new_field": "value"}', '{"count": 1}'))
grammar.rule("nested_path", choice("stats,views", "metrics,count"))
grammar.rule("key_column", ref("column"))
grammar.rule("value_column", ref("column"))

# ==================== Full-Text Search ====================

grammar.rule("fts_query", choice(
    ref("text_search_simple"),
    ref("text_search_advanced"),
    ref("text_search_ranking"),
    ref("text_search_highlighting")
))

grammar.rule("text_search_simple", choice(
    template("SELECT * FROM {table} WHERE to_tsvector('english', {text_column}) @@ to_tsquery('english', '{search_term}')"),
    template("SELECT * FROM {table} WHERE {text_column} @@ '{search_term}'::tsquery"),
    template("SELECT * FROM {table} WHERE to_tsvector({text_column}) @@ plainto_tsquery('{search_phrase}')")
))

grammar.rule("text_search_advanced", choice(
    # Phrase search
    template("SELECT * FROM {table} WHERE to_tsvector('english', {text_column}) @@ phraseto_tsquery('english', '{search_phrase}')"),
    
    # Weighted search
    template("""SELECT * FROM {table} 
WHERE setweight(to_tsvector('english', title), 'A') ||
      setweight(to_tsvector('english', description), 'B') ||
      setweight(to_tsvector('english', content), 'C')
      @@ to_tsquery('english', '{search_term}')"""),
    
    # Complex queries
    template("SELECT * FROM {table} WHERE to_tsvector({text_column}) @@ to_tsquery('{complex_query}')")
))

grammar.rule("text_search_ranking", template(
    """SELECT 
  *,
  ts_rank(to_tsvector('english', {text_column}), query) as rank,
  ts_rank_cd(to_tsvector('english', {text_column}), query) as rank_cd
FROM {table},
     to_tsquery('english', '{search_term}') as query
WHERE to_tsvector('english', {text_column}) @@ query
ORDER BY rank DESC
LIMIT 10"""
))

grammar.rule("text_search_highlighting", template(
    """SELECT 
  {column},
  ts_headline('english', {text_column}, query, 
              'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') as highlighted
FROM {table},
     to_tsquery('english', '{search_term}') as query
WHERE to_tsvector('english', {text_column}) @@ query"""
))

grammar.rule("text_column", choice("title", "description", "content", "body", "comments"))
grammar.rule("search_term", choice("postgresql", "database", "query", "index"))
grammar.rule("search_phrase", choice("full text search", "database management", "query optimization"))
grammar.rule("complex_query", choice(
    "postgresql & database",
    "query & (optimization | performance)",
    "index & !btree"
))

# ==================== Range Types ====================

grammar.rule("range_query", choice(
    ref("range_operations"),
    ref("range_functions"),
    ref("range_aggregation")
))

grammar.rule("range_operations", choice(
    # Range containment
    template("SELECT * FROM {table} WHERE {range_column} @> {point_value}"),
    template("SELECT * FROM {table} WHERE {range_column} && {range_value}"),
    template("SELECT * FROM {table} WHERE {range_column} <@ {range_value}"),
    
    # Range operations
    template("SELECT * FROM {table} WHERE isempty({range_column})"),
    template("SELECT {range_column} * {range_value} as intersection FROM {table}"),
    template("SELECT {range_column} + {range_value} as union FROM {table}")
))

grammar.rule("range_functions", choice(
    template("SELECT lower({range_column}), upper({range_column}) FROM {table}"),
    template("SELECT lower_inc({range_column}), upper_inc({range_column}) FROM {table}"),
    template("SELECT range_merge({range_column}, {range_value}) FROM {table}")
))

grammar.rule("range_aggregation", template(
    """SELECT 
  {group_column},
  range_agg({range_column}) as combined_range
FROM {table}
GROUP BY {group_column}
HAVING NOT isempty(range_agg({range_column}))"""
))

grammar.rule("range_column", choice(
    "valid_period", "price_range", "date_range", "version_range"
))

grammar.rule("point_value", choice(
    "'2024-01-15'::date",
    "100::numeric",
    "CURRENT_DATE"
))

grammar.rule("range_value", choice(
    "'[2024-01-01,2024-12-31]'::daterange",
    "'[100,200)'::numrange",
    "'[1.0,2.0]'::numrange"
))

# ==================== UUID Operations ====================

grammar.rule("uuid_query", choice(
    template("SELECT gen_random_uuid()"),
    template("INSERT INTO {table} (id, {columns}) VALUES (gen_random_uuid(), {values})"),
    template("SELECT * FROM {table} WHERE id = '{uuid_value}'::uuid"),
    template("""SELECT 
  substring(id::text from 1 for 8) as short_id,
  id
FROM {table}
WHERE id::text LIKE '{uuid_prefix}%'""")
))

grammar.rule("uuid_value", Lambda(lambda ctx: 
    f"{ctx.rng.choice(['550e8400', 'a0eebc99', '6ba7b810'])}-" +
    f"{ctx.rng.choice(['e29b', '9c47', '9b11'])}-" +
    f"{ctx.rng.choice(['41d4', '4af9', '4bec'])}-" +
    f"{ctx.rng.choice(['a716', 'b651', '9bd2'])}-" +
    f"{ctx.rng.randint(100000000000, 999999999999)}"
))

grammar.rule("uuid_prefix", Lambda(lambda ctx:
    ctx.rng.choice(['550e8400', 'a0eebc99', '6ba7b810'])
))

grammar.rule("columns", choice("name, created_at", "data, status"))
grammar.rule("values", choice("'test', CURRENT_TIMESTAMP", "'{}', 'active'"))

# ==================== PostGIS Spatial Queries ====================

grammar.rule("spatial_query", choice(
    ref("spatial_operations"),
    ref("spatial_relationships"),
    ref("spatial_aggregation")
))

grammar.rule("spatial_operations", choice(
    # Distance queries
    template("SELECT * FROM {spatial_table} WHERE ST_DWithin(location, ST_MakePoint({lon}, {lat}), {distance})"),
    template("SELECT *, ST_Distance(location, ST_MakePoint({lon}, {lat})) as distance FROM {spatial_table} ORDER BY location <-> ST_MakePoint({lon}, {lat}) LIMIT 10"),
    
    # Geometry operations
    template("SELECT ST_Area(geometry), ST_Perimeter(geometry) FROM {spatial_table}"),
    template("SELECT ST_Buffer(location, {buffer_distance}) FROM {spatial_table}"),
    template("SELECT ST_Union(geometry) FROM {spatial_table} GROUP BY {group_column}")
))

grammar.rule("spatial_relationships", choice(
    template("SELECT a.*, b.* FROM {spatial_table} a, {spatial_table} b WHERE ST_Intersects(a.geometry, b.geometry) AND a.id != b.id"),
    template("SELECT * FROM {spatial_table} WHERE ST_Contains(geometry, ST_MakePoint({lon}, {lat}))"),
    template("SELECT * FROM {spatial_table} WHERE ST_Within(location, ST_MakeEnvelope({min_lon}, {min_lat}, {max_lon}, {max_lat}, 4326))")
))

grammar.rule("spatial_aggregation", template(
    """SELECT 
  {group_column},
  ST_Union(geometry) as combined_geometry,
  ST_Collect(location) as point_collection,
  COUNT(*) as feature_count
FROM {spatial_table}
GROUP BY {group_column}"""
))

grammar.rule("spatial_table", choice("locations", "regions", "buildings", "routes"))
grammar.rule("lon", Lambda(lambda ctx: f"{ctx.rng.uniform(-180, 180):.6f}"))
grammar.rule("lat", Lambda(lambda ctx: f"{ctx.rng.uniform(-90, 90):.6f}"))
grammar.rule("distance", choice("1000", "5000", "10000"))  # meters
grammar.rule("buffer_distance", choice("10", "50", "100"))
grammar.rule("min_lon", "-74.0")
grammar.rule("min_lat", "40.7")
grammar.rule("max_lon", "-73.9")
grammar.rule("max_lat", "40.8")

# ==================== LISTEN/NOTIFY ====================

grammar.rule("listen_notify", choice(
    template("LISTEN {channel_name}"),
    template("NOTIFY {channel_name}, '{payload}'"),
    template("UNLISTEN {channel_name}"),
    template("UNLISTEN *"),
    template("SELECT pg_notify('{channel_name}', '{payload}')")
))

grammar.rule("channel_name", choice(
    "table_updates", "user_notifications", "cache_invalidation", "job_queue"
))

grammar.rule("payload", choice(
    '{"table": "users", "action": "insert", "id": 123}',
    '{"event": "data_changed", "timestamp": "2024-01-01T00:00:00Z"}',
    'refresh_cache'
))

# ==================== Advisory Locks ====================

grammar.rule("advisory_lock", choice(
    # Session-level locks
    template("SELECT pg_advisory_lock({lock_id})"),
    template("SELECT pg_advisory_lock({lock_id1}, {lock_id2})"),
    template("SELECT pg_try_advisory_lock({lock_id})"),
    
    # Transaction-level locks
    template("SELECT pg_advisory_xact_lock({lock_id})"),
    template("SELECT pg_try_advisory_xact_lock({lock_id})"),
    
    # Unlock
    template("SELECT pg_advisory_unlock({lock_id})"),
    template("SELECT pg_advisory_unlock_all()"),
    
    # Lock queries
    template("""SELECT 
  l.pid,
  l.locktype,
  l.objid,
  l.objsubid,
  a.application_name,
  a.state
FROM pg_locks l
JOIN pg_stat_activity a ON l.pid = a.pid
WHERE l.locktype = 'advisory'""")
))

grammar.rule("lock_id", Lambda(lambda ctx: str(ctx.rng.randint(1, 1000000))))
grammar.rule("lock_id1", Lambda(lambda ctx: str(ctx.rng.randint(1, 1000))))
grammar.rule("lock_id2", Lambda(lambda ctx: str(ctx.rng.randint(1, 1000))))

# ==================== Main PostgreSQL Feature Rule ====================

grammar.rule("postgres_feature", choice(
    ref("array_query"),
    ref("json_query"),
    ref("fts_query"),
    ref("range_query"),
    ref("uuid_query"),
    ref("spatial_query"),
    ref("listen_notify"),
    ref("advisory_lock"),
    weights=[15, 20, 15, 10, 10, 15, 7, 8]
))

# ==================== Entry Point ====================

if __name__ == "__main__":
    """Test PostgreSQL-specific features."""
    
    print("PostgreSQL Features Grammar - Sample Queries")
    print("=" * 50)
    
    features = [
        ("Array Operations", "array_query"),
        ("JSON/JSONB", "json_query"),
        ("Full-Text Search", "fts_query"),
        ("Range Types", "range_query"),
        ("UUID Operations", "uuid_query"),
        ("Spatial Queries", "spatial_query"),
        ("LISTEN/NOTIFY", "listen_notify"),
        ("Advisory Locks", "advisory_lock")
    ]
    
    for feature_name, feature_rule in features:
        print(f"\n{feature_name}:")
        print("-" * 50)
        
        query = grammar.generate(feature_rule, seed=len(feature_name) * 10)
        print(f"\n{query};\n")
    
    print("\nMixed PostgreSQL features:")
    print("-" * 50)
    
    for i in range(5):
        query = grammar.generate("postgres_feature", seed=i * 33)
        print(f"\n{query};")