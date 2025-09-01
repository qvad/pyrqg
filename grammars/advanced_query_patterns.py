"""
Advanced Query Patterns Grammar for PyRQG
Implements recursive CTEs, LATERAL joins, window functions, and complex analytical queries
"""

import sys
from pathlib import Path

from pyrqg.dsl.core import Grammar, choice, template, ref, number, maybe, repeat, Lambda

g = Grammar("advanced_query_patterns")

# ============================================================================
# Main Advanced Query Types
# ============================================================================

g.rule("query",
    choice(
        ref("recursive_cte"),
        ref("lateral_join_query"),
        ref("advanced_window_query"),
        ref("complex_analytical"),
        ref("hierarchical_query"),
        ref("pivot_unpivot_query"),
        ref("advanced_aggregation"),
        ref("set_operations_complex"),
        weights=[25, 20, 15, 15, 10, 5, 5, 5]
    )
)

# ============================================================================
# Recursive CTEs
# ============================================================================

g.rule("recursive_cte",
    choice(
        # Organizational hierarchy
        template("""WITH RECURSIVE {cte_name} AS (
    -- Base case: Top-level items
    SELECT {base_columns}, 1 as level, ARRAY[id] as path
    FROM {table} 
    WHERE {base_condition}
    
    UNION ALL
    
    -- Recursive case: Child items
    SELECT {recursive_columns}, {cte_name}.level + 1, {cte_name}.path || {table}.id
    FROM {table}
    JOIN {cte_name} ON {table}.{parent_col} = {cte_name}.id
    WHERE {cte_name}.level < {max_depth}
)
SELECT {final_columns}, 
       repeat('  ', level - 1) || name as indented_name,
       array_length(path, 1) as depth
FROM {cte_name} 
{final_condition}
ORDER BY path""",
            cte_name=ref("cte_name"),
            base_columns=ref("hierarchy_base_columns"),
            table=ref("hierarchy_table"),
            base_condition=ref("hierarchy_base_condition"),
            recursive_columns=ref("hierarchy_recursive_columns"),
            parent_col=ref("parent_column"),
            max_depth=Lambda(lambda ctx: ctx.rng.randint(5, 10)),
            final_columns=ref("hierarchy_final_columns"),
            final_condition=maybe(template("WHERE level <= {max_level}", 
                max_level=Lambda(lambda ctx: ctx.rng.randint(3, 6))))
        ),
        
        # Graph traversal
        template("""WITH RECURSIVE {cte_name} AS (
    -- Starting nodes
    SELECT {node_columns}, 0 as distance, ARRAY[{start_node}] as visited
    FROM {graph_table}
    WHERE {start_condition}
    
    UNION ALL
    
    -- Connected nodes
    SELECT {target_columns}, {cte_name}.distance + 1, {cte_name}.visited || {graph_table}.{target_col}
    FROM {graph_table}
    JOIN {cte_name} ON {graph_table}.{source_col} = {cte_name}.{node_id}
    WHERE NOT {graph_table}.{target_col} = ANY({cte_name}.visited)
      AND {cte_name}.distance < {max_distance}
)
SELECT DISTINCT ON ({node_id}) *
FROM {cte_name}
ORDER BY {node_id}, distance""",
            cte_name=ref("graph_cte_name"),
            node_columns=ref("graph_node_columns"),
            start_node=Lambda(lambda ctx: ctx.rng.randint(1, 100)),
            graph_table=ref("graph_table"),
            start_condition=ref("graph_start_condition"),
            target_columns=ref("graph_target_columns"),
            target_col=ref("target_column"),
            source_col=ref("source_column"),
            node_id=ref("node_id_column"),
            max_distance=Lambda(lambda ctx: ctx.rng.randint(3, 8))
        ),
        
        # Fibonacci/sequence generation
        template("""WITH RECURSIVE {sequence_name} AS (
    -- Base cases
    SELECT 1 as n, {base_val1} as value
    UNION ALL
    SELECT 2 as n, {base_val2} as value
    
    UNION ALL
    
    -- Recursive generation
    SELECT n + 1, 
           {sequence_formula}
    FROM {sequence_name}
    WHERE n < {sequence_length}
)
SELECT n, value, 
       value - LAG(value) OVER (ORDER BY n) as difference,
       ROUND(value::numeric / NULLIF(LAG(value) OVER (ORDER BY n), 0), 4) as ratio
FROM {sequence_name}
ORDER BY n""",
            sequence_name=ref("sequence_cte_name"),
            base_val1=Lambda(lambda ctx: ctx.rng.randint(0, 2)),
            base_val2=Lambda(lambda ctx: ctx.rng.randint(1, 3)),
            sequence_formula=ref("sequence_formula"),
            sequence_length=Lambda(lambda ctx: ctx.rng.randint(15, 25))
        )
    )
)

# ============================================================================
# LATERAL Joins
# ============================================================================

g.rule("lateral_join_query",
    choice(
        # Top N per group with LATERAL
        template("""SELECT {main_cols}, lateral_data.*
FROM {main_table},
LATERAL (
    SELECT {sub_columns}
    FROM {sub_table}
    WHERE {sub_table}.{link_column} = {main_table}.id
    {sub_condition}
    ORDER BY {order_column} {order_direction}
    LIMIT {limit_count}
) AS lateral_data
WHERE {main_condition}""",
            main_cols=ref("main_table_columns"),
            main_table=ref("main_table"),
            sub_columns=ref("lateral_sub_columns"),
            sub_table=ref("sub_table"),
            link_column=ref("foreign_key_column"),
            sub_condition=maybe(template("AND {condition}", condition=ref("sub_where_condition"))),
            order_column=ref("order_column"),
            order_direction=choice("ASC", "DESC"),
            limit_count=Lambda(lambda ctx: ctx.rng.randint(1, 5)),
            main_condition=ref("main_where_condition")
        ),
        
        # Correlated calculations with LATERAL
        template("""SELECT {main_table}.*, 
       calculations.{calc_column1},
       calculations.{calc_column2},
       calculations.{calc_column3}
FROM {main_table},
LATERAL (
    SELECT 
        {aggregation1}({agg_column1}) as {calc_column1},
        {aggregation2}({agg_column2}) as {calc_column2},
        {calculation_expression} as {calc_column3}
    FROM {related_table}
    WHERE {related_table}.{relation_column} = {main_table}.id
      {related_condition}
) AS calculations
WHERE calculations.{calc_column1} IS NOT NULL""",
            main_table=ref("main_table"),
            calc_column1=ref("calculation_alias"),
            calc_column2=ref("calculation_alias"),
            calc_column3=ref("calculation_alias"),
            aggregation1=choice("SUM", "AVG", "COUNT", "MAX", "MIN"),
            aggregation2=choice("SUM", "AVG", "COUNT", "MAX", "MIN"),
            agg_column1=ref("numeric_column"),
            agg_column2=ref("numeric_column"),
            calculation_expression=ref("complex_calculation"),
            related_table=ref("related_table"),
            relation_column=ref("foreign_key_column"),
            related_condition=maybe(template("AND {condition}", condition=ref("related_where_condition")))
        ),
        
        # Dynamic pivot with LATERAL
        template("""SELECT {main_table}.{group_column},
       pivot_data.*
FROM {main_table},
LATERAL (
    SELECT {pivot_expressions}
    FROM {detail_table}
    WHERE {detail_table}.{group_link} = {main_table}.id
) AS pivot_data
WHERE {main_table}.{filter_column} {operator} {filter_value}""",
            main_table=ref("main_table"),
            group_column=ref("group_column"),
            pivot_expressions=ref("pivot_lateral_expressions"),
            detail_table=ref("detail_table"),
            group_link=ref("foreign_key_column"),
            filter_column=ref("filter_column"),
            operator=choice("=", ">", "<", ">=", "<=", "!="),
            filter_value=ref("filter_value")
        )
    )
)

# ============================================================================
# Advanced Window Functions
# ============================================================================

g.rule("advanced_window_query",
    choice(
        # Complex ranking with multiple windows
        template("""SELECT {base_columns},
       ROW_NUMBER() OVER (PARTITION BY {partition1} ORDER BY {order1} DESC) as row_num,
       RANK() OVER (PARTITION BY {partition2} ORDER BY {order2} DESC) as rank_val,
       DENSE_RANK() OVER (ORDER BY {order3} DESC) as dense_rank_val,
       NTILE({buckets}) OVER (ORDER BY {order4}) as quartile,
       PERCENT_RANK() OVER (PARTITION BY {partition3} ORDER BY {order5}) as percent_rank
FROM {table}
WHERE {condition}
QUALIFY row_num <= {top_n}""",
            base_columns=ref("base_select_columns"),
            partition1=ref("partition_column"),
            partition2=ref("partition_column"),
            partition3=ref("partition_column"),
            order1=ref("order_column"),
            order2=ref("order_column"),
            order3=ref("order_column"),
            order4=ref("order_column"),
            order5=ref("order_column"),
            buckets=choice("4", "5", "10"),
            table=ref("table_name"),
            condition=ref("base_where_condition"),
            top_n=Lambda(lambda ctx: ctx.rng.randint(3, 10))
        ),
        
        # Lead/Lag with complex frames
        template("""SELECT {columns},
       LAG({lag_column}, {lag_offset}, {lag_default}) OVER (
           PARTITION BY {partition} 
           ORDER BY {order_col}
       ) as previous_value,
       LEAD({lead_column}, {lead_offset}) OVER (
           PARTITION BY {partition} 
           ORDER BY {order_col}
       ) as next_value,
       {current_column} - LAG({lag_column}, 1, 0) OVER (
           PARTITION BY {partition} 
           ORDER BY {order_col}
       ) as difference,
       FIRST_VALUE({first_col}) OVER (
           PARTITION BY {partition} 
           ORDER BY {order_col}
           {window_frame}
       ) as first_in_group,
       LAST_VALUE({last_col}) OVER (
           PARTITION BY {partition} 
           ORDER BY {order_col}
           {window_frame}
       ) as last_in_group
FROM {table}
WHERE {condition}""",
            columns=ref("select_columns"),
            lag_column=ref("numeric_column"),
            lag_offset=Lambda(lambda ctx: ctx.rng.randint(1, 3)),
            lag_default=Lambda(lambda ctx: ctx.rng.randint(0, 100)),
            lead_column=ref("numeric_column"),
            lead_offset=Lambda(lambda ctx: ctx.rng.randint(1, 3)),
            current_column=ref("numeric_column"),
            first_col=ref("column_name"),
            last_col=ref("column_name"),
            partition=ref("partition_column"),
            order_col=ref("order_column"),
            window_frame=ref("window_frame_clause"),
            table=ref("table_name"),
            condition=ref("base_where_condition")
        ),
        
        # Running totals and moving averages
        template("""SELECT {date_column}, {value_column},
       SUM({value_column}) OVER (
           ORDER BY {date_column}
           ROWS UNBOUNDED PRECEDING
       ) as running_total,
       AVG({value_column}) OVER (
           ORDER BY {date_column}
           ROWS BETWEEN {lookback} PRECEDING AND CURRENT ROW
       ) as moving_average,
       {value_column} / SUM({value_column}) OVER (
           PARTITION BY EXTRACT(YEAR FROM {date_column})
       ) * 100 as percent_of_year,
       SUM({value_column}) OVER (
           PARTITION BY EXTRACT(YEAR FROM {date_column}), EXTRACT(MONTH FROM {date_column})
           ORDER BY {date_column}
       ) as monthly_cumulative
FROM {table}
WHERE {date_condition}
ORDER BY {date_column}""",
            date_column=ref("date_column"),
            value_column=ref("numeric_column"),
            lookback=Lambda(lambda ctx: ctx.rng.randint(3, 12)),
            table=ref("table_name"),
            date_condition=ref("date_where_condition")
        )
    )
)

# ============================================================================
# Complex Analytical Queries
# ============================================================================

g.rule("complex_analytical",
    choice(
        # Cohort analysis
        template("""WITH cohorts AS (
    SELECT user_id, 
           DATE_TRUNC('month', first_activity) as cohort_month
    FROM (
        SELECT user_id, MIN({date_column}) as first_activity
        FROM {activity_table}
        GROUP BY user_id
    ) first_activities
),
activity_periods AS (
    SELECT c.cohort_month,
           DATE_TRUNC('month', a.{date_column}) as activity_month,
           COUNT(DISTINCT a.user_id) as active_users
    FROM cohorts c
    JOIN {activity_table} a ON c.user_id = a.user_id
    GROUP BY c.cohort_month, DATE_TRUNC('month', a.{date_column})
)
SELECT cohort_month,
       activity_month,
       active_users,
       EXTRACT(EPOCH FROM activity_month - cohort_month) / 2592000 as months_since_cohort,
       active_users::float / FIRST_VALUE(active_users) OVER (
           PARTITION BY cohort_month 
           ORDER BY activity_month
       ) as retention_rate
FROM activity_periods
ORDER BY cohort_month, activity_month""",
            date_column=ref("date_column"),
            activity_table=ref("activity_table")
        ),
        
        # Time series analysis with gaps
        template("""WITH date_series AS (
    SELECT generate_series(
        DATE_TRUNC('day', MIN({date_column})),
        DATE_TRUNC('day', MAX({date_column})),
        INTERVAL '1 hour'
    ) as hour_timestamp
    FROM {timeseries_table}
),
hourly_data AS (
    SELECT DATE_TRUNC('hour', {date_column}) as hour_timestamp,
           SUM({value_column}) as hourly_value,
           COUNT(*) as record_count
    FROM {timeseries_table}
    GROUP BY DATE_TRUNC('hour', {date_column})
)
SELECT ds.hour_timestamp,
       COALESCE(hd.hourly_value, 0) as value,
       COALESCE(hd.record_count, 0) as records,
       CASE 
           WHEN hd.hourly_value IS NULL THEN 'gap'
           ELSE 'data'
       END as status,
       AVG(COALESCE(hd.hourly_value, 0)) OVER (
           ORDER BY ds.hour_timestamp
           ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
       ) as smoothed_value
FROM date_series ds
LEFT JOIN hourly_data hd ON ds.hour_timestamp = hd.hour_timestamp
WHERE ds.hour_timestamp >= CURRENT_DATE - INTERVAL '{days} days'
ORDER BY ds.hour_timestamp""",
            date_column=ref("date_column"),
            value_column=ref("numeric_column"),
            timeseries_table=ref("timeseries_table"),
            days=Lambda(lambda ctx: ctx.rng.randint(7, 30))
        )
    )
)

# ============================================================================
# Helper Rules and Data Sources
# ============================================================================

g.rule("cte_name",
    choice(
        "hierarchy_tree", "org_structure", "category_tree", "employee_chain",
        "department_tree", "product_hierarchy", "location_structure"
    )
)

g.rule("graph_cte_name",
    choice("graph_traversal", "network_paths", "connection_graph", "relationship_map"))

g.rule("sequence_cte_name",
    choice("fibonacci_seq", "number_series", "growth_sequence", "calculation_series"))

g.rule("hierarchy_table",
    choice("employees", "categories", "departments", "locations", "products"))

g.rule("graph_table",
    choice("connections", "relationships", "network_links", "graph_edges"))

g.rule("main_table",
    choice("customers", "orders", "users", "companies", "projects"))

g.rule("sub_table",
    choice("order_items", "transactions", "activities", "events", "logs"))

g.rule("related_table",
    choice("sales", "purchases", "interactions", "measurements", "metrics"))

g.rule("activity_table",
    choice("user_activities", "transactions", "events", "sessions"))

g.rule("timeseries_table",
    choice("metrics", "measurements", "sensor_data", "performance_data"))

g.rule("hierarchy_base_columns",
    choice("id, name, parent_id", "id, title, manager_id", "id, category, parent_category"))

g.rule("hierarchy_recursive_columns",
    choice("t.id, t.name, t.parent_id", "t.id, t.title, t.manager_id"))

g.rule("hierarchy_final_columns",
    choice("id, name, level", "id, title, level, path"))

g.rule("sequence_formula",
    choice(
        "(SELECT SUM(value) FROM {sequence_name} WHERE n >= {sequence_name}.n - 1)",
        "value * 2",
        "value + n"
    )
)

g.rule("window_frame_clause",
    choice(
        "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
        "RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW",
        "ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING",
        "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
    )
)

g.rule("pivot_lateral_expressions",
    Lambda(lambda ctx: ", ".join([
        f"SUM(CASE WHEN category = '{cat}' THEN amount ELSE 0 END) as {cat.lower()}_total"
        for cat in ctx.rng.sample(['A', 'B', 'C', 'D'], k=3)
    ]))
)

# Basic helper rules
g.rule("table_name", choice("sales", "users", "orders", "products", "analytics"))
g.rule("column_name", choice("id", "name", "amount", "date", "status", "category"))
g.rule("numeric_column", choice("amount", "price", "quantity", "score", "value"))
g.rule("date_column", choice("created_at", "updated_at", "transaction_date", "event_date"))
g.rule("partition_column", choice("department", "category", "region", "type"))
g.rule("order_column", choice("amount DESC", "date ASC", "score DESC", "id ASC"))
g.rule("parent_column", choice("parent_id", "manager_id", "category_parent"))
g.rule("foreign_key_column", choice("user_id", "customer_id", "order_id", "product_id"))

g.rule("hierarchy_base_condition",
    choice("parent_id IS NULL", "manager_id IS NULL", "level = 0"))

g.rule("base_where_condition",
    choice("status = 'active'", "amount > 100", "created_at >= CURRENT_DATE - INTERVAL '30 days'"))

g.rule("main_where_condition",
    choice("status IN ('active', 'pending')", "created_at >= CURRENT_DATE - INTERVAL '1 year'"))

g.rule("date_where_condition",
    choice(
        "created_at >= CURRENT_DATE - INTERVAL '90 days'",
        "transaction_date BETWEEN CURRENT_DATE - INTERVAL '1 year' AND CURRENT_DATE"
    ))

if __name__ == "__main__":
    print("Advanced Query Patterns Grammar Test")
    print("=" * 60)
    
    # Test different pattern types
    pattern_types = [
        "recursive_cte",
        "lateral_join_query", 
        "advanced_window_query",
        "complex_analytical"
    ]
    
    for i, pattern_type in enumerate(pattern_types):
        print(f"\n{i+1}. {pattern_type.upper()}:")
        query = g.generate(pattern_type, seed=i)
        print(query)
        print("-" * 70)
    
    # Test random advanced queries
    print("\nRANDOM ADVANCED QUERIES:")
    for i in range(2):
        query = g.generate("query", seed=i + 200)
        print(f"\n{i+1}. {query[:200]}...")