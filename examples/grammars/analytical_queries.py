#!/usr/bin/env python3
"""
analytical_queries.py - Complex Analytical Query Grammar

This grammar generates sophisticated analytical queries:
- Window functions (ROW_NUMBER, RANK, LAG/LEAD, etc.)
- Common Table Expressions (CTEs)
- Recursive queries
- ROLLUP and CUBE operations
- Statistical aggregations
- Time-series analysis

Demonstrates advanced SQL patterns for analytics and reporting.
"""

from pyrqg.dsl.core import Grammar, choice, template, maybe, repeat, ref, Lambda

# Create the grammar
grammar = Grammar("analytical")

# Define analytical tables
grammar.define_tables(
    sales=1000000,
    customers=50000,
    products=5000,
    time_dimension=3650,  # 10 years of dates
    regions=50,
    categories=100,
    transactions=5000000
)

# ==================== Window Functions ====================

grammar.rule("window_function", choice(
    # Ranking functions
    template("ROW_NUMBER() OVER ({window_spec})"),
    template("RANK() OVER ({window_spec})"),
    template("DENSE_RANK() OVER ({window_spec})"),
    template("PERCENT_RANK() OVER ({window_spec})"),
    template("NTILE({bucket_count}) OVER ({window_spec})"),
    
    # Aggregate window functions
    template("SUM({numeric_column}) OVER ({window_spec})"),
    template("AVG({numeric_column}) OVER ({window_spec})"),
    template("COUNT(*) OVER ({window_spec})"),
    template("MAX({column}) OVER ({window_spec})"),
    template("MIN({column}) OVER ({window_spec})"),
    
    # Offset functions
    template("LAG({column}, {offset}) OVER ({window_spec})"),
    template("LEAD({column}, {offset}) OVER ({window_spec})"),
    template("FIRST_VALUE({column}) OVER ({window_spec})"),
    template("LAST_VALUE({column}) OVER ({window_spec})"),
    
    # Statistical functions
    template("STDDEV({numeric_column}) OVER ({window_spec})"),
    template("VARIANCE({numeric_column}) OVER ({window_spec})"),
    
    weights=[8, 7, 7, 3, 5, 10, 8, 8, 6, 6, 7, 7, 4, 4, 3, 3]
))

grammar.rule("window_spec", choice(
    ref("simple_window"),
    ref("complex_window")
))

grammar.rule("simple_window", choice(
    template("ORDER BY {order_column}"),
    template("PARTITION BY {partition_column} ORDER BY {order_column}"),
    template("ORDER BY {order_column} {frame_clause}")
))

grammar.rule("complex_window", template(
    "PARTITION BY {partition_list} ORDER BY {order_list} {frame_clause}"
))

grammar.rule("partition_column", choice(
    "category_id", "region_id", "customer_id", "product_id",
    "DATE_TRUNC('month', sale_date)", "DATE_TRUNC('quarter', sale_date)"
))

grammar.rule("partition_list", repeat(
    ref("partition_column"),
    min=1,
    max=3,
    separator=", "
))

grammar.rule("order_column", choice(
    "sale_date", "amount", "quantity", "revenue", "created_at"
))

grammar.rule("order_list", repeat(
    template("{order_column} {direction}"),
    min=1,
    max=2,
    separator=", "
))

grammar.rule("direction", choice("ASC", "DESC"))

grammar.rule("frame_clause", choice(
    "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
    "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING",
    "ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING",
    "RANGE BETWEEN INTERVAL '1 month' PRECEDING AND CURRENT ROW",
    "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"
))

grammar.rule("bucket_count", choice(4, 5, 10, 100))
grammar.rule("offset", choice(1, 2, 3))

grammar.rule("numeric_column", choice(
    "amount", "quantity", "price", "revenue", "profit", "discount"
))

grammar.rule("column", choice(
    "product_name", "customer_name", "category", "status",
    "amount", "quantity", "sale_date"
))

# ==================== Common Table Expressions (CTEs) ====================

grammar.rule("cte_query", template(
    """WITH {cte_list}
{main_query}""",
    cte_list=ref("cte_definitions"),
    main_query=ref("cte_main_query")
))

grammar.rule("cte_definitions", repeat(
    ref("single_cte"),
    min=1,
    max=3,
    separator=",\n"
))

grammar.rule("single_cte", choice(
    ref("simple_cte"),
    ref("recursive_cte")
))

grammar.rule("simple_cte", template(
    """{cte_name} AS (
  {cte_select}
)""",
    cte_name=ref("cte_name"),
    cte_select=ref("cte_select_statement")
))

grammar.rule("cte_name", choice(
    "monthly_sales", "customer_metrics", "product_performance",
    "ranked_data", "aggregated_results", "filtered_set"
))

grammar.rule("cte_select_statement", choice(
    # Aggregation CTE
    template("""SELECT 
    {grouping_column},
    COUNT(*) as record_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
  FROM sales
  WHERE sale_date >= CURRENT_DATE - INTERVAL '1 year'
  GROUP BY {grouping_column}"""),
    
    # Ranking CTE
    template("""SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY revenue DESC) as rn
  FROM products"""),
    
    # Date series CTE
    template("""SELECT 
    DATE_TRUNC('day', sale_date) as sale_day,
    SUM(amount) as daily_total
  FROM sales
  GROUP BY DATE_TRUNC('day', sale_date)""")
))

grammar.rule("grouping_column", choice(
    "customer_id", "product_id", "category_id",
    "DATE_TRUNC('month', sale_date)", "region_id"
))

grammar.rule("cte_main_query", choice(
    # Simple select from CTE
    template("SELECT * FROM {cte_name}"),
    
    # Join CTEs
    template("""SELECT 
  a.*,
  b.avg_amount
FROM monthly_sales a
JOIN customer_metrics b ON a.customer_id = b.customer_id"""),
    
    # Complex aggregation
    template("""SELECT 
  {final_grouping},
  SUM(total_amount) as grand_total,
  AVG(avg_amount) as overall_average
FROM {cte_name}
GROUP BY {final_grouping}
ORDER BY grand_total DESC""")
))

grammar.rule("final_grouping", choice(
    "category", "region", "EXTRACT(quarter FROM sale_date)"
))

# ==================== Recursive CTEs ====================

grammar.rule("recursive_cte", template(
    """RECURSIVE {recursive_name} AS (
  -- Anchor member
  {anchor_query}
  
  UNION ALL
  
  -- Recursive member
  {recursive_query}
)""",
    recursive_name=ref("recursive_cte_name"),
    anchor_query=ref("anchor_query"),
    recursive_query=ref("recursive_query")
))

grammar.rule("recursive_cte_name", choice(
    "hierarchy", "path_traversal", "running_totals", "date_series"
))

grammar.rule("anchor_query", choice(
    # Hierarchy traversal
    """SELECT id, parent_id, name, 1 as level
  FROM categories
  WHERE parent_id IS NULL""",
    
    # Date series generation
    """SELECT DATE '2024-01-01' as date_value, 0 as day_number""",
    
    # Running total
    """SELECT 
    sale_date,
    amount,
    amount as running_total
  FROM sales
  WHERE sale_date = (SELECT MIN(sale_date) FROM sales)"""
))

grammar.rule("recursive_query", choice(
    # Hierarchy traversal
    """SELECT c.id, c.parent_id, c.name, h.level + 1
  FROM categories c
  JOIN hierarchy h ON c.parent_id = h.id
  WHERE h.level < 5""",
    
    # Date series generation
    """SELECT 
    date_value + INTERVAL '1 day',
    day_number + 1
  FROM date_series
  WHERE date_value < DATE '2024-12-31'""",
    
    # Running total
    """SELECT 
    s.sale_date,
    s.amount,
    rt.running_total + s.amount
  FROM sales s
  JOIN running_totals rt ON s.sale_date = rt.sale_date + INTERVAL '1 day'"""
))

# ==================== ROLLUP and CUBE ====================

grammar.rule("grouping_sets_query", choice(
    ref("rollup_query"),
    ref("cube_query"),
    ref("grouping_sets_explicit")
))

grammar.rule("rollup_query", template(
    """SELECT 
  {rollup_columns},
  {aggregations}
FROM sales s
JOIN products p ON s.product_id = p.id
JOIN categories c ON p.category_id = c.id
WHERE {time_filter}
GROUP BY ROLLUP({rollup_columns})
ORDER BY {rollup_columns}""",
    rollup_columns=ref("rollup_column_list"),
    aggregations=ref("rollup_aggregations"),
    time_filter=ref("time_filter")
))

grammar.rule("rollup_column_list", choice(
    "c.category_name, p.product_name",
    "EXTRACT(year FROM sale_date), EXTRACT(month FROM sale_date)",
    "region_id, customer_id"
))

grammar.rule("rollup_aggregations", choice(
    """COUNT(*) as transaction_count,
  SUM(amount) as total_revenue,
  AVG(amount) as avg_transaction""",
    
    """SUM(quantity) as units_sold,
  SUM(amount) as revenue,
  SUM(amount - cost) as profit"""
))

grammar.rule("cube_query", template(
    """SELECT 
  {cube_dimensions},
  {cube_measures}
FROM sales
WHERE {time_filter}
GROUP BY CUBE({cube_dimensions})
HAVING {having_condition}""",
    cube_dimensions=ref("cube_dimensions"),
    cube_measures=ref("cube_measures"),
    time_filter=ref("time_filter"),
    having_condition=ref("cube_having")
))

grammar.rule("cube_dimensions", choice(
    "category_id, region_id",
    "product_id, customer_segment",
    "DATE_TRUNC('quarter', sale_date), sales_channel"
))

grammar.rule("cube_measures", choice(
    "COUNT(*) as cnt, SUM(amount) as total",
    "MIN(amount) as min_sale, MAX(amount) as max_sale, AVG(amount) as avg_sale"
))

grammar.rule("cube_having", choice(
    "COUNT(*) > 100",
    "SUM(amount) > 10000",
    "AVG(amount) BETWEEN 50 AND 500"
))

grammar.rule("grouping_sets_explicit", template(
    """SELECT 
  {grouping_columns},
  {grouping_aggregates}
FROM sales
GROUP BY GROUPING SETS (
  ({set1}),
  ({set2}),
  ()
)""",
    grouping_columns=ref("grouping_columns"),
    grouping_aggregates=ref("grouping_aggregates"),
    set1=ref("grouping_set"),
    set2=ref("grouping_set")
))

grammar.rule("grouping_columns", choice(
    "category_id, product_id, region_id",
    "customer_id, DATE_TRUNC('month', sale_date)"
))

grammar.rule("grouping_aggregates", choice(
    "COUNT(*) as transactions, SUM(amount) as revenue",
    "AVG(quantity) as avg_units, STDDEV(amount) as amount_stddev"
))

grammar.rule("grouping_set", choice(
    "category_id",
    "category_id, product_id",
    "region_id",
    "DATE_TRUNC('month', sale_date)"
))

# ==================== Time Series Analysis ====================

grammar.rule("time_series_query", choice(
    ref("moving_average"),
    ref("year_over_year"),
    ref("cumulative_sum"),
    ref("time_bucket_analysis")
))

grammar.rule("moving_average", template(
    """SELECT 
  sale_date,
  daily_revenue,
  AVG(daily_revenue) OVER (
    ORDER BY sale_date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) as moving_avg_7_days,
  AVG(daily_revenue) OVER (
    ORDER BY sale_date 
    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
  ) as moving_avg_30_days
FROM (
  SELECT 
    DATE_TRUNC('day', sale_date) as sale_date,
    SUM(amount) as daily_revenue
  FROM sales
  GROUP BY DATE_TRUNC('day', sale_date)
) daily_sales
ORDER BY sale_date"""
))

grammar.rule("year_over_year", template(
    """WITH yearly_metrics AS (
  SELECT 
    DATE_TRUNC('{time_unit}', sale_date) as period,
    EXTRACT(year FROM sale_date) as year,
    {metric_calculation} as metric_value
  FROM sales
  GROUP BY DATE_TRUNC('{time_unit}', sale_date), EXTRACT(year FROM sale_date)
)
SELECT 
  period,
  year,
  metric_value,
  LAG(metric_value, 1) OVER (
    PARTITION BY EXTRACT({time_part} FROM period) 
    ORDER BY year
  ) as previous_year,
  (metric_value - LAG(metric_value, 1) OVER (
    PARTITION BY EXTRACT({time_part} FROM period) 
    ORDER BY year
  )) / LAG(metric_value, 1) OVER (
    PARTITION BY EXTRACT({time_part} FROM period) 
    ORDER BY year
  ) * 100 as yoy_growth_percent
FROM yearly_metrics
ORDER BY period, year""",
    time_unit=choice("month", "quarter"),
    time_part=choice("month", "quarter"),
    metric_calculation=choice("SUM(amount)", "COUNT(*)", "AVG(amount)")
))

grammar.rule("cumulative_sum", template(
    """SELECT 
  {time_bucket} as period,
  {metric} as period_value,
  SUM({metric}) OVER (
    ORDER BY {time_bucket}
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as cumulative_value,
  SUM({metric}) OVER (
    PARTITION BY EXTRACT(year FROM {time_bucket})
    ORDER BY {time_bucket}
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as ytd_value
FROM (
  SELECT 
    DATE_TRUNC('{bucket_size}', sale_date) as {time_bucket},
    {metric_expr} as {metric}
  FROM sales
  WHERE sale_date >= CURRENT_DATE - INTERVAL '2 years'
  GROUP BY DATE_TRUNC('{bucket_size}', sale_date)
) bucketed
ORDER BY period""",
    time_bucket="time_bucket",
    bucket_size=choice("day", "week", "month"),
    metric="metric",
    metric_expr=choice("SUM(amount)", "COUNT(DISTINCT customer_id)", "AVG(quantity)")
))

grammar.rule("time_bucket_analysis", template(
    """SELECT 
  time_bucket('{interval}', sale_date) as bucket,
  COUNT(*) as transactions,
  SUM(amount) as revenue,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median_amount,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) as p95_amount
FROM sales
WHERE sale_date >= CURRENT_DATE - INTERVAL '{lookback}'
GROUP BY time_bucket('{interval}', sale_date)
ORDER BY bucket""",
    interval=choice("1 hour", "1 day", "1 week"),
    lookback=choice("7 days", "30 days", "90 days")
))

grammar.rule("time_filter", choice(
    "sale_date >= CURRENT_DATE - INTERVAL '30 days'",
    "sale_date >= CURRENT_DATE - INTERVAL '1 year'",
    "EXTRACT(year FROM sale_date) = EXTRACT(year FROM CURRENT_DATE)"
))

# ==================== Statistical Queries ====================

grammar.rule("statistical_query", choice(
    ref("correlation_analysis"),
    ref("distribution_analysis"),
    ref("outlier_detection")
))

grammar.rule("correlation_analysis", template(
    """SELECT 
  CORR(a.value1, b.value2) as correlation_coefficient,
  REGR_SLOPE(a.value1, b.value2) as regression_slope,
  REGR_INTERCEPT(a.value1, b.value2) as regression_intercept,
  REGR_R2(a.value1, b.value2) as r_squared
FROM (
  SELECT customer_id, SUM(amount) as value1
  FROM sales
  GROUP BY customer_id
) a
JOIN (
  SELECT customer_id, COUNT(*) as value2
  FROM sales
  GROUP BY customer_id
) b ON a.customer_id = b.customer_id"""
))

grammar.rule("distribution_analysis", template(
    """WITH stats AS (
  SELECT 
    AVG({numeric_column}) as mean,
    STDDEV({numeric_column}) as stddev,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {numeric_column}) as q1,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {numeric_column}) as median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {numeric_column}) as q3
  FROM sales
)
SELECT 
  WIDTH_BUCKET({numeric_column}, 0, (SELECT MAX({numeric_column}) FROM sales), 20) as bucket,
  COUNT(*) as frequency,
  MIN({numeric_column}) as bucket_min,
  MAX({numeric_column}) as bucket_max,
  AVG({numeric_column}) as bucket_avg
FROM sales
CROSS JOIN stats
GROUP BY bucket
ORDER BY bucket"""
))

grammar.rule("outlier_detection", template(
    """WITH quartiles AS (
  SELECT 
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {numeric_column}) as q1,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {numeric_column}) as q3
  FROM sales
),
iqr_bounds AS (
  SELECT 
    q1,
    q3,
    q3 - q1 as iqr,
    q1 - 1.5 * (q3 - q1) as lower_bound,
    q3 + 1.5 * (q3 - q1) as upper_bound
  FROM quartiles
)
SELECT 
  s.*,
  CASE 
    WHEN s.{numeric_column} < ib.lower_bound THEN 'Lower Outlier'
    WHEN s.{numeric_column} > ib.upper_bound THEN 'Upper Outlier'
    ELSE 'Normal'
  END as outlier_status
FROM sales s
CROSS JOIN iqr_bounds ib
WHERE s.{numeric_column} < ib.lower_bound 
   OR s.{numeric_column} > ib.upper_bound"""
))

# ==================== Main Analytical Rule ====================

grammar.rule("analytical_query", choice(
    ref("window_function_query"),
    ref("cte_query"),
    ref("grouping_sets_query"),
    ref("time_series_query"),
    ref("statistical_query"),
    weights=[25, 25, 15, 20, 15]
))

grammar.rule("window_function_query", template(
    """SELECT 
  {regular_columns},
  {window_function} as window_result
FROM sales s
JOIN products p ON s.product_id = p.id
WHERE {time_filter}
ORDER BY {order_by}
LIMIT 100""",
    regular_columns=choice("s.*, p.product_name", "sale_date, amount, customer_id"),
    window_function=ref("window_function"),
    time_filter=ref("time_filter"),
    order_by=choice("sale_date DESC", "window_result DESC", "amount DESC")
))

# ==================== Entry Point ====================

if __name__ == "__main__":
    """Test the grammar by generating analytical queries."""
    
    print("Analytical Query Grammar - Sample Queries")
    print("=" * 50)
    
    query_types = [
        ("Window Functions", "window_function_query"),
        ("Common Table Expressions", "cte_query"),
        ("ROLLUP/CUBE", "grouping_sets_query"),
        ("Time Series Analysis", "time_series_query"),
        ("Statistical Analysis", "statistical_query")
    ]
    
    for query_name, query_rule in query_types:
        print(f"\n{query_name}:")
        print("-" * 50)
        
        query = grammar.generate(query_rule, seed=len(query_name))
        print(f"\n{query};\n")
    
    print("\nMixed analytical queries:")
    print("-" * 50)
    
    for i in range(3):
        query = grammar.generate("analytical_query", seed=i * 25)
        print(f"\n-- Query {i+1}\n{query};\n")