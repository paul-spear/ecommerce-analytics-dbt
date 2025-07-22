{{
  config(
    materialized='table',
    cluster_by=["customer_segment"]
  )
}}

select
    customer_segment,
    count(*) as customer_count,
    sum(lifetime_revenue) as total_revenue,
    avg(lifetime_revenue) as avg_revenue_per_customer,
    avg(lifetime_orders) as avg_orders_per_customer,
    avg(avg_order_value) as avg_order_value,
    avg(estimated_clv) as avg_estimated_clv,
    
    -- Distribution metrics
    min(lifetime_revenue) as min_revenue,
    max(lifetime_revenue) as max_revenue,
    
    -- BigQuery percentile functions
    approx_quantiles(lifetime_revenue, 4)[offset(1)] as revenue_25th_percentile,
    approx_quantiles(lifetime_revenue, 4)[offset(2)] as revenue_median,
    approx_quantiles(lifetime_revenue, 4)[offset(3)] as revenue_75th_percentile,
    
    -- Segment health metrics
    sum(case when days_since_registration <= 30 then 1 else 0 end) as new_customers_30d,
    sum(case when days_since_registration <= 90 then 1 else 0 end) as new_customers_90d,
    
    current_timestamp() as last_updated_at
    
from {{ ref('dim_customers') }}
where customer_segment != 'No Orders'
group by customer_segment