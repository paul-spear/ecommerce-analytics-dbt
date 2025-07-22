{{
  config(
    materialized='table',
    cluster_by=["customer_segment", "country"]
  )
}}

select
    c.customer_id,
    email,
    full_name,
    first_name,
    last_name,
    city,
    state,
    country,
    customer_created_at,
    c.acquisition_cohort,
    days_since_registration,
    
    -- Add customer metrics from intermediate models
    coalesce(ich.total_orders, 0) as lifetime_orders,
    coalesce(ich.total_revenue, 0) as lifetime_revenue,
    coalesce(ich.avg_order_value, 0) as avg_order_value,
    coalesce(ich.customer_segment, 'No Orders') as customer_segment,
    coalesce(ich.estimated_clv, 0) as estimated_clv,
    
    -- BigQuery-specific: add current timestamp for data freshness
    current_timestamp() as last_updated_at
    
from {{ ref('stg_customers') }} c
left join {{ ref('int_customer_order_history') }} ich
    on c.customer_id = ich.customer_id