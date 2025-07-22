{{
  config(
    materialized='table',
    cluster_by=["category", "abc_class"]
  )
}}

select
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price,
    p.cost,
    p.margin_amount,
    p.margin_percentage,
    p.price_tier,
    
    -- Add performance metrics
    coalesce(ipp.total_orders, 0) as total_orders,
    coalesce(ipp.total_quantity_sold, 0) as total_quantity_sold,
    coalesce(ipp.total_revenue, 0) as total_revenue,
    coalesce(ipp.total_profit, 0) as total_profit,
    coalesce(ipp.abc_class, 'No Sales') as abc_class,
    coalesce(ipp.revenue_rank, 999) as revenue_rank,
    coalesce(ipp.daily_velocity, 0) as daily_velocity,
    
    -- Add current timestamp
    current_timestamp() as last_updated_at
    
from {{ ref('stg_products') }} p
left join {{ ref('int_product_performance') }} ipp
    on p.product_id = ipp.product_id