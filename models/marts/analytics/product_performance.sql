{{
  config(
    materialized='table',
    cluster_by=["category", "abc_class"]
  )
}}

select
    category,
    abc_class,
    count(*) as product_count,
    sum(total_revenue) as category_revenue,
    sum(total_quantity_sold) as category_quantity,
    sum(total_profit) as category_profit,
    avg(margin_percentage) as avg_margin_percentage,
    
    -- Performance metrics
    avg(daily_velocity) as avg_daily_velocity,
    sum(total_orders) as total_orders,
    count(distinct case when total_orders > 0 then product_id end) as active_products,
    
    -- Top performers
    array_agg(
        struct(
            product_name,
            total_revenue,
            revenue_rank
        )
        order by revenue_rank 
        limit 3
    ) as top_products,
    
    current_timestamp() as last_updated_at
    
from {{ ref('dim_products') }}
group by category, abc_class