{{
  config(
    materialized='incremental',
    unique_key='date',
    partition_by={
      "field": "date",
      "data_type": "date"
    },
    cluster_by=["date"]
  )
}}

with daily_metrics as (
    select
        o.order_date as date,
        
        -- Order metrics
        count(distinct o.order_id) as total_orders,
        count(distinct o.customer_id) as unique_customers,
        sum(o.total_amount) as total_revenue,
        sum(o.product_amount) as product_revenue,
        sum(o.shipping_cost) as shipping_revenue,
        sum(o.tax_amount) as tax_revenue,
        avg(o.total_amount) as avg_order_value,
        
        -- Customer metrics
        count(distinct case when o.customer_order_sequence = 1 then o.customer_id end) as new_customers,
        count(distinct case when o.customer_order_sequence > 1 then o.customer_id end) as returning_customers,
        
        -- Product metrics
        sum(o.total_items) as total_items_sold,
        sum(o.total_quantity) as total_quantity_sold,
        sum(o.total_profit) as total_profit,
        
        -- Calculate key ratios
        case 
            when sum(o.total_amount) > 0 
            then round((sum(o.total_profit) / sum(o.total_amount)) * 100, 2)
            else 0
        end as profit_margin_percentage
        
    from {{ ref('fct_orders') }} o
    where o.order_status = 'delivered'
    
    {% if is_incremental() %}
        and o.order_date > (select max(date) from {{ this }})
    {% endif %}
    
    group by o.order_date
)

select
    *,
    -- Add moving averages using BigQuery window functions
    avg(total_revenue) over (
        order by date 
        rows between 6 preceding and current row
    ) as revenue_7day_avg,
    
    avg(total_orders) over (
        order by date 
        rows between 6 preceding and current row
    ) as orders_7day_avg,
    
    -- Calculate growth rates
    lag(total_revenue, 1) over (order by date) as prev_day_revenue,
    lag(total_revenue, 7) over (order by date) as prev_week_revenue,
    
    current_timestamp() as last_updated_at
    
from daily_metrics