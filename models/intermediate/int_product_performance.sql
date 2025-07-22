with product_sales as (
    select
        oi.product_id,
        p.product_name,
        p.category,
        p.brand,
        p.price_tier,
        
        -- Sales metrics
        count(distinct oi.order_id) as total_orders,
        sum(oi.quantity) as total_quantity_sold,
        sum(oi.total_price) as total_revenue,
        avg(oi.unit_price) as avg_selling_price,
        count(distinct o.customer_id) as unique_customers,
        
        -- Profitability
        sum(oi.quantity * p.cost) as total_cost,
        sum(oi.total_price) - sum(oi.quantity * p.cost) as total_profit,
        
        -- Time metrics
        min(o.order_date) as first_sale_date,
        max(o.order_date) as last_sale_date,
        
        -- BigQuery-specific: days between first and last sale
        date_diff(max(o.order_date), min(o.order_date), day) as days_selling
        
    from {{ ref('stg_order_items') }} oi
    inner join {{ ref('stg_orders') }} o
        on oi.order_id = o.order_id
    inner join {{ ref('stg_products') }} p
        on oi.product_id = p.product_id
    where o.order_status = 'delivered'
    group by 
    oi.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price_tier
),

product_rankings as (
    select
        *,
        -- Calculate rankings
        row_number() over (order by total_revenue desc) as revenue_rank,
        row_number() over (order by total_quantity_sold desc) as quantity_rank,
        row_number() over (order by total_profit desc) as profit_rank,
        
        -- Calculate percentiles for ABC analysis
        percent_rank() over (order by total_revenue) as revenue_percentile,
        
        -- Add velocity metrics
        case 
            when days_selling > 0 then total_quantity_sold / days_selling
            else 0
        end as daily_velocity
        
    from product_sales
)

select 
    *,
    -- ABC Classification
    case 
        when revenue_percentile >= 0.8 then 'A'
        when revenue_percentile >= 0.6 then 'B'
        else 'C'
    end as abc_class
from product_rankings