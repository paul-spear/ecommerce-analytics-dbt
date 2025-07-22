with customer_orders as (
    select
        o.customer_id,
        o.order_id,
        o.order_date,
        o.total_amount,
        o.order_month_year,
        c.acquisition_cohort,
        
        -- Add order sequence using BigQuery window functions
        row_number() over (
            partition by o.customer_id 
            order by o.order_date
        ) as order_number
        
    from {{ ref('stg_orders') }} o
    inner join {{ ref('stg_customers') }} c
        on o.customer_id = c.customer_id
    where o.order_status = 'delivered'
),

customer_metrics as (
    select
        customer_id,
        acquisition_cohort,
        {{ customer_order_metrics() }}
        
    from customer_orders
    group by customer_id, acquisition_cohort
),

final as (
    select
        cm.*,
        {{ calculate_rfm_score('cm.days_since_last_order', 'cm.total_orders', 'cm.total_revenue') }},
        
        -- Calculate customer lifetime value using BigQuery functions
        case 
            when date_diff(last_order_date, first_order_date, day) > 0
            then round((total_revenue / date_diff(last_order_date, first_order_date, day)) * 365, 2)
            else total_revenue
        end as estimated_clv
        
    from customer_metrics cm
)

select 
    *,
    {{ segment_customers('recency_score', 'frequency_score', 'monetary_score') }} as customer_segment
from final