{{
  config(
    materialized='table',
    partition_by={
      "field": "acquisition_cohort",
      "data_type": "date"
    },
    cluster_by=["period_number"]
  )
}}

with cohort_data as (
    select
        c.acquisition_cohort,
        o.order_month_year,
        {{ cohort_analysis('c.acquisition_cohort', 'o.order_month_year') }},
        
        -- Customer metrics by cohort period
        count(distinct o.customer_id) as active_customers,
        sum(o.total_amount) as cohort_revenue,
        count(distinct o.order_id) as cohort_orders,
        avg(o.total_amount) as avg_order_value
        
    from {{ ref('stg_orders') }} o
    inner join {{ ref('stg_customers') }} c
        on o.customer_id = c.customer_id
    where o.order_status = 'delivered'
    group by 
        c.acquisition_cohort,
        o.order_month_year
),

cohort_base as (
    select
        acquisition_cohort,
        count(distinct customer_id) as cohort_size
    from {{ ref('stg_customers') }}
    group by acquisition_cohort
),

cohort_metrics as (
    select
        cd.acquisition_cohort,
        cd.period_number,
        cd.order_month_year,
        cb.cohort_size,
        cd.active_customers,
        cd.cohort_revenue,
        cd.cohort_orders,
        cd.avg_order_value,
        
        -- Calculate retention rates
        round((cd.active_customers / cb.cohort_size) * 100, 2) as retention_rate,
        
        -- Calculate cumulative metrics
        sum(cd.cohort_revenue) over (
            partition by cd.acquisition_cohort 
            order by cd.period_number
        ) as cumulative_revenue,
        
        sum(cd.active_customers) over (
            partition by cd.acquisition_cohort 
            order by cd.period_number
        ) as cumulative_customers
        
    from cohort_data cd
    inner join cohort_base cb
        on cd.acquisition_cohort = cb.acquisition_cohort
)

select
    *,
    -- Calculate customer lifetime value by cohort
    round(cumulative_revenue / cohort_size, 2) as avg_clv_to_date,
    
    current_timestamp() as last_updated_at
    
from cohort_metrics