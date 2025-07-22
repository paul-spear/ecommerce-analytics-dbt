{{
  config(
    materialized='table',
    cluster_by=["check_type", "status"]
  )
}}

with quality_checks as (
    -- Orders quality checks
    select
        'orders' as table_name,
        'completeness' as check_type,
        'order_amount_null' as check_name,
        count(*) as failing_records,
        current_timestamp() as check_timestamp,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as status
    from {{ ref('fct_orders') }}
    where total_amount is null
    
    union all
    
    -- Customer quality checks
    select
        'customers' as table_name,
        'uniqueness' as check_type,
        'duplicate_emails' as check_name,
        count(*) - count(distinct email) as failing_records,
        current_timestamp() as check_timestamp,
        case when count(*) = count(distinct email) then 'PASS' else 'FAIL' end as status
    from {{ ref('dim_customers') }}
    
    union all
    
    -- Revenue consistency check
    select
        'revenue' as table_name,
        'consistency' as check_type,
        'negative_revenue' as check_name,
        count(*) as failing_records,
        current_timestamp() as check_timestamp,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as status
    from {{ ref('daily_sales_summary') }}
    where total_revenue < 0
    
    union all
    
    -- Freshness check
    select
        'orders' as table_name,
        'freshness' as check_type,
        'data_freshness' as check_name,
        date_diff(current_date(), max(order_date), day) as failing_records,
        current_timestamp() as check_timestamp,
        case when date_diff(current_date(), max(order_date), day) <= 1 then 'PASS' else 'FAIL' end as status
    from {{ ref('fct_orders') }}
)

select
    *,
    -- Add severity levels
    case 
        when check_type = 'freshness' and failing_records > 3 then 'HIGH'
        when check_type = 'completeness' and failing_records > 100 then 'HIGH'
        when check_type = 'consistency' and failing_records > 0 then 'HIGH'
        when failing_records > 0 then 'MEDIUM'
        else 'LOW'
    end as severity
from quality_checks