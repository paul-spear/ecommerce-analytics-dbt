{{
  config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={
      "field": "order_date",
      "data_type": "date"
    },
    cluster_by=["customer_id", "order_status"]
  )
}}

with source_data as (
    select
        order_id,
        customer_id,
        order_date,
        order_status,
        total_amount,
        shipping_cost,
        tax_amount
    from {{ source('ecommerce_seeds', 'sample_orders') }}
    
    {% if is_incremental() %}
        -- Only process new orders in incremental runs
        where order_date > (select max(order_date) from {{ this }})
    {% endif %}
),

cleaned_data as (
    select
        order_id,
        customer_id,
        case 
            when order_date is null then current_date()
            else date(order_date)
        end as order_date,
        lower(trim(order_status)) as order_status,
        total_amount,
        coalesce(shipping_cost, 0) as shipping_cost,
        coalesce(tax_amount, 0) as tax_amount,
        
        -- Calculate net amounts
        total_amount - coalesce(shipping_cost, 0) - coalesce(tax_amount, 0) as product_amount,
        
        -- Extract date parts
        extract(year from date(order_date)) as order_year,
        extract(month from date(order_date)) as order_month,
        extract(quarter from date(order_date)) as order_quarter,
        date_trunc(date(order_date), month) as order_month_year
        
    from source_data
)

select * from cleaned_data