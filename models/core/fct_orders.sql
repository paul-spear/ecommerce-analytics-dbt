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

with order_details as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        o.total_amount,
        o.shipping_cost,
        o.tax_amount,
        o.product_amount,
        o.order_year,
        o.order_month,
        o.order_quarter,
        o.order_month_year,
        
        -- Add customer information
        c.customer_segment,
        c.acquisition_cohort,
        
        -- Add order metrics
        count(oi.order_item_id) as total_items,
        sum(oi.quantity) as total_quantity,
        count(distinct oi.product_id) as unique_products,
        sum(oi.quantity * p.cost) as total_cost,
        sum(oi.total_price) - sum(oi.quantity * p.cost) as total_profit,
        
        -- Calculate order sequence for customer
        row_number() over (
            partition by o.customer_id 
            order by o.order_date
        ) as customer_order_sequence
        
    from {{ ref('stg_orders') }} o
    inner join {{ ref('stg_order_items') }} oi
        on o.order_id = oi.order_id
    inner join {{ ref('stg_products') }} p
        on oi.product_id = p.product_id
    left join {{ ref('dim_customers') }} c
        on o.customer_id = c.customer_id
    
    {% if is_incremental() %}
        where o.order_date > (select max(order_date) from {{ this }})
    {% endif %}
    
    group by 
        o.order_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        o.total_amount,
        o.shipping_cost,
        o.tax_amount,
        o.product_amount,
        o.order_year,
        o.order_month,
        o.order_quarter,
        o.order_month_year,
        c.customer_segment,
        c.acquisition_cohort
)

select * from order_details