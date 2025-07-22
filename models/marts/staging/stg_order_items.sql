with source_data as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        total_price
    from {{ source('ecommerce_seeds', 'sample_order_items') }}
),

cleaned_data as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        total_price,
        
        -- Calculate line-level metrics
        quantity * unit_price as calculated_total_price,
        
        -- Add data quality flags
        case 
            when abs(total_price - (quantity * unit_price)) > 0.01 then true
            else false
        end as price_discrepancy_flag
        
    from source_data
)

select * from cleaned_data