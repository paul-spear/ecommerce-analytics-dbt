with source_data as (
    select
        product_id,
        product_name,
        category,
        brand,
        price,
        cost
    from {{ source('ecommerce_seeds', 'sample_products') }}
),

cleaned_data as (
    select
        product_id,
        trim(product_name) as product_name,
        trim(category) as category,
        trim(brand) as brand,
        price,
        cost,
        
        -- Calculate margins
        price - cost as margin_amount,
        case 
            when price > 0 then round(((price - cost) / price) * 100, 2)
            else 0
        end as margin_percentage,
        
        -- Price categorization using BigQuery CASE
        case 
            when price < 50 then 'Low'
            when price between 50 and 200 then 'Medium'
            when price between 200 and 500 then 'High'
            else 'Premium'
        end as price_tier
        
    from source_data
)

select * from cleaned_data