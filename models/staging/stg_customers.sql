with source_data as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        phone,
        city,
        state,
        country,
        created_at
    from {{ source('ecommerce_seeds', 'sample_customers') }}
),

cleaned_data as (
    select
        customer_id,
        lower(trim(email)) as email,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        phone,
        upper(trim(city)) as city,
        upper(trim(state)) as state,
        upper(trim(country)) as country,
        date(created_at) as customer_created_at,
        
        -- Calculate customer tenure
        date_diff(current_date(), date(created_at), day) as days_since_registration,
        
        -- Customer acquisition cohort
        date_trunc(date(created_at), month) as acquisition_cohort
        
    from source_data
)

select
    customer_id,
    email,
    first_name,
    last_name,
    concat(first_name, ' ', last_name) as full_name,
    phone,
    city,
    state,
    country,
    customer_created_at,
    days_since_registration,
    acquisition_cohort
from cleaned_data