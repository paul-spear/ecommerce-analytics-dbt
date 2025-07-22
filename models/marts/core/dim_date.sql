{{ config(materialized='table') }}

with date_spine as (
    {{ bigquery_date_spine('2023-01-01', '2025-12-31', 'day') }}
),

date_dimensions as (
    select
        date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(quarter from date_day) as quarter,
        extract(dayofweek from date_day) as day_of_week,
        extract(dayofyear from date_day) as day_of_year,
        extract(week from date_day) as week_of_year,
        
        -- Format functions
        format_date('%A', date_day) as day_name,
        format_date('%B', date_day) as month_name,
        format_date('%Y-%m', date_day) as year_month,
        
        -- Business logic
        case 
            when extract(dayofweek from date_day) in (1, 7) then false
            else true
        end as is_weekday,
        
        -- Fiscal periods (assuming fiscal year starts in January)
        case 
            when extract(month from date_day) <= 3 then 'Q1'
            when extract(month from date_day) <= 6 then 'Q2'
            when extract(month from date_day) <= 9 then 'Q3'
            else 'Q4'
        end as fiscal_quarter
        
    from date_spine
)

select * from date_dimensions