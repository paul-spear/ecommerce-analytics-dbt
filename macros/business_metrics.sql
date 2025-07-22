{# Macro to calculate RFM scores - BigQuery optimized #}
{% macro calculate_rfm_score(recency_days, frequency_orders, monetary_value) %}
    -- Recency Score (lower days = higher score)
    case 
        when {{ recency_days }} <= 30 then 5
        when {{ recency_days }} <= 60 then 4
        when {{ recency_days }} <= 90 then 3
        when {{ recency_days }} <= 180 then 2
        else 1
    end as recency_score,
    
    -- Frequency Score (more orders = higher score)
    case 
        when {{ frequency_orders }} >= 10 then 5
        when {{ frequency_orders }} >= 5 then 4
        when {{ frequency_orders }} >= 3 then 3
        when {{ frequency_orders }} >= 2 then 2
        else 1
    end as frequency_score,
    
    -- Monetary Score (higher spend = higher score)
    case 
        when {{ monetary_value }} >= 1000 then 5
        when {{ monetary_value }} >= 500 then 4
        when {{ monetary_value }} >= 200 then 3
        when {{ monetary_value }} >= 100 then 2
        else 1
    end as monetary_score
{% endmacro %}

{# Macro to segment customers based on RFM scores #}
{% macro segment_customers(recency_score, frequency_score, monetary_score) %}
    case 
        when {{ recency_score }} >= 4 and {{ frequency_score }} >= 4 and {{ monetary_score }} >= 4 then 'Champions'
        when {{ recency_score }} >= 3 and {{ frequency_score }} >= 3 and {{ monetary_score }} >= 3 then 'Loyal Customers'
        when {{ recency_score }} >= 4 and {{ frequency_score }} <= 2 then 'New Customers'
        when {{ recency_score }} >= 3 and {{ frequency_score }} >= 2 and {{ monetary_score }} <= 2 then 'Potential Loyalists'
        when {{ recency_score }} <= 2 and {{ frequency_score }} >= 3 and {{ monetary_score }} >= 3 then 'At Risk'
        when {{ recency_score }} <= 2 and {{ frequency_score }} >= 4 and {{ monetary_score }} >= 4 then 'Cannot Lose Them'
        when {{ recency_score }} <= 3 and {{ frequency_score }} <= 2 and {{ monetary_score }} <= 2 then 'Hibernating'
        else 'Others'
    end
{% endmacro %}

{# Macro for customer metrics calculation - BigQuery optimized #}
{% macro customer_order_metrics() %}
    count(distinct order_id) as total_orders,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_order_value,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    date_diff(current_date(), max(order_date), day) as days_since_last_order
{% endmacro %}

{# Macro for cohort analysis - BigQuery optimized #}
{% macro cohort_analysis(acquisition_cohort, order_month_year) %}
    date_diff({{ order_month_year }}, {{ acquisition_cohort }}, month) as period_number
{% endmacro %}

{# BigQuery-specific utility macros #}
{% macro bigquery_date_spine(start_date, end_date, datepart) %}
    select
        date_add(date('{{ start_date }}'), interval row_number() over (order by 1) - 1 {{ datepart }}) as date_day
    from
        unnest(generate_array(1, date_diff(date('{{ end_date }}'), date('{{ start_date }}'), {{ datepart }}) + 1)) as row_num
{% endmacro %}

{# Macro for BigQuery arrays and structs #}
{% macro create_product_array(product_ids, product_names) %}
    array(
        select as struct
            product_id,
            product_name
        from unnest([{{ product_ids }}]) as product_id with offset
        join unnest([{{ product_names }}]) as product_name with offset using (offset)
    )
{% endmacro %}