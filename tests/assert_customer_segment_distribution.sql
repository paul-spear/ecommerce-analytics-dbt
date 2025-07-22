-- Test that customer segments are properly distributed
select
    customer_segment,
    count(*) as customer_count
from {{ ref('dim_customers') }}
where customer_segment in ('Champions', 'Loyal Customers', 'New Customers')
group by customer_segment
having count(*) = 0