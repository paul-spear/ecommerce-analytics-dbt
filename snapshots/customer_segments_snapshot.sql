{% snapshot customer_segments_snapshot %}

{{
    config(
      target_dataset='ecommerce_analytics',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='last_updated_at',
    )
}}

select
    customer_id,
    customer_segment,
    lifetime_revenue,
    lifetime_orders,
    estimated_clv,
    last_updated_at
from {{ ref('dim_customers') }}
where customer_segment != 'No Orders'

{% endsnapshot %}