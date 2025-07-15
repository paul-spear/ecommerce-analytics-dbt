{{
  config(
    materialized='table',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["customer_id", "order_status"]
  )
}}

select
    order_id,
    customer_id,
    order_date,
    order_status,
    total_amount
from {{ ref('stg_orders') }}