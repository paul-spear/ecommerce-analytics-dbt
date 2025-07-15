{# Macro for BigQuery date partitioning #}
{% macro bigquery_partition_by_date(column_name) %}
  partition_by={
    "field": "{{ column_name }}",
    "data_type": "date",
    "granularity": "day"
  }
{% endmacro %}

{# Macro for BigQuery clustering #}
{% macro bigquery_cluster_by(columns) %}
  cluster_by={{ columns }}
{% endmacro %}

{# Macro for BigQuery table expiration #}
{% macro bigquery_table_expiration_days(days) %}
  hours_to_expiration={{ days * 24 }}
{% endmacro %}