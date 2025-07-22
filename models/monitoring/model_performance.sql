-- Monitor model performance and execution times
select
    model_name,
    materialization,
    status,
    execution_time_seconds,
    rows_affected,
    bytes_processed,
    slot_time_ms,
    created_at
from
    `{{ target.project }}.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
where
    job_type = 'QUERY'
    and statement_type = 'SELECT'
    and creation_time >= timestamp_sub(current_timestamp(), interval 24 hour)
    and job_id like '%dbt%'
order by creation_time desc