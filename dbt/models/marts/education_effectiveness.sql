select
  toDate(event_timestamp) as day,
  countIf(task_result = 'success') * 100.0 / count() as success_rate_percent
from {{ ref('stg_task_results') }}
group by day
order by day
