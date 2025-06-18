WITH base AS (
  SELECT
    toDate(event_time) AS event_date,
    user_id,
    event_type,
    is_success
  FROM {{ ref('events_flat') }}
  WHERE event_type IN ('task_start', 'task_finish')
    AND event_time BETWEEN '{{ var("run_date") }} 00:00:00' AND '{{ var("run_date") }} 23:59:59'
),
agg AS (
  SELECT
    event_date,
    user_id,
    countIf(event_type = 'task_start') AS tasks_started,
    countIf(event_type = 'task_finish') AS tasks_completed,
    countIf(event_type = 'task_finish' AND is_success = 1) AS tasks_successful
  FROM base
  GROUP BY event_date, user_id
)
SELECT * FROM agg