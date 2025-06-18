{{ config(materialized='table') }}

SELECT
  toDate(event_time) AS event_date,
  event_time,
  user_id,
  event_type,
  properties['course_id'] AS course_id,
  properties['screen'] AS screen,
  properties['task_id'] AS task_id,
  properties['is_success']::UInt8 AS is_success,
  properties['amount']::Float64 AS amount
FROM raw.events_raw
WHERE event_date = '{{ var("run_date") }}' 