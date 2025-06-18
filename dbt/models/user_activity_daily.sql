WITH base AS (
  SELECT
    toDate(event_time) AS event_date,
    user_id,
    event_type
  FROM {{ ref('events_flat') }}
  WHERE event_time BETWEEN '{{ var("run_date") }} 00:00:00' AND '{{ var("run_date") }} 23:59:59'
),
agg AS (
  SELECT
    event_date,
    user_id,
    minIf(event_type = 'app_open', 1, 0) AS is_new_user,
    countIf(event_type = 'app_open') AS sessions_count,
    countIf(event_type = 'screen_view') AS screens_viewed
  FROM base
  GROUP BY event_date, user_id
)
SELECT * FROM agg