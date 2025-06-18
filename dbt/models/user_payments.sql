SELECT
  toDate(event_time) AS event_date,
  user_id,
  amount,
  course_id,
  CASE
    WHEN purchase_type = 'renewal' THEN 'renewal'
    ELSE 'initial'
  END AS purchase_type
FROM {{ ref('events_flat') }}
WHERE event_type = 'purchase' 