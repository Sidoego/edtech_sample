WITH registrations AS (
  SELECT
    user_id,
    MIN(event_date) AS cohort_date
  FROM {{ ref('user_activity_daily') }}
  GROUP BY user_id
),
activity AS (
  SELECT
    user_id,
    event_date
  FROM {{ ref('user_activity_daily') }}
),
joined AS (
  SELECT
    r.cohort_date,
    a.event_date,
    datediff('day', r.cohort_date, a.event_date) AS day_offset
  FROM registrations r
  JOIN activity a USING (user_id)
  WHERE a.event_date >= r.cohort_date
    AND datediff('day', r.cohort_date, a.event_date) <= 30
)
SELECT
  cohort_date,
  day_offset,
  count(DISTINCT user_id) AS retained_users
FROM joined
GROUP BY cohort_date, day_offset