WITH test_users AS (
  SELECT user_id, test_id, variant
  FROM {{ ref('ab_test_assignments') }}
),
activity AS (
  SELECT user_id
  FROM {{ ref('user_activity_daily') }}
  WHERE event_date = '{{ var("run_date") }}'
),
joined AS (
  SELECT
    tu.test_id,
    tu.variant,
    a.user_id
  FROM test_users tu
  JOIN activity a USING (user_id)
)
SELECT
  test_id,
  variant,
  'DAU' AS metric_name,
  count(*) AS value,
  count(*) AS users_count
FROM joined
GROUP BY test_id, variant