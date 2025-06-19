select
  ab_test_id,
  count(distinct user_id) as users_count,
  avgIf(
    user_id in (select user_id from {{ ref('stg_task_results') }} where task_result = 'success'),
    user_id is not null
  ) as success_rate
from {{ ref('stg_ab_test_assignments') }}
group by ab_test_id
order by ab_test_id
