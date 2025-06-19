with user_first_event as (
  select user_id, min(event_timestamp) as first_event_date from {{ ref('stg_events') }}
  group by user_id
),
events_with_cohort as (
  select
    e.user_id,
    e.event_timestamp,
    toDate(ufe.first_event_date) as cohort_date,
    toDate(e.event_timestamp) as event_date
  from {{ ref('stg_events') }} e
  join user_first_event ufe on e.user_id = ufe.user_id
)
select
  cohort_date,
  event_date,
  count(distinct user_id) as active_users,
  round(
    active_users * 100.0 /
    (select count(distinct user_id) from {{ ref('stg_events') }} where toDate(event_timestamp) = cohort_date),
    2
  ) as retention_percent
from events_with_cohort
group by cohort_date, event_date
order by cohort_date, event_date
