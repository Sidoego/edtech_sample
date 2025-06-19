with daily_active_users as (
  select toDate(event_timestamp) as day, count(distinct user_id) as dau from {{ ref('stg_events') }}
  group by day
),
daily_payers as (
  select toDate(event_timestamp) as day, count(distinct user_id) as paying_users from {{ ref('stg_purchases') }}
  group by day
)
select
  dau.day,
  dau,
  paying_users,
  paying_users * 100.0 / dau as conversion_percent
from daily_active_users dau
left join daily_payers using(day)
order by dau.day
