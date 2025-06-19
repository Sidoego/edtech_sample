with events as (
    select
        user_id,
        session_id,
        event_timestamp::Date as event_date
    from {{ ref('stg_events') }}
),

-- Считаем уникальные пользователей по дням, неделям, месяцам
daily_active_users as (
    select
        event_date,
        count(distinct user_id) as dau
    from events
    group by event_date
),

weekly_active_users as (
    select
        toStartOfWeek(event_date) as week_start,
        count(distinct user_id) as wau
    from events
    group by week_start
),

monthly_active_users as (
    select
        toStartOfMonth(event_date) as month_start,
        count(distinct user_id) as mau
    from events
    group by month_start
),

sessions_per_user as (
    select
        event_date,
        count(distinct session_id) as sessions,
        count(distinct user_id) as users
    from events
    group by event_date
),

events_per_session as (
    select
        event_date,
        count(*) as total_events,
        count(distinct session_id) as total_sessions
    from events
    group by event_date
)

-- Объединяем всё по дате
select
    d.event_date,
    d.dau,
    w.wau,
    m.mau,
    round(d.dau / nullIf(w.wau, 0), 4) as dau_wau_ratio,
    round(d.dau / nullIf(m.mau, 0), 4) as dau_mau_ratio,
    round(w.wau / nullIf(m.mau, 0), 4) as wau_mau_ratio,
    s.sessions,
    round(s.sessions / nullIf(s.users, 0), 2) as avg_sessions_per_user,
    round(e.total_events / nullIf(e.total_sessions, 0), 2) as avg_events_per_session
from daily_active_users d
left join weekly_active_users w on w.week_start = toStartOfWeek(d.event_date)
left join monthly_active_users m on m.month_start = toStartOfMonth(d.event_date)
left join sessions_per_user s on s.event_date = d.event_date
left join events_per_session e on e.event_date = d.event_date
order by d.event_date
