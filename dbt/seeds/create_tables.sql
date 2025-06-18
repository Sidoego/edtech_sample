-- Сырые данные
CREATE TABLE IF NOT EXISTS raw.events_raw (
  event_time DateTime,
  user_id String,
  event_type String,
  properties Map(String, String)
) ENGINE = MergeTree()
ORDER BY (event_time);

CREATE TABLE IF NOT EXISTS raw.ab_test_assignments (
  user_id String,
  test_id String,
  variant String
) ENGINE = MergeTree()
ORDER BY (test_id, user_id);

-- Промежуточная таблица: нормализованные события
CREATE TABLE IF NOT EXISTS analytics.events_flat (
  event_date Date,
  event_time DateTime,
  user_id String,
  event_type String,
  course_id Nullable(String),
  screen Nullable(String),
  task_id Nullable(String),
  is_success Nullable(UInt8),
  amount Nullable(Float64)
) ENGINE = MergeTree()
ORDER BY (event_time);

-- Ежедневная активность пользователей
CREATE TABLE IF NOT EXISTS analytics.user_activity_daily (
  event_date Date,
  user_id String,
  is_new_user UInt8,
  sessions_count UInt32,
  screens_viewed UInt32
) ENGINE = MergeTree()
ORDER BY (event_date, user_id);

-- Эффективность решений задач
CREATE TABLE IF NOT EXISTS analytics.task_performance_daily (
  event_date Date,
  user_id String,
  tasks_started UInt32,
  tasks_completed UInt32,
  tasks_successful UInt32
) ENGINE = MergeTree()
ORDER BY (event_date, user_id);

-- История покупок
CREATE TABLE IF NOT EXISTS analytics.user_payments (
  event_date Date,
  user_id String,
  amount Float64,
  course_id Nullable(String),
  purchase_type String
) ENGINE = MergeTree()
ORDER BY (event_date, user_id);

-- Ретеншн
CREATE TABLE IF NOT EXISTS analytics.user_retention (
  cohort_date Date,
  day_offset UInt8,
  retained_users UInt32
) ENGINE = MergeTree()
ORDER BY (cohort_date, day_offset);

-- Метрики A/B тестов
CREATE TABLE IF NOT EXISTS analytics.ab_test_metrics (
  test_id String,
  variant String,
  metric_name String,
  value UInt32,
  users_count UInt32
) ENGINE = MergeTree()
ORDER BY (test_id, variant, metric_name);
