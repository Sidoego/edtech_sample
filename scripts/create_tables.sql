CREATE DATABASE IF NOT EXISTS edtech_analytics;

USE edtech_analytics;

-- Таблица событий
CREATE TABLE IF NOT EXISTS events (
    event_id String,
    user_id String,
    event_type String,
    event_timestamp DateTime,
    session_id String,
    platform String,
    screen_name String
) ENGINE = MergeTree()
ORDER BY (event_timestamp, user_id);

-- Таблица покупок
CREATE TABLE IF NOT EXISTS purchases (
    event_id String,
    user_id String,
    event_timestamp DateTime,
    purchase_amount Float64,
    session_id String
) ENGINE = MergeTree()
ORDER BY (event_timestamp, user_id);

-- Таблица результатов задач
CREATE TABLE IF NOT EXISTS task_results (
    event_id String,
    user_id String,
    event_timestamp DateTime,
    task_id String,
    task_result String
) ENGINE = MergeTree()
ORDER BY (event_timestamp, user_id);

-- Таблица сессий
CREATE TABLE IF NOT EXISTS sessions (
    session_id String,
    user_id String,
    session_start DateTime,
    session_end DateTime,
    event_count UInt32
) ENGINE = MergeTree()
ORDER BY (session_start, user_id);

-- Таблица назначений A/B теста
CREATE TABLE IF NOT EXISTS ab_test_assignments (
    user_id String,
    ab_test_id String,
    assignment_timestamp DateTime
) ENGINE = MergeTree()
ORDER BY (assignment_timestamp, user_id);
