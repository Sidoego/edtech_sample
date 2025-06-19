import pandas as pd
from clickhouse_connect import Client
import os

INPUT_FILE = os.getenv('INPUT_FILE', 'raw_events.jsonl')

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "default")

client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB
)

def load_df(table_name, df):
    df = df.where(pd.notnull(df), None)
    client.insert_df(table_name, df)
    print(f"[INFO] Inserted {len(df)} rows into {table_name}")

def main():
    raw_events = pd.read_json(INPUT_FILE, lines=True)
    raw_events['event_timestamp'] = pd.to_datetime(raw_events['event_timestamp'])

    events = raw_events[['event_id', 'user_id', 'event_type', 'event_timestamp', 'session_id', 'platform', 'screen_name']].copy()
    load_df('events', events)

    purchases = raw_events[raw_events['event_type'] == 'purchase'][['event_id', 'user_id', 'event_timestamp', 'purchase_amount', 'session_id']]
    load_df('purchases', purchases)

    task_results = raw_events[raw_events['event_type'] == 'task_finish'][['event_id', 'user_id', 'event_timestamp', 'task_id', 'task_result']]
    load_df('task_results', task_results)

    sessions = raw_events[['session_id', 'user_id', 'event_timestamp']].dropna(subset=['session_id'])
    sessions = sessions.groupby(['session_id', 'user_id']).agg(
        session_start=('event_timestamp', 'min'),
        session_end=('event_timestamp', 'max'),
        event_count=('event_timestamp', 'count')
    ).reset_index()
    load_df('sessions', sessions)

    ab = raw_events[['user_id', 'ab_test_id']].dropna().drop_duplicates()
    ab['assignment_timestamp'] = pd.Timestamp.utcnow()
    load_df('ab_test_assignments', ab)

if __name__ == "__main__":
    main()
