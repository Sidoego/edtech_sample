import sys
import boto3
from datetime import datetime

def extract_events(run_date):
    s3 = boto3.client('s3')
    key = f"events/date={run_date}/events.json"
    s3.download_file('my-bucket', key, f"/data/events_{run_date}.json")

if __name__ == '__main__':
    extract_events(sys.argv[1])