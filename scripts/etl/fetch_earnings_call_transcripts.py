def cleanup_s3_json(bucket: str, prefix: str, s3_client) -> int:
    """Delete all .json files in the S3 prefix."""
    deleted = 0
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            json_objects = [obj for obj in page['Contents'] if obj['Key'].endswith('.json')]
            if json_objects:
                s3_client.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': obj['Key']} for obj in json_objects]})
                deleted += len(json_objects)
    print(f"🧹 Cleaned up S3 bucket: s3://{bucket}/{prefix} ({deleted} .json files deleted)")
    return deleted
import os
import datetime
import requests
import pandas as pd
import snowflake.connector
import boto3
import csv
import json
from io import StringIO

# Constants
# Constants
API_KEY = None  # Will be set in main()
API_URL = "https://www.alphavantage.co/query"
START_YEAR = 2010
START_QUARTER = 1
TODAY = datetime.date.today()
S3_PREFIX = "earnings_call_transcript/"
def upload_to_s3_transcript(symbol: str, year: int, quarter: int, data: dict, s3_client, bucket: str) -> bool:
    """Upload transcript data to S3 as JSON."""
    try:
        s3_key = f"{S3_PREFIX}{symbol}_{year}Q{quarter}.json"
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(data).encode('utf-8'),
            ContentType='application/json'
        )
        print(f"✅ Uploaded: s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        print(f"❌ S3 upload failed for {symbol} {year}Q{quarter}: {e}")
        return False

def get_quarters(start_date, end_date):
    """Generate (year, quarter) tuples from start_date to end_date."""
    quarters = []
    year = start_date.year
    quarter = (start_date.month - 1) // 3 + 1
    while (year < end_date.year) or (year == end_date.year and quarter <= ((end_date.month - 1) // 3 + 1)):
        quarters.append((year, quarter))
        quarter += 1
        if quarter > 4:
            quarter = 1
            year += 1
    return quarters

def first_full_quarter_after(date):
    """Return the first full quarter after a given date."""
    month = ((date.month - 1) // 3 + 1) * 3 + 1
    year = date.year
    if month > 12:
        month = 1
        year += 1
    return datetime.date(year, month, 1)

import time
from requests.exceptions import RequestException

def fetch_transcript(symbol, year, quarter, api_key, max_retries=3, backoff=2):
    params = {
        "function": "EARNINGS_CALL_TRANSCRIPT",
        "symbol": symbol,
        "quarter": f"{year}Q{quarter}",
        "apikey": api_key
    }
    for attempt in range(max_retries):
        try:
            response = requests.get(API_URL, params=params, timeout=30)
            if response.status_code == 200 and response.json():
                return response.json()
            else:
                # Optionally log non-200 responses
                pass
        except RequestException as e:
            # Optionally log error: print(f"Network error for {symbol} {year}Q{quarter}: {e}")
            if attempt < max_retries - 1:
                time.sleep(backoff ** attempt)
            else:
                return None
    return None

def main():
    # Get API key from environment (strict)
    api_key = os.environ["ALPHAVANTAGE_API_KEY"]
    bucket = os.environ["S3_BUCKET"]
    s3_client = boto3.client("s3")
    # Clean up .json files in S3 bucket before starting
    cleanup_s3_json(bucket, S3_PREFIX, s3_client)
    # Get API key from environment (strict)
    api_key = os.environ["ALPHAVANTAGE_API_KEY"]
    # Connect to Snowflake and get eligible symbols
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="FIN_TRADE_EXTRACT",
        schema="RAW"
    )
    cur = conn.cursor()
    max_symbols = os.getenv("MAX_SYMBOLS")
    try:
        max_symbols = int(max_symbols) if max_symbols else None
    except Exception:
        max_symbols = None

    query = """
        SELECT SYMBOL, IPO_DATE
        FROM ETL_WATERMARKS
        WHERE TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT'
          AND API_ELIGIBLE = 'YES'
          AND STATUS = 'Active'
    """
    if max_symbols:
        query += f"\n        LIMIT {max_symbols}"
    cur.execute(query)
    rows = cur.fetchall()
    for symbol, ipo_date in rows:
        if ipo_date is None or ipo_date < datetime.date(START_YEAR, 1, 1):
            start_date = datetime.date(START_YEAR, 1, 1)
        else:
            start_date = first_full_quarter_after(ipo_date)
        quarters = get_quarters(start_date, TODAY)
        found_data = False
        for year, quarter in quarters:
            data = fetch_transcript(symbol, year, quarter, api_key)
            if data and "transcript" in data and data["transcript"]:
                # print(f"API successful for {symbol} {year}Q{quarter}")
                pass
                found_data = True
                upload_to_s3_transcript(symbol, year, quarter, data, s3_client, bucket)
            else:
                # print(f"API failed for {symbol} {year}Q{quarter}")
                pass
        if not found_data:
            print(f"⚠️  No earnings call transcript data for {symbol}")
            # Update watermark to set API_ELIGIBLE = 'SUS'
            cur.execute("""
                UPDATE ETL_WATERMARKS
                SET API_ELIGIBLE = 'SUS', UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE SYMBOL = %s AND TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT'
            """, (symbol,))
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()