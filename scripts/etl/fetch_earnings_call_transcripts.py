

import os
import sys
import datetime
import requests
import pandas as pd
import snowflake.connector
import boto3
import csv
import json
from io import StringIO

# Constants
API_KEY = None  # Will be set in main()
API_URL = "https://www.alphavantage.co/query"
START_YEAR = 2010
START_QUARTER = 1
TODAY = datetime.date.today()
S3_PREFIX = "earnings_call_transcript/"

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
            time.sleep(0.80)  # Fixed delay to avoid rate limiting
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

def bulk_update_watermarks(cur, successful_updates, failed_symbols):
    """
    Bulk update ETL_WATERMARKS for earnings call transcripts.
    successful_updates: list of dicts with symbol, first_date, last_date
    failed_symbols: list of symbols (str)
    """
    # Successful updates: batch MERGE
    if successful_updates:
        cur.execute("""
            CREATE TEMPORARY TABLE WATERMARK_UPDATES (
                SYMBOL VARCHAR(20),
                FIRST_DATE DATE,
                LAST_DATE DATE
            )
        """)
        values_list = []
        for update in successful_updates:
            values_list.append(
                f"('{update['symbol']}', TO_DATE('{update['first_date']}', 'YYYY-MM-DD'), TO_DATE('{update['last_date']}', 'YYYY-MM-DD'))"
            )
        values_clause = ',\n'.join(values_list)
        cur.execute(f"""
            INSERT INTO WATERMARK_UPDATES (SYMBOL, FIRST_DATE, LAST_DATE)
            VALUES {values_clause}
        """)
        cur.execute(f"""
            MERGE INTO ETL_WATERMARKS target
            USING WATERMARK_UPDATES source
            ON target.TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT'
               AND target.SYMBOL = source.SYMBOL
            WHEN MATCHED THEN UPDATE SET
                FIRST_FISCAL_DATE = COALESCE(target.FIRST_FISCAL_DATE, source.FIRST_DATE),
                LAST_FISCAL_DATE = source.LAST_DATE,
                LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(),
                CONSECUTIVE_FAILURES = 0,
                UPDATED_AT = CURRENT_TIMESTAMP()
        """)
        print(f"✅ Bulk updated {len(successful_updates)} successful watermarks.")
    # Failed symbols: batch UPDATE
    if failed_symbols:
        symbols_list = "', '".join(failed_symbols)
        # Increment CONSECUTIVE_FAILURES for failed symbols
        cur.execute(f"""
            UPDATE ETL_WATERMARKS
            SET CONSECUTIVE_FAILURES = COALESCE(CONSECUTIVE_FAILURES, 0) + 1,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT'
              AND SYMBOL IN ('{symbols_list}')
        """)
        print(f"✅ Updated {len(failed_symbols)} failed watermarks.")

        # Mark as SUS if threshold exceeded (batch)
        threshold = int(os.getenv('CONSECUTIVE_FAILURE_THRESHOLD', '3'))
        cur.execute(f"""
            UPDATE ETL_WATERMARKS
            SET API_ELIGIBLE = 'SUS', UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT'
              AND SYMBOL IN ('{symbols_list}')
              AND COALESCE(CONSECUTIVE_FAILURES, 0) >= {threshold}
        """)
        print(f"🔒 Marked symbols as SUS if failures >= {threshold}.")

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
    # Load private key for key pair authentication
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.backends import default_backend
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_rsa_key.der")
    if not os.path.isfile(key_path):
        print(f"❌ Private key file not found: {key_path}")
        print(f"Current working directory: {os.getcwd()}")
        print("Make sure the key is decoded and present before running this script.")
        sys.exit(1)
    with open(key_path, "rb") as key_file:
        private_key = serialization.load_der_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    pk_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        role='ETL_ROLE',
        private_key=pk_bytes,
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

    # New: skip symbols with latest fiscal date within X days (SQL logic)
    query = """
        SELECT SYMBOL, IPO_DATE, LAST_FISCAL_DATE
        FROM ETL_WATERMARKS
        WHERE TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT'
          AND API_ELIGIBLE = 'YES'
          AND STATUS = 'Active'
          AND (LAST_FISCAL_DATE IS NULL 
               OR LAST_FISCAL_DATE < DATEADD(day, -135, CURRENT_DATE())
               )
             AND (LAST_SUCCESSFUL_RUN IS NULL 
                   OR LAST_SUCCESSFUL_RUN < DATEADD(hour, -168, CURRENT_TIMESTAMP()))
    """
    if max_symbols:
        query += f"\n        LIMIT {max_symbols}"
    cur.execute(query)
    rows = cur.fetchall()
        # Initialize a list of symbols not found
    successful_updates = []
    failed_symbols = []
    sus_symbols = []
    for symbol, ipo_date, last_fiscal_date in rows:
        if ipo_date is None or ipo_date < datetime.date(START_YEAR, 1, 1):
            start_date = datetime.date(START_YEAR, 1, 1)
        else:
            start_date = first_full_quarter_after(ipo_date)
        quarters = get_quarters(start_date, TODAY)
        found_data = False
        first_date = None
        last_date = None
        for year, quarter in quarters:
            data = fetch_transcript(symbol, year, quarter, api_key)
            if data and "transcript" in data and data["transcript"]:
                found_data = True
                # Set first/last date for watermark update
                fiscal_date = f"{year}-{'0' if quarter < 10 else ''}{(quarter-1)*3+1:02d}-01"
                if not first_date:
                    first_date = fiscal_date
                last_date = fiscal_date
                upload_to_s3_transcript(symbol, year, quarter, data, s3_client, bucket)
        if found_data:
            successful_updates.append({
                'symbol': symbol,
                'first_date': first_date,
                'last_date': last_date
            })
        else:
            failed_symbols.append(symbol)
            sus_symbols.append(symbol)
            print(f"⚠️  No earnings call transcript data for {symbol}")

    # Batch update SUS for all symbols with no records pulled
    if sus_symbols:
        sus_list = "', '".join(sus_symbols)
        cur.execute(f"""
            UPDATE ETL_WATERMARKS
            SET API_ELIGIBLE = 'SUS', UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT'
              AND SYMBOL IN ('{sus_list}')
        """)
        print(f"🔒 Marked {len(sus_symbols)} symbols as SUS due to no records pulled.")

    bulk_update_watermarks(cur, successful_updates, failed_symbols)
    if len(successful_updates) == 0 and len(failed_symbols) > 0:
        print("❌ All symbols failed to fetch transcripts. Aborting downstream steps.")
        cur.close()
        conn.close()
        sys.exit(1)
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()