import os
import datetime
import requests
import pandas as pd
import snowflake.connector
import boto3
import csv
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
    """Upload transcript data to S3 as CSV, including sentiment score."""
    try:
        s3_key = f"{S3_PREFIX}{symbol}_{year}Q{quarter}.csv"
        output = StringIO()
        writer = csv.writer(output)
        # Write header
        writer.writerow(["SYMBOL", "QUARTER", "TRANSCRIPT_DATE", "TRANSCRIPT_TEXT", "SPEAKER", "ROLE", "SENTIMENT"])
        for entry in data["transcript"]:
            writer.writerow([
                symbol,
                f"{year}Q{quarter}",
                entry.get("date", ""),
                entry.get("content", entry.get("text", "")),
                entry.get("speaker", ""),
                entry.get("title", entry.get("role", "")),
                entry.get("sentiment", "")
            ])
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=output.getvalue().encode('utf-8'),
            ContentType='text/csv'
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

def fetch_transcript(symbol, year, quarter, api_key):
    params = {
        "function": "EARNINGS_CALL_TRANSCRIPT",
        "symbol": symbol,
        "quarter": f"{year}Q{quarter}",
        "apikey": api_key
    }
    response = requests.get(API_URL, params=params)
    if response.status_code == 200 and response.json():
        return response.json()
    return None

def main():
    # Get API key from environment (strict)
    api_key = os.environ["ALPHAVANTAGE_API_KEY"]
    bucket = os.environ["S3_BUCKET"]
    s3_client = boto3.client("s3")
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
                print(f"API successful for {symbol} {year}Q{quarter}")
                found_data = True
                upload_to_s3_transcript(symbol, year, quarter, data, s3_client, bucket)
            else:
                print(f"API failed for {symbol} {year}Q{quarter}")
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