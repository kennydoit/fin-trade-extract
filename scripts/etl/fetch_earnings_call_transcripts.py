import os
import datetime
import requests
import pandas as pd
import snowflake.connector

# Constants
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
API_URL = "https://www.alphavantage.co/query"
START_YEAR = 2010
START_QUARTER = 1
TODAY = datetime.date.today()

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

def fetch_transcript(symbol, year, quarter):
    params = {
        "function": "EARNINGS_CALL_TRANSCRIPT",
        "symbol": symbol,
        "year": year,
        "quarter": quarter,
        "apikey": API_KEY
    }
    response = requests.get(API_URL, params=params)
    if response.status_code == 200 and response.json():
        return response.json()
    return None

def main():
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
    cur.execute("""
        SELECT SYMBOL, IPO_DATE
        FROM ETL_WATERMARK
        WHERE TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT'
          AND API_ELIGIBLE = 'YES'
          AND STATUS = 'Active'
    """)
    rows = cur.fetchall()
    for symbol, ipo_date in rows:
        if ipo_date is None or ipo_date < datetime.date(START_YEAR, 1, 1):
            start_date = datetime.date(START_YEAR, 1, 1)
        else:
            start_date = first_full_quarter_after(ipo_date)
        quarters = get_quarters(start_date, TODAY)
        found_data = False
        for year, quarter in quarters:
            data = fetch_transcript(symbol, year, quarter)
            if data and "transcript" in data:
                # Save data to S3 or local as per your pattern
                found_data = True
                # ... (save logic here)
        if not found_data:
            # Update watermark to set API_ELIGIBLE = 'SUS'
            cur.execute("""
                UPDATE ETL_WATERMARK
                SET API_ELIGIBLE = 'SUS', UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE SYMBOL = %s AND TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT'
            """, (symbol,))
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()