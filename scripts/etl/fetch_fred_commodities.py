
# FRED Commodities ETL for Alpha Vantage (full-refresh, S3, Snowflake)
# Follows the same structure as other ETLs (fetch, CSV, S3, Snowflake)

import os
import requests
import csv
import boto3
import snowflake.connector
from io import StringIO
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

ALPHAVANTAGE_API_KEY = os.environ["ALPHAVANTAGE_API_KEY"]
S3_BUCKET = os.environ.get("S3_BUCKET") or "fin-trade-craft-landing"
AWS_REGION = os.environ.get("AWS_REGION", "us-east-2")
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_DATABASE = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA = os.environ["SNOWFLAKE_SCHEMA"]
SNOWFLAKE_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]


# Map commodity names to Alpha Vantage function names (per docs)
COMMODITIES = [
    ("WTI", "WTI"),
    ("BRENT", "BRENT"),
    ("NATURAL_GAS", "NATURAL_GAS"),
    ("COPPER", "COPPER"),
    ("ALUMINUM", "ALUMINUM"),
    ("WHEAT", "WHEAT"),
    ("CORN", "CORN"),
    ("COTTON", "COTTON"),
    ("SUGAR", "SUGAR"),
    ("ALL_COMMODITIES", "ALL_COMMODITIES"),
]

API_URL = "https://www.alphavantage.co/query"
S3_PREFIX = os.environ.get("S3_FRED_COMMODITIES_PREFIX", "commodities/")

def fetch_commodity_series(function_name):
    params = {
        "function": function_name,
        "interval": "monthly",
        "apikey": ALPHAVANTAGE_API_KEY
    }
    resp = requests.get(API_URL, params=params, timeout=30)
    if resp.status_code == 200:
        data = resp.json()
        # The time series is under a key like 'data' or 'monthly' or similar; try to find it
        for key in ["data", "monthly", "Monthly Time Series", "Monthly Prices", "Time Series (Monthly)"]:
            if key in data:
                return data[key]
        # If not found, log and return None
        logger.warning(f"No recognized data key in response for {function_name}: {list(data.keys())}")
        return None
    else:
        logger.error(f"Failed to fetch {function_name}: {resp.status_code}")
        return None


def write_csv_to_buffer(commodity, data):
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(["commodity", "date", "value"])
    # Data may be a list of dicts or a dict of date: value
    if isinstance(data, list):
        for row in data:
            writer.writerow([commodity, row.get("date"), row.get("value")])
    elif isinstance(data, dict):
        for date, value in data.items():
            writer.writerow([commodity, date, value])
    return buf.getvalue()

def upload_to_s3(csv_content, commodity):
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    s3_key = f"{S3_PREFIX}{commodity}_{timestamp}.csv"
    s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=csv_content.encode("utf-8"), ContentType="text/csv")
    logger.info(f"✅ Uploaded {commodity} to s3://{S3_BUCKET}/{s3_key}")
    return s3_key

def load_into_snowflake(s3_key, commodity):
    private_key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH", "snowflake_rsa_key.der")
    with open(private_key_path, "rb") as key_file:
        private_key = key_file.read()
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        private_key=private_key,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE
    )
    cur = conn.cursor()
    table = f"FRED_COMMODITIES_{commodity.upper()}"
    # Create table if not exists
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            COMMODITY STRING,
            DATE DATE,
            VALUE FLOAT
        )
    """)
    # Remove old data (full refresh)
    cur.execute(f"DELETE FROM {table}")
    # Copy from S3
    s3_url = f"s3://{S3_BUCKET}/{s3_key}"
    cur.execute(f"""
        COPY INTO {table}
        FROM '{s3_url}'
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        FORCE=TRUE
    """)
    logger.info(f"✅ Loaded {commodity} into Snowflake table {table}")
    cur.close()
    conn.close()



def main():
    logger.info("🚀 Starting FRED Commodities Fetch (Alpha Vantage)")
    for commodity, function_name in COMMODITIES:
        logger.info(f"Fetching {commodity} ({function_name}) from Alpha Vantage...")
        data = fetch_commodity_series(function_name)
        if not data:
            logger.warning(f"No data for {commodity} ({function_name})")
            continue
        csv_content = write_csv_to_buffer(commodity, data)
        upload_to_s3(csv_content, commodity)
    logger.info("🎉 FRED Commodities fetch complete! Data uploaded to S3.")

if __name__ == "__main__":
    main()
