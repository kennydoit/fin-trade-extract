#!/usr/bin/env python3
"""
Watermark-Based ETF Profile ETL
Fetches ETF_PROFILE data using the ETL_WATERMARKS table for incremental processing.
"""

import os
import time
import json
import requests
import boto3
import snowflake.connector
from datetime import datetime

API_URL = "https://www.alphavantage.co/query"
FUNCTION = "ETF_PROFILE"
S3_PREFIX = "etf_profile/"


def get_snowflake_connection():
    import cryptography.hazmat.primitives.serialization as serialization
    from cryptography.hazmat.backends import default_backend
    private_key_path = os.environ.get('SNOWFLAKE_PRIVATE_KEY_PATH', 'snowflake_rsa_key.der')
    with open(private_key_path, 'rb') as key_file:
        p_key = key_file.read()
    private_key = serialization.load_der_private_key(
        p_key,
        password=None,
        backend=default_backend()
    )
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    return snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        private_key=pkb,
        database=os.environ.get('SNOWFLAKE_DATABASE', 'FIN_TRADE_EXTRACT'),
        schema=os.environ.get('SNOWFLAKE_SCHEMA', 'RAW'),
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE']
    )

def get_eligible_etf_symbols(conn):
    # Select eligible ETF symbols from LISTING_STATUS, excluding those already in ETF_PROFILE
    query = """
        SELECT SYMBOL FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS ls
        WHERE ls.TABLE_NAME = 'LISTING_STATUS'
          AND UPPER(ls.ASSET_TYPE) = 'ETF'
          AND UPPER(ls.STATUS) = 'ACTIVE'
          AND NOT EXISTS (
              SELECT 1 FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS ep
              WHERE ep.TABLE_NAME = 'ETF_PROFILE'
                AND ep.SYMBOL = ls.SYMBOL
          )
    """
    cur = conn.cursor()
    cur.execute(query)
    symbols = [row[0] for row in cur.fetchall()]
    cur.close()
    return symbols

def fetch_etf_profile(symbol, api_key):
    url = API_URL
    params = {
        'function': FUNCTION,
        'symbol': symbol,
        'apikey': api_key
    }
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data or 'Symbol' not in data:
            print(f"No ETF profile data for {symbol}")
            return None
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

def upload_json_to_s3(symbol, data, s3_client, bucket, prefix):
    key = f"{prefix}{symbol}.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data).encode('utf-8')
    )
    print(f"Uploaded {symbol} ETF profile to s3://{bucket}/{key}")

def update_watermark(conn, symbol):
    cur = conn.cursor()
    cur.execute(f"""
        UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
        SET LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(), CONSECUTIVE_FAILURES = 0
        WHERE TABLE_NAME = 'ETF_PROFILE' AND SYMBOL = %s
    """, (symbol,))
    conn.commit()
    cur.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description='ETF Profile ETL')
    parser.add_argument('--max-symbols', type=int, default=None, help='Maximum number of symbols to process')
    args = parser.parse_args()

    api_key = os.environ['ALPHAVANTAGE_API_KEY']
    s3_bucket = os.environ.get('S3_BUCKET', 'fin-trade-craft-landing')
    s3_prefix = S3_PREFIX
    conn = get_snowflake_connection()
    s3_client = boto3.client('s3')
    symbols = get_eligible_etf_symbols(conn)
    if args.max_symbols is not None:
        symbols = symbols[:args.max_symbols]
    print(f"Found {len(symbols)} eligible ETF symbols.")
    for idx, symbol in enumerate(symbols, 1):
        print(f"[{idx}] {symbol}")
        data = fetch_etf_profile(symbol, api_key)
        if data:
            upload_json_to_s3(symbol, data, s3_client, s3_bucket, s3_prefix)
            update_watermark(conn, symbol)
        else:
            print(f"Skipping {symbol} due to missing data.")
    conn.close()
    print("ETF Profile ETL complete.")

if __name__ == "__main__":
    main()
