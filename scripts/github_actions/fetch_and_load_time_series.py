#!/usr/bin/env python3
"""
Fetch Alpha Vantage TIME_SERIES_DAILY_ADJUSTED data and load directly into Snowflake.
This approach bypasses S3 COPY INTO complexity by inserting data directly via Python.
"""

import os
import sys
import time
from datetime import datetime
from io import StringIO
import csv
import logging

import boto3
import requests
import snowflake.connector
from snowflake.connector import DictCursor

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _parse_alpha_vantage_error(response):
    """Extract Alpha Vantage error messages from a response."""
    try:
        payload = response.json()
    except ValueError:
        return None

    if isinstance(payload, dict):
        for key in ("Note", "Information", "Error Message", "message"):
            if payload.get(key):
                return payload[key]
    return None


def fetch_time_series_data(symbol, api_key, *, max_retries=5, backoff_seconds=15):
    """
    Fetch TIME_SERIES_DAILY_ADJUSTED data from Alpha Vantage API.

    Args:
        symbol: Stock symbol (e.g., 'AAPL')
        api_key: Alpha Vantage API key
        max_retries: Maximum number of attempts before giving up
        backoff_seconds: Base seconds to wait between retries (multiplied by attempt)

    Returns:
        CSV string with time series data
    """
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "datatype": "csv",
        "outputsize": "compact",  # Last 100 data points (about 4 months)
        "apikey": api_key
    }
    
    logger.info(f"Fetching TIME_SERIES_DAILY_ADJUSTED data for {symbol}...")
    
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()

            # Check if response looks like CSV (starts with expected header)
            response_text = response.text.strip()
            if response_text.startswith('timestamp,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient'):
                logger.info(f"Successfully fetched time series data for {symbol}")
                return response_text

            # Alpha Vantage sometimes responds with JSON (rate limiting, errors).
            error_message = _parse_alpha_vantage_error(response)
            if error_message:
                logger.error(f"Alpha Vantage returned an error for {symbol}: {error_message}")
            else:
                logger.warning(f"Unexpected response for {symbol}: {response.text[:200]}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {symbol}: {e}")

        if attempt < max_retries:
            sleep_for = backoff_seconds * attempt
            logger.info(f"Retrying in {sleep_for} seconds (attempt {attempt}/{max_retries})...")
            time.sleep(sleep_for)
        else:
            logger.error(f"Exceeded maximum retries ({max_retries}) for {symbol}")

    return None


def parse_csv_data(csv_data, symbol, load_date):
    """
    Parse CSV data into list of records ready for database insertion.
    
    Args:
        csv_data: CSV string from Alpha Vantage
        symbol: Stock symbol
        load_date: Date when data was loaded (YYYY-MM-DD format)
        
    Returns:
        List of tuples ready for database insertion
    """
    logger.info(f"Parsing CSV data for {symbol}")
    
    records = []
    csv_reader = csv.DictReader(StringIO(csv_data))
    
    for row in csv_reader:
        try:
            # Parse and validate the row
            record = (
                symbol,                                          # symbol
                datetime.strptime(row['timestamp'], '%Y-%m-%d').date(),  # date
                float(row['open']),                             # open
                float(row['high']),                             # high
                float(row['low']),                              # low
                float(row['close']),                            # close
                float(row['adjusted_close']),                   # adjusted_close
                int(row['volume']),                             # volume
                float(row['dividend_amount']),                  # dividend_amount
                float(row['split_coefficient']),                # split_coefficient
                datetime.strptime(load_date, '%Y%m%d').date()   # load_date
            )
            records.append(record)
            
        except (ValueError, KeyError) as e:
            logger.warning(f"Skipping invalid row for {symbol}: {e} - {row}")
            continue
    
    logger.info(f"Parsed {len(records)} valid records for {symbol}")
    return records


def upload_to_s3(csv_data, symbol, load_date, bucket, s3_prefix, region):
    """
    Upload CSV data to S3 for backup/archive purposes.
    """
    try:
        s3_key = f"{s3_prefix}time_series_daily_adjusted_{symbol}_{load_date}.csv"
        logger.info(f"Uploading backup to S3: s3://{bucket}/{s3_key}")
        
        s3_client = boto3.client('s3', region_name=region)
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_data.encode('utf-8'),
            ContentType='text/csv'
        )
        
        logger.info(f"Successfully uploaded {symbol} backup to S3")
        return True
        
    except Exception as e:
        logger.error(f"S3 backup failed for {symbol}: {e}")
        return False


def create_snowflake_connection(account, user, password, warehouse, database, schema):
    """Create Snowflake connection."""
    try:
        logger.info("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        logger.info("Successfully connected to Snowflake")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


def ensure_table_exists(conn):
    """Ensure the time series table exists with proper structure."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS TIME_SERIES_DAILY_ADJUSTED (
        symbol VARCHAR(20) NOT NULL,
        date DATE NOT NULL,
        open NUMBER(15,4),
        high NUMBER(15,4),
        low NUMBER(15,4),
        close NUMBER(15,4),
        adjusted_close NUMBER(15,4),
        volume NUMBER(20,0),
        dividend_amount NUMBER(15,6),
        split_coefficient NUMBER(10,6),
        load_date DATE,
        PRIMARY KEY (symbol, date)
    )
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            logger.info("Ensured TIME_SERIES_DAILY_ADJUSTED table exists")
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        raise


def insert_or_update_records(conn, records, symbol):
    """
    Insert or update records in Snowflake using MERGE (upsert).
    """
    if not records:
        logger.warning(f"No records to insert for {symbol}")
        return 0

    # Create temporary table for batch processing
    temp_table = f"TEMP_TIME_SERIES_{symbol}_{int(time.time())}"
    
    try:
        with conn.cursor() as cursor:
            # Create temporary table
            create_temp_sql = f"""
            CREATE TEMPORARY TABLE {temp_table} (
                symbol VARCHAR(20),
                date DATE,
                open NUMBER(15,4),
                high NUMBER(15,4),
                low NUMBER(15,4),
                close NUMBER(15,4),
                adjusted_close NUMBER(15,4),
                volume NUMBER(20,0),
                dividend_amount NUMBER(15,6),
                split_coefficient NUMBER(10,6),
                load_date DATE
            )
            """
            cursor.execute(create_temp_sql)
            logger.info(f"Created temporary table {temp_table}")

            # Insert records into temporary table
            insert_sql = f"""
            INSERT INTO {temp_table} 
            (symbol, date, open, high, low, close, adjusted_close, volume, dividend_amount, split_coefficient, load_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(insert_sql, records)
            logger.info(f"Inserted {len(records)} records into temporary table")

            # Merge from temporary table to main table
            merge_sql = f"""
            MERGE INTO TIME_SERIES_DAILY_ADJUSTED tgt
            USING {temp_table} src
            ON tgt.symbol = src.symbol AND tgt.date = src.date
            WHEN MATCHED THEN UPDATE SET
                open = src.open,
                high = src.high,
                low = src.low,
                close = src.close,
                adjusted_close = src.adjusted_close,
                volume = src.volume,
                dividend_amount = src.dividend_amount,
                split_coefficient = src.split_coefficient,
                load_date = src.load_date
            WHEN NOT MATCHED THEN INSERT (
                symbol, date, open, high, low, close, adjusted_close, 
                volume, dividend_amount, split_coefficient, load_date
            ) VALUES (
                src.symbol, src.date, src.open, src.high, src.low, src.close, 
                src.adjusted_close, src.volume, src.dividend_amount, 
                src.split_coefficient, src.load_date
            )
            """
            
            result = cursor.execute(merge_sql)
            rows_affected = cursor.rowcount
            logger.info(f"Merged {rows_affected} records for {symbol}")
            
            return rows_affected

    except Exception as e:
        logger.error(f"Failed to insert/update records for {symbol}: {e}")
        raise
    finally:
        # Clean up temporary table
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        except:
            pass  # Ignore cleanup errors


def main():
    """Main function to fetch time series data and load directly into Snowflake."""
    
    # Get required environment variables
    symbol = os.environ.get('SYMBOL')
    api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
    
    # S3 config (for backup)
    bucket = os.environ.get('S3_BUCKET')
    s3_prefix = os.environ.get('S3_TIME_SERIES_PREFIX', 'time_series_daily_adjusted/')
    region = os.environ.get('AWS_REGION', 'us-east-1')
    load_date = os.environ.get('LOAD_DATE', datetime.now().strftime('%Y%m%d'))
    
    # Snowflake config
    sf_account = os.environ.get('SNOWFLAKE_ACCOUNT')
    sf_user = os.environ.get('SNOWFLAKE_USER')
    sf_password = os.environ.get('SNOWFLAKE_PASSWORD')
    sf_warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
    sf_database = os.environ.get('SNOWFLAKE_DATABASE')
    sf_schema = os.environ.get('SNOWFLAKE_SCHEMA')
    
    # Validate required parameters
    required_vars = [symbol, api_key, sf_account, sf_user, sf_password, sf_warehouse, sf_database, sf_schema]
    if not all(required_vars):
        logger.error("Missing required environment variables")
        logger.error("Required: SYMBOL, ALPHAVANTAGE_API_KEY, SNOWFLAKE_* credentials")
        sys.exit(1)
    
    logger.info(f"Starting time series ETL for symbol: {symbol}")
    logger.info(f"Load date: {load_date}")
    
    # Step 1: Fetch data from Alpha Vantage
    csv_data = fetch_time_series_data(symbol, api_key)
    if not csv_data:
        logger.error(f"Failed to fetch data for {symbol}")
        sys.exit(1)
    
    # Step 2: Parse CSV data into records
    records = parse_csv_data(csv_data, symbol, load_date)
    if not records:
        logger.error(f"No valid records parsed for {symbol}")
        sys.exit(1)
    
    # Step 3: Upload to S3 for backup (optional - don't fail if this fails)
    if bucket:
        upload_to_s3(csv_data, symbol, load_date, bucket, s3_prefix, region)
    
    # Step 4: Connect to Snowflake and load data
    try:
        conn = create_snowflake_connection(sf_account, sf_user, sf_password, sf_warehouse, sf_database, sf_schema)
        
        # Ensure table exists
        ensure_table_exists(conn)
        
        # Insert/update records
        rows_affected = insert_or_update_records(conn, records, symbol)
        
        # Verify results
        with conn.cursor(DictCursor) as cursor:
            cursor.execute("SELECT COUNT(*) as total_count FROM TIME_SERIES_DAILY_ADJUSTED WHERE symbol = %s", (symbol,))
            result = cursor.fetchone()
            total_records = result['TOTAL_COUNT'] if result else 0
            
        logger.info(f"Successfully completed ETL for {symbol}")
        logger.info(f"Records processed: {len(records)}")  
        logger.info(f"Records affected: {rows_affected}")
        logger.info(f"Total records for {symbol}: {total_records}")
        
    except Exception as e:
        logger.error(f"Snowflake ETL failed for {symbol}: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Closed Snowflake connection")


if __name__ == "__main__":
    main()