#!/usr/bin/env python3
"""
Fetch Alpha Vantage TIME_SERIES_DAILY_ADJUSTED data and upload to S3.
This script focuses solely on data extraction and S3 storage, following proper separation of concerns.
Snowflake loading is handled separately via SQL runbooks.
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
        "outputsize": "full",  # Full-length time series (20+ years of historical data)
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


def validate_csv_data(csv_data, symbol):
    """
    Validate that the CSV data has the expected structure and quality.
    
    Args:
        csv_data: CSV string to validate
        symbol: Stock symbol for error reporting
        
    Returns:
        True if valid, False otherwise
    """
    try:
        # Parse CSV and check structure
        csv_reader = csv.DictReader(StringIO(csv_data))
        
        # Expected columns from Alpha Vantage TIME_SERIES_DAILY_ADJUSTED
        expected_columns = {
            'timestamp', 'open', 'high', 'low', 'close', 
            'adjusted_close', 'volume', 'dividend_amount', 'split_coefficient'
        }
        
        actual_columns = set(csv_reader.fieldnames) if csv_reader.fieldnames else set()
        
        if not expected_columns.issubset(actual_columns):
            missing = expected_columns - actual_columns
            logger.error(f"Missing columns for {symbol}: {missing}")
            return False
            
        # Check that we have at least some data rows
        rows = list(csv_reader)
        row_count = len(rows)
        
        if row_count == 0:
            logger.error(f"No data rows found for {symbol}")
            return False
        
        # Basic data quality checks
        valid_rows = 0
        for i, row in enumerate(rows):
            try:
                # Validate timestamp format
                datetime.strptime(row['timestamp'], '%Y-%m-%d')
                
                # Validate numeric fields
                float(row['open'])
                float(row['high'])
                float(row['low'])
                float(row['close'])
                float(row['adjusted_close'])
                int(row['volume'])
                float(row['dividend_amount'])
                float(row['split_coefficient'])
                
                valid_rows += 1
                
            except (ValueError, KeyError) as e:
                logger.warning(f"Invalid row {i+2} for {symbol}: {e}")
                continue
        
        if valid_rows == 0:
            logger.error(f"No valid data rows found for {symbol}")
            return False
            
        validity_ratio = valid_rows / row_count
        if validity_ratio < 0.95:  # At least 95% of rows should be valid
            logger.error(f"Data quality too low for {symbol}: {validity_ratio:.2%} valid rows")
            return False
            
        logger.info(f"Validated CSV for {symbol}: {row_count} total rows, {valid_rows} valid rows ({validity_ratio:.2%})")
        return True
        
    except Exception as e:
        logger.error(f"CSV validation failed for {symbol}: {e}")
        return False


def upload_to_s3(csv_data, symbol, load_date, bucket, s3_prefix, region):
    """
    Upload CSV data to S3 with proper naming and metadata.
    
    Args:
        csv_data: CSV string to upload
        symbol: Stock symbol 
        load_date: Date for the filename (YYYYMMDD format)
        bucket: S3 bucket name
        s3_prefix: S3 prefix/folder
        region: AWS region
        
    Returns:
        S3 key if successful, None otherwise
    """
    try:
        # Generate S3 key following established pattern
        s3_key = f"{s3_prefix}time_series_daily_adjusted_{symbol}_{load_date}.csv"
        
        logger.info(f"Uploading to S3: s3://{bucket}/{s3_key}")
        
        # Create S3 client (uses AWS credentials from environment/OIDC)
        s3_client = boto3.client('s3', region_name=region)
        
        # Calculate metadata
        row_count = len(csv_data.strip().split('\n')) - 1  # Subtract header
        
        # Upload the CSV data with metadata
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_data.encode('utf-8'),
            ContentType='text/csv',
            Metadata={
                'symbol': symbol,
                'load_date': load_date,
                'row_count': str(row_count),
                'upload_timestamp': datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Successfully uploaded {symbol} data to S3: {row_count} rows")
        return s3_key
        
    except Exception as e:
        logger.error(f"S3 upload failed for {symbol}: {e}")
        return None


def main():
    """Main function to fetch time series data and upload to S3."""
    
    # Get required environment variables
    symbol = os.environ.get('SYMBOL')
    api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
    bucket = os.environ.get('S3_BUCKET')
    s3_prefix = os.environ.get('S3_TIME_SERIES_PREFIX', 'time_series_daily_adjusted/')
    region = os.environ.get('AWS_REGION', 'us-east-1')
    load_date = os.environ.get('LOAD_DATE', datetime.now().strftime('%Y%m%d'))
    
    # Validate required parameters
    if not all([symbol, api_key, bucket]):
        logger.error("Missing required environment variables")
        logger.error("Required: SYMBOL, ALPHAVANTAGE_API_KEY, S3_BUCKET")
        sys.exit(1)
    
    logger.info(f"Starting time series extraction for symbol: {symbol}")
    logger.info(f"Load date: {load_date}")
    logger.info(f"Target: s3://{bucket}/{s3_prefix}")
    
    # Step 1: Fetch data from Alpha Vantage
    csv_data = fetch_time_series_data(symbol, api_key)
    if not csv_data:
        logger.error(f"Failed to fetch data for {symbol}")
        sys.exit(1)
    
    # Step 2: Validate CSV structure and data quality
    if not validate_csv_data(csv_data, symbol):
        logger.error(f"Invalid CSV data for {symbol}")
        sys.exit(1)
    
    # Step 3: Upload to S3
    s3_key = upload_to_s3(csv_data, symbol, load_date, bucket, s3_prefix, region)
    if not s3_key:
        logger.error(f"Failed to upload {symbol} data to S3")
        sys.exit(1)
    
    logger.info(f"✅ Successfully completed time series extraction for {symbol}")
    logger.info(f"📁 File location: s3://{bucket}/{s3_key}")
    logger.info(f"🔄 Ready for Snowflake staging load")


if __name__ == "__main__":
    main()