#!/usr/bin/env python3
"""
Fetch Alpha Vantage TIME_SERIES_DAILY_ADJUSTED data for a specific symbol and upload to S3.
Simplified version focused on GitHub Actions workflow integration.
"""

import os
import sys
from datetime import datetime
from io import StringIO
import csv

import boto3
import requests


def fetch_time_series_data(symbol, api_key):
    """
    Fetch TIME_SERIES_DAILY_ADJUSTED data from Alpha Vantage API.
    
    Args:
        symbol: Stock symbol (e.g., 'AAPL')
        api_key: Alpha Vantage API key
        
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
    
    print(f"Fetching TIME_SERIES_DAILY_ADJUSTED data for {symbol}...")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        # Check if we got CSV data (successful response)
        if response.headers.get("content-type", "").startswith("text/csv") or "timestamp" in response.text.lower():
            print(f"Successfully fetched time series data for {symbol}")
            return response.text
        else:
            # Might be JSON error response
            print(f"Error response for {symbol}: {response.text[:200]}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Request failed for {symbol}: {e}")
        return None


def validate_csv_data(csv_data, symbol):
    """
    Validate that the CSV data has the expected structure.
    
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
            print(f"Missing columns for {symbol}: {missing}")
            return False
            
        # Check that we have at least some data rows
        row_count = sum(1 for _ in csv_reader)
        if row_count == 0:
            print(f"No data rows found for {symbol}")
            return False
            
        print(f"Validated CSV for {symbol}: {row_count} rows with correct columns")
        return True
        
    except Exception as e:
        print(f"CSV validation failed for {symbol}: {e}")
        return False


def upload_to_s3(csv_data, symbol, load_date, bucket, s3_prefix, region):
    """
    Upload CSV data to S3.
    
    Args:
        csv_data: CSV string to upload
        symbol: Stock symbol 
        load_date: Date for the filename (YYYYMMDD format)
        bucket: S3 bucket name
        s3_prefix: S3 prefix/folder
        region: AWS region
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Generate S3 key (file path)
        s3_key = f"{s3_prefix}time_series_daily_adjusted_{symbol}_{load_date}.csv"
        
        print(f"Uploading to S3: s3://{bucket}/{s3_key}")
        
        # Create S3 client (uses AWS credentials from environment/OIDC)
        s3_client = boto3.client('s3', region_name=region)
        
        # Upload the CSV data
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_data.encode('utf-8'),
            ContentType='text/csv'
        )
        
        print(f"Successfully uploaded {symbol} data to S3")
        return True
        
    except Exception as e:
        print(f"S3 upload failed for {symbol}: {e}")
        return False


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
        print("Error: Missing required environment variables")
        print("Required: SYMBOL, ALPHAVANTAGE_API_KEY, S3_BUCKET")
        sys.exit(1)
    
    print(f"Starting time series extraction for symbol: {symbol}")
    print(f"Load date: {load_date}")
    
    # Fetch data from Alpha Vantage
    csv_data = fetch_time_series_data(symbol, api_key)
    if not csv_data:
        print(f"Failed to fetch data for {symbol}")
        sys.exit(1)
    
    # Validate CSV structure
    if not validate_csv_data(csv_data, symbol):
        print(f"Invalid CSV data for {symbol}")
        sys.exit(1)
    
    # Upload to S3
    success = upload_to_s3(csv_data, symbol, load_date, bucket, s3_prefix, region)
    if not success:
        print(f"Failed to upload {symbol} data to S3")
        sys.exit(1)
    
    print(f"Successfully completed time series extraction for {symbol}")


if __name__ == "__main__":
    main()
