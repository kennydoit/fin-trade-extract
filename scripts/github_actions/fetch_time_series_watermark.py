#!/usr/bin/env python3
"""
Watermark-Based Time Series ETL
Fetches TIME_SERIES_DAILY_ADJUSTED data using the ETL_WATERMARKS table for incremental processing.
"""

import os
import sys
import time
import json
from datetime import datetime, timedelta
from io import StringIO
import csv
import logging
from typing import List, Dict, Optional, Tuple

import boto3
import requests
import snowflake.connector

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class WatermarkETLManager:
    """Manages ETL processing using the ETL_WATERMARKS table."""
    
    def __init__(self, snowflake_config: Dict[str, str], table_name: str = 'TIME_SERIES_DAILY_ADJUSTED'):
        self.snowflake_config = snowflake_config
        self.table_name = table_name
        self.connection = None
        
    def connect(self):
        """Establish Snowflake connection."""
        if not self.connection:
            self.connection = snowflake.connector.connect(**self.snowflake_config)
            logger.info("✅ Connected to Snowflake")
            
    def close(self):
        """Close Snowflake connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("🔒 Snowflake connection closed")
    
    def get_symbols_to_process(self, exchange_filter: Optional[str] = None,
                               max_symbols: Optional[int] = None,
                               staleness_days: int = 5) -> List[Dict]:
        """
        Get symbols to process from ETL_WATERMARKS table.
        
        Returns list of dicts with:
        - symbol: stock ticker
        - processing_mode: 'full' or 'compact'
        - last_fiscal_date: last data point date (or None)
        """
        self.connect()
        
        query = f"""
            SELECT 
                SYMBOL,
                EXCHANGE,
                ASSET_TYPE,
                STATUS,
                FIRST_FISCAL_DATE,
                LAST_FISCAL_DATE,
                LAST_SUCCESSFUL_RUN,
                CONSECUTIVE_FAILURES
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = '{self.table_name}'
              AND API_ELIGIBLE = 'YES'
        """
        
        if exchange_filter:
            query += f"\n              AND UPPER(EXCHANGE) = '{exchange_filter.upper()}'"
        
        query += "\n            ORDER BY SYMBOL"
        
        if max_symbols:
            query += f"\n            LIMIT {max_symbols}"
        
        logger.info(f"📊 Querying watermarks for {self.table_name}...")
        if exchange_filter:
            logger.info(f"🏢 Exchange filter: {exchange_filter}")
        if max_symbols:
            logger.info(f"🔒 Symbol limit: {max_symbols}")
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        
        symbols_to_process = []
        full_count = 0
        compact_count = 0
        
        for row in results:
            symbol = row[0]
            first_fiscal = row[4]
            last_fiscal = row[5]
            
            # Determine processing mode based on watermark logic
            if first_fiscal is None and last_fiscal is None:
                # Never processed - get full history
                processing_mode = 'full'
                full_count += 1
            elif first_fiscal is not None and last_fiscal is not None:
                # Previously processed - check staleness
                days_since_last = (datetime.now().date() - last_fiscal).days
                if days_since_last < staleness_days:
                    # Recent data - use compact mode
                    processing_mode = 'compact'
                    compact_count += 1
                else:
                    # Stale data - refresh full history
                    processing_mode = 'full'
                    full_count += 1
            else:
                # Partial data - get full history to be safe
                processing_mode = 'full'
                full_count += 1
            
            symbols_to_process.append({
                'symbol': symbol,
                'processing_mode': processing_mode,
                'last_fiscal_date': last_fiscal
            })
        
        logger.info(f"📋 Found {len(symbols_to_process)} symbols to process:")
        logger.info(f"   🔄 Full refresh: {full_count} symbols")
        logger.info(f"   ⚡ Compact update: {compact_count} symbols")
        
        return symbols_to_process
    
    def update_watermark(self, symbol: str, first_date: str, last_date: str, success: bool = True):
        """
        Update watermark for a symbol after processing.
        
        Args:
            symbol: Stock ticker
            first_date: Earliest date in the data (YYYY-MM-DD)
            last_date: Most recent date in the data (YYYY-MM-DD)
            success: Whether processing was successful
        """
        self.connect()
        
        cursor = self.connection.cursor()
        
        if success:
            update_sql = f"""
                UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
                SET 
                    FIRST_FISCAL_DATE = COALESCE(FIRST_FISCAL_DATE, TO_DATE('{first_date}', 'YYYY-MM-DD')),
                    LAST_FISCAL_DATE = TO_DATE('{last_date}', 'YYYY-MM-DD'),
                    LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(),
                    CONSECUTIVE_FAILURES = 0,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TABLE_NAME = '{self.table_name}'
                  AND SYMBOL = '{symbol}'
            """
        else:
            update_sql = f"""
                UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
                SET 
                    CONSECUTIVE_FAILURES = COALESCE(CONSECUTIVE_FAILURES, 0) + 1,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TABLE_NAME = '{self.table_name}'
                  AND SYMBOL = '{symbol}'
            """
        
        cursor.execute(update_sql)
        self.connection.commit()
        cursor.close()


class AlphaVantageRateLimiter:
    """Rate limiter for Alpha Vantage API."""
    
    def __init__(self, calls_per_minute: int = 75):
        default_delay = 60.0 / calls_per_minute
        self.min_delay = float(os.getenv('API_DELAY_SECONDS', str(default_delay)))
        self.last_call_time = 0.0
        
    def wait_if_needed(self):
        """Wait if necessary to respect rate limits."""
        current_time = time.time()
        time_since_last = current_time - self.last_call_time
        
        if time_since_last < self.min_delay:
            wait_time = self.min_delay - time_since_last
            time.sleep(wait_time)
        
        self.last_call_time = time.time()


def fetch_time_series_data(symbol: str, api_key: str, output_size: str = 'full') -> Optional[Dict]:
    """
    Fetch time series data from Alpha Vantage.
    
    Args:
        symbol: Stock ticker
        api_key: Alpha Vantage API key
        output_size: 'full' or 'compact'
    
    Returns:
        Dict with time series data or None if error
    """
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'TIME_SERIES_DAILY_ADJUSTED',
        'symbol': symbol,
        'apikey': api_key,
        'datatype': 'csv',
        'outputsize': output_size
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        # Check for API error messages
        if 'Error Message' in response.text or 'Invalid API call' in response.text:
            logger.warning(f"❌ API error for {symbol}: {response.text[:200]}")
            return None
        
        # Parse CSV response
        csv_data = StringIO(response.text)
        reader = csv.DictReader(csv_data)
        records = list(reader)
        
        if not records:
            logger.warning(f"⚠️  No data returned for {symbol}")
            return None
        
        # Get date range
        dates = [r['timestamp'] for r in records]
        first_date = min(dates)
        last_date = max(dates)
        
        return {
            'symbol': symbol,
            'records': records,
            'first_date': first_date,
            'last_date': last_date,
            'record_count': len(records)
        }
        
    except Exception as e:
        logger.error(f"❌ Error fetching {symbol}: {e}")
        return None


def upload_to_s3(data: Dict, s3_client, bucket: str, prefix: str) -> bool:
    """Upload time series data to S3."""
    try:
        symbol = data['symbol']
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"{prefix}{symbol}_{timestamp}.csv"
        
        # Convert records back to CSV
        if not data['records']:
            return False
        
        csv_buffer = StringIO()
        fieldnames = data['records'][0].keys()
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data['records'])
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_buffer.getvalue().encode('utf-8')
        )
        
        logger.info(f"✅ Uploaded {symbol} to s3://{bucket}/{s3_key} ({data['record_count']} records)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error uploading {data['symbol']} to S3: {e}")
        return False


def main():
    """Main ETL execution."""
    logger.info("🚀 Starting Watermark-Based Time Series ETL")
    
    # Get configuration from environment
    api_key = os.environ['ALPHAVANTAGE_API_KEY']
    s3_bucket = os.environ.get('S3_BUCKET', 'fin-trade-craft-landing')
    s3_prefix = os.environ.get('S3_TIME_SERIES_PREFIX', 'time_series_daily_adjusted/')
    exchange_filter = os.environ.get('EXCHANGE_FILTER')  # NYSE, NASDAQ, or None for all
    max_symbols = int(os.environ['MAX_SYMBOLS']) if os.environ.get('MAX_SYMBOLS') else None
    staleness_days = int(os.environ.get('STALENESS_DAYS', '5'))
    batch_size = int(os.environ.get('BATCH_SIZE', '50'))
    
    # Snowflake configuration
    snowflake_config = {
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'user': os.environ['SNOWFLAKE_USER'],
        'password': os.environ['SNOWFLAKE_PASSWORD'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
        'schema': os.environ['SNOWFLAKE_SCHEMA'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE']
    }
    
    # Initialize managers
    watermark_manager = WatermarkETLManager(snowflake_config)
    rate_limiter = AlphaVantageRateLimiter()
    s3_client = boto3.client('s3')
    
    try:
        # Get symbols to process from watermarks
        symbols_to_process = watermark_manager.get_symbols_to_process(
            exchange_filter=exchange_filter,
            max_symbols=max_symbols,
            staleness_days=staleness_days
        )
        
        if not symbols_to_process:
            logger.warning("⚠️  No symbols to process")
            return
        
        # Process symbols in batches
        results = {
            'total_symbols': len(symbols_to_process),
            'successful': 0,
            'failed': 0,
            'start_time': datetime.now().isoformat(),
            'details': []
        }
        
        for i, symbol_info in enumerate(symbols_to_process, 1):
            symbol = symbol_info['symbol']
            mode = symbol_info['processing_mode']
            output_size = 'full' if mode == 'full' else 'compact'
            
            logger.info(f"📊 [{i}/{len(symbols_to_process)}] Processing {symbol} ({mode} mode)...")
            
            # Rate limit
            rate_limiter.wait_if_needed()
            
            # Fetch data
            data = fetch_time_series_data(symbol, api_key, output_size)
            
            if data:
                # Upload to S3
                if upload_to_s3(data, s3_client, s3_bucket, s3_prefix):
                    # Update watermark on success
                    watermark_manager.update_watermark(
                        symbol,
                        data['first_date'],
                        data['last_date'],
                        success=True
                    )
                    results['successful'] += 1
                    results['details'].append({
                        'symbol': symbol,
                        'status': 'success',
                        'mode': mode,
                        'records': data['record_count']
                    })
                else:
                    watermark_manager.update_watermark(symbol, None, None, success=False)
                    results['failed'] += 1
            else:
                watermark_manager.update_watermark(symbol, None, None, success=False)
                results['failed'] += 1
                results['details'].append({
                    'symbol': symbol,
                    'status': 'failed',
                    'mode': mode
                })
        
        # Save results
        results['end_time'] = datetime.now().isoformat()
        results['duration_minutes'] = (datetime.fromisoformat(results['end_time']) - 
                                      datetime.fromisoformat(results['start_time'])).total_seconds() / 60
        
        with open('/tmp/watermark_etl_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info("🎉 ETL processing complete!")
        logger.info(f"✅ Successful: {results['successful']}/{results['total_symbols']}")
        logger.info(f"❌ Failed: {results['failed']}/{results['total_symbols']}")
        
    finally:
        watermark_manager.close()


if __name__ == '__main__':
    main()
