#!/usr/bin/env python3
"""
Watermark-Based Insider Transactions ETL
Fetches INSIDER_TRADING data using the ETL_WATERMARKS table for incremental processing.
"""

import os
import sys
import time
import json
from datetime import datetime
from io import StringIO
import csv
import logging
from typing import List, Dict, Optional

import boto3
import requests
import snowflake.connector

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class WatermarkETLManager:
    """Manages ETL processing using the ETL_WATERMARKS table."""
    
    def __init__(self, snowflake_config: Dict[str, str], table_name: str = 'INSIDER_TRANSACTIONS'):
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
                               skip_recent_hours: Optional[int] = None) -> List[Dict]:
        """
        Get symbols to process from ETL_WATERMARKS table.
        """
        self.connect()
        
        query = f"""
            SELECT 
                SYMBOL,
                EXCHANGE,
                ASSET_TYPE,
                STATUS,
                LAST_SUCCESSFUL_RUN
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = '{self.table_name}'
              AND API_ELIGIBLE = 'YES'
        """
        
        if skip_recent_hours:
            query += f"""
              AND (LAST_SUCCESSFUL_RUN IS NULL 
                   OR LAST_SUCCESSFUL_RUN < DATEADD(hour, -{skip_recent_hours}, CURRENT_TIMESTAMP()))
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
        if skip_recent_hours:
            logger.info(f"⏭️  Skip recent: {skip_recent_hours} hours")
        
        cursor = self.connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        
        symbols_to_process = [{'symbol': row[0], 'exchange': row[1], 'asset_type': row[2], 'status': row[3]} for row in results]
        
        logger.info(f"📈 Found {len(symbols_to_process)} symbols to process")
        
        return symbols_to_process
    
    def bulk_update_watermarks(self, successful_symbols: List[str], failed_symbols: List[str]):
        """
        Bulk update watermarks for successful and failed symbols.
        """
        if not self.connection:
            raise RuntimeError("❌ No active Snowflake connection. Call connect() first.")
        
        cursor = self.connection.cursor()
        
        if successful_symbols:
            logger.info(f"📝 Bulk updating {len(successful_symbols)} successful watermarks...")
            symbols_list = "', '".join(successful_symbols)
            cursor.execute(f"""
                UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
                SET 
                    LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(),
                    CONSECUTIVE_FAILURES = 0,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TABLE_NAME = '{self.table_name}'
                  AND SYMBOL IN ('{symbols_list}')
            """)
            logger.info(f"✅ Bulk updated {len(successful_symbols)} successful watermarks")

        if failed_symbols:
            logger.info(f"📝 Updating {len(failed_symbols)} failed watermarks...")
            symbols_list = "', '".join(failed_symbols)
            cursor.execute(f"""
                UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
                SET 
                    CONSECUTIVE_FAILURES = COALESCE(CONSECUTIVE_FAILURES, 0) + 1,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TABLE_NAME = '{self.table_name}'
                  AND SYMBOL IN ('{symbols_list}')
            """)
            logger.info(f"✅ Updated {len(failed_symbols)} failed watermarks")
        
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
        time_since_last_call = current_time - self.last_call_time
        
        if time_since_last_call < self.min_delay:
            sleep_time = self.min_delay - time_since_last_call
            time.sleep(sleep_time)
        
        self.last_call_time = time.time()


def cleanup_s3_bucket(bucket: str, prefix: str, s3_client) -> int:
    """
    Delete all existing files in the S3 prefix.
    """
    logger.info(f"🧹 Cleaning up S3 bucket: s3://{bucket}/{prefix}")
    
    deleted_count = 0
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
            s3_client.delete_objects(Bucket=bucket, Delete={'Objects': objects_to_delete})
            deleted_count += len(objects_to_delete)
            logger.info(f"   Deleted {len(objects_to_delete)} files (total: {deleted_count})")
    
    logger.info(f"✅ S3 cleanup complete: {deleted_count} files deleted")
    return deleted_count


def fetch_insider_transactions_data(symbol: str, api_key: str) -> Optional[List[Dict]]:
    """
    Fetch insider trading data from Alpha Vantage API.
    """
    url = "https://www.alphavantage.co/query"
    params = {'function': 'INSIDER_TRADING', 'symbol': symbol, 'apikey': api_key}
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        if 'Thank you for using Alpha Vantage!' in response.text:
             logger.warning(f"⚠️  API rate limit hit for {symbol}")
             return None
        
        if not response.text.strip() or "transactionDate" not in response.text:
            logger.warning(f"⚠️  No insider transactions data for {symbol}")
            return None

        csv_reader = csv.DictReader(StringIO(response.text))
        data = [row for row in csv_reader]

        if not data:
            logger.warning(f"⚠️  No insider transactions data for {symbol}")
            return None

        logger.info(f"✅ Fetched {symbol}: {len(data)} insider transactions")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Request failed for {symbol}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error for {symbol}: {e}")
        return None


def upload_to_s3(symbol: str, data: List[Dict], s3_client, bucket: str, prefix: str) -> bool:
    """Upload insider transactions data to S3 as CSV."""
    s3_key = f"{prefix}{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    try:
        csv_buffer = StringIO()
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
        
        s3_client.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue().encode('utf-8'))
        
        logger.info(f"✅ Uploaded {symbol} to s3://{bucket}/{s3_key} ({len(data)} records)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error uploading {symbol} to S3: {e}")
        return False


def main():
    """Main ETL execution."""
    logger.info("🚀 Starting Watermark-Based Insider Transactions ETL")
    
    api_key = os.environ['ALPHAVANTAGE_API_KEY']
    s3_bucket = os.environ.get('S3_BUCKET', 'fin-trade-craft-landing')
    s3_prefix = os.environ.get('S3_INSIDER_TRANSACTIONS_PREFIX', 'insider_transactions/')
    exchange_filter = os.environ.get('EXCHANGE_FILTER')
    max_symbols = int(os.environ['MAX_SYMBOLS']) if os.environ.get('MAX_SYMBOLS') else None
    skip_recent_hours = int(os.environ['SKIP_RECENT_HOURS']) if os.environ.get('SKIP_RECENT_HOURS') else None
    
    snowflake_config = {
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'user': os.environ['SNOWFLAKE_USER'],
        'password': os.environ['SNOWFLAKE_PASSWORD'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
        'schema': os.environ['SNOWFLAKE_SCHEMA'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE']
    }
    
    watermark_manager = WatermarkETLManager(snowflake_config)
    rate_limiter = AlphaVantageRateLimiter()
    s3_client = boto3.client('s3')
    
    cleanup_s3_bucket(s3_bucket, s3_prefix, s3_client)
    
    try:
        symbols_to_process = watermark_manager.get_symbols_to_process(
            exchange_filter=exchange_filter, max_symbols=max_symbols, skip_recent_hours=skip_recent_hours
        )
    finally:
        watermark_manager.close()
    
    if not symbols_to_process:
        logger.warning("⚠️  No symbols to process")
        return

    results = {'total_symbols': len(symbols_to_process), 'successful': 0, 'failed': 0, 'successful_symbols': [], 'failed_symbols': []}
    
    for i, symbol_info in enumerate(symbols_to_process, 1):
        symbol = symbol_info['symbol']
        logger.info(f"📊 [{i}/{len(symbols_to_process)}] Processing {symbol}...")
        rate_limiter.wait_if_needed()
        
        data = fetch_insider_transactions_data(symbol, api_key)
        
        if data and upload_to_s3(symbol, data, s3_client, s3_bucket, s3_prefix):
            results['successful'] += 1
            results['successful_symbols'].append(symbol)
        else:
            results['failed'] += 1
            results['failed_symbols'].append(symbol)

    try:
        watermark_manager.connect()
        watermark_manager.bulk_update_watermarks(results['successful_symbols'], results['failed_symbols'])
        watermark_manager.connection.commit()
    finally:
        watermark_manager.close()
    
    logger.info("🎉 ETL processing complete!")
    logger.info(f"✅ Successful: {results['successful']}/{results['total_symbols']}")
    logger.info(f"❌ Failed: {results['failed']}/{results['total_symbols']}")


if __name__ == '__main__':
    main()
