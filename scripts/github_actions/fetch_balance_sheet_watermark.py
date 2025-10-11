#!/usr/bin/env python3
"""
Watermark-Based Balance Sheet ETL
Fetches BALANCE_SHEET data using the ETL_WATERMARKS table for incremental processing.
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
    
    def __init__(self, snowflake_config: Dict[str, str], table_name: str = 'BALANCE_SHEET'):
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
        
        Args:
            exchange_filter: Filter by exchange (NYSE, NASDAQ, etc.)
            max_symbols: Maximum number of symbols to return
            skip_recent_hours: Skip symbols processed within this many hours (for incremental runs)
        
        Returns list of dicts with symbol information
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
        
        # Skip recently processed symbols if requested
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
        
        symbols_to_process = []
        
        for row in results:
            symbol = row[0]
            symbols_to_process.append({
                'symbol': symbol,
                'exchange': row[1],
                'asset_type': row[2],
                'status': row[3]
            })
        
        logger.info(f"📈 Found {len(symbols_to_process)} symbols to process")
        
        return symbols_to_process
    
    def update_watermark(self, symbol: str, first_date: str, last_date: str, success: bool = True):
        """
        Update watermark for a symbol after processing.
        
        Args:
            symbol: Stock ticker
            first_date: Earliest fiscal date in the data (YYYY-MM-DD)
            last_date: Most recent fiscal date in the data (YYYY-MM-DD)
            success: Whether processing was successful
        """
        self.connect()
        
        cursor = self.connection.cursor()
        
        if success:
            # Check if symbol has a delisting date - if so, mark as delisted after successful pull
            update_sql = f"""
                UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
                SET 
                    FIRST_FISCAL_DATE = COALESCE(FIRST_FISCAL_DATE, TO_DATE('{first_date}', 'YYYY-MM-DD')),
                    LAST_FISCAL_DATE = TO_DATE('{last_date}', 'YYYY-MM-DD'),
                    LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(),
                    CONSECUTIVE_FAILURES = 0,
                    API_ELIGIBLE = CASE 
                        WHEN DELISTING_DATE IS NOT NULL AND DELISTING_DATE <= CURRENT_DATE() 
                        THEN 'DEL'
                        ELSE API_ELIGIBLE 
                    END,
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
        time_since_last_call = current_time - self.last_call_time
        
        if time_since_last_call < self.min_delay:
            sleep_time = self.min_delay - time_since_last_call
            time.sleep(sleep_time)
        
        self.last_call_time = time.time()


def cleanup_s3_bucket(bucket: str, prefix: str, s3_client) -> int:
    """
    Delete all existing files in the S3 prefix.
    This prevents duplicate loading when using COPY FROM s3://.../*.csv pattern.
    
    Returns: Number of files deleted
    """
    logger.info(f"🧹 Cleaning up S3 bucket: s3://{bucket}/{prefix}")
    
    deleted_count = 0
    continuation_token = None
    
    while True:
        # List objects
        list_params = {'Bucket': bucket, 'Prefix': prefix}
        if continuation_token:
            list_params['ContinuationToken'] = continuation_token
        
        response = s3_client.list_objects_v2(**list_params)
        
        # Delete objects if any exist
        if 'Contents' in response:
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            s3_client.delete_objects(
                Bucket=bucket,
                Delete={'Objects': objects_to_delete}
            )
            deleted_count += len(objects_to_delete)
            logger.info(f"   Deleted {len(objects_to_delete)} files (total: {deleted_count})")
        
        # Check if there are more objects to list
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break
    
    logger.info(f"✅ S3 cleanup complete: {deleted_count} files deleted")
    return deleted_count


def fetch_balance_sheet_data(symbol: str, api_key: str) -> Optional[Dict]:
    """
    Fetch balance sheet data from Alpha Vantage API.
    
    Returns dict with:
    - symbol: stock ticker
    - annual_reports: list of annual balance sheet data
    - quarterly_reports: list of quarterly balance sheet data
    - record_count: total number of records
    - first_date: earliest fiscal date
    - last_date: most recent fiscal date
    """
    url = "https://www.alphavantage.co/query"
    params = {
        'function': 'BALANCE_SHEET',
        'symbol': symbol,
        'apikey': api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Check for API errors
        if 'Error Message' in data:
            logger.warning(f"⚠️  API error for {symbol}: {data['Error Message']}")
            return None
        
        if 'Note' in data:
            logger.warning(f"⚠️  API rate limit hit for {symbol}: {data['Note']}")
            return None
        
        # Extract reports
        annual_reports = data.get('annualReports', [])
        quarterly_reports = data.get('quarterlyReports', [])
        
        if not annual_reports and not quarterly_reports:
            logger.warning(f"⚠️  No balance sheet data for {symbol}")
            return None
        
        # Get fiscal dates
        all_dates = []
        for report in annual_reports + quarterly_reports:
            if 'fiscalDateEnding' in report:
                all_dates.append(report['fiscalDateEnding'])
        
        if not all_dates:
            logger.warning(f"⚠️  No fiscal dates found for {symbol}")
            return None
        
        all_dates.sort()
        
        logger.info(f"✅ Fetched {symbol}: {len(annual_reports)} annual + {len(quarterly_reports)} quarterly reports")
        
        return {
            'symbol': symbol,
            'annual_reports': annual_reports,
            'quarterly_reports': quarterly_reports,
            'record_count': len(annual_reports) + len(quarterly_reports),
            'first_date': all_dates[0],
            'last_date': all_dates[-1]
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Request failed for {symbol}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error for {symbol}: {e}")
        return None


def upload_to_s3(data: Dict, s3_client, bucket: str, prefix: str) -> bool:
    """Upload balance sheet data to S3 as CSV."""
    symbol = data['symbol']
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"{prefix}{symbol}_{timestamp}.csv"
    
    try:
        # Convert to CSV format
        csv_buffer = StringIO()
        
        # Define CSV columns (flatten the nested structure)
        fieldnames = ['symbol', 'fiscal_date_ending', 'period_type', 'reported_currency',
                     'total_assets', 'total_current_assets', 'cash_and_cash_equivalents',
                     'cash_and_short_term_investments', 'inventory', 'current_net_receivables',
                     'total_non_current_assets', 'property_plant_equipment', 'accumulated_depreciation_amortization_ppe',
                     'intangible_assets', 'goodwill', 'investments', 'long_term_investments',
                     'short_term_investments', 'other_current_assets', 'other_non_current_assets',
                     'total_liabilities', 'total_current_liabilities', 'current_accounts_payable',
                     'deferred_revenue', 'current_debt', 'short_term_debt', 'total_non_current_liabilities',
                     'capital_lease_obligations', 'long_term_debt', 'current_long_term_debt',
                     'long_term_debt_noncurrent', 'other_current_liabilities', 'other_non_current_liabilities',
                     'total_shareholder_equity', 'treasury_stock', 'retained_earnings',
                     'common_stock', 'common_stock_shares_outstanding']
        
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        # Write annual reports
        for report in data['annual_reports']:
            row = {
                'symbol': symbol,
                'fiscal_date_ending': report.get('fiscalDateEnding'),
                'period_type': 'annual',
                'reported_currency': report.get('reportedCurrency'),
                'total_assets': report.get('totalAssets'),
                'total_current_assets': report.get('totalCurrentAssets'),
                'cash_and_cash_equivalents': report.get('cashAndCashEquivalentsAtCarryingValue'),
                'cash_and_short_term_investments': report.get('cashAndShortTermInvestments'),
                'inventory': report.get('inventory'),
                'current_net_receivables': report.get('currentNetReceivables'),
                'total_non_current_assets': report.get('totalNonCurrentAssets'),
                'property_plant_equipment': report.get('propertyPlantEquipment'),
                'accumulated_depreciation_amortization_ppe': report.get('accumulatedDepreciationAmortizationPPE'),
                'intangible_assets': report.get('intangibleAssets'),
                'goodwill': report.get('goodwill'),
                'investments': report.get('investments'),
                'long_term_investments': report.get('longTermInvestments'),
                'short_term_investments': report.get('shortTermInvestments'),
                'other_current_assets': report.get('otherCurrentAssets'),
                'other_non_current_assets': report.get('otherNonCurrentAssets'),
                'total_liabilities': report.get('totalLiabilities'),
                'total_current_liabilities': report.get('totalCurrentLiabilities'),
                'current_accounts_payable': report.get('currentAccountsPayable'),
                'deferred_revenue': report.get('deferredRevenue'),
                'current_debt': report.get('currentDebt'),
                'short_term_debt': report.get('shortTermDebt'),
                'total_non_current_liabilities': report.get('totalNonCurrentLiabilities'),
                'capital_lease_obligations': report.get('capitalLeaseObligations'),
                'long_term_debt': report.get('longTermDebt'),
                'current_long_term_debt': report.get('currentLongTermDebt'),
                'long_term_debt_noncurrent': report.get('longTermDebtNoncurrent'),
                'other_current_liabilities': report.get('otherCurrentLiabilities'),
                'other_non_current_liabilities': report.get('otherNonCurrentLiabilities'),
                'total_shareholder_equity': report.get('totalShareholderEquity'),
                'treasury_stock': report.get('treasuryStock'),
                'retained_earnings': report.get('retainedEarnings'),
                'common_stock': report.get('commonStock'),
                'common_stock_shares_outstanding': report.get('commonStockSharesOutstanding')
            }
            writer.writerow(row)
        
        # Write quarterly reports
        for report in data['quarterly_reports']:
            row = {
                'symbol': symbol,
                'fiscal_date_ending': report.get('fiscalDateEnding'),
                'period_type': 'quarterly',
                'reported_currency': report.get('reportedCurrency'),
                'total_assets': report.get('totalAssets'),
                'total_current_assets': report.get('totalCurrentAssets'),
                'cash_and_cash_equivalents': report.get('cashAndCashEquivalentsAtCarryingValue'),
                'cash_and_short_term_investments': report.get('cashAndShortTermInvestments'),
                'inventory': report.get('inventory'),
                'current_net_receivables': report.get('currentNetReceivables'),
                'total_non_current_assets': report.get('totalNonCurrentAssets'),
                'property_plant_equipment': report.get('propertyPlantEquipment'),
                'accumulated_depreciation_amortization_ppe': report.get('accumulatedDepreciationAmortizationPPE'),
                'intangible_assets': report.get('intangibleAssets'),
                'goodwill': report.get('goodwill'),
                'investments': report.get('investments'),
                'long_term_investments': report.get('longTermInvestments'),
                'short_term_investments': report.get('shortTermInvestments'),
                'other_current_assets': report.get('otherCurrentAssets'),
                'other_non_current_assets': report.get('otherNonCurrentAssets'),
                'total_liabilities': report.get('totalLiabilities'),
                'total_current_liabilities': report.get('totalCurrentLiabilities'),
                'current_accounts_payable': report.get('currentAccountsPayable'),
                'deferred_revenue': report.get('deferredRevenue'),
                'current_debt': report.get('currentDebt'),
                'short_term_debt': report.get('shortTermDebt'),
                'total_non_current_liabilities': report.get('totalNonCurrentLiabilities'),
                'capital_lease_obligations': report.get('capitalLeaseObligations'),
                'long_term_debt': report.get('longTermDebt'),
                'current_long_term_debt': report.get('currentLongTermDebt'),
                'long_term_debt_noncurrent': report.get('longTermDebtNoncurrent'),
                'other_current_liabilities': report.get('otherCurrentLiabilities'),
                'other_non_current_liabilities': report.get('otherNonCurrentLiabilities'),
                'total_shareholder_equity': report.get('totalShareholderEquity'),
                'treasury_stock': report.get('treasuryStock'),
                'retained_earnings': report.get('retainedEarnings'),
                'common_stock': report.get('commonStock'),
                'common_stock_shares_outstanding': report.get('commonStockSharesOutstanding')
            }
            writer.writerow(row)
        
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
    logger.info("🚀 Starting Watermark-Based Balance Sheet ETL")
    
    # Get configuration from environment
    api_key = os.environ['ALPHAVANTAGE_API_KEY']
    s3_bucket = os.environ.get('S3_BUCKET', 'fin-trade-craft-landing')
    s3_prefix = os.environ.get('S3_BALANCE_SHEET_PREFIX', 'balance_sheet/')
    exchange_filter = os.environ.get('EXCHANGE_FILTER')  # NYSE, NASDAQ, or None for all
    max_symbols = int(os.environ['MAX_SYMBOLS']) if os.environ.get('MAX_SYMBOLS') else None
    skip_recent_hours = int(os.environ['SKIP_RECENT_HOURS']) if os.environ.get('SKIP_RECENT_HOURS') else None
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
        # Clean up S3 bucket before extraction (critical for COPY FROM s3://.../*.csv)
        logger.info("=" * 60)
        logger.info("🧹 STEP 1: Clean up existing S3 files")
        logger.info("=" * 60)
        deleted_count = cleanup_s3_bucket(s3_bucket, s3_prefix, s3_client)
        logger.info(f"✅ Cleanup complete: {deleted_count} old files removed")
        logger.info("")
        
        # Get symbols to process from watermarks
        logger.info("=" * 60)
        logger.info("🔍 STEP 2: Query watermarks for symbols to process")
        logger.info("=" * 60)
        symbols_to_process = watermark_manager.get_symbols_to_process(
            exchange_filter=exchange_filter,
            max_symbols=max_symbols,
            skip_recent_hours=skip_recent_hours
        )
        
        if not symbols_to_process:
            logger.warning("⚠️  No symbols to process")
            return
        
        logger.info("")
        
        # Process symbols in batches
        logger.info("=" * 60)
        logger.info("🚀 STEP 3: Extract balance sheet data from Alpha Vantage")
        logger.info("=" * 60)
        
        results = {
            'total_symbols': len(symbols_to_process),
            'successful': 0,
            'failed': 0,
            'start_time': datetime.now().isoformat(),
            'details': []
        }
        
        for i, symbol_info in enumerate(symbols_to_process, 1):
            symbol = symbol_info['symbol']
            
            logger.info(f"📊 [{i}/{len(symbols_to_process)}] Processing {symbol}...")
            
            # Rate limit
            rate_limiter.wait_if_needed()
            
            # Fetch data
            data = fetch_balance_sheet_data(symbol, api_key)
            
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
                    'status': 'failed'
                })
        
        # Save results
        results['end_time'] = datetime.now().isoformat()
        results['duration_minutes'] = (datetime.fromisoformat(results['end_time']) - 
                                      datetime.fromisoformat(results['start_time'])).total_seconds() / 60
        
        # Check how many delisted symbols were marked
        cursor = watermark_manager.connection.cursor()
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = '{watermark_manager.table_name}'
              AND API_ELIGIBLE = 'DEL'
        """)
        delisted_count = cursor.fetchone()[0]
        cursor.close()
        results['delisted_marked'] = delisted_count
        
        with open('/tmp/watermark_etl_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info("🎉 ETL processing complete!")
        logger.info(f"✅ Successful: {results['successful']}/{results['total_symbols']}")
        logger.info(f"❌ Failed: {results['failed']}/{results['total_symbols']}")
        if delisted_count > 0:
            logger.info(f"🔒 Delisted symbols marked as 'DEL': {delisted_count}")
        
    finally:
        watermark_manager.close()


if __name__ == '__main__':
    main()
