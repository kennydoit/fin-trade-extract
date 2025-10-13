#!/usr/bin/env python3
"""
Watermark-Based Income Statement ETL
Fetches INCOME_STATEMENT data using the ETL_WATERMARKS table for incremental processing.
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
    
    def __init__(self, snowflake_config: Dict[str, str], table_name: str = 'INCOME_STATEMENT'):
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
        
        # FUNDAMENTALS-SPECIFIC LOGIC: Only pull if 135 days have passed since LAST_FISCAL_DATE
        # This prevents unnecessary API calls when new quarterly data isn't available yet
        # 135 days = 90 days (1 quarter) + 45 days (grace period for filing delays)
        query += """
              AND (LAST_FISCAL_DATE IS NULL 
                   OR LAST_FISCAL_DATE < DATEADD(day, -135, CURRENT_DATE()))
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
        logger.info(f"📅 Fundamentals logic: Only symbols with LAST_FISCAL_DATE older than 135 days (or NULL)")
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
    
    def bulk_update_watermarks(self, successful_updates: List[Dict], failed_symbols: List[str]):
        """
        Bulk update watermarks using a single MERGE statement (MUCH faster than individual UPDATEs).
        
        Args:
            successful_updates: List of dicts with {symbol, first_date, last_date}
            failed_symbols: List of symbols that failed processing
        """
        if not self.connection:
            raise RuntimeError("❌ No active Snowflake connection. Call connect() first.")
        
        cursor = self.connection.cursor()
        
        # Build VALUES clause for successful updates
        if successful_updates:
            logger.info(f"📝 Bulk updating {len(successful_updates)} successful watermarks...")
            
            # Create temporary table with updates
            cursor.execute("""
                CREATE TEMPORARY TABLE WATERMARK_UPDATES (
                    SYMBOL VARCHAR(20),
                    FIRST_DATE DATE,
                    LAST_DATE DATE
                )
            """)
            
            # Build INSERT statement with all values
            values_list = []
            for update in successful_updates:
                values_list.append(
                    f"('{update['symbol']}', "
                    f"TO_DATE('{update['first_date']}', 'YYYY-MM-DD'), "
                    f"TO_DATE('{update['last_date']}', 'YYYY-MM-DD'))"
                )
            
            # Insert all updates at once (batch insert is fast)
            values_clause = ',\n'.join(values_list)
            cursor.execute(f"""
                INSERT INTO WATERMARK_UPDATES (SYMBOL, FIRST_DATE, LAST_DATE)
                VALUES {values_clause}
            """)
            
            # Single MERGE to update all watermarks at once
            cursor.execute(f"""
                MERGE INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS target
                USING WATERMARK_UPDATES source
                ON target.TABLE_NAME = '{self.table_name}'
                   AND target.SYMBOL = source.SYMBOL
                WHEN MATCHED THEN UPDATE SET
                    FIRST_FISCAL_DATE = COALESCE(target.FIRST_FISCAL_DATE, source.FIRST_DATE),
                    LAST_FISCAL_DATE = source.LAST_DATE,
                    LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(),
                    CONSECUTIVE_FAILURES = 0,
                    API_ELIGIBLE = CASE 
                        WHEN target.DELISTING_DATE IS NOT NULL AND target.DELISTING_DATE <= CURRENT_DATE() 
                        THEN 'DEL'
                        ELSE target.API_ELIGIBLE 
                    END,
                    UPDATED_AT = CURRENT_TIMESTAMP()
            """)
            
            logger.info(f"✅ Bulk updated {len(successful_updates)} successful watermarks in single MERGE")
        
        # Handle failed symbols (much smaller batch, can use simple UPDATE with IN clause)
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


class RateLimiter:
    """Simple rate limiter for API calls."""
    
    def __init__(self, calls_per_minute: int = 75):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
        self.last_call_time = 0
    
    def wait_if_needed(self):
        """Wait if necessary to respect rate limit."""
        current_time = time.time()
        time_since_last_call = current_time - self.last_call_time
        
        if time_since_last_call < self.min_interval:
            sleep_time = self.min_interval - time_since_last_call
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
            logger.info(f"  🗑️  Deleted {len(objects_to_delete)} files")
        
        # Check if more pages exist
        if response.get('IsTruncated'):
            continuation_token = response.get('NextContinuationToken')
        else:
            break
    
    return deleted_count


def fetch_income_statement(symbol: str, api_key: str, rate_limiter: RateLimiter) -> Optional[Dict]:
    """
    Fetch income statement data from Alpha Vantage API.
    Returns both annual and quarterly reports in a single call.
    """
    rate_limiter.wait_if_needed()
    
    url = 'https://www.alphavantage.co/query'
    params = {
        'function': 'INCOME_STATEMENT',
        'symbol': symbol,
        'apikey': api_key
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Check for API errors
        if 'Error Message' in data:
            logger.warning(f"⚠️  API Error for {symbol}: {data['Error Message']}")
            return None
        
        if 'Note' in data:
            logger.warning(f"⚠️  API Rate Limit for {symbol}: {data['Note']}")
            return None
        
        # Check if data exists
        if 'annualReports' not in data and 'quarterlyReports' not in data:
            logger.warning(f"⚠️  No income statement data for {symbol}")
            return None
        
        return data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Request failed for {symbol}: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"❌ JSON decode error for {symbol}: {e}")
        return None


def upload_to_s3(symbol: str, data: Dict, s3_client, bucket: str, prefix: str) -> bool:
    """Upload income statement data to S3 as CSV."""
    try:
        # Define CSV columns with explicit mapping
        fieldnames = [
            'symbol', 'fiscal_date_ending', 'period_type', 'reported_currency',
            'gross_profit', 'total_revenue', 'cost_of_revenue', 'cost_of_goods_and_services_sold',
            'operating_income', 'selling_general_and_administrative', 'research_and_development',
            'operating_expenses', 'investment_income_net', 'net_interest_income', 'interest_income',
            'interest_expense', 'non_interest_income', 'other_non_operating_income',
            'depreciation', 'depreciation_and_amortization', 'income_before_tax',
            'income_tax_expense', 'interest_and_debt_expense', 'net_income_from_continuing_operations',
            'comprehensive_income_net_of_tax', 'ebit', 'ebitda', 'net_income'
        ]
        
        # Convert to CSV
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        # Write annual reports with explicit field mapping
        for report in data.get('annualReports', []):
            row = {
                'symbol': symbol,
                'fiscal_date_ending': report.get('fiscalDateEnding'),
                'period_type': 'annual',
                'reported_currency': report.get('reportedCurrency'),
                'gross_profit': report.get('grossProfit'),
                'total_revenue': report.get('totalRevenue'),
                'cost_of_revenue': report.get('costOfRevenue'),
                'cost_of_goods_and_services_sold': report.get('costofGoodsAndServicesSold'),
                'operating_income': report.get('operatingIncome'),
                'selling_general_and_administrative': report.get('sellingGeneralAndAdministrative'),
                'research_and_development': report.get('researchAndDevelopment'),
                'operating_expenses': report.get('operatingExpenses'),
                'investment_income_net': report.get('investmentIncomeNet'),
                'net_interest_income': report.get('netInterestIncome'),
                'interest_income': report.get('interestIncome'),
                'interest_expense': report.get('interestExpense'),
                'non_interest_income': report.get('nonInterestIncome'),
                'other_non_operating_income': report.get('otherNonOperatingIncome'),
                'depreciation': report.get('depreciation'),
                'depreciation_and_amortization': report.get('depreciationAndAmortization'),
                'income_before_tax': report.get('incomeBeforeTax'),
                'income_tax_expense': report.get('incomeTaxExpense'),
                'interest_and_debt_expense': report.get('interestAndDebtExpense'),
                'net_income_from_continuing_operations': report.get('netIncomeFromContinuingOperations'),
                'comprehensive_income_net_of_tax': report.get('comprehensiveIncomeNetOfTax'),
                'ebit': report.get('ebit'),
                'ebitda': report.get('ebitda'),
                'net_income': report.get('netIncome')
            }
            writer.writerow(row)
        
        # Write quarterly reports with explicit field mapping
        for report in data.get('quarterlyReports', []):
            row = {
                'symbol': symbol,
                'fiscal_date_ending': report.get('fiscalDateEnding'),
                'period_type': 'quarterly',
                'reported_currency': report.get('reportedCurrency'),
                'gross_profit': report.get('grossProfit'),
                'total_revenue': report.get('totalRevenue'),
                'cost_of_revenue': report.get('costOfRevenue'),
                'cost_of_goods_and_services_sold': report.get('costofGoodsAndServicesSold'),
                'operating_income': report.get('operatingIncome'),
                'selling_general_and_administrative': report.get('sellingGeneralAndAdministrative'),
                'research_and_development': report.get('researchAndDevelopment'),
                'operating_expenses': report.get('operatingExpenses'),
                'investment_income_net': report.get('investmentIncomeNet'),
                'net_interest_income': report.get('netInterestIncome'),
                'interest_income': report.get('interestIncome'),
                'interest_expense': report.get('interestExpense'),
                'non_interest_income': report.get('nonInterestIncome'),
                'other_non_operating_income': report.get('otherNonOperatingIncome'),
                'depreciation': report.get('depreciation'),
                'depreciation_and_amortization': report.get('depreciationAndAmortization'),
                'income_before_tax': report.get('incomeBeforeTax'),
                'income_tax_expense': report.get('incomeTaxExpense'),
                'interest_and_debt_expense': report.get('interestAndDebtExpense'),
                'net_income_from_continuing_operations': report.get('netIncomeFromContinuingOperations'),
                'comprehensive_income_net_of_tax': report.get('comprehensiveIncomeNetOfTax'),
                'ebit': report.get('ebit'),
                'ebitda': report.get('ebitda'),
                'net_income': report.get('netIncome')
            }
            writer.writerow(row)
        
        # Upload to S3
        csv_content = output.getvalue()
        s3_key = f"{prefix}{symbol}.csv"
        
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )
        
        total_reports = len(data.get('annualReports', [])) + len(data.get('quarterlyReports', []))
        logger.info(f"✅ Uploaded: s3://{bucket}/{s3_key} ({total_reports} reports)")
        return True
    
    except Exception as e:
        logger.error(f"❌ S3 upload failed for {symbol}: {e}")
        return False


def process_symbols_in_batches(symbols: List[Dict], api_key: str, s3_client, 
                               bucket: str, prefix: str, batch_size: int = 50) -> Dict:
    """Process symbols in batches with progress tracking."""
    rate_limiter = RateLimiter(calls_per_minute=75)  # Premium tier
    
    total = len(symbols)
    successful = []
    failed = []
    
    for i, symbol_info in enumerate(symbols, 1):
        symbol = symbol_info['symbol']
        
        logger.info(f"📊 Processing {symbol} ({i}/{total})...")
        
        # Fetch from API
        data = fetch_income_statement(symbol, api_key, rate_limiter)
        
        if data is None:
            failed.append(symbol)
            continue
        
        # Upload to S3
        if upload_to_s3(symbol, data, s3_client, bucket, prefix):
            # Track date range for watermark update
            first_date = None
            last_date = None
            
            for report in data.get('annualReports', []) + data.get('quarterlyReports', []):
                fiscal_date = report.get('fiscalDateEnding')
                if fiscal_date:
                    if first_date is None or fiscal_date < first_date:
                        first_date = fiscal_date
                    if last_date is None or fiscal_date > last_date:
                        last_date = fiscal_date
            
            if first_date and last_date:
                successful.append({
                    'symbol': symbol,
                    'first_date': first_date,
                    'last_date': last_date
                })
        else:
            failed.append(symbol)
        
        # Progress indicator every 10 symbols
        if i % 10 == 0:
            logger.info(f"🔄 Progress: {i}/{total} symbols processed")
    
    return {
        'successful': successful,
        'failed': failed
    }


def main():
    """Main ETL execution."""
    start_time = datetime.now()
    
    # Get configuration from environment
    api_key = os.environ.get('ALPHAVANTAGE_API_KEY')
    s3_bucket = os.environ.get('S3_BUCKET', 'fin-trade-craft-landing')
    s3_prefix = os.environ.get('S3_INCOME_STATEMENT_PREFIX', 'income_statement/')
    exchange_filter = os.environ.get('EXCHANGE_FILTER')
    max_symbols = os.environ.get('MAX_SYMBOLS')
    skip_recent_hours = os.environ.get('SKIP_RECENT_HOURS')
    batch_size = int(os.environ.get('BATCH_SIZE', '50'))
    
    if max_symbols:
        max_symbols = int(max_symbols)
    if skip_recent_hours:
        skip_recent_hours = int(skip_recent_hours)
    
    # Validate required config
    if not api_key:
        logger.error("❌ ALPHAVANTAGE_API_KEY environment variable not set")
        sys.exit(1)
    
    # Snowflake configuration
    snowflake_config = {
        'user': os.environ.get('SNOWFLAKE_USER'),
        'password': os.environ.get('SNOWFLAKE_PASSWORD'),
        'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
        'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE'),
        'database': os.environ.get('SNOWFLAKE_DATABASE'),
        'schema': os.environ.get('SNOWFLAKE_SCHEMA')
    }
    
    # Initialize managers
    watermark_manager = WatermarkETLManager(snowflake_config)
    s3_client = boto3.client('s3')
    
    # Clean up S3 bucket before extraction (critical for COPY FROM s3://.../*.csv)
    logger.info("=" * 60)
    logger.info("🧹 STEP 1: Clean up existing S3 files")
    logger.info("=" * 60)
    deleted_count = cleanup_s3_bucket(s3_bucket, s3_prefix, s3_client)
    logger.info(f"✅ Cleanup complete: {deleted_count} old files removed")
    logger.info("")
    
    # STEP 2: Get symbols to process from watermarks, then CLOSE connection
    logger.info("=" * 60)
    logger.info("🔍 STEP 2: Query watermarks for symbols to process")
    logger.info("=" * 60)
    try:
        symbols_to_process = watermark_manager.get_symbols_to_process(
            exchange_filter=exchange_filter,
            max_symbols=max_symbols,
            skip_recent_hours=skip_recent_hours
        )
    finally:
        # CRITICAL: Close Snowflake connection immediately after getting symbols
        watermark_manager.close()
        logger.info("🔌 Snowflake connection closed after watermark query")
    
    if not symbols_to_process:
        logger.warning("⚠️  No symbols to process")
        return
    
    logger.info("")
    
    # STEP 3: Extract from Alpha Vantage (NO Snowflake connection - prevents idle warehouse costs)
    logger.info("=" * 60)
    logger.info("🚀 STEP 3: Extract income statement data from Alpha Vantage")
    logger.info("=" * 60)
    
    results = process_symbols_in_batches(
        symbols_to_process,
        api_key,
        s3_client,
        s3_bucket,
        s3_prefix,
        batch_size
    )
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("📊 EXTRACTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✅ Successful: {len(results['successful'])} symbols")
    logger.info(f"❌ Failed: {len(results['failed'])} symbols")
    
    if results['failed']:
        logger.info(f"Failed symbols: {', '.join(results['failed'][:10])}")
        if len(results['failed']) > 10:
            logger.info(f"  ... and {len(results['failed']) - 10} more")
    
    # STEP 4: Update watermarks (reconnect to Snowflake ONLY for this step)
    logger.info("")
    logger.info("=" * 60)
    logger.info("💾 STEP 4: Update watermarks in Snowflake")
    logger.info("=" * 60)
    try:
        watermark_manager.connect()
        watermark_manager.bulk_update_watermarks(
            results['successful'],
            results['failed']
        )
    finally:
        watermark_manager.close()
    
    # Calculate duration
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds() / 60
    
    # Save results for GitHub Actions artifact
    results_summary = {
        'total_symbols': len(symbols_to_process),
        'successful': len(results['successful']),
        'failed': len(results['failed']),
        'duration_minutes': round(duration, 2),
        'start_time': start_time.isoformat(),
        'end_time': end_time.isoformat(),
        'failed_symbols': results['failed']
    }
    
    # Check for delisted symbols that were successfully processed
    delisted_marked = 0
    for update in results['successful']:
        # This will be handled in the watermark MERGE logic
        pass
    results_summary['delisted_marked'] = delisted_marked
    
    with open('/tmp/watermark_etl_results.json', 'w') as f:
        json.dump(results_summary, f, indent=2)
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ INCOME STATEMENT ETL COMPLETE!")
    logger.info("=" * 60)
    logger.info(f"⏱️  Total duration: {duration:.2f} minutes")
    logger.info(f"📊 Processing rate: {len(symbols_to_process)/duration:.1f} symbols/minute")
    logger.info("")


if __name__ == '__main__':
    main()
