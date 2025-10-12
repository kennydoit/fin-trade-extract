#!/usr/bin/env python3
"""
Cash Flow Statement Watermark-Based ETL
Fetches cash flow data from Alpha Vantage API and uploads to S3.
"""

import os
import sys
import time
import json
import logging
import requests
import boto3
import csv
from io import StringIO
from datetime import datetime
from typing import Dict, List, Optional
import snowflake.connector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WatermarkETLManager:
    """Manages ETL watermark state for cash flow data."""
    
    def __init__(self, snowflake_config: Dict):
        self.config = snowflake_config
        self.connection = None
        self.table_name = 'CASH_FLOW'
    
    def connect(self):
        """Establish Snowflake connection."""
        logger.info("🔌 Connecting to Snowflake...")
        self.connection = snowflake.connector.connect(**self.config)
        logger.info("✅ Connected to Snowflake")
    
    def close(self):
        """Close Snowflake connection."""
        if self.connection:
            self.connection.close()
            logger.info("🔒 Snowflake connection closed")
    
    def get_symbols_to_process(
        self,
        exchange_filter: Optional[str] = None,
        max_symbols: Optional[int] = None,
        skip_recent_hours: Optional[int] = None
    ) -> List[Dict]:
        """
        Get symbols that need cash flow data extraction based on watermarks.
        
        Applies 135-day staleness check: Only fetch if LAST_FISCAL_DATE is NULL
        or older than 135 days (quarterly data + 45-day filing grace period).
        """
        if not self.connection:
            raise RuntimeError("❌ No active Snowflake connection. Call connect() first.")
        
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
            
            # Single MERGE statement to update all watermarks at once (100x faster!)
            merge_sql = f"""
                MERGE INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS target
                USING WATERMARK_UPDATES source
                ON target.TABLE_NAME = '{self.table_name}' 
                   AND target.SYMBOL = source.SYMBOL
                WHEN MATCHED THEN UPDATE SET
                    FIRST_FISCAL_DATE = COALESCE(target.FIRST_FISCAL_DATE, source.FIRST_DATE),
                    LAST_FISCAL_DATE = source.LAST_DATE,
                    LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(),
                    CONSECUTIVE_FAILURES = 0,
                    UPDATED_AT = CURRENT_TIMESTAMP(),
                    API_ELIGIBLE = CASE 
                        WHEN target.DELISTING_DATE IS NOT NULL 
                             AND target.DELISTING_DATE <= CURRENT_DATE() 
                        THEN 'DEL'
                        ELSE target.API_ELIGIBLE 
                    END
            """
            
            cursor.execute(merge_sql)
            logger.info(f"✅ Bulk watermark update complete: {len(successful_updates)} symbols")
        
        # Handle failed symbols (increment failure counter)
        if failed_symbols:
            logger.info(f"❌ Updating {len(failed_symbols)} failed symbols...")
            
            for symbol in failed_symbols:
                update_sql = f"""
                    UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
                    SET CONSECUTIVE_FAILURES = CONSECUTIVE_FAILURES + 1,
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE TABLE_NAME = '{self.table_name}'
                      AND SYMBOL = '{symbol}'
                """
            
            cursor.execute(update_sql)
        
        cursor.close()
        # Note: Commit is handled by caller to allow batching multiple updates


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


def fetch_cash_flow_data(symbol: str, api_key: str) -> Optional[Dict]:
    """
    Fetch cash flow data from Alpha Vantage API.
    
    Returns dict with:
    - symbol: stock ticker
    - annual_reports: list of annual cash flow data
    - quarterly_reports: list of quarterly cash flow data
    - record_count: total number of records
    - first_date: earliest fiscal date
    - last_date: most recent fiscal date
    """
    url = "https://www.alphavantage.co/query"
    params = {
        'function': 'CASH_FLOW',
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
            logger.warning(f"⚠️  No cash flow data for {symbol}")
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
    """Upload cash flow data to S3 as CSV."""
    symbol = data['symbol']
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"{prefix}{symbol}_{timestamp}.csv"
    
    try:
        # Convert to CSV format
        csv_buffer = StringIO()
        
        # Define CSV columns (flatten the nested structure)
        fieldnames = ['symbol', 'fiscal_date_ending', 'period_type', 'reported_currency',
                     'operating_cashflow', 'payments_for_operating_activities',
                     'proceeds_from_operating_activities', 'change_in_operating_liabilities',
                     'change_in_operating_assets', 'depreciation_depletion_and_amortization',
                     'capital_expenditures', 'change_in_receivables', 'change_in_inventory',
                     'profit_loss', 'cashflow_from_investment', 'cashflow_from_financing',
                     'proceeds_from_repurchase_of_equity', 'proceeds_from_sale_of_long_term_investments',
                     'payments_for_acquisition_of_long_term_investments', 'proceeds_from_issuance_of_long_term_debt',
                     'proceeds_from_issuance_of_common_stock', 'proceeds_from_repayments_of_short_term_debt',
                     'payments_for_repurchase_of_common_stock', 'payments_for_repurchase_of_preferred_stock',
                     'dividend_payout', 'dividend_payout_common_stock', 'dividend_payout_preferred_stock',
                     'proceeds_from_sale_of_ppe', 'change_in_cash_and_cash_equivalents',
                     'change_in_exchange_rate', 'net_income']
        
        writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        # Write annual reports
        for report in data['annual_reports']:
            row = {
                'symbol': symbol,
                'fiscal_date_ending': report.get('fiscalDateEnding'),
                'period_type': 'annual',
                'reported_currency': report.get('reportedCurrency'),
                'operating_cashflow': report.get('operatingCashflow'),
                'payments_for_operating_activities': report.get('paymentsForOperatingActivities'),
                'proceeds_from_operating_activities': report.get('proceedsFromOperatingActivities'),
                'change_in_operating_liabilities': report.get('changeInOperatingLiabilities'),
                'change_in_operating_assets': report.get('changeInOperatingAssets'),
                'depreciation_depletion_and_amortization': report.get('depreciationDepletionAndAmortization'),
                'capital_expenditures': report.get('capitalExpenditures'),
                'change_in_receivables': report.get('changeInReceivables'),
                'change_in_inventory': report.get('changeInInventory'),
                'profit_loss': report.get('profitLoss'),
                'cashflow_from_investment': report.get('cashflowFromInvestment'),
                'cashflow_from_financing': report.get('cashflowFromFinancing'),
                'proceeds_from_repurchase_of_equity': report.get('proceedsFromRepurchaseOfEquity'),
                'proceeds_from_sale_of_long_term_investments': report.get('proceedsFromSaleOfLongTermInvestments'),
                'payments_for_acquisition_of_long_term_investments': report.get('paymentsForAcquisitionOfLongTermInvestments'),
                'proceeds_from_issuance_of_long_term_debt': report.get('proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet'),
                'proceeds_from_issuance_of_common_stock': report.get('proceedsFromIssuanceOfCommonStock'),
                'proceeds_from_repayments_of_short_term_debt': report.get('proceedsFromRepaymentsOfShortTermDebt'),
                'payments_for_repurchase_of_common_stock': report.get('paymentsForRepurchaseOfCommonStock'),
                'payments_for_repurchase_of_preferred_stock': report.get('paymentsForRepurchaseOfPreferredStock'),
                'dividend_payout': report.get('dividendPayout'),
                'dividend_payout_common_stock': report.get('dividendPayoutCommonStock'),
                'dividend_payout_preferred_stock': report.get('dividendPayoutPreferredStock'),
                'proceeds_from_sale_of_ppe': report.get('proceedsFromSaleOfPropertyPlantAndEquipment'),
                'change_in_cash_and_cash_equivalents': report.get('changeInCashAndCashEquivalents'),
                'change_in_exchange_rate': report.get('changeInExchangeRate'),
                'net_income': report.get('netIncome')
            }
            writer.writerow(row)
        
        # Write quarterly reports
        for report in data['quarterly_reports']:
            row = {
                'symbol': symbol,
                'fiscal_date_ending': report.get('fiscalDateEnding'),
                'period_type': 'quarterly',
                'reported_currency': report.get('reportedCurrency'),
                'operating_cashflow': report.get('operatingCashflow'),
                'payments_for_operating_activities': report.get('paymentsForOperatingActivities'),
                'proceeds_from_operating_activities': report.get('proceedsFromOperatingActivities'),
                'change_in_operating_liabilities': report.get('changeInOperatingLiabilities'),
                'change_in_operating_assets': report.get('changeInOperatingAssets'),
                'depreciation_depletion_and_amortization': report.get('depreciationDepletionAndAmortization'),
                'capital_expenditures': report.get('capitalExpenditures'),
                'change_in_receivables': report.get('changeInReceivables'),
                'change_in_inventory': report.get('changeInInventory'),
                'profit_loss': report.get('profitLoss'),
                'cashflow_from_investment': report.get('cashflowFromInvestment'),
                'cashflow_from_financing': report.get('cashflowFromFinancing'),
                'proceeds_from_repurchase_of_equity': report.get('proceedsFromRepurchaseOfEquity'),
                'proceeds_from_sale_of_long_term_investments': report.get('proceedsFromSaleOfLongTermInvestments'),
                'payments_for_acquisition_of_long_term_investments': report.get('paymentsForAcquisitionOfLongTermInvestments'),
                'proceeds_from_issuance_of_long_term_debt': report.get('proceedsFromIssuanceOfLongTermDebtAndCapitalSecuritiesNet'),
                'proceeds_from_issuance_of_common_stock': report.get('proceedsFromIssuanceOfCommonStock'),
                'proceeds_from_repayments_of_short_term_debt': report.get('proceedsFromRepaymentsOfShortTermDebt'),
                'payments_for_repurchase_of_common_stock': report.get('paymentsForRepurchaseOfCommonStock'),
                'payments_for_repurchase_of_preferred_stock': report.get('paymentsForRepurchaseOfPreferredStock'),
                'dividend_payout': report.get('dividendPayout'),
                'dividend_payout_common_stock': report.get('dividendPayoutCommonStock'),
                'dividend_payout_preferred_stock': report.get('dividendPayoutPreferredStock'),
                'proceeds_from_sale_of_ppe': report.get('proceedsFromSaleOfPropertyPlantAndEquipment'),
                'change_in_cash_and_cash_equivalents': report.get('changeInCashAndCashEquivalents'),
                'change_in_exchange_rate': report.get('changeInExchangeRate'),
                'net_income': report.get('netIncome')
            }
            writer.writerow(row)
        
        # Upload to S3
        csv_content = csv_buffer.getvalue()
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=csv_content.encode('utf-8'),
            ContentType='text/csv'
        )
        
        logger.info(f"✅ Uploaded to s3://{bucket}/{s3_key}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to upload {symbol} to S3: {e}")
        return False


def main():
    """Main execution function."""
    logger.info("🚀 Starting Cash Flow Watermark-Based ETL")
    
    # Get parameters from environment
    api_key = os.environ['ALPHAVANTAGE_API_KEY']
    s3_bucket = os.environ['S3_BUCKET']
    s3_prefix = os.environ.get('S3_CASH_FLOW_PREFIX', 'cash_flow/')
    
    exchange_filter = os.environ.get('EXCHANGE_FILTER')
    max_symbols = os.environ.get('MAX_SYMBOLS')
    skip_recent_hours = os.environ.get('SKIP_RECENT_HOURS')
    
    if max_symbols:
        max_symbols = int(max_symbols)
    if skip_recent_hours:
        skip_recent_hours = int(skip_recent_hours)
    
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
    logger.info("🚀 STEP 3: Extract cash flow data from Alpha Vantage")
    logger.info("=" * 60)
    
    results = {
        'total_symbols': len(symbols_to_process),
        'successful': 0,
        'failed': 0,
        'start_time': datetime.now().isoformat(),
        'details': [],
        'successful_updates': []  # Track successful updates for bulk watermark update
    }
    
    for i, symbol_info in enumerate(symbols_to_process, 1):
        symbol = symbol_info['symbol']
        
        logger.info(f"📊 [{i}/{len(symbols_to_process)}] Processing {symbol}...")
        
        # Rate limit
        rate_limiter.wait_if_needed()
        
        # Fetch data
        data = fetch_cash_flow_data(symbol, api_key)
        
        if data:
            # Upload to S3
            if upload_to_s3(data, s3_client, s3_bucket, s3_prefix):
                # Track for bulk watermark update (don't update one-by-one)
                results['successful_updates'].append({
                    'symbol': symbol,
                    'first_date': data['first_date'],
                    'last_date': data['last_date']
                })
                results['successful'] += 1
                results['details'].append({
                    'symbol': symbol,
                    'status': 'success',
                    'records': data['record_count']
                })
            else:
                results['failed'] += 1
                results['details'].append({
                    'symbol': symbol,
                    'status': 'failed'
                })
        else:
            results['failed'] += 1
            results['details'].append({
                'symbol': symbol,
                'status': 'failed'
            })
    
    # Save results
    results['end_time'] = datetime.now().isoformat()
    results['duration_minutes'] = (datetime.fromisoformat(results['end_time']) - 
                                  datetime.fromisoformat(results['start_time'])).total_seconds() / 60
    
    # STEP 4: Open NEW Snowflake connection to update watermarks
    logger.info("")
    logger.info("=" * 60)
    logger.info("🔄 STEP 4: Update watermarks for successful extractions")
    logger.info("=" * 60)
    
    watermark_manager = WatermarkETLManager(snowflake_config)
    watermark_manager.connect()
    
    try:
        # Bulk update all watermarks in a single MERGE statement (100x faster!)
        failed_symbols = [d['symbol'] for d in results['details'] if d.get('status') == 'failed']
        watermark_manager.bulk_update_watermarks(results['successful_updates'], failed_symbols)
        
        # Commit all watermark updates at once
        logger.info("💾 Committing watermark updates...")
        watermark_manager.connection.commit()
        logger.info("✅ Watermark updates committed")
        
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
        
    finally:
        watermark_manager.close()
        logger.info("🔌 Snowflake connection closed after watermark updates")
    
    # Save results
    with open('/tmp/watermark_etl_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    logger.info("")
    logger.info("🎉 ETL processing complete!")
    logger.info(f"✅ Successful: {results['successful']}/{results['total_symbols']}")
    logger.info(f"❌ Failed: {results['failed']}/{results['total_symbols']}")
    if delisted_count > 0:
        logger.info(f"🔒 Delisted symbols marked as 'DEL': {delisted_count}")


if __name__ == '__main__':
    main()
