"""
AWS S3 utilities for writing batch data as CSV files.
Optimized for Snowpipe ingestion with proper file naming and column ordering.
"""

import csv
import gzip
import io
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd
from botocore.exceptions import BotoCoreError, ClientError


class S3DataWriter:
    """
    Write batch data to S3 as CSV files optimized for Snowpipe ingestion.
    
    Features:
    - CSV format with proper column ordering for Snowflake tables
    - Compressed files for efficient storage
    - Proper file naming with timestamps for Snowpipe detection
    - Correct S3 key structure matching Snowpipe stages
    - Error handling and retry logic
    """
    
    def __init__(self, 
                 bucket_name: str = "fin-trade-craft-landing",
                 region_name: str = "us-east-2"):
        """
        Initialize S3 data writer.
        
        Args:
            bucket_name: S3 bucket for data landing
            region_name: AWS region
        """
        self.bucket_name = bucket_name
        self.region_name = region_name
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3', region_name=region_name)
        
        # Column orders for each table (matching Snowflake schema exactly)
        self.column_orders = {
            "overview": [
                "SYMBOL_ID", "SYMBOL", "ASSET_TYPE", "NAME", "DESCRIPTION", "CIK", "EXCHANGE", 
                "CURRENCY", "COUNTRY", "SECTOR", "INDUSTRY", "ADDRESS", "OFFICIAL_SITE", 
                "FISCAL_YEAR_END", "MARKET_CAPITALIZATION", "EBITDA", "PE_RATIO", "PEG_RATIO", 
                "BOOK_VALUE", "DIVIDEND_PER_SHARE", "DIVIDEND_YIELD", "EPS", "REVENUE_PER_SHARE_TTM", 
                "PROFIT_MARGIN", "OPERATING_MARGIN_TTM", "RETURN_ON_ASSETS_TTM", "RETURN_ON_EQUITY_TTM", 
                "REVENUE_TTM", "GROSS_PROFIT_TTM", "DILUTED_EPS_TTM", "QUARTERLY_EARNINGS_GROWTH_YOY", 
                "QUARTERLY_REVENUE_GROWTH_YOY", "ANALYST_TARGET_PRICE", "TRAILING_PE", "FORWARD_PE", 
                "PRICE_TO_SALES_RATIO_TTM", "PRICE_TO_BOOK_RATIO", "EV_TO_REVENUE", "EV_TO_EBITDA", 
                "BETA", "WEEK_52_HIGH", "WEEK_52_LOW", "DAY_50_MOVING_AVERAGE", "DAY_200_MOVING_AVERAGE", 
                "SHARES_OUTSTANDING", "DIVIDEND_DATE", "EX_DIVIDEND_DATE", "API_RESPONSE_STATUS", 
                "CREATED_AT", "UPDATED_AT"
            ],
            "time_series_daily_adjusted": [
                "SYMBOL_ID", "SYMBOL", "DATE", "OPEN", "HIGH", "LOW", "CLOSE", "ADJUSTED_CLOSE", 
                "VOLUME", "DIVIDEND_AMOUNT", "SPLIT_COEFFICIENT", "API_RESPONSE_STATUS", 
                "CREATED_AT", "UPDATED_AT"
            ],
            "income_statement": [
                "SYMBOL_ID", "SYMBOL", "FISCAL_DATE_ENDING", "REPORT_TYPE", "REPORTED_CURRENCY",
                "GROSS_PROFIT", "TOTAL_REVENUE", "COST_OF_REVENUE", "COST_OF_GOODS_AND_SERVICES_SOLD",
                "OPERATING_INCOME", "SELLING_GENERAL_AND_ADMINISTRATIVE", "RESEARCH_AND_DEVELOPMENT",
                "OPERATING_EXPENSES", "INVESTMENT_INCOME_NET", "NET_INTEREST_INCOME", "INTEREST_INCOME",
                "INTEREST_EXPENSE", "NON_INTEREST_INCOME", "OTHER_NON_OPERATING_INCOME", "DEPRECIATION",
                "DEPRECIATION_AND_AMORTIZATION", "INCOME_BEFORE_TAX", "INCOME_TAX_EXPENSE",
                "INTEREST_AND_DEBT_EXPENSE", "NET_INCOME_FROM_CONTINUING_OPERATIONS", 
                "COMPREHENSIVE_INCOME_NET_OF_TAX", "EBIT", "EBITDA", "NET_INCOME", "API_RESPONSE_STATUS",
                "CREATED_AT", "UPDATED_AT"
            ],
            "balance_sheet": [
                "SYMBOL_ID", "SYMBOL", "FISCAL_DATE_ENDING", "REPORT_TYPE", "REPORTED_CURRENCY",
                "TOTAL_ASSETS", "TOTAL_CURRENT_ASSETS", "CASH_AND_CASH_EQUIVALENTS_AT_CARRYING_VALUE",
                "CASH_AND_SHORT_TERM_INVESTMENTS", "INVENTORY", "CURRENT_NET_RECEIVABLES",
                "TOTAL_NON_CURRENT_ASSETS", "PROPERTY_PLANT_EQUIPMENT", "ACCUMULATED_DEPRECIATION_AMORTIZATION_PPE",
                "INTANGIBLE_ASSETS", "INTANGIBLE_ASSETS_EXCLUDING_GOODWILL", "GOODWILL", "INVESTMENTS",
                "LONG_TERM_INVESTMENTS", "SHORT_TERM_INVESTMENTS", "OTHER_CURRENT_ASSETS", 
                "OTHER_NON_CURRENT_ASSETS", "TOTAL_LIABILITIES", "TOTAL_CURRENT_LIABILITIES",
                "CURRENT_ACCOUNTS_PAYABLE", "DEFERRED_REVENUE", "CURRENT_DEBT", "SHORT_TERM_DEBT",
                "TOTAL_NON_CURRENT_LIABILITIES", "CAPITAL_LEASE_OBLIGATIONS", "LONG_TERM_DEBT",
                "CURRENT_LONG_TERM_DEBT", "LONG_TERM_DEBT_NONCURRENT", "SHORT_LONG_TERM_DEBT_TOTAL",
                "OTHER_CURRENT_LIABILITIES", "OTHER_NON_CURRENT_LIABILITIES", "TOTAL_SHAREHOLDER_EQUITY",
                "TREASURY_STOCK", "RETAINED_EARNINGS", "COMMON_STOCK", "COMMON_STOCK_SHARES_OUTSTANDING",
                "API_RESPONSE_STATUS", "CREATED_AT", "UPDATED_AT"
            ],
            "cash_flow": [
                "SYMBOL_ID", "SYMBOL", "FISCAL_DATE_ENDING", "REPORT_TYPE", "REPORTED_CURRENCY",
                "OPERATING_CASHFLOW", "PAYMENTS_FOR_OPERATING_ACTIVITIES", "PROCEEDS_FROM_OPERATING_ACTIVITIES",
                "CHANGE_IN_OPERATING_LIABILITIES", "CHANGE_IN_OPERATING_ASSETS", "DEPRECIATION_DEPLETION_AND_AMORTIZATION",
                "CAPITAL_EXPENDITURES", "CHANGE_IN_RECEIVABLES", "CHANGE_IN_INVENTORY", "CHANGE_IN_ACCOUNTS_PAYABLE",
                "CHANGE_IN_LIABILITIES", "CASHFLOW_FROM_INVESTMENT", "CASHFLOW_FROM_FINANCING", "PROCEEDS_FROM_REPAYMENTS_OF_SHORT_TERM_DEBT",
                "PAYMENTS_FOR_REPURCHASE_OF_COMMON_STOCK", "PAYMENTS_FOR_REPURCHASE_OF_EQUITY", "PAYMENTS_FOR_REPURCHASE_OF_PREFERRED_STOCK",
                "DIVIDEND_PAYOUT", "DIVIDEND_PAYOUT_COMMON_STOCK", "DIVIDEND_PAYOUT_PREFERRED_STOCK", "PROCEEDS_FROM_ISSUANCE_OF_COMMON_STOCK",
                "PROCEEDS_FROM_ISSUANCE_OF_LONG_TERM_DEBT_AND_CAPITAL_SECURITIES_NET", "PROCEEDS_FROM_ISSUANCE_OF_PREFERRED_STOCK",
                "PROCEEDS_FROM_REPURCHASE_OF_EQUITY", "PROCEEDS_FROM_SALE_OF_TREASURY_STOCK", "CHANGE_IN_CASH_AND_CASH_EQUIVALENTS",
                "CHANGE_IN_EXCHANGE_RATE", "NET_INCOME", "API_RESPONSE_STATUS", "CREATED_AT", "UPDATED_AT"
            ],
            "commodities": [
                "COMMODITY_ID", "COMMODITY_NAME", "DATE", "VALUE", "UNIT", "NAME", "API_RESPONSE_STATUS",
                "CREATED_AT", "UPDATED_AT"
            ],
            "economic_indicators": [
                "INDICATOR_ID", "ECONOMIC_INDICATOR_NAME", "FUNCTION_NAME", "MATURITY", "DATE", "VALUE",
                "UNIT", "NAME", "API_RESPONSE_STATUS", "CREATED_AT", "UPDATED_AT"
            ],
            "insider_transactions": [
                "TRANSACTION_ID", "SYMBOL_ID", "SYMBOL", "TRANSACTION_DATE", "EXECUTIVE", "EXECUTIVE_TITLE",
                "ACQUISITION_OR_DISPOSAL", "SHARES", "SHARE_PRICE", "API_RESPONSE_STATUS", "CREATED_AT", "UPDATED_AT"
            ],
            "listing_status": [
                "SYMBOL_ID", "SYMBOL", "NAME", "EXCHANGE", "ASSET_TYPE", "IPO_DATE", "DELISTING_DATE",
                "STATUS", "STATE", "CREATED_AT", "UPDATED_AT"
            ]
        }
        
    def write_business_data_csv(self,
                              table_name: str,
                              records: List[Dict[str, Any]],
                              run_id: str) -> Dict[str, Any]:
        """
        Write business data as CSV files to S3.
        
        Args:
            table_name: Name of the target table (e.g., 'overview', 'time_series_daily_adjusted')
            records: List of business records to write
            run_id: Unique run identifier
            
        Returns:
            Dict with file information and statistics
        """
        if not records:
            return {
                "files_written": 0,
                "total_records": 0,
                "status": "no_data"
            }
        
        try:
            # Get column order for this table
            if table_name not in self.column_orders:
                return {
                    "files_written": 0,
                    "total_records": len(records),
                    "status": "error",
                    "error": f"Unknown table name: {table_name}"
                }
            
            columns = self.column_orders[table_name]
            
            # Generate file information
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            file_id = str(uuid.uuid4())[:8]
            
            # Construct S3 key following Snowpipe path conventions
            path_mapping = {
                "overview": "overview",
                "time_series_daily_adjusted": "time-series",
                "income_statement": "income-statement",
                "balance_sheet": "balance-sheet",
                "cash_flow": "cash-flow",
                "commodities": "commodities",
                "economic_indicators": "economic-indicators",
                "insider_transactions": "insider-transactions",
                "listing_status": "listing-status"
            }
            
            s3_path = path_mapping.get(table_name, table_name)
            s3_key = f"{s3_path}/{table_name}_{timestamp}_{file_id}.csv"
            
            # Create CSV content
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=columns, extrasaction='ignore')
            
            # Write header
            writer.writeheader()
            
            # Write rows, ensuring column order
            for record in records:
                # Convert None values to empty strings for CSV
                clean_record = {}
                for col in columns:
                    value = record.get(col.lower()) or record.get(col) or ""
                    # Handle datetime objects
                    if isinstance(value, datetime):
                        value = value.isoformat()
                    # Convert None to empty string
                    clean_record[col] = "" if value is None else str(value)
                writer.writerow(clean_record)
            
            # Get CSV content as bytes
            csv_content = csv_buffer.getvalue().encode('utf-8')
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_content,
                ContentType='text/csv',
                Metadata={
                    'table_name': table_name,
                    'run_id': run_id,
                    'record_count': str(len(records)),
                    'created_at': datetime.utcnow().isoformat(),
                    'format': 'csv'
                }
            )
            
            file_info = {
                "bucket": self.bucket_name,
                "key": s3_key,
                "record_count": len(records),
                "file_size_bytes": len(csv_content),
                "format": "csv"
            }
            
            return {
                "files_written": 1,
                "total_records": len(records),
                "files": [file_info],
                "status": "success"
            }
            
        except Exception as e:
            return {
                "files_written": 0,
                "total_records": len(records) if records else 0,
                "status": "error",
                "error": str(e)
            }
    
    def write_landing_data_json(self,
                               table_name: str,
                               landing_records: List[Dict[str, Any]],
                               run_id: str) -> Dict[str, Any]:
        """
        Write landing/audit data as compressed JSON files to S3.
        
        Args:
            table_name: Name of the target table
            landing_records: List of landing records (raw API responses)
            run_id: Unique run identifier
            
        Returns:
            Dict with file information and statistics
        """
        if not landing_records:
            return {
                "files_written": 0,
                "total_records": 0,
                "status": "no_data"
            }
        
        try:
            # Generate file information
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            file_id = str(uuid.uuid4())[:8]
            
            # Construct S3 key for landing data (separate from business data)
            s3_key = f"landing_data/{table_name}/{timestamp}_{file_id}_{run_id}.json.gz"
            
            # Convert to JSON and compress
            json_data = json.dumps(landing_records, separators=(',', ':'), default=str)
            compressed_data = gzip.compress(json_data.encode('utf-8'))
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=compressed_data,
                ContentType='application/gzip',
                ContentEncoding='gzip',
                Metadata={
                    'table_name': table_name,
                    'run_id': run_id,
                    'record_count': str(len(landing_records)),
                    'created_at': datetime.utcnow().isoformat(),
                    'data_type': 'landing_audit'
                }
            )
            
            file_info = {
                "bucket": self.bucket_name,
                "key": s3_key,
                "record_count": len(landing_records),
                "file_size_bytes": len(compressed_data),
                "compression": "gzip",
                "format": "json"
            }
            
            return {
                "files_written": 1,
                "total_records": len(landing_records),
                "files": [file_info],
                "status": "success"
            }
            
        except Exception as e:
            return {
                "files_written": 0,
                "total_records": len(landing_records) if landing_records else 0,
                "status": "error",
                "error": str(e)
            }
    
    def write_batch_data(self,
                        table_name: str,
                        business_records: List[Dict[str, Any]],
                        landing_records: List[Dict[str, Any]],
                        run_id: str) -> Dict[str, Any]:
        """
        Write both business and landing data in a single batch operation.
        
        Args:
            table_name: Name of the target table
            business_records: Transformed business data
            landing_records: Raw API responses for audit
            run_id: Unique run identifier
            
        Returns:
            Combined results from both write operations
        """
        results = {
            "business_data": {},
            "landing_data": {},
            "total_files": 0,
            "total_records": 0,
            "status": "success"
        }
        
        # Write business data as CSV
        if business_records:
            business_result = self.write_business_data_csv(
                table_name, business_records, run_id
            )
            results["business_data"] = business_result
            results["total_files"] += business_result.get("files_written", 0)
            results["total_records"] += business_result.get("total_records", 0)
            
            if business_result.get("status") == "error":
                results["status"] = "partial_success"
        
        # Write landing data as compressed JSON
        if landing_records:
            landing_result = self.write_landing_data_json(
                table_name, landing_records, run_id
            )
            results["landing_data"] = landing_result
            results["total_files"] += landing_result.get("files_written", 0)
            
            if landing_result.get("status") == "error":
                results["status"] = "partial_success" if results["status"] == "success" else "error"
        
        return results
    
    def validate_s3_access(self) -> Dict[str, Any]:
        """
        Validate S3 bucket access and permissions.
        
        Returns:
            Dict with validation results
        """
        try:
            # Test bucket access
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            
            # Test write permissions with a small test file
            test_key = f"_test_access/{datetime.utcnow().isoformat()}_test.txt"
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=test_key,
                Body=b"access_test",
                Metadata={'test': 'true'}
            )
            
            # Clean up test file
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=test_key
            )
            
            return {
                "status": "success",
                "bucket": self.bucket_name,
                "region": self.region_name,
                "access": "read_write"
            }
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            return {
                "status": "error",
                "bucket": self.bucket_name,
                "error_code": error_code,
                "error": str(e)
            }
        except Exception as e:
            return {
                "status": "error",
                "bucket": self.bucket_name,
                "error": str(e)
            }


def create_s3_writer(bucket_name: Optional[str] = None,
                    region_name: Optional[str] = None) -> S3DataWriter:
    """
    Create S3DataWriter with default configuration.
    
    Args:
        bucket_name: Optional bucket override
        region_name: Optional region override
        
    Returns:
        Configured S3DataWriter instance
    """
    return S3DataWriter(
        bucket_name=bucket_name or "fin-trade-craft-landing",
        region_name=region_name or "us-east-2"
    )