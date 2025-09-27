"""
Simple S3 CSV writer for Lambda deployment testing.
This is a simplified version that avoids pandas dependency issues.
"""
import boto3
import csv
import io
import json
from typing import Dict, List, Any, Optional


def create_s3_writer():
    """Create and return an S3 writer instance."""
    return SimpleS3Writer()


class SimpleS3Writer:
    """Simplified S3 writer for Lambda testing."""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.bucket_name = "fin-trade-craft-landing"
        
    def write_business_data_csv(self, business_data: List[Dict[str, Any]], 
                               s3_key_prefix: str,
                               run_id: str) -> Dict[str, Any]:
        """
        Write business data as CSV to S3.
        
        Args:
            business_data: List of dictionaries containing business data
            s3_key_prefix: S3 key prefix (e.g., 'overview')
            run_id: Unique run identifier
            
        Returns:
            Dictionary with processing results
        """
        
        if not business_data:
            return {
                "success": True,
                "records_processed": 0,
                "csv_files": [],
                "json_files": []
            }
            
        # Column mappings for different data types
        column_orders = {
            'overview': [
                'SYMBOL_ID', 'SYMBOL', 'ASSET_TYPE', 'NAME', 'DESCRIPTION',
                'CIK', 'EXCHANGE', 'CURRENCY', 'COUNTRY', 'SECTOR', 'INDUSTRY',
                'ADDRESS', 'FISCAL_YEAR_END', 'LATEST_QUARTER', 'MARKET_CAPITALIZATION',
                'EBITDA', 'PE_RATIO', 'PEG_RATIO', 'BOOK_VALUE', 'DIVIDEND_PER_SHARE',
                'DIVIDEND_YIELD', 'EPS', 'REVENUE_PER_SHARE_TTM', 'PROFIT_MARGIN',
                'OPERATING_MARGIN_TTM', 'RETURN_ON_ASSETS_TTM', 'RETURN_ON_EQUITY_TTM',
                'REVENUE_TTM', 'GROSS_PROFIT_TTM', 'DILUTED_EPS_TTM', 'QUARTERLY_EARNINGS_GROWTH_YOY',
                'QUARTERLY_REVENUE_GROWTH_YOY', 'ANALYST_TARGET_PRICE', 'TRAILING_PE',
                'FORWARD_PE', 'PRICE_TO_SALES_RATIO_TTM', 'PRICE_TO_BOOK_RATIO',
                'EV_TO_REVENUE', 'EV_TO_EBITDA', 'BETA', 'WEEK_52_HIGH', 'WEEK_52_LOW',
                'MOVING_AVERAGE_50_DAY', 'MOVING_AVERAGE_200_DAY', 'SHARES_OUTSTANDING',
                'DIVIDEND_DATE', 'EX_DIVIDEND_DATE'
            ]
        }
        
        # Get column order for this data type
        columns = column_orders.get(s3_key_prefix, list(business_data[0].keys()))
        
        # Create CSV content
        csv_buffer = io.StringIO()
        writer = csv.DictWriter(csv_buffer, fieldnames=columns)
        writer.writeheader()
        for record in business_data:
            # Filter to only include columns that exist in the data
            filtered_record = {k: record.get(k, '') for k in columns}
            writer.writerow(filtered_record)
        
        # Generate S3 key
        timestamp = "2025-09-26T18:35:00Z"  # Simple timestamp for testing
        s3_key = f"{s3_key_prefix}/{run_id}_{timestamp}.csv"
        
        # Upload to S3
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=csv_buffer.getvalue(),
                ContentType='text/csv'
            )
            
            return {
                "success": True,
                "records_processed": len(business_data),
                "csv_files": [s3_key],
                "json_files": []
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "records_processed": 0,
                "csv_files": [],
                "json_files": []
            }
    
    def write_batch_data(self, table_name: str, business_records: List[Dict[str, Any]], 
                        landing_records: List[Dict[str, Any]], run_id: str) -> Dict[str, Any]:
        """
        Write batch data to S3 (wrapper for compatibility).
        
        Args:
            table_name: Name of the table/data type
            business_records: List of business data records
            landing_records: List of landing data records  
            run_id: Unique run identifier
            
        Returns:
            Dictionary with processing results
        """
        return self.write_business_data_csv(business_records, table_name, run_id)