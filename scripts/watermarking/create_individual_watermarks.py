#!/usr/bin/env python3
"""
Create Individual Data Source Watermarks
Supports creating watermarks for specific data sources one at a time
"""

import os
import sys
import logging
import argparse
import snowflake.connector
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# SQL templates for each data source
WATERMARK_TEMPLATES = {
    'TIME_SERIES_DAILY_ADJUSTED': {
        'description': 'Time series daily adjusted price data',
        'eligibility': "All symbols (active and delisted)",
        'sql': """
            INSERT INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
                (TABLE_NAME, SYMBOL_ID, SYMBOL, NAME, EXCHANGE, ASSET_TYPE, STATUS, API_ELIGIBLE, 
                 IPO_DATE, DELISTING_DATE, CREATED_AT, UPDATED_AT)
            SELECT 
                'TIME_SERIES_DAILY_ADJUSTED' as TABLE_NAME,
                SYMBOL_ID, SYMBOL, NAME, EXCHANGE, ASSET_TYPE, STATUS,
                'YES' as API_ELIGIBLE,
                IPO_DATE, DELISTING_DATE,
                CURRENT_TIMESTAMP() as CREATED_AT,
                CURRENT_TIMESTAMP() as UPDATED_AT
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = 'LISTING_STATUS'
              AND NOT EXISTS (
                  SELECT 1 FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w2
                  WHERE w2.TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
                    AND w2.SYMBOL_ID = ETL_WATERMARKS.SYMBOL_ID
              );
        """
    },
    'COMPANY_OVERVIEW': {
        'description': 'Company overview and fundamental data',
        'eligibility': "Common stocks only (ASSET_TYPE = 'Stock')",
        'sql': """
            INSERT INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
                (TABLE_NAME, SYMBOL_ID, SYMBOL, NAME, EXCHANGE, ASSET_TYPE, STATUS, API_ELIGIBLE, 
                 IPO_DATE, DELISTING_DATE, CREATED_AT, UPDATED_AT)
            SELECT 
                'COMPANY_OVERVIEW' as TABLE_NAME,
                SYMBOL_ID, SYMBOL, NAME, EXCHANGE, ASSET_TYPE, STATUS,
                CASE WHEN UPPER(ASSET_TYPE) = 'STOCK' THEN 'YES' ELSE 'NO' END as API_ELIGIBLE,
                IPO_DATE, DELISTING_DATE,
                CURRENT_TIMESTAMP() as CREATED_AT,
                CURRENT_TIMESTAMP() as UPDATED_AT
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = 'LISTING_STATUS'
              AND NOT EXISTS (
                  SELECT 1 FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w2
                  WHERE w2.TABLE_NAME = 'COMPANY_OVERVIEW'
                    AND w2.SYMBOL_ID = ETL_WATERMARKS.SYMBOL_ID
              );
        """
    },
    'BALANCE_SHEET': {
        'description': 'Balance sheet financial statements',
        'eligibility': "Active common stocks only",
        'sql': """
            INSERT INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
                (TABLE_NAME, SYMBOL_ID, SYMBOL, NAME, EXCHANGE, ASSET_TYPE, STATUS, API_ELIGIBLE, 
                 IPO_DATE, DELISTING_DATE, CREATED_AT, UPDATED_AT)
            SELECT 
                'BALANCE_SHEET' as TABLE_NAME,
                SYMBOL_ID, SYMBOL, NAME, EXCHANGE, ASSET_TYPE, STATUS,
                CASE WHEN UPPER(ASSET_TYPE) = 'STOCK' AND UPPER(STATUS) = 'ACTIVE' THEN 'YES' ELSE 'NO' END as API_ELIGIBLE,
                IPO_DATE, DELISTING_DATE,
                CURRENT_TIMESTAMP() as CREATED_AT,
                CURRENT_TIMESTAMP() as UPDATED_AT
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = 'LISTING_STATUS'
              AND NOT EXISTS (
                  SELECT 1 FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w2
                  WHERE w2.TABLE_NAME = 'BALANCE_SHEET'
                    AND w2.SYMBOL_ID = ETL_WATERMARKS.SYMBOL_ID
              );
        """
    },
    'CASH_FLOW': {
        'description': 'Cash flow financial statements',
        'eligibility': "Active common stocks only",
        'sql': """
            INSERT INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
                (TABLE_NAME, SYMBOL_ID, SYMBOL, NAME, EXCHANGE, ASSET_TYPE, STATUS, API_ELIGIBLE, 
                 IPO_DATE, DELISTING_DATE, CREATED_AT, UPDATED_AT)
            SELECT 
                'CASH_FLOW' as TABLE_NAME,
                SYMBOL_ID, SYMBOL, NAME, EXCHANGE, ASSET_TYPE, STATUS,
                CASE WHEN UPPER(ASSET_TYPE) = 'STOCK' AND UPPER(STATUS) = 'ACTIVE' THEN 'YES' ELSE 'NO' END as API_ELIGIBLE,
                IPO_DATE, DELISTING_DATE,
                CURRENT_TIMESTAMP() as CREATED_AT,
                CURRENT_TIMESTAMP() as UPDATED_AT
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = 'LISTING_STATUS'
              AND NOT EXISTS (
                  SELECT 1 FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w2
                  WHERE w2.TABLE_NAME = 'CASH_FLOW'
                    AND w2.SYMBOL_ID = ETL_WATERMARKS.SYMBOL_ID
              );
        """
    },
    # Add more as needed...
}

def get_snowflake_config():
    """Get Snowflake configuration from environment variables."""
    required_vars = [
        'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_DATABASE', 'SNOWFLAKE_SCHEMA', 'SNOWFLAKE_WAREHOUSE'
    ]
    
    config = {}
    missing_vars = []
    
    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            config[var.lower().replace('snowflake_', '')] = value
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")
        
    return config

def create_data_source_watermarks(connection, data_source):
    """Create watermarks for a specific data source."""
    if data_source not in WATERMARK_TEMPLATES:
        raise ValueError(f"Unknown data source: {data_source}")
    
    template = WATERMARK_TEMPLATES[data_source]
    
    logger.info(f"🎯 Creating watermarks for: {data_source}")
    logger.info(f"📝 Description: {template['description']}")
    logger.info(f"✅ Eligibility: {template['eligibility']}")
    
    cursor = connection.cursor()
    
    try:
        # Execute the INSERT statement
        cursor.execute(template['sql'])
        rows_inserted = cursor.rowcount
        logger.info(f"✅ Inserted {rows_inserted} watermark records")
        
        # Get summary
        summary_sql = f"""
            SELECT 
                COUNT(*) as TOTAL,
                COUNT(CASE WHEN API_ELIGIBLE = 'YES' THEN 1 END) as ELIGIBLE,
                COUNT(CASE WHEN API_ELIGIBLE = 'NO' THEN 1 END) as NOT_ELIGIBLE
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = '{data_source}'
        """
        cursor.execute(summary_sql)
        result = cursor.fetchone()
        
        logger.info(f"📊 Summary:")
        logger.info(f"   Total: {result[0]}")
        logger.info(f"   API Eligible: {result[1]}")
        logger.info(f"   Not Eligible: {result[2]}")
        
        cursor.close()
        
    except Exception as e:
        logger.error(f"❌ Failed to create watermarks: {e}")
        raise

def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Create data source watermarks')
    parser.add_argument('--data-source', required=True, 
                       choices=list(WATERMARK_TEMPLATES.keys()) + ['ALL_SOURCES'],
                       help='Data source to create watermarks for')
    
    args = parser.parse_args()
    
    logger.info("🚀 Starting Data Source Watermarks Creation")
    logger.info(f"🎯 Target: {args.data_source}")
    
    try:
        # Get configuration
        config = get_snowflake_config()
        
        # Connect to Snowflake
        logger.info("🔌 Connecting to Snowflake...")
        connection = snowflake.connector.connect(**config)
        logger.info("✅ Connected to Snowflake successfully")
        
        try:
            if args.data_source == 'ALL_SOURCES':
                # Create all data sources
                for data_source in WATERMARK_TEMPLATES.keys():
                    logger.info(f"\n{'='*60}")
                    create_data_source_watermarks(connection, data_source)
            else:
                # Create specific data source
                create_data_source_watermarks(connection, args.data_source)
            
        finally:
            connection.close()
            logger.info("🔒 Snowflake connection closed")
        
        logger.info("🎉 Watermarks creation completed successfully!")
        return 0
        
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
