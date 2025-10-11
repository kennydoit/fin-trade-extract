#!/usr/bin/env python3
"""
Systematic Watermarking Table Management

This script creates and manages ETL watermarking tables for systematic data processing.
It can be run manually before adding new data types to establish proper tracking.

Features:
- Creates ETL_WATERMARKS table with proper schema
- Initializes watermarks for specific data types
- Validates table structure and constraints
- Supports multiple data types with different refresh frequencies

Usage:
    python scripts/watermarking/create_watermarks.py --table-name TIME_SERIES_DAILY_ADJUSTED
    python scripts/watermarking/create_watermarks.py --table-name COMPANY_OVERVIEW
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import snowflake.connector

# Add parent directories to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


class WatermarkManager:
    """Manages ETL watermarking tables and initialization."""
    
    # Define data types and their configurations
    DATA_TYPE_CONFIGS = {
        'TIME_SERIES_DAILY_ADJUSTED': {
            'name': 'time_series_daily_adjusted',
            'table_name': 'TIME_SERIES_DAILY_ADJUSTED',
            'refresh_frequency_days': 1,  # Daily refresh
            'priority': 1,  # High priority
            'description': 'Daily adjusted stock prices and volume data'
        },
        'COMPANY_OVERVIEW': {
            'name': 'company_overview',
            'table_name': 'COMPANY_OVERVIEW', 
            'refresh_frequency_days': 30,  # Monthly refresh
            'priority': 3,  # Medium priority
            'description': 'Company fundamental information and business details'
        },
        'LISTING_STATUS': {
            'name': 'listing_status',
            'table_name': 'LISTING_STATUS',
            'refresh_frequency_days': 7,  # Weekly refresh
            'priority': 2,  # High priority
            'description': 'Stock listing status and exchange information'
        },
        'BALANCE_SHEET': {
            'name': 'balance_sheet',
            'table_name': 'BALANCE_SHEET',
            'refresh_frequency_days': 90,  # Quarterly refresh
            'priority': 4,  # Lower priority
            'description': 'Company balance sheet fundamental data'
        }
    }
    
    def __init__(self):
        """Initialize watermark manager with Snowflake connection."""
        self.snowflake_config = self._get_snowflake_config()
        self.connection = None
        
    def _get_snowflake_config(self) -> Dict[str, str]:
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
    
    def connect(self):
        """Establish Snowflake connection."""
        try:
            self.connection = snowflake.connector.connect(**self.snowflake_config)
            logger.info("✅ Connected to Snowflake successfully")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Snowflake: {e}")
            raise
    
    def disconnect(self):
        """Close Snowflake connection."""
        if self.connection:
            self.connection.close()
            logger.info("🔒 Snowflake connection closed")
    
    def create_watermarks_table(self):
        """Create the ETL_WATERMARKS table if it doesn't exist."""
        logger.info("🔧 Creating ETL_WATERMARKS table...")
        
        # Force recreation of table to ensure correct schema
        drop_table_sql = "DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;"
        
        create_table_sql = """
        CREATE TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS (
            WATERMARK_ID NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
            SYMBOL VARCHAR(20) NOT NULL,
            DATA_TYPE VARCHAR(50) NOT NULL,
            PROCESSING_STATUS VARCHAR(20) NOT NULL,
            LAST_PROCESSED_DATE DATE,
            LAST_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            ERROR_MESSAGE VARCHAR(2000),
            RETRY_COUNT NUMBER(10,0) DEFAULT 0,
            CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            
            -- Create unique constraint on symbol + data_type combination
            CONSTRAINT UK_ETL_WATERMARKS_SYMBOL_DATA_TYPE UNIQUE (SYMBOL, DATA_TYPE)
        );
        """
        
        try:
            cursor = self.connection.cursor()
            # First drop existing table if it has wrong schema
            cursor.execute(drop_table_sql)
            # Then create the correct table
            cursor.execute(create_table_sql)
            logger.info("✅ ETL_WATERMARKS table created successfully")
            cursor.close()
        except Exception as e:
            logger.error(f"❌ Failed to create ETL_WATERMARKS table: {e}")
            raise
    
    def validate_table_structure(self):
        """Validate that the ETL_WATERMARKS table has the expected structure."""
        logger.info("🔍 Validating ETL_WATERMARKS table structure...")
        
        check_structure_sql = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = 'RAW' 
          AND TABLE_NAME = 'ETL_WATERMARKS'
        ORDER BY ORDINAL_POSITION;
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(check_structure_sql)
            results = cursor.fetchall()
            
            if not results:
                raise Exception("ETL_WATERMARKS table not found")
            
            logger.info("📋 ETL_WATERMARKS table structure:")
            for row in results:
                logger.info(f"  - {row[0]} ({row[1]}) {'NULL' if row[2] == 'YES' else 'NOT NULL'}")
            
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to validate table structure: {e}")
            raise
    
    def initialize_data_type_watermarks(self, table_name: str):
        """Initialize watermarks for a specific data type."""
        if table_name not in self.DATA_TYPE_CONFIGS:
            raise ValueError(f"Unknown table name: {table_name}. Supported: {list(self.DATA_TYPE_CONFIGS.keys())}")
        
        config = self.DATA_TYPE_CONFIGS[table_name]
        logger.info(f"🎯 Initializing watermarks for {config['description']}")
        logger.info(f"📊 Data type: {config['name']}")
        logger.info(f"🔄 Refresh frequency: {config['refresh_frequency_days']} days")
        logger.info(f"⭐ Priority: {config['priority']}")
        
        # Get active symbols from LISTING_STATUS table (if it exists)
        symbols = self._get_active_symbols()
        
        if not symbols:
            logger.warning("⚠️ No symbols found - watermarks will be empty initially")
            logger.info("💡 Watermarks will be populated as symbols are processed")
            return
        
        # Initialize watermarks for all active symbols
        self._insert_initial_watermarks(config['name'], symbols)
        logger.info(f"✅ Initialized {len(symbols)} watermarks for {config['name']}")
    
    def _get_active_symbols(self) -> List[str]:
        """Get list of active symbols from LISTING_STATUS table."""
        try:
            cursor = self.connection.cursor()
            
            # Check if LISTING_STATUS table exists
            cursor.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_SCHEMA = 'RAW' AND TABLE_NAME = 'LISTING_STATUS'
            """)
            
            if cursor.fetchone()[0] == 0:
                logger.info("📝 LISTING_STATUS table not found - skipping symbol initialization")
                cursor.close()
                return []
            
            # Get active symbols
            cursor.execute("""
                SELECT DISTINCT SYMBOL 
                FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS
                WHERE STATUS = 'Active' 
                  AND ASSETTYPE = 'Stock'
                  AND SYMBOL IS NOT NULL
                  AND SYMBOL != ''
                ORDER BY SYMBOL
                LIMIT 1000
            """)
            
            symbols = [row[0] for row in cursor.fetchall()]
            logger.info(f"📊 Found {len(symbols)} active symbols")
            cursor.close()
            return symbols
            
        except Exception as e:
            logger.warning(f"⚠️ Could not get active symbols: {e}")
            return []
    
    def _insert_initial_watermarks(self, data_type: str, symbols: List[str]):
        """Insert initial watermark records for symbols."""
        logger.info(f"💾 Inserting initial watermarks for {len(symbols)} symbols...")
        
        # Use MERGE to avoid duplicates
        merge_sql = """
        MERGE INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS AS target
        USING (SELECT ? as symbol, ? as data_type) AS source
        ON target.SYMBOL = source.symbol AND target.DATA_TYPE = source.data_type
        WHEN NOT MATCHED THEN
            INSERT (SYMBOL, DATA_TYPE, PROCESSING_STATUS, LAST_UPDATED, CREATED_AT)
            VALUES (source.symbol, source.data_type, 'pending', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """
        
        try:
            cursor = self.connection.cursor()
            
            # Insert in batches of 100 to avoid timeout
            batch_size = 100
            for i in range(0, len(symbols), batch_size):
                batch = symbols[i:i + batch_size]
                
                for symbol in batch:
                    cursor.execute(merge_sql, (symbol, data_type))
                
                self.connection.commit()
                logger.info(f"💾 Inserted batch {i//batch_size + 1}/{(len(symbols) + batch_size - 1)//batch_size}")
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"❌ Failed to insert initial watermarks: {e}")
            raise
    
    def show_watermark_summary(self, table_name: str = None):
        """Show summary of watermarks for a specific data type or all."""
        logger.info("📊 Watermark Summary:")
        
        if table_name and table_name in self.DATA_TYPE_CONFIGS:
            data_type = self.DATA_TYPE_CONFIGS[table_name]['name']
            where_clause = f"WHERE DATA_TYPE = '{data_type}'"
        else:
            where_clause = ""
        
        summary_sql = f"""
        SELECT 
            DATA_TYPE,
            PROCESSING_STATUS,
            COUNT(*) as COUNT,
            MIN(LAST_UPDATED) as EARLIEST_UPDATE,
            MAX(LAST_UPDATED) as LATEST_UPDATE
        FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
        {where_clause}
        GROUP BY DATA_TYPE, PROCESSING_STATUS
        ORDER BY DATA_TYPE, PROCESSING_STATUS
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(summary_sql)
            results = cursor.fetchall()
            
            if not results:
                logger.info("📝 No watermarks found")
            else:
                logger.info("📋 Watermark Status:")
                for row in results:
                    logger.info(f"  {row[0]} | {row[1]} | {row[2]} records | {row[3]} to {row[4]}")
            
            cursor.close()
            
        except Exception as e:
            logger.error(f"❌ Failed to get watermark summary: {e}")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Systematic ETL Watermarking Management')
    parser.add_argument('--table-name', 
                       required=True,
                       choices=['TIME_SERIES_DAILY_ADJUSTED', 'COMPANY_OVERVIEW', 'LISTING_STATUS', 'BALANCE_SHEET'],
                       help='Table name to initialize watermarks for')
    parser.add_argument('--create-table-only', 
                       action='store_true',
                       help='Only create the watermarks table, do not initialize data')
    
    args = parser.parse_args()
    
    logger.info("🚀 Starting Systematic Watermarking Management")
    logger.info(f"🎯 Target table: {args.table_name}")
    
    try:
        # Initialize watermark manager
        manager = WatermarkManager()
        manager.connect()
        
        # Create watermarks table
        manager.create_watermarks_table()
        manager.validate_table_structure()
        
        if not args.create_table_only:
            # Initialize watermarks for the specified data type
            manager.initialize_data_type_watermarks(args.table_name)
        
        # Show summary
        manager.show_watermark_summary(args.table_name)
        
        manager.disconnect()
        logger.info("✅ Watermarking management completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Watermarking management failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()