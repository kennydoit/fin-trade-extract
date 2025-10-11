#!/usr/bin/env python3
"""
Test script to verify incremental processing logic.
Shows what symbols would be processed with different skip_recent_hours settings.
"""

import os
import sys
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'github_actions'))

from fetch_time_series_watermark import WatermarkETLManager

def test_incremental_logic():
    """Test incremental processing logic with different settings."""
    
    # Snowflake configuration
    snowflake_config = {
        'account': os.environ['SNOWFLAKE_ACCOUNT'],
        'user': os.environ['SNOWFLAKE_USER'],
        'password': os.environ['SNOWFLAKE_PASSWORD'],
        'database': os.environ['SNOWFLAKE_DATABASE'],
        'schema': os.environ['SNOWFLAKE_SCHEMA'],
        'warehouse': os.environ['SNOWFLAKE_WAREHOUSE']
    }
    
    manager = WatermarkETLManager(snowflake_config)
    
    try:
        print("🧪 TESTING INCREMENTAL PROCESSING LOGIC")
        print("=" * 80)
        print()
        
        # Test 1: No skip (re-process all)
        print("📊 TEST 1: No skip_recent_hours (default behavior)")
        print("-" * 80)
        symbols_no_skip = manager.get_symbols_to_process(
            max_symbols=10,
            staleness_days=5,
            skip_recent_hours=None
        )
        print(f"Symbols to process: {len(symbols_no_skip)}")
        for i, s in enumerate(symbols_no_skip[:5], 1):
            print(f"  {i}. {s['symbol']:10s} - Mode: {s['processing_mode']:8s} - Last Fiscal: {s['last_fiscal_date']}")
        if len(symbols_no_skip) > 5:
            print(f"  ... and {len(symbols_no_skip) - 5} more")
        print()
        
        # Test 2: Skip last 1 hour
        print("📊 TEST 2: skip_recent_hours=1 (skip symbols processed in last hour)")
        print("-" * 80)
        symbols_skip_1h = manager.get_symbols_to_process(
            max_symbols=10,
            staleness_days=5,
            skip_recent_hours=1
        )
        print(f"Symbols to process: {len(symbols_skip_1h)}")
        for i, s in enumerate(symbols_skip_1h[:5], 1):
            print(f"  {i}. {s['symbol']:10s} - Mode: {s['processing_mode']:8s} - Last Fiscal: {s['last_fiscal_date']}")
        if len(symbols_skip_1h) > 5:
            print(f"  ... and {len(symbols_skip_1h) - 5} more")
        print()
        
        # Test 3: Skip last 24 hours
        print("📊 TEST 3: skip_recent_hours=24 (skip symbols processed in last 24 hours)")
        print("-" * 80)
        symbols_skip_24h = manager.get_symbols_to_process(
            max_symbols=10,
            staleness_days=5,
            skip_recent_hours=24
        )
        print(f"Symbols to process: {len(symbols_skip_24h)}")
        for i, s in enumerate(symbols_skip_24h[:5], 1):
            print(f"  {i}. {s['symbol']:10s} - Mode: {s['processing_mode']:8s} - Last Fiscal: {s['last_fiscal_date']}")
        if len(symbols_skip_24h) > 5:
            print(f"  ... and {len(symbols_skip_24h) - 5} more")
        print()
        
        # Summary
        print("=" * 80)
        print("📈 SUMMARY")
        print("=" * 80)
        print(f"No skip:           {len(symbols_no_skip):4d} symbols (re-processes all with smart mode)")
        print(f"Skip 1 hour:       {len(symbols_skip_1h):4d} symbols (skips recent runs)")
        print(f"Skip 24 hours:     {len(symbols_skip_24h):4d} symbols (skips today's runs)")
        print()
        
        # Show actual watermark data for first few symbols
        print("=" * 80)
        print("🔍 WATERMARK DETAILS (First 3 symbols, no skip)")
        print("=" * 80)
        
        manager.connect()
        cursor = manager.connection.cursor()
        cursor.execute("""
            SELECT 
                SYMBOL,
                LAST_FISCAL_DATE,
                LAST_SUCCESSFUL_RUN,
                DATEDIFF(hour, LAST_SUCCESSFUL_RUN, CURRENT_TIMESTAMP()) as HOURS_SINCE_RUN,
                DATEDIFF(day, LAST_FISCAL_DATE, CURRENT_DATE()) as DAYS_SINCE_DATA
            FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
            WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
              AND API_ELIGIBLE = 'YES'
            ORDER BY SYMBOL
            LIMIT 3
        """)
        
        results = cursor.fetchall()
        cursor.close()
        
        for row in results:
            symbol, last_fiscal, last_run, hours_since, days_since = row
            print(f"\nSymbol: {symbol}")
            print(f"  Last Fiscal Date:     {last_fiscal}")
            print(f"  Last Successful Run:  {last_run}")
            print(f"  Hours since run:      {hours_since if hours_since else 'Never'}")
            print(f"  Days since data:      {days_since if days_since else 'Never'}")
            
            # Determine what would happen
            if hours_since is None:
                print(f"  ➡️  Would process:     YES (never processed) - FULL mode")
            elif hours_since < 1:
                print(f"  ➡️  Skip if hours=1:   YES (processed {hours_since:.1f}h ago)")
            elif hours_since < 24:
                print(f"  ➡️  Skip if hours=24:  YES (processed {hours_since:.1f}h ago)")
            else:
                print(f"  ➡️  Would process:     YES (last run {hours_since:.1f}h ago)")
            
            if days_since is not None and days_since < 5:
                print(f"  📊 Processing mode:   COMPACT (data is {days_since} days old)")
            else:
                print(f"  📊 Processing mode:   FULL (data is stale or missing)")
        
        print()
        print("✅ Incremental logic test complete!")
        
    finally:
        manager.close()


if __name__ == '__main__':
    test_incremental_logic()
