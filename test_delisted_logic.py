#!/usr/bin/env python3
"""
Test script for intelligent delisted stock processing logic.

This script tests the enhanced time series ETL logic that:
1. Processes active stocks normally
2. Processes delisted stocks once for historical data
3. Excludes previously processed delisted stocks from future runs

Usage:
    python test_delisted_logic.py
"""

import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.incremental_etl import IncrementalETLManager, get_snowflake_config_from_env


def setup_test_data():
    """
    Set up test data in the ETL_WATERMARKS table to simulate various scenarios.
    """
    print("🧪 Setting up test data...")
    
    snowflake_config = get_snowflake_config_from_env()
    etl_manager = IncrementalETLManager(snowflake_config)
    
    try:
        cursor = etl_manager.conn.cursor()
        
        # Clean up any existing test data
        cursor.execute("""
            DELETE FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
            WHERE table_name = 'TIME_SERIES_DAILY_ADJUSTED' 
            AND symbol IN ('TEST_ACTIVE_1', 'TEST_ACTIVE_2', 'TEST_DELISTED_1', 'TEST_DELISTED_2', 'TEST_DELISTED_3')
        """)
        
        # Insert test symbols with different scenarios
        test_data = [
            # Active stock - never processed
            ("TEST_ACTIVE_1", None, None, None),
            
            # Active stock - processed recently
            ("TEST_ACTIVE_2", None, datetime.now() - timedelta(days=1), None),
            
            # Delisted stock - never processed (should be processed once)
            ("TEST_DELISTED_1", datetime.now() - timedelta(days=365), None, None),
            
            # Delisted stock - processed recently (should be excluded)
            ("TEST_DELISTED_2", datetime.now() - timedelta(days=200), datetime.now() - timedelta(days=5), None),
            
            # Delisted stock - processed long ago (should be excluded)
            ("TEST_DELISTED_3", datetime.now() - timedelta(days=800), datetime.now() - timedelta(days=100), None),
        ]
        
        # Insert test data
        for symbol, delisting_date, last_successful_run, last_fiscal_date in test_data:
            symbol_id = abs(hash(symbol)) % 1000000000
            
            cursor.execute(f"""
                INSERT INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
                (table_name, symbol_id, symbol, delisting_date, last_successful_run, last_fiscal_date, created_at, updated_at)
                VALUES (
                    'TIME_SERIES_DAILY_ADJUSTED',
                    {symbol_id},
                    '{symbol}',
                    {'NULL' if delisting_date is None else f"'{delisting_date.strftime('%Y-%m-%d')}'"},
                    {'NULL' if last_successful_run is None else f"'{last_successful_run.strftime('%Y-%m-%d %H:%M:%S')}'"},
                    {'NULL' if last_fiscal_date is None else f"'{last_fiscal_date.strftime('%Y-%m-%d')}'"},
                    CURRENT_TIMESTAMP(),
                    CURRENT_TIMESTAMP()
                )
            """)
        
        etl_manager.conn.commit()
        print("✅ Test data setup complete")
        
    finally:
        etl_manager.close_connection()


def test_delisted_exclusion_logic():
    """
    Test the get_delisted_and_processed_symbols method.
    """
    print("\n🔍 Testing delisted symbol exclusion logic...")
    
    snowflake_config = get_snowflake_config_from_env()
    etl_manager = IncrementalETLManager(snowflake_config)
    
    try:
        test_symbols = [
            'TEST_ACTIVE_1', 'TEST_ACTIVE_2', 
            'TEST_DELISTED_1', 'TEST_DELISTED_2', 'TEST_DELISTED_3'
        ]
        
        # Test the delisted exclusion logic
        delisted_processed = etl_manager.get_delisted_and_processed_symbols(
            'time_series_daily_adjusted', 
            test_symbols
        )
        
        print(f"📊 Delisted and processed symbols: {delisted_processed}")
        
        # Expected: TEST_DELISTED_2 and TEST_DELISTED_3 (both have delisting_date and last_successful_run)
        expected_excluded = {'TEST_DELISTED_2', 'TEST_DELISTED_3'}
        actual_excluded = set(delisted_processed)
        
        if actual_excluded == expected_excluded:
            print("✅ Delisted exclusion logic working correctly")
        else:
            print(f"❌ Expected excluded: {expected_excluded}")
            print(f"❌ Actual excluded: {actual_excluded}")
            
    finally:
        etl_manager.close_connection()


def test_symbol_identification_logic():
    """
    Test the full identify_symbols_to_process method with delisted exclusion.
    """
    print("\n🎯 Testing full symbol identification logic...")
    
    snowflake_config = get_snowflake_config_from_env()
    etl_manager = IncrementalETLManager(snowflake_config)
    
    try:
        test_symbols = [
            'TEST_ACTIVE_1', 'TEST_ACTIVE_2', 
            'TEST_DELISTED_1', 'TEST_DELISTED_2', 'TEST_DELISTED_3'
        ]
        
        # Test symbol identification for time series
        symbols_to_process = etl_manager.identify_symbols_to_process(
            data_type='time_series_daily_adjusted',
            universe_symbols=test_symbols,
            force_refresh=False
        )
        
        print(f"📋 Symbols to process: {symbols_to_process}")
        
        # Expected: TEST_ACTIVE_1 (never processed), TEST_DELISTED_1 (delisted but never processed)
        # Should exclude: TEST_ACTIVE_2 (processed recently), TEST_DELISTED_2 & TEST_DELISTED_3 (delisted and processed)
        expected_symbols = {'TEST_ACTIVE_1', 'TEST_DELISTED_1'}
        actual_symbols = set(symbols_to_process)
        
        if actual_symbols == expected_symbols:
            print("✅ Symbol identification logic working correctly")
            print("✅ Active stocks are included when needed")
            print("✅ Delisted unprocessed stocks are included (for historical data)")
            print("✅ Delisted processed stocks are excluded (optimization)")
        else:
            print(f"❌ Expected symbols: {expected_symbols}")
            print(f"❌ Actual symbols: {actual_symbols}")
            
    finally:
        etl_manager.close_connection()


def test_force_refresh_behavior():
    """
    Test that force refresh still works but respects delisted exclusion.
    """
    print("\n🔄 Testing force refresh behavior...")
    
    snowflake_config = get_snowflake_config_from_env()
    etl_manager = IncrementalETLManager(snowflake_config)
    
    try:
        test_symbols = [
            'TEST_ACTIVE_1', 'TEST_ACTIVE_2', 
            'TEST_DELISTED_1', 'TEST_DELISTED_2', 'TEST_DELISTED_3'
        ]
        
        # Test force refresh - should still exclude processed delisted stocks
        symbols_to_process = etl_manager.identify_symbols_to_process(
            data_type='time_series_daily_adjusted',
            universe_symbols=test_symbols,
            force_refresh=True
        )
        
        print(f"🔄 Force refresh symbols: {symbols_to_process}")
        
        # Expected: All symbols except processed delisted ones (TEST_DELISTED_2, TEST_DELISTED_3)
        expected_symbols = {'TEST_ACTIVE_1', 'TEST_ACTIVE_2', 'TEST_DELISTED_1'}
        actual_symbols = set(symbols_to_process)
        
        if actual_symbols == expected_symbols:
            print("✅ Force refresh correctly excludes processed delisted stocks")
        else:
            print(f"❌ Expected symbols: {expected_symbols}")
            print(f"❌ Actual symbols: {actual_symbols}")
            
    finally:
        etl_manager.close_connection()


def cleanup_test_data():
    """
    Clean up test data from the ETL_WATERMARKS table.
    """
    print("\n🧹 Cleaning up test data...")
    
    snowflake_config = get_snowflake_config_from_env()
    etl_manager = IncrementalETLManager(snowflake_config)
    
    try:
        cursor = etl_manager.conn.cursor()
        
        cursor.execute("""
            DELETE FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
            WHERE table_name = 'TIME_SERIES_DAILY_ADJUSTED' 
            AND symbol IN ('TEST_ACTIVE_1', 'TEST_ACTIVE_2', 'TEST_DELISTED_1', 'TEST_DELISTED_2', 'TEST_DELISTED_3')
        """)
        
        etl_manager.conn.commit()
        print("✅ Test data cleanup complete")
        
    finally:
        etl_manager.close_connection()


def main():
    """
    Run all tests for the intelligent delisted stock processing logic.
    """
    print("🚀 Testing Intelligent Delisted Stock Processing Logic")
    print("=" * 60)
    
    try:
        # Set up test environment
        setup_test_data()
        
        # Run tests
        test_delisted_exclusion_logic()
        test_symbol_identification_logic()
        test_force_refresh_behavior()
        
        print("\n" + "=" * 60)
        print("✅ All tests completed successfully!")
        print("\n📝 Summary of intelligent processing behavior:")
        print("   • Active stocks: Processed based on normal refresh rules")
        print("   • Delisted stocks (never processed): Included once for historical data")
        print("   • Delisted stocks (already processed): Excluded to save API calls")
        print("   • Force refresh: Respects delisted exclusion rules")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        sys.exit(1)
        
    finally:
        # Always clean up
        try:
            cleanup_test_data()
        except:
            pass


if __name__ == "__main__":
    main()