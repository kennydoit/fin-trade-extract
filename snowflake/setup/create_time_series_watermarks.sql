-- ============================================================================
-- Create TIME_SERIES_DAILY_ADJUSTED Watermarks Only
-- 
-- This script creates watermark entries specifically for TIME_SERIES_DAILY_ADJUSTED
-- by copying from the base LISTING_STATUS watermarks.
--
-- Usage: Run this script after the base ETL_WATERMARKS table is created
-- ============================================================================

-- Set context
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- ============================================================================
-- TIME_SERIES_DAILY_ADJUSTED Watermarks
-- ============================================================================
-- All symbols are eligible for time series data (both active and delisted)

INSERT INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
    (TABLE_NAME, SYMBOL_ID, SYMBOL, NAME, EXCHANGE, ASSET_TYPE, STATUS, API_ELIGIBLE, 
     IPO_DATE, DELISTING_DATE, CREATED_AT, UPDATED_AT)
SELECT 
    'TIME_SERIES_DAILY_ADJUSTED' as TABLE_NAME,
    SYMBOL_ID,
    SYMBOL,
    NAME,
    EXCHANGE,
    ASSET_TYPE,
    STATUS,
    'YES' as API_ELIGIBLE,  -- All symbols are eligible for time series
    IPO_DATE,
    DELISTING_DATE,
    CURRENT_TIMESTAMP() as CREATED_AT,
    CURRENT_TIMESTAMP() as UPDATED_AT
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'LISTING_STATUS'
  AND NOT EXISTS (
      SELECT 1 FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w2
      WHERE w2.TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
        AND w2.SYMBOL_ID = ETL_WATERMARKS.SYMBOL_ID
  );

-- Show summary of created watermarks
SELECT 'TIME_SERIES_DAILY_ADJUSTED watermarks created:' as MESSAGE;

SELECT 
    EXCHANGE,
    ASSET_TYPE,
    STATUS,
    API_ELIGIBLE,
    COUNT(*) as SYMBOL_COUNT
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
GROUP BY EXCHANGE, ASSET_TYPE, STATUS, API_ELIGIBLE
ORDER BY EXCHANGE, ASSET_TYPE, STATUS;

-- Show total count
SELECT 
    'Total TIME_SERIES_DAILY_ADJUSTED watermarks:' as MESSAGE;

SELECT 
    TABLE_NAME,
    COUNT(*) as TOTAL_SYMBOLS,
    COUNT(CASE WHEN API_ELIGIBLE = 'YES' THEN 1 END) as API_ELIGIBLE_COUNT,
    COUNT(CASE WHEN API_ELIGIBLE = 'NO' THEN 1 END) as NOT_ELIGIBLE_COUNT
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
GROUP BY TABLE_NAME;

-- Show sample records
SELECT 'Sample TIME_SERIES_DAILY_ADJUSTED watermarks:' as MESSAGE;

SELECT TOP 10
    SYMBOL,
    NAME,
    EXCHANGE,
    ASSET_TYPE,
    STATUS,
    API_ELIGIBLE,
    IPO_DATE,
    DELISTING_DATE
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
ORDER BY SYMBOL;

SELECT '✅ TIME_SERIES_DAILY_ADJUSTED watermarks created successfully!' as COMPLETION_STATUS;
