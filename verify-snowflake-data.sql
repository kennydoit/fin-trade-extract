-- ============================================================================
-- Snowflake UI Data Verification and Refresh
-- Run this to verify data and force UI refresh
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: Comprehensive Data Verification
-- ============================================================================

-- Check exactly what's in the table
SELECT 'CURRENT TABLE CONTENTS' as step;

SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    COUNT(CASE WHEN NAME IS NOT NULL AND TRIM(NAME) != '' THEN 1 END) as records_with_names,
    MIN(LOADED_AT) as earliest_record,
    MAX(LOADED_AT) as latest_record
FROM OVERVIEW;

-- Show all records with key information
SELECT 'ALL RECORDS SUMMARY' as step;
SELECT 
    SYMBOL,
    NAME,
    MARKET_CAPITALIZATION,
    PE_RATIO,
    LOADED_AT,
    CASE 
        WHEN NAME IS NOT NULL AND TRIM(NAME) != '' THEN 'REAL_DATA'
        ELSE 'EMPTY_DATA'
    END as data_status
FROM OVERVIEW
ORDER BY LOADED_AT DESC;

-- Specific check for MSFT
SELECT 'MSFT SPECIFIC CHECK' as step;
SELECT * FROM OVERVIEW WHERE SYMBOL = 'MSFT';

-- Check for AAPL too
SELECT 'AAPL SPECIFIC CHECK' as step;
SELECT * FROM OVERVIEW WHERE SYMBOL = 'AAPL';

-- ============================================================================
-- STEP 2: Force Table Statistics Refresh
-- ============================================================================

-- These commands can help refresh table metadata and statistics
SELECT 'REFRESHING TABLE METADATA' as step;

-- Analyze table to update statistics
ALTER TABLE OVERVIEW SET TAG ('data_updated' = TO_VARCHAR(CURRENT_TIMESTAMP()));

-- Get fresh table information
SHOW TABLES LIKE 'OVERVIEW';

-- Get current table details
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES,
    CREATED,
    LAST_ALTERED
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'RAW' 
    AND TABLE_NAME = 'OVERVIEW';

-- ============================================================================
-- STEP 3: UI Refresh Commands
-- ============================================================================

-- Run a query that forces the UI to refresh its cache
SELECT 'UI REFRESH QUERY' as step;

-- This query will force Snowflake to re-examine the table
SELECT 
    'Table Status' as metric,
    COUNT(*) as value
FROM OVERVIEW

UNION ALL

SELECT 
    'Records with Real Data' as metric,
    COUNT(*) as value
FROM OVERVIEW 
WHERE NAME IS NOT NULL AND TRIM(NAME) != ''

UNION ALL

SELECT 
    'Latest Load Time' as metric,
    COUNT(CASE WHEN LOADED_AT > DATEADD(HOUR, -1, CURRENT_TIMESTAMP()) THEN 1 END) as value
FROM OVERVIEW;

-- ============================================================================
-- STEP 4: Verify Data Quality
-- ============================================================================

SELECT 'DATA QUALITY CHECK' as step;

-- Show sample of actual data to verify it's real
SELECT 
    SYMBOL,
    SUBSTRING(NAME, 1, 30) as company_name_preview,
    MARKET_CAPITALIZATION,
    SUBSTRING(DESCRIPTION, 1, 100) as description_preview,
    LOADED_AT
FROM OVERVIEW
WHERE NAME IS NOT NULL 
    AND TRIM(NAME) != ''
ORDER BY LOADED_AT DESC
LIMIT 5;

-- ============================================================================
-- STEP 5: UI Navigation Suggestions
-- ============================================================================

SELECT 'UI TROUBLESHOOTING STEPS' as step,
       'Try these steps in Snowflake Web UI:' as instructions;

-- Instructions for user
SELECT 
    1 as step_number,
    'Refresh the browser page (F5 or Ctrl+R)' as action,
    'UI cache might be showing old state' as reason

UNION ALL

SELECT 
    2,
    'Click away from OVERVIEW table, then click back on it',
    'Forces UI to reload table metadata'

UNION ALL

SELECT 
    3,
    'Try changing the warehouse and changing back',
    'Sometimes warehouse switching refreshes UI state'

UNION ALL

SELECT 
    4,
    'Click "Data" tab, then "Databases", then navigate back to table',
    'Full UI navigation refresh'

UNION ALL

SELECT 
    5,
    'Use "SELECT * FROM OVERVIEW LIMIT 10" in worksheet instead of clicking table',
    'Worksheet always shows current data state'

ORDER BY step_number;

-- ============================================================================
-- FINAL VERIFICATION
-- ============================================================================

SELECT 'FINAL DATA CONFIRMATION' as step;

-- This should show your real data
SELECT 
    'If you see company names and financial data below, the table HAS data' as confirmation,
    'The UI might just need refreshing' as solution;

-- Final data sample
SELECT 
    SYMBOL,
    NAME,
    MARKET_CAPITALIZATION / 1000000000 as market_cap_billions,
    PE_RATIO,
    DIVIDEND_YIELD,
    LOADED_AT
FROM OVERVIEW 
WHERE NAME IS NOT NULL 
ORDER BY MARKET_CAPITALIZATION DESC
LIMIT 10;