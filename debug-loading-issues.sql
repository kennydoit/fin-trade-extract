-- ============================================================================
-- Debug Snowpipe Loading Issues - Multiple Records Not Loading
-- CSV has 20 companies but only 1 shows up in Snowflake
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: Check current table state
-- ============================================================================

SELECT 'CURRENT TABLE STATE' as step;

SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    STRING_AGG(SYMBOL, ', ') as symbols_loaded
FROM OVERVIEW;

-- Show what we currently have
SELECT 'CURRENT RECORDS' as step;
SELECT SYMBOL, SYMBOL_ID, NAME, LOADED_AT FROM OVERVIEW ORDER BY LOADED_AT DESC;

-- ============================================================================
-- STEP 2: Check Snowpipe loading history
-- ============================================================================

SELECT 'SNOWPIPE LOADING HISTORY' as step;

-- Check recent copy operations
SELECT 
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    STATUS,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY 
WHERE TABLE_NAME = 'OVERVIEW' 
    AND TABLE_SCHEMA = 'RAW'
    AND LAST_LOAD_TIME > DATEADD(HOUR, -2, CURRENT_TIMESTAMP())
ORDER BY LAST_LOAD_TIME DESC;

-- ============================================================================
-- STEP 3: Manual load test to force all data
-- ============================================================================

SELECT 'MANUAL LOAD TEST' as step;

-- Try to manually load the bulk file
COPY INTO OVERVIEW 
FROM @OVERVIEW_STAGE 
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
PATTERN = '.*quick-bulk-load-20250927-121559.*'
ON_ERROR = 'CONTINUE'
FORCE = TRUE;  -- Force reload even if file was already processed

-- Check results after manual load
SELECT 'POST MANUAL LOAD RESULTS' as step;

SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    STRING_AGG(DISTINCT SYMBOL, ', ') as all_symbols_loaded
FROM OVERVIEW;

-- Show sample of new data
SELECT 'SAMPLE OF ALL DATA' as step;
SELECT 
    SYMBOL,
    SYMBOL_ID, 
    SUBSTRING(NAME, 1, 30) as company_name,
    MARKET_CAPITALIZATION / 1000000000 as market_cap_billions,
    LOADED_AT
FROM OVERVIEW 
ORDER BY MARKET_CAPITALIZATION DESC
LIMIT 10;

-- ============================================================================
-- STEP 4: Check for duplicate handling issues
-- ============================================================================

SELECT 'DUPLICATE CHECK' as step;

-- Check if there are duplicates being filtered out
SELECT 
    SYMBOL,
    COUNT(*) as record_count,
    MIN(LOADED_AT) as first_load,
    MAX(LOADED_AT) as last_load
FROM OVERVIEW
GROUP BY SYMBOL
HAVING COUNT(*) > 1
ORDER BY record_count DESC;

-- ============================================================================
-- STEP 5: Identify SYMBOL_ID format issue
-- ============================================================================

SELECT 'SYMBOL_ID FORMAT ISSUE' as step;

-- Show current SYMBOL_ID format
SELECT 
    SYMBOL,
    SYMBOL_ID,
    CASE 
        WHEN SYMBOL_ID LIKE '%_ID' THEN 'NEEDS_FIX'
        WHEN TRY_CAST(SYMBOL_ID AS NUMBER) IS NOT NULL THEN 'NUMERIC_ID'
        ELSE 'OTHER_FORMAT'
    END as id_format_status
FROM OVERVIEW
ORDER BY SYMBOL;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 'FINAL STATUS CHECK' as step;

-- Count by symbol
SELECT 
    SYMBOL,
    COUNT(*) as records,
    MAX(LOADED_AT) as last_loaded
FROM OVERVIEW 
GROUP BY SYMBOL
ORDER BY last_loaded DESC;

-- Show if we now have the expected 20 companies
SELECT 
    CASE 
        WHEN COUNT(DISTINCT SYMBOL) = 20 THEN 'SUCCESS - All 20 companies loaded'
        WHEN COUNT(DISTINCT SYMBOL) > 1 THEN CONCAT('PARTIAL - Only ', COUNT(DISTINCT SYMBOL), ' companies loaded') 
        ELSE 'ISSUE - Only 1 company loaded'
    END as loading_status,
    COUNT(DISTINCT SYMBOL) as unique_companies,
    COUNT(*) as total_records
FROM OVERVIEW;