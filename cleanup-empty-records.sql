-- ============================================================================
-- Clean Up Empty Records and Prepare for Real Data
-- Run this in Snowflake to clean the empty CSV records
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: Analyze current data quality
-- ============================================================================

SELECT 'CURRENT DATA ANALYSIS' as step;

-- Check what we have
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    COUNT(CASE WHEN NAME IS NOT NULL AND TRIM(NAME) != '' THEN 1 END) as records_with_names,
    COUNT(CASE WHEN MARKET_CAPITALIZATION IS NOT NULL THEN 1 END) as records_with_market_cap
FROM OVERVIEW;

-- Show sample of what we have
SELECT 'SAMPLE RECORDS' as step;
SELECT SYMBOL, SYMBOL_ID, NAME, MARKET_CAPITALIZATION, LOADED_AT 
FROM OVERVIEW 
ORDER BY LOADED_AT DESC 
LIMIT 10;

-- ============================================================================
-- STEP 2: Clean up empty records
-- ============================================================================

SELECT 'CLEANING EMPTY RECORDS' as step;

-- Delete records with no actual financial data (empty from failed Lambda calls)
DELETE FROM OVERVIEW 
WHERE (NAME IS NULL OR TRIM(NAME) = '')
  AND (MARKET_CAPITALIZATION IS NULL)
  AND (DESCRIPTION IS NULL OR TRIM(DESCRIPTION) = '');

-- Check results after cleanup
SELECT 'CLEANUP RESULTS' as step;
SELECT 
    COUNT(*) as remaining_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols_remaining
FROM OVERVIEW;

-- ============================================================================
-- STEP 3: Set up for proper data loading
-- ============================================================================

-- Create a view for monitoring data quality
CREATE OR REPLACE VIEW OVERVIEW_QUALITY AS
SELECT 
    SYMBOL,
    SYMBOL_ID,
    NAME,
    CASE 
        WHEN NAME IS NOT NULL AND TRIM(NAME) != '' AND MARKET_CAPITALIZATION IS NOT NULL 
        THEN 'COMPLETE'
        WHEN NAME IS NOT NULL AND TRIM(NAME) != '' 
        THEN 'PARTIAL'
        ELSE 'EMPTY'
    END as data_quality,
    MARKET_CAPITALIZATION,
    PE_RATIO,
    DIVIDEND_YIELD,
    LOADED_AT
FROM OVERVIEW
ORDER BY 
    CASE data_quality WHEN 'COMPLETE' THEN 1 WHEN 'PARTIAL' THEN 2 ELSE 3 END,
    SYMBOL;

-- Show current quality status
SELECT 'DATA QUALITY STATUS' as step;
SELECT 
    data_quality,
    COUNT(*) as count
FROM OVERVIEW_QUALITY
GROUP BY data_quality
ORDER BY count DESC;

-- ============================================================================
-- STEP 4: Prepare for Lambda function fix
-- ============================================================================

-- Create a tracking table for Lambda function tests
CREATE OR REPLACE TABLE LAMBDA_TEST_RESULTS (
    test_id                 VARCHAR(100),
    symbol                  VARCHAR(20),
    test_timestamp          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    success                 BOOLEAN,
    has_name                BOOLEAN,
    has_market_cap          BOOLEAN,
    has_financial_data      BOOLEAN,
    error_message           VARCHAR(16777216)
);

SELECT 'SETUP COMPLETE' as step,
       'Table cleaned and ready for proper data' as status,
       'Use OVERVIEW_QUALITY view to monitor data quality' as monitoring,
       'Use LAMBDA_TEST_RESULTS to track Lambda function fixes' as testing;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Final verification
SELECT 'FINAL STATUS' as step;
SELECT * FROM OVERVIEW_QUALITY;

SELECT 'READY FOR LAMBDA FIXES' as next_step,
       'Pipeline: Fixed Lambda → S3 → Snowpipe → Clean Data' as flow;