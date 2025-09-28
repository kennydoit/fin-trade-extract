-- ============================================================================
-- IMMEDIATE FIX: Load All Companies and Fix SYMBOL_ID Format
-- Run this to get all 20 companies loaded with proper SYMBOL_IDs
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: Force load ALL data from the bulk CSV file
-- ============================================================================

SELECT 'FORCE LOADING ALL BULK DATA' as step;

-- Clear existing data to avoid duplicates
TRUNCATE TABLE OVERVIEW;

-- Force load all data from the bulk file
COPY INTO OVERVIEW 
FROM @OVERVIEW_STAGE 
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
PATTERN = '.*quick-bulk-load-20250927-121559.*'
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Check how many loaded
SELECT 'LOAD RESULTS' as step;
SELECT COUNT(*) as total_loaded, COUNT(DISTINCT SYMBOL) as unique_companies FROM OVERVIEW;

-- ============================================================================
-- STEP 2: Fix SYMBOL_ID format from "AAPL_ID" to numeric
-- ============================================================================

SELECT 'FIXING SYMBOL_ID FORMAT' as step;

-- Update SYMBOL_ID to proper numeric format
UPDATE OVERVIEW 
SET SYMBOL_ID = ABS(HASH(SYMBOL)) % 100000000;

-- ============================================================================
-- STEP 3: Verify results
-- ============================================================================

SELECT 'VERIFICATION - ALL COMPANIES' as step;

-- Show all companies loaded
SELECT 
    SYMBOL,
    SYMBOL_ID,
    SUBSTRING(NAME, 1, 40) as company_name,
    MARKET_CAPITALIZATION / 1000000000 as market_cap_billions
FROM OVERVIEW 
WHERE NAME IS NOT NULL
ORDER BY MARKET_CAPITALIZATION DESC;

-- Summary statistics
SELECT 'SUMMARY STATISTICS' as step;
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as unique_companies,
    COUNT(CASE WHEN NAME IS NOT NULL AND TRIM(NAME) != '' THEN 1 END) as companies_with_data,
    MIN(MARKET_CAPITALIZATION) / 1000000000 as min_market_cap_billions,
    MAX(MARKET_CAPITALIZATION) / 1000000000 as max_market_cap_billions
FROM OVERVIEW;

-- Show top 10 companies by market cap
SELECT 'TOP 10 COMPANIES BY MARKET CAP' as step;
SELECT 
    ROW_NUMBER() OVER (ORDER BY MARKET_CAPITALIZATION DESC) as rank,
    SYMBOL,
    SYMBOL_ID,
    NAME,
    ROUND(MARKET_CAPITALIZATION / 1000000000, 1) as market_cap_billions,
    PE_RATIO,
    DIVIDEND_YIELD
FROM OVERVIEW 
WHERE MARKET_CAPITALIZATION IS NOT NULL
ORDER BY MARKET_CAPITALIZATION DESC
LIMIT 10;