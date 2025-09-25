-- ============================================================================
-- Data Quality Checks for fin-trade-extract Pipeline
-- Run these queries regularly to ensure data integrity
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;

-- ============================================================================
-- BASIC DATA VALIDATION CHECKS
-- ============================================================================

-- Check for duplicate symbols in listing_status
SELECT 
    'Duplicate Symbols in LISTING_STATUS' AS CHECK_NAME,
    COUNT(*) AS ISSUE_COUNT
FROM (
    SELECT SYMBOL, COUNT(*) as cnt
    FROM RAW.LISTING_STATUS
    GROUP BY SYMBOL
    HAVING COUNT(*) > 1
);

-- Check for missing symbol_id references
SELECT 
    'Missing Symbol ID in Overview' AS CHECK_NAME,
    COUNT(*) AS ISSUE_COUNT
FROM RAW.OVERVIEW o
WHERE NOT EXISTS (
    SELECT 1 FROM RAW.LISTING_STATUS ls 
    WHERE ls.SYMBOL_ID = o.SYMBOL_ID
);

-- Check for time series data with impossible values
SELECT 
    'Time Series Invalid Prices' AS CHECK_NAME,
    COUNT(*) AS ISSUE_COUNT
FROM RAW.TIME_SERIES_DAILY_ADJUSTED
WHERE OPEN <= 0 OR HIGH <= 0 OR LOW <= 0 OR CLOSE <= 0
   OR HIGH < LOW 
   OR OPEN > HIGH * 1.5  -- Extreme price gaps
   OR CLOSE > HIGH * 1.1  -- Close higher than high
   OR VOLUME < 0;

-- ============================================================================
-- DATA COMPLETENESS CHECKS
-- ============================================================================

-- Overview data completeness by symbol
SELECT 
    'Overview Data Completeness' AS CHECK_NAME,
    COUNT(CASE WHEN NAME IS NULL THEN 1 END) AS MISSING_NAME,
    COUNT(CASE WHEN SECTOR IS NULL THEN 1 END) AS MISSING_SECTOR,
    COUNT(CASE WHEN INDUSTRY IS NULL THEN 1 END) AS MISSING_INDUSTRY,
    COUNT(CASE WHEN MARKET_CAPITALIZATION IS NULL THEN 1 END) AS MISSING_MARKET_CAP,
    COUNT(*) AS TOTAL_RECORDS
FROM RAW.OVERVIEW;

-- Financial statement data availability
SELECT 
    'Financial Statement Data Availability' AS CHECK_NAME,
    (SELECT COUNT(DISTINCT SYMBOL_ID) FROM RAW.INCOME_STATEMENT) AS SYMBOLS_WITH_INCOME_STMT,
    (SELECT COUNT(DISTINCT SYMBOL_ID) FROM RAW.BALANCE_SHEET) AS SYMBOLS_WITH_BALANCE_SHEET,
    (SELECT COUNT(DISTINCT SYMBOL_ID) FROM RAW.CASH_FLOW) AS SYMBOLS_WITH_CASH_FLOW,
    (SELECT COUNT(*) FROM RAW.LISTING_STATUS WHERE STATE = 'active') AS TOTAL_ACTIVE_SYMBOLS;

-- ============================================================================
-- DATA CONSISTENCY CHECKS
-- ============================================================================

-- Check for symbol consistency across tables
SELECT 
    'Symbol Consistency Issues' AS CHECK_NAME,
    table_name,
    COUNT(*) AS MISMATCHED_RECORDS
FROM (
    SELECT 'OVERVIEW' as table_name, SYMBOL_ID, SYMBOL FROM RAW.OVERVIEW
    UNION ALL
    SELECT 'TIME_SERIES' as table_name, SYMBOL_ID, SYMBOL FROM RAW.TIME_SERIES_DAILY_ADJUSTED
    UNION ALL
    SELECT 'INCOME_STATEMENT' as table_name, SYMBOL_ID, SYMBOL FROM RAW.INCOME_STATEMENT
) t
WHERE NOT EXISTS (
    SELECT 1 FROM RAW.LISTING_STATUS ls 
    WHERE ls.SYMBOL_ID = t.SYMBOL_ID AND ls.SYMBOL = t.SYMBOL
)
GROUP BY table_name;

-- Check for future dates in historical data
SELECT 
    'Future Dates in Historical Data' AS CHECK_NAME,
    'TIME_SERIES' AS TABLE_NAME,
    COUNT(*) AS ISSUE_COUNT
FROM RAW.TIME_SERIES_DAILY_ADJUSTED
WHERE DATE > CURRENT_DATE()

UNION ALL

SELECT 
    'Future Dates in Historical Data' AS CHECK_NAME,
    'INCOME_STATEMENT' AS TABLE_NAME,
    COUNT(*) AS ISSUE_COUNT
FROM RAW.INCOME_STATEMENT
WHERE FISCAL_DATE_ENDING > CURRENT_DATE()

UNION ALL

SELECT 
    'Future Dates in Historical Data' AS CHECK_NAME,
    'INSIDER_TRANSACTIONS' AS TABLE_NAME,
    COUNT(*) AS ISSUE_COUNT
FROM RAW.INSIDER_TRANSACTIONS
WHERE TRANSACTION_DATE > CURRENT_DATE();

-- ============================================================================
-- API RESPONSE STATUS CHECKS
-- ============================================================================

-- Check API response status distribution
SELECT 
    'API Response Status Summary' AS CHECK_NAME,
    API_RESPONSE_STATUS,
    COUNT(*) AS COUNT
FROM (
    SELECT 'OVERVIEW' as source, API_RESPONSE_STATUS FROM RAW.OVERVIEW
    UNION ALL
    SELECT 'INCOME_STATEMENT' as source, API_RESPONSE_STATUS FROM RAW.INCOME_STATEMENT
    UNION ALL
    SELECT 'BALANCE_SHEET' as source, API_RESPONSE_STATUS FROM RAW.BALANCE_SHEET
    UNION ALL
    SELECT 'CASH_FLOW' as source, API_RESPONSE_STATUS FROM RAW.CASH_FLOW
    UNION ALL
    SELECT 'COMMODITIES' as source, API_RESPONSE_STATUS FROM RAW.COMMODITIES
    UNION ALL
    SELECT 'ECONOMIC_INDICATORS' as source, API_RESPONSE_STATUS FROM RAW.ECONOMIC_INDICATORS
    UNION ALL
    SELECT 'INSIDER_TRANSACTIONS' as source, API_RESPONSE_STATUS FROM RAW.INSIDER_TRANSACTIONS
    UNION ALL
    SELECT 'EARNINGS_CALL_TRANSCRIPTS' as source, API_RESPONSE_STATUS FROM RAW.EARNINGS_CALL_TRANSCRIPTS
)
GROUP BY API_RESPONSE_STATUS
ORDER BY COUNT DESC;

-- ============================================================================
-- DATA VOLUME CHECKS
-- ============================================================================

-- Check record counts by table
SELECT 
    'Record Counts by Table' AS CHECK_NAME,
    'LISTING_STATUS' AS TABLE_NAME,
    COUNT(*) AS RECORD_COUNT,
    MAX(UPDATED_AT) AS LAST_UPDATED
FROM RAW.LISTING_STATUS

UNION ALL

SELECT 
    'Record Counts by Table' AS CHECK_NAME,
    'OVERVIEW' AS TABLE_NAME,
    COUNT(*) AS RECORD_COUNT,
    MAX(UPDATED_AT) AS LAST_UPDATED
FROM RAW.OVERVIEW

UNION ALL

SELECT 
    'Record Counts by Table' AS CHECK_NAME,
    'TIME_SERIES_DAILY_ADJUSTED' AS TABLE_NAME,
    COUNT(*) AS RECORD_COUNT,
    MAX(UPDATED_AT) AS LAST_UPDATED
FROM RAW.TIME_SERIES_DAILY_ADJUSTED

UNION ALL

SELECT 
    'Record Counts by Table' AS CHECK_NAME,
    'INCOME_STATEMENT' AS TABLE_NAME,
    COUNT(*) AS RECORD_COUNT,
    MAX(UPDATED_AT) AS LAST_UPDATED
FROM RAW.INCOME_STATEMENT

UNION ALL

SELECT 
    'Record Counts by Table' AS CHECK_NAME,
    'BALANCE_SHEET' AS TABLE_NAME,
    COUNT(*) AS RECORD_COUNT,
    MAX(UPDATED_AT) AS LAST_UPDATED
FROM RAW.BALANCE_SHEET

UNION ALL

SELECT 
    'Record Counts by Table' AS CHECK_NAME,
    'CASH_FLOW' AS TABLE_NAME,
    COUNT(*) AS RECORD_COUNT,
    MAX(UPDATED_AT) AS LAST_UPDATED
FROM RAW.CASH_FLOW

ORDER BY TABLE_NAME;

-- ============================================================================
-- BUSINESS LOGIC VALIDATION
-- ============================================================================

-- Check for companies with negative market cap
SELECT 
    'Companies with Invalid Market Cap' AS CHECK_NAME,
    COUNT(*) AS ISSUE_COUNT
FROM RAW.OVERVIEW
WHERE MARKET_CAPITALIZATION < 0;

-- Check for extreme P/E ratios
SELECT 
    'Companies with Extreme P/E Ratios' AS CHECK_NAME,
    COUNT(CASE WHEN PE_RATIO < -100 THEN 1 END) AS EXTREME_NEGATIVE_PE,
    COUNT(CASE WHEN PE_RATIO > 1000 THEN 1 END) AS EXTREME_POSITIVE_PE,
    COUNT(*) AS TOTAL_RECORDS
FROM RAW.OVERVIEW
WHERE PE_RATIO IS NOT NULL;

-- Check for insider transactions with zero or negative values
SELECT 
    'Insider Transactions with Invalid Values' AS CHECK_NAME,
    COUNT(*) AS ISSUE_COUNT
FROM RAW.INSIDER_TRANSACTIONS
WHERE SHARES <= 0 OR SHARE_PRICE <= 0;

-- ============================================================================
-- SUMMARY DATA QUALITY REPORT
-- ============================================================================

-- Create a comprehensive data quality summary
WITH quality_checks AS (
    SELECT 
        'Symbol Duplicates' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        COUNT(*) AS issue_count
    FROM (
        SELECT SYMBOL FROM RAW.LISTING_STATUS GROUP BY SYMBOL HAVING COUNT(*) > 1
    )
    
    UNION ALL
    
    SELECT 
        'Missing Overview Data' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'WARN' END AS status,
        COUNT(*) AS issue_count
    FROM RAW.LISTING_STATUS ls
    WHERE NOT EXISTS (SELECT 1 FROM RAW.OVERVIEW o WHERE o.SYMBOL_ID = ls.SYMBOL_ID)
    
    UNION ALL
    
    SELECT 
        'Invalid Time Series Prices' AS check_name,
        CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
        COUNT(*) AS issue_count
    FROM RAW.TIME_SERIES_DAILY_ADJUSTED
    WHERE OPEN <= 0 OR HIGH <= 0 OR LOW <= 0 OR CLOSE <= 0
)
SELECT 
    'Data Quality Summary' AS report_name,
    check_name,
    status,
    issue_count
FROM quality_checks
ORDER BY 
    CASE WHEN status = 'FAIL' THEN 1 WHEN status = 'WARN' THEN 2 ELSE 3 END,
    check_name;