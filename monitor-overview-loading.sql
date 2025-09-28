-- ============================================================================
-- Monitor OVERVIEW Table Loading Progress 
-- Run these queries in Snowflake to track bulk loading
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;
USE SCHEMA RAW;

-- ============================================================================
-- Current Status Checks
-- ============================================================================

-- 1. Current row count and basic stats
SELECT 
    COUNT(*) as total_companies,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    MIN(LOADED_AT) as first_load,
    MAX(LOADED_AT) as last_load
FROM OVERVIEW;

-- 2. Loading progress by symbol
SELECT 
    SYMBOL,
    NAME,
    SECTOR,
    API_RESPONSE_STATUS,
    LOADED_AT
FROM OVERVIEW 
ORDER BY LOADED_AT DESC
LIMIT 20;

-- 3. Check for duplicates (should be minimal)
SELECT 
    SYMBOL,
    COUNT(*) as record_count
FROM OVERVIEW
GROUP BY SYMBOL
HAVING COUNT(*) > 1
ORDER BY record_count DESC;

-- ============================================================================
-- Snowpipe Status and Error Monitoring
-- ============================================================================

-- 4. Check pipe status
SELECT SYSTEM$PIPE_STATUS('RAW.OVERVIEW_PIPE') as pipe_status;

-- 5. Recent loading activity 
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
ORDER BY LAST_LOAD_TIME DESC 
LIMIT 20;

-- 6. Loading errors (if any)
SELECT 
    FILE_NAME,
    STATUS,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY 
WHERE TABLE_NAME = 'OVERVIEW' 
    AND TABLE_SCHEMA = 'RAW'
    AND STATUS != 'LOADED'
ORDER BY LAST_LOAD_TIME DESC;

-- ============================================================================
-- Data Quality Checks
-- ============================================================================

-- 7. Check data completeness by key fields
SELECT 
    'Total Records' as metric,
    COUNT(*) as count
FROM OVERVIEW

UNION ALL

SELECT 
    'Records with Names' as metric,
    COUNT(*) as count
FROM OVERVIEW 
WHERE NAME IS NOT NULL AND NAME != ''

UNION ALL

SELECT 
    'Records with Market Cap' as metric,
    COUNT(*) as count
FROM OVERVIEW 
WHERE MARKET_CAPITALIZATION IS NOT NULL

UNION ALL

SELECT 
    'Successful API Responses' as metric,
    COUNT(*) as count
FROM OVERVIEW 
WHERE API_RESPONSE_STATUS = 'success';

-- 8. Top companies by market cap
SELECT 
    SYMBOL,
    NAME,
    SECTOR,
    MARKET_CAPITALIZATION,
    PE_RATIO,
    DIVIDEND_YIELD
FROM OVERVIEW 
WHERE MARKET_CAPITALIZATION IS NOT NULL
    AND API_RESPONSE_STATUS = 'success'
ORDER BY MARKET_CAPITALIZATION DESC 
LIMIT 10;

-- 9. Sector distribution
SELECT 
    SECTOR,
    COUNT(*) as company_count,
    AVG(MARKET_CAPITALIZATION) as avg_market_cap,
    AVG(PE_RATIO) as avg_pe_ratio
FROM OVERVIEW 
WHERE API_RESPONSE_STATUS = 'success'
    AND SECTOR IS NOT NULL
    AND SECTOR != ''
GROUP BY SECTOR
ORDER BY company_count DESC;

-- ============================================================================
-- Pipeline Health Summary
-- ============================================================================

-- 10. Overall pipeline health
SELECT 
    'OVERVIEW Pipeline Health' as component,
    CASE 
        WHEN COUNT(*) = 0 THEN 'NO DATA - Check Lambda and S3'
        WHEN COUNT(CASE WHEN API_RESPONSE_STATUS = 'success' THEN 1 END) / COUNT(*) > 0.8 
        THEN 'HEALTHY - ' || COUNT(*) || ' records, ' || ROUND(100 * COUNT(CASE WHEN API_RESPONSE_STATUS = 'success' THEN 1 END) / COUNT(*), 1) || '% success rate'
        WHEN COUNT(*) > 0 THEN 'ISSUES - ' || COUNT(*) || ' records, ' || ROUND(100 * COUNT(CASE WHEN API_RESPONSE_STATUS = 'success' THEN 1 END) / COUNT(*), 1) || '% success rate'
        ELSE 'UNKNOWN'
    END as status,
    MAX(LOADED_AT) as last_update
FROM OVERVIEW;