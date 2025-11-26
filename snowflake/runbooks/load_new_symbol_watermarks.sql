-- ============================================================================
-- Load New Symbol Watermarks
-- 
-- Purpose: Automatically create watermark entries for new symbols in LISTING_STATUS
--          across all ETL data sources
-- Source: FIN_TRADE_EXTRACT.RAW.LISTING_STATUS
-- Destination: FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
-- Strategy: Cross join new symbols with all existing table names, set API_ELIGIBLE
--           based on asset type and status requirements per data source
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Step 1: Identify new symbols not yet in watermarks
CREATE OR REPLACE TEMPORARY TABLE NEW_SYMBOLS AS
SELECT 
    SYMBOL_ID,
    SYMBOL,
    NAME,
    EXCHANGE,
    ASSET_TYPE,
    STATUS,
    IPO_DATE,
    DELISTING_DATE
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS
WHERE SYMBOL NOT IN (
    SELECT DISTINCT SYMBOL 
    FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
);

-- Step 2: Get list of all table names from existing watermarks
CREATE OR REPLACE TEMPORARY TABLE ETL_TABLE_NAMES AS
SELECT DISTINCT TABLE_NAME
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;
    t.TABLE_NAME,
    s.SYMBOL_ID,
    s.SYMBOL,
    s.NAME,
    s.EXCHANGE,
    s.ASSET_TYPE,
    s.STATUS,
    s.IPO_DATE,
    s.DELISTING_DATE
FROM NEW_SYMBOLS s
CROSS JOIN ETL_TABLE_NAMES t;

-- Step 4: Insert new watermark records with API_ELIGIBLE logic
INSERT INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS (
    TABLE_NAME,
    SYMBOL_ID,
    SYMBOL,
    NAME,
    EXCHANGE,
    ASSET_TYPE,
    STATUS,
    API_ELIGIBLE,
    IPO_DATE,
    DELISTING_DATE,
    CONSECUTIVE_FAILURES,
    CREATED_AT,
    UPDATED_AT
)
SELECT 
    TABLE_NAME,
    SYMBOL_ID,
    SYMBOL,
    NAME,
    EXCHANGE,
    ASSET_TYPE,
    STATUS,
    -- API_ELIGIBLE logic based on table name and asset type/status
    CASE 
        -- ETF_PROFILE: Only ETFs
        WHEN TABLE_NAME = 'ETF_PROFILE' AND UPPER(ASSET_TYPE) = 'ETF' THEN 'YES'
        
        -- EARNINGS_CALL_TRANSCRIPT: Only active stocks
        WHEN TABLE_NAME = 'EARNINGS_CALL_TRANSCRIPT' 
             AND UPPER(ASSET_TYPE) = 'STOCK' 
             AND UPPER(STATUS) = 'ACTIVE' THEN 'YES'
        
        -- TIME_SERIES_DAILY_ADJUSTED: All symbols
        WHEN TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED' THEN 'YES'
        
        -- COMPANY_OVERVIEW: All stocks (active or delisted)
        WHEN TABLE_NAME = 'COMPANY_OVERVIEW' AND UPPER(ASSET_TYPE) = 'STOCK' THEN 'YES'
        
        -- BALANCE_SHEET: Only active stocks
        WHEN TABLE_NAME = 'BALANCE_SHEET' 
             AND UPPER(ASSET_TYPE) = 'STOCK' 
             AND UPPER(STATUS) = 'ACTIVE' THEN 'YES'
        
        -- CASH_FLOW: Only active stocks
        WHEN TABLE_NAME = 'CASH_FLOW' 
             AND UPPER(ASSET_TYPE) = 'STOCK' 
             AND UPPER(STATUS) = 'ACTIVE' THEN 'YES'
        
        -- INCOME_STATEMENT: Only active stocks
        WHEN TABLE_NAME = 'INCOME_STATEMENT' 
             AND UPPER(ASSET_TYPE) = 'STOCK' 
             AND UPPER(STATUS) = 'ACTIVE' THEN 'YES'
        
        -- INSIDER_TRANSACTIONS: Only active stocks
        WHEN TABLE_NAME = 'INSIDER_TRANSACTIONS' 
             AND UPPER(ASSET_TYPE) = 'STOCK' 
             AND UPPER(STATUS) = 'ACTIVE' THEN 'YES'
        
        ELSE 'NO'
    END AS API_ELIGIBLE,
    IPO_DATE,
    DELISTING_DATE,
    0 AS CONSECUTIVE_FAILURES,
    CURRENT_TIMESTAMP() AS CREATED_AT,
    CURRENT_TIMESTAMP() AS UPDATED_AT
FROM NEW_SYMBOLS_TABLES;

-- Step 5: Report results
SELECT 
    '✅ New symbol watermarks created successfully!' AS STATUS,
    COUNT(*) AS TOTAL_RECORDS_INSERTED,
    COUNT(DISTINCT SYMBOL) AS NEW_SYMBOLS_ADDED,
    COUNT(DISTINCT TABLE_NAME) AS TABLE_NAMES_COVERED
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE CREATED_AT >= DATEADD(SECOND, -10, CURRENT_TIMESTAMP());

-- Step 6: Summary by table name
SELECT 
    TABLE_NAME,
    COUNT(*) AS NEW_RECORDS,
    COUNT(CASE WHEN API_ELIGIBLE = 'YES' THEN 1 END) AS API_ELIGIBLE_YES,
    COUNT(CASE WHEN API_ELIGIBLE = 'NO' THEN 1 END) AS API_ELIGIBLE_NO
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE CREATED_AT >= DATEADD(SECOND, -10, CURRENT_TIMESTAMP())
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME;

-- Cleanup temporary tables
DROP TABLE IF EXISTS NEW_SYMBOLS;
DROP TABLE IF EXISTS ETL_TABLE_NAMES;
DROP TABLE IF EXISTS NEW_SYMBOLS_TABLES;
