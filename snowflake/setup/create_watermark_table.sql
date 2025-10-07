-- ============================================================================
-- ETL Watermarking Table Creation Script
-- 
-- This script creates the comprehensive watermarking table for tracking
-- processing status across all data types in the fin-trade-extract pipeline.
--
-- Run this script once to set up the watermarking infrastructure.
-- ============================================================================

-- Set context
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- ============================================================================
-- 1) Create ETL Watermarks Table
-- ============================================================================

CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS (
    -- Primary Keys
    TABLE_NAME                  VARCHAR(100) NOT NULL,     -- Target table name (e.g., 'BALANCE_SHEET', 'TIME_SERIES_DAILY_ADJUSTED')
    SYMBOL_ID                   NUMBER(38,0) NOT NULL,     -- Hash-based symbol identifier for consistent joins
    
    -- Symbol Reference
    SYMBOL                      VARCHAR(20) NOT NULL,      -- Actual symbol for debugging and reference
    
    -- Listing Information  
    IPO_DATE                    DATE,                      -- IPO date from listing status
    DELISTING_DATE              DATE,                      -- Delisting date from listing status
    
    -- Processing Tracking
    LAST_FISCAL_DATE            DATE,                      -- Last fiscal/data date available in the data
    LAST_SUCCESSFUL_RUN         TIMESTAMP_NTZ,             -- Last successful processing timestamp
    CONSECUTIVE_FAILURES        NUMBER(5,0) DEFAULT 0,     -- Count of consecutive processing failures
    
    -- Audit Fields
    CREATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT PK_ETL_WATERMARKS PRIMARY KEY (TABLE_NAME, SYMBOL_ID)
)
COMMENT = 'ETL watermarking table for tracking processing status and symbol lifecycle across all data types'
CLUSTER BY (TABLE_NAME, LAST_SUCCESSFUL_RUN);

-- ============================================================================
-- 2) Initialize with Listing Status Data
-- ============================================================================

-- This populates the table with all symbols from LISTING_STATUS (both active and delisted) as the foundation
INSERT INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
    (TABLE_NAME, SYMBOL_ID, SYMBOL, IPO_DATE, DELISTING_DATE, CREATED_AT, UPDATED_AT)
SELECT DISTINCT
    'LISTING_STATUS' as TABLE_NAME,
    ABS(HASH(ls.symbol)) % 1000000000 as SYMBOL_ID,
    ls.symbol as SYMBOL,
    TRY_TO_DATE(ls.ipoDate, 'YYYY-MM-DD') as IPO_DATE,
    TRY_TO_DATE(ls.delistingDate, 'YYYY-MM-DD') as DELISTING_DATE,
    CURRENT_TIMESTAMP() as CREATED_AT,
    CURRENT_TIMESTAMP() as UPDATED_AT
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS ls
WHERE ls.symbol IS NOT NULL
  AND ls.symbol != ''
  AND NOT EXISTS (
      SELECT 1 FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w 
      WHERE w.TABLE_NAME = 'LISTING_STATUS' 
        AND w.SYMBOL_ID = ABS(HASH(ls.symbol)) % 1000000000
  );

-- Show initialization summary by listing status
SELECT 
    CASE 
        WHEN IPO_DATE IS NOT NULL AND DELISTING_DATE IS NOT NULL THEN 'DELISTED_WITH_DATES'
        WHEN IPO_DATE IS NOT NULL AND DELISTING_DATE IS NULL THEN 'ACTIVE_WITH_IPO'
        WHEN DELISTING_DATE IS NOT NULL THEN 'DELISTED_NO_IPO'
        ELSE 'MINIMAL_INFO'
    END as symbol_category,
    COUNT(*) as symbol_count
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
WHERE TABLE_NAME = 'LISTING_STATUS'
GROUP BY 1
ORDER BY symbol_count DESC;

-- ============================================================================
-- 3) Create Supporting Views for Analytics
-- ============================================================================

-- Create analytics schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS FIN_TRADE_EXTRACT.ANALYTICS
COMMENT = 'Analytics views and reporting objects for fin-trade-extract pipeline';

-- Enhanced Data Coverage Dashboard
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.ANALYTICS.WATERMARK_STATUS AS
SELECT 
    -- Symbol Information
    w.SYMBOL,
    w.SYMBOL_ID,
    w.TABLE_NAME,
    
    -- Listing Information
    w.IPO_DATE,
    w.DELISTING_DATE,
    DATEDIFF('day', w.IPO_DATE, CURRENT_DATE()) as DAYS_SINCE_IPO,
    CASE 
        WHEN w.DELISTING_DATE IS NOT NULL THEN 'DELISTED'
        WHEN w.IPO_DATE > CURRENT_DATE() THEN 'FUTURE_IPO'
        ELSE 'ACTIVE'
    END as LISTING_STATUS,
    
    -- Processing Status
    w.LAST_FISCAL_DATE,
    w.LAST_SUCCESSFUL_RUN,
    w.CONSECUTIVE_FAILURES,
    DATEDIFF('day', w.LAST_SUCCESSFUL_RUN, CURRENT_TIMESTAMP()) as DAYS_SINCE_LAST_SUCCESS,
    
    -- Data Freshness Assessment
    CASE w.TABLE_NAME
        WHEN 'TIME_SERIES_DAILY_ADJUSTED' THEN 
            CASE 
                WHEN DATEDIFF('day', w.LAST_SUCCESSFUL_RUN, CURRENT_TIMESTAMP()) <= 1 THEN 'CURRENT'
                WHEN DATEDIFF('day', w.LAST_SUCCESSFUL_RUN, CURRENT_TIMESTAMP()) <= 7 THEN 'STALE'
                ELSE 'VERY_STALE'
            END
        WHEN 'BALANCE_SHEET' THEN
            CASE 
                WHEN DATEDIFF('day', w.LAST_SUCCESSFUL_RUN, CURRENT_TIMESTAMP()) <= 30 THEN 'CURRENT'
                WHEN DATEDIFF('day', w.LAST_SUCCESSFUL_RUN, CURRENT_TIMESTAMP()) <= 90 THEN 'STALE'
                ELSE 'VERY_STALE'
            END
        ELSE 
            CASE 
                WHEN DATEDIFF('day', w.LAST_SUCCESSFUL_RUN, CURRENT_TIMESTAMP()) <= 7 THEN 'CURRENT'
                WHEN DATEDIFF('day', w.LAST_SUCCESSFUL_RUN, CURRENT_TIMESTAMP()) <= 30 THEN 'STALE'
                ELSE 'VERY_STALE'
            END
    END as FRESHNESS_STATUS,
    
    -- Failure Analysis
    CASE 
        WHEN w.CONSECUTIVE_FAILURES = 0 THEN 'SUCCESS'
        WHEN w.CONSECUTIVE_FAILURES <= 3 THEN 'MINOR_ISSUES'
        WHEN w.CONSECUTIVE_FAILURES <= 10 THEN 'MAJOR_ISSUES'
        ELSE 'CRITICAL_ISSUES'
    END as FAILURE_CATEGORY,
    
    -- Audit Information
    w.CREATED_AT,
    w.UPDATED_AT
    
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w
ORDER BY w.SYMBOL, w.TABLE_NAME;

-- Processing Summary by Data Type
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.ANALYTICS.PROCESSING_SUMMARY AS
SELECT 
    w.TABLE_NAME,
    COUNT(*) as TOTAL_SYMBOLS,
    COUNT(CASE WHEN w.CONSECUTIVE_FAILURES = 0 THEN 1 END) as SUCCESSFUL_SYMBOLS,
    COUNT(CASE WHEN w.CONSECUTIVE_FAILURES > 0 THEN 1 END) as FAILED_SYMBOLS,
    ROUND((COUNT(CASE WHEN w.CONSECUTIVE_FAILURES = 0 THEN 1 END) * 100.0 / COUNT(*)), 2) as SUCCESS_RATE_PCT,
    MAX(w.LAST_SUCCESSFUL_RUN) as MOST_RECENT_SUCCESS,
    MIN(w.LAST_SUCCESSFUL_RUN) as OLDEST_SUCCESS,
    AVG(w.CONSECUTIVE_FAILURES) as AVG_CONSECUTIVE_FAILURES,
    MAX(w.CONSECUTIVE_FAILURES) as MAX_CONSECUTIVE_FAILURES,
    CURRENT_TIMESTAMP() as REPORT_TIMESTAMP
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w
GROUP BY w.TABLE_NAME
ORDER BY w.TABLE_NAME;

-- ============================================================================
-- 4) Verification Queries
-- ============================================================================

-- Show table structure
SELECT 'Table created successfully with the following structure:' as MESSAGE;
DESCRIBE TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;

-- Show initialization results
SELECT 
    'ETL_WATERMARKS initialized with ' || COUNT(*) || ' symbols from LISTING_STATUS' as INITIALIZATION_RESULT
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
WHERE TABLE_NAME = 'LISTING_STATUS';

-- Show sample records
SELECT 'Sample watermark records:' as MESSAGE;
SELECT TOP 10 
    SYMBOL,
    TABLE_NAME,
    IPO_DATE,
    DELISTING_DATE,
    CONSECUTIVE_FAILURES,
    CREATED_AT
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
ORDER BY CREATED_AT DESC;

-- Show processing summary
SELECT 'Current processing status by data type:' as MESSAGE;
SELECT * FROM FIN_TRADE_EXTRACT.ANALYTICS.PROCESSING_SUMMARY;

SELECT '✅ ETL_WATERMARKS table setup complete!' as COMPLETION_STATUS;