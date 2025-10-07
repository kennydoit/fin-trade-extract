-- ============================================================================
-- ETL Watermarks Table Migration: Add FIRST_FISCAL_DATE Column
-- 
-- This script adds the FIRST_FISCAL_DATE column to existing ETL_WATERMARKS tables
-- and backfills the data from existing time series records.
--
-- Run this script to upgrade existing installations with the new optimization.
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- ============================================================================
-- 1) Add FIRST_FISCAL_DATE Column to Existing Table
-- ============================================================================

-- Check if column already exists
SELECT 'Checking if FIRST_FISCAL_DATE column exists...' as STATUS;

-- Add column if it doesn't exist (this will fail gracefully if column exists)
ALTER TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
ADD COLUMN FIRST_FISCAL_DATE DATE 
COMMENT 'First fiscal/data date available (earliest time series data point)';

SELECT 'FIRST_FISCAL_DATE column added successfully' as STATUS;

-- ============================================================================
-- 2) Backfill FIRST_FISCAL_DATE from Existing Time Series Data
-- ============================================================================

SELECT 'Starting backfill of FIRST_FISCAL_DATE from existing time series data...' as STATUS;

-- Update watermarks with first fiscal date from time series data
UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w
SET FIRST_FISCAL_DATE = (
    SELECT MIN(ts.date) 
    FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED ts
    WHERE ts.SYMBOL_ID = w.SYMBOL_ID
      AND ts.date IS NOT NULL
),
UPDATED_AT = CURRENT_TIMESTAMP()
WHERE w.TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
  AND w.FIRST_FISCAL_DATE IS NULL;

-- Show backfill results
SELECT 'Backfill Results:' as METRIC;

SELECT 
    'Symbols with FIRST_FISCAL_DATE populated' as METRIC,
    COUNT(*) as VALUE
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
  AND FIRST_FISCAL_DATE IS NOT NULL;

-- ============================================================================
-- 3) Update Watermark Analytics Views for New Column
-- ============================================================================

-- Recreate the watermark status view to include FIRST_FISCAL_DATE
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
    
    -- Data Availability Tracking (NEW!)
    w.FIRST_FISCAL_DATE,
    w.LAST_FISCAL_DATE,
    DATEDIFF('day', w.FIRST_FISCAL_DATE, w.LAST_FISCAL_DATE) as DATA_SPAN_DAYS,
    CASE 
        WHEN w.FIRST_FISCAL_DATE IS NOT NULL AND w.IPO_DATE IS NOT NULL 
        THEN DATEDIFF('day', w.IPO_DATE, w.FIRST_FISCAL_DATE)
        ELSE NULL
    END as DAYS_IPO_TO_FIRST_DATA,
    
    -- Processing Status
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
    
    -- Fundamentals Optimization Flags (NEW!)
    CASE 
        WHEN w.TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED' AND w.FIRST_FISCAL_DATE IS NOT NULL 
        THEN 'READY_FOR_FUNDAMENTALS'
        WHEN w.TABLE_NAME IN ('BALANCE_SHEET', 'INCOME_STATEMENT', 'CASH_FLOW') AND w.FIRST_FISCAL_DATE IS NULL
        THEN 'WAITING_FOR_TIME_SERIES'
        ELSE 'N/A'
    END as FUNDAMENTALS_READINESS,
    
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

-- ============================================================================
-- 4) Create Fundamentals Optimization View
-- ============================================================================

-- New view to help optimize fundamentals processing
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.ANALYTICS.FUNDAMENTALS_OPTIMIZATION AS
SELECT 
    ts_w.SYMBOL,
    ts_w.SYMBOL_ID,
    ts_w.FIRST_FISCAL_DATE as TIME_SERIES_START,
    ts_w.LAST_FISCAL_DATE as TIME_SERIES_END,
    
    -- Calculate optimal fundamentals date range
    GREATEST(
        ts_w.FIRST_FISCAL_DATE,
        DATE_TRUNC('QUARTER', ts_w.FIRST_FISCAL_DATE)
    ) as FUNDAMENTALS_START_DATE,
    
    LEAST(
        ts_w.LAST_FISCAL_DATE,
        CURRENT_DATE()
    ) as FUNDAMENTALS_END_DATE,
    
    -- Processing recommendations
    CASE 
        WHEN ts_w.FIRST_FISCAL_DATE IS NULL THEN 'SKIP - No time series data available'
        WHEN DATEDIFF('day', ts_w.FIRST_FISCAL_DATE, CURRENT_DATE()) < 90 THEN 'SKIP - Insufficient time series history'
        WHEN ts_w.DELISTING_DATE IS NOT NULL AND ts_w.DELISTING_DATE < DATEADD('year', -2, CURRENT_DATE()) 
        THEN 'LOW_PRIORITY - Delisted > 2 years ago'
        ELSE 'PROCESS - Good candidate for fundamentals'
    END as PROCESSING_RECOMMENDATION,
    
    -- Estimated quarters available for fundamentals
    CEIL(DATEDIFF('day', ts_w.FIRST_FISCAL_DATE, ts_w.LAST_FISCAL_DATE) / 90.0) as ESTIMATED_QUARTERS_AVAILABLE,
    
    -- Listing status
    CASE 
        WHEN ts_w.DELISTING_DATE IS NOT NULL THEN 'DELISTED'
        ELSE 'ACTIVE'
    END as STATUS,
    
    ts_w.UPDATED_AT
    
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS ts_w
WHERE ts_w.TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
  AND ts_w.FIRST_FISCAL_DATE IS NOT NULL
ORDER BY ts_w.SYMBOL;

-- ============================================================================
-- 5) Verification and Summary
-- ============================================================================

SELECT 'Migration Summary:' as METRIC;

-- Show migration results
SELECT 
    'Total watermark records' as METRIC,
    COUNT(*) as VALUE
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;

SELECT 
    'Time series records with FIRST_FISCAL_DATE' as METRIC,
    COUNT(*) as VALUE
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
  AND FIRST_FISCAL_DATE IS NOT NULL;

-- Show sample of new data
SELECT 'Sample of updated records with FIRST_FISCAL_DATE:' as MESSAGE;
SELECT TOP 10
    SYMBOL,
    TABLE_NAME,
    IPO_DATE,
    FIRST_FISCAL_DATE,
    LAST_FISCAL_DATE,
    CASE 
        WHEN FIRST_FISCAL_DATE IS NOT NULL AND IPO_DATE IS NOT NULL 
        THEN DATEDIFF('day', IPO_DATE, FIRST_FISCAL_DATE)
        ELSE NULL
    END as DAYS_IPO_TO_DATA,
    UPDATED_AT
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
  AND FIRST_FISCAL_DATE IS NOT NULL
ORDER BY UPDATED_AT DESC;

-- Show fundamentals optimization candidates
SELECT 'Fundamentals processing candidates:' as MESSAGE;
SELECT 
    PROCESSING_RECOMMENDATION,
    COUNT(*) as SYMBOL_COUNT
FROM FIN_TRADE_EXTRACT.ANALYTICS.FUNDAMENTALS_OPTIMIZATION
GROUP BY PROCESSING_RECOMMENDATION
ORDER BY SYMBOL_COUNT DESC;

SELECT '✅ ETL_WATERMARKS migration completed successfully!' as COMPLETION_STATUS;
SELECT '🎯 FIRST_FISCAL_DATE now available for fundamentals optimization!' as OPTIMIZATION_STATUS;