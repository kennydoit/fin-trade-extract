-- ============================================================================
-- ETL Watermarks Table Enhancement Migration
-- 
-- This script adds the missing fiscal date columns to the existing ETL_WATERMARKS table.
-- Run this script to upgrade the table schema to support enhanced watermarking features.
-- ============================================================================

-- Set context
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- ============================================================================
-- 1) Show current table structure
-- ============================================================================
SELECT 'Current ETL_WATERMARKS table structure:' as MESSAGE;
DESCRIBE TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;

-- ============================================================================
-- 2) Add missing columns to ETL_WATERMARKS table
-- ============================================================================

-- Add FIRST_FISCAL_DATE column
ALTER TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
ADD COLUMN IF NOT EXISTS FIRST_FISCAL_DATE DATE COMMENT 'First fiscal/data date available (earliest time series data point)';

-- Add LAST_FISCAL_DATE column  
ALTER TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
ADD COLUMN IF NOT EXISTS LAST_FISCAL_DATE DATE COMMENT 'Last fiscal/data date available in the data';

-- Add IPO_DATE column
ALTER TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
ADD COLUMN IF NOT EXISTS IPO_DATE DATE COMMENT 'IPO date from listing status';

-- Add DELISTING_DATE column
ALTER TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
ADD COLUMN IF NOT EXISTS DELISTING_DATE DATE COMMENT 'Delisting date from listing status';

-- Add LAST_SUCCESSFUL_RUN column
ALTER TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
ADD COLUMN IF NOT EXISTS LAST_SUCCESSFUL_RUN TIMESTAMP_NTZ COMMENT 'Last successful processing timestamp';

-- Add CONSECUTIVE_FAILURES column
ALTER TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
ADD COLUMN IF NOT EXISTS CONSECUTIVE_FAILURES NUMBER(5,0) DEFAULT 0 COMMENT 'Count of consecutive processing failures';

-- Add enhanced audit columns if they don't exist
ALTER TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
ADD COLUMN IF NOT EXISTS CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record creation timestamp';

ALTER TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
ADD COLUMN IF NOT EXISTS UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() COMMENT 'Record last update timestamp';

-- ============================================================================
-- 3) Populate IPO and delisting dates from listing status
-- ============================================================================

-- Update existing records with IPO and delisting dates from LISTING_STATUS
UPDATE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w
SET 
    IPO_DATE = (
        SELECT TRY_TO_DATE(ls.ipoDate, 'YYYY-MM-DD') 
        FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS ls 
        WHERE ls.symbol = w.symbol 
        LIMIT 1
    ),
    DELISTING_DATE = (
        SELECT TRY_TO_DATE(ls.delistingDate, 'YYYY-MM-DD') 
        FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS ls 
        WHERE ls.symbol = w.symbol 
        LIMIT 1
    ),
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE w.IPO_DATE IS NULL OR w.DELISTING_DATE IS NULL;

-- ============================================================================
-- 4) Show updated table structure
-- ============================================================================
SELECT 'Updated ETL_WATERMARKS table structure:' as MESSAGE;
DESCRIBE TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;

-- ============================================================================
-- 5) Show sample records with new columns
-- ============================================================================
SELECT 'Sample records with enhanced watermarking columns:' as MESSAGE;
SELECT TOP 10 
    SYMBOL,
    TABLE_NAME,
    IPO_DATE,
    DELISTING_DATE,
    FIRST_FISCAL_DATE,
    LAST_FISCAL_DATE,
    LAST_SUCCESSFUL_RUN,
    CONSECUTIVE_FAILURES,
    CREATED_AT,
    UPDATED_AT
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
ORDER BY UPDATED_AT DESC;

-- ============================================================================
-- 6) Show migration summary
-- ============================================================================
SELECT 
    'ETL_WATERMARKS migration completed successfully!' as STATUS,
    COUNT(*) as TOTAL_RECORDS,
    COUNT(CASE WHEN IPO_DATE IS NOT NULL THEN 1 END) as RECORDS_WITH_IPO_DATE,
    COUNT(CASE WHEN DELISTING_DATE IS NOT NULL THEN 1 END) as RECORDS_WITH_DELISTING_DATE
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;

SELECT '✅ Enhanced watermarking features are now available!' as COMPLETION_MESSAGE;