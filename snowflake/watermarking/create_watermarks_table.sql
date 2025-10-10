-- Systematic ETL Watermarking - Manual Table Creation and Inspection
-- Run this manually to create and inspect the ETL_WATERMARKS table

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ACCOUNTADMIN;

-- =====================================================================
-- STEP 1: Create the ETL_WATERMARKS table
-- =====================================================================
SELECT 'Creating ETL_WATERMARKS table...' as MESSAGE;

CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS (
    WATERMARK_ID NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    SYMBOL VARCHAR(20) NOT NULL,
    DATA_TYPE VARCHAR(50) NOT NULL,
    PROCESSING_STATUS VARCHAR(20) NOT NULL,
    LAST_PROCESSED_DATE DATE,
    LAST_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    ERROR_MESSAGE VARCHAR(2000),
    RETRY_COUNT NUMBER(10,0) DEFAULT 0,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    
    -- Create unique constraint on symbol + data_type combination
    CONSTRAINT UK_ETL_WATERMARKS_SYMBOL_DATA_TYPE UNIQUE (SYMBOL, DATA_TYPE)
);

-- =====================================================================
-- STEP 2: Validate table structure
-- =====================================================================
SELECT 'ETL_WATERMARKS table structure:' as MESSAGE;

SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT,
    COMMENT
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'RAW' 
  AND TABLE_NAME = 'ETL_WATERMARKS'
ORDER BY ORDINAL_POSITION;

-- =====================================================================
-- STEP 3: Check constraints
-- =====================================================================
SELECT 'ETL_WATERMARKS table constraints:' as MESSAGE;

SELECT 
    CONSTRAINT_NAME,
    CONSTRAINT_TYPE,
    TABLE_NAME,
    COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
WHERE tc.TABLE_SCHEMA = 'RAW' 
  AND tc.TABLE_NAME = 'ETL_WATERMARKS'
ORDER BY tc.CONSTRAINT_TYPE, tc.CONSTRAINT_NAME;

-- =====================================================================
-- STEP 4: Show current table status
-- =====================================================================
SELECT 'Current ETL_WATERMARKS status:' as MESSAGE;

SELECT 
    COUNT(*) as TOTAL_RECORDS,
    COUNT(DISTINCT DATA_TYPE) as UNIQUE_DATA_TYPES,
    COUNT(DISTINCT SYMBOL) as UNIQUE_SYMBOLS,
    MIN(CREATED_AT) as FIRST_RECORD,
    MAX(LAST_UPDATED) as LAST_UPDATE
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;

-- =====================================================================
-- STEP 5: Show sample records (if any exist)
-- =====================================================================
SELECT 'Sample ETL_WATERMARKS records:' as MESSAGE;

SELECT TOP 10
    WATERMARK_ID,
    SYMBOL,
    DATA_TYPE,
    PROCESSING_STATUS,
    LAST_PROCESSED_DATE,
    LAST_UPDATED,
    CREATED_AT
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
ORDER BY CREATED_AT DESC;

-- =====================================================================
-- STEP 6: Data type summary (if any records exist)
-- =====================================================================
SELECT 'Data type summary:' as MESSAGE;

SELECT 
    DATA_TYPE,
    PROCESSING_STATUS,
    COUNT(*) as RECORD_COUNT,
    MIN(LAST_UPDATED) as EARLIEST_UPDATE,
    MAX(LAST_UPDATED) as LATEST_UPDATE
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
GROUP BY DATA_TYPE, PROCESSING_STATUS
ORDER BY DATA_TYPE, PROCESSING_STATUS;

-- =====================================================================
-- STEP 7: Processing status distribution
-- =====================================================================
SELECT 'Processing status distribution:' as MESSAGE;

SELECT 
    PROCESSING_STATUS,
    COUNT(*) as COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as PERCENTAGE
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
GROUP BY PROCESSING_STATUS
ORDER BY COUNT DESC;

-- =====================================================================
-- STEP 8: Show possible status values and their meanings
-- =====================================================================
SELECT 'ETL Watermark Status Values and Meanings:' as MESSAGE;

SELECT 
    'pending' as STATUS,
    'Symbol needs to be processed (never processed before)' as MEANING
    
UNION ALL

SELECT 
    'completed' as STATUS,
    'Symbol was successfully processed and data loaded' as MEANING
    
UNION ALL

SELECT 
    'failed' as STATUS,
    'Symbol processing failed (check ERROR_MESSAGE)' as MEANING
    
UNION ALL

SELECT 
    'processing' as STATUS,
    'Symbol is currently being processed' as MEANING
    
UNION ALL

SELECT 
    'skipped' as STATUS,
    'Symbol was skipped (e.g., no data available)' as MEANING;

-- =====================================================================
-- STEP 9: Check if other tables exist for reference
-- =====================================================================
SELECT 'Other tables in RAW schema:' as MESSAGE;

SELECT 
    TABLE_NAME,
    TABLE_TYPE,
    ROW_COUNT,
    BYTES,
    CREATED
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'RAW'
  AND TABLE_NAME != 'ETL_WATERMARKS'
ORDER BY TABLE_NAME;

-- =====================================================================
-- COMPLETION MESSAGE
-- =====================================================================
SELECT 'ETL_WATERMARKS table setup completed!' as MESSAGE;
SELECT 'Next steps:' as MESSAGE;
SELECT '1. Run watermarking workflow with desired table name' as STEP;
SELECT '2. Initialize watermarks for your first data type' as STEP;
SELECT '3. Begin systematic data extraction with proper tracking' as STEP;