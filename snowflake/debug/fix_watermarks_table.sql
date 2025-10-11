-- Investigate ETL_WATERMARKS Table Issue
-- Check what table actually exists and fix the schema

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;

-- 1. Check what tables exist that might be named ETL_WATERMARKS
SELECT 'Tables with WATERMARK in name:' as MESSAGE;

SELECT 
    TABLE_NAME,
    TABLE_TYPE,
    ROW_COUNT,
    CREATED
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'RAW' 
  AND UPPER(TABLE_NAME) LIKE '%WATERMARK%'
ORDER BY TABLE_NAME;

-- 2. Show the actual structure of ETL_WATERMARKS table
SELECT 'Actual ETL_WATERMARKS table structure:' as MESSAGE;

SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COLUMN_DEFAULT
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'RAW' 
  AND TABLE_NAME = 'ETL_WATERMARKS'
ORDER BY ORDINAL_POSITION;

-- 3. Check if there's sample data in the existing table
SELECT 'Sample data from existing ETL_WATERMARKS:' as MESSAGE;

SELECT TOP 5 *
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;

-- 4. Check if the table has the old incremental ETL structure
SELECT 'Checking if this is the old incremental ETL table:' as MESSAGE;

SELECT 
    CASE 
        WHEN COUNT(*) > 0 THEN 'This appears to be the old incremental ETL table structure'
        ELSE 'This is not the old structure'
    END as diagnosis
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'RAW' 
  AND TABLE_NAME = 'ETL_WATERMARKS'
  AND COLUMN_NAME IN ('TABLE_NAME', 'SYMBOL_ID', 'IPO_DATE', 'DELISTING_DATE');

-- 5. Solution: Drop the old table and create the correct one
SELECT 'Next steps:' as MESSAGE;
SELECT '1. The existing ETL_WATERMARKS table has wrong schema' as STEP;
SELECT '2. Need to drop it and recreate with correct schema' as STEP;
SELECT '3. Run the commands below to fix it' as STEP;

-- SOLUTION: Drop the incorrect table and create the correct one
SELECT 'Run these commands to fix the issue:' as MESSAGE;

/*
-- Uncomment and run these commands to fix:

DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;

CREATE TABLE FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS (
    WATERMARK_ID NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    SYMBOL VARCHAR(20) NOT NULL,
    DATA_TYPE VARCHAR(50) NOT NULL,
    PROCESSING_STATUS VARCHAR(20) NOT NULL,
    LAST_PROCESSED_DATE DATE,
    LAST_UPDATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    ERROR_MESSAGE VARCHAR(2000),
    RETRY_COUNT NUMBER(10,0) DEFAULT 0,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT UK_ETL_WATERMARKS_SYMBOL_DATA_TYPE UNIQUE (SYMBOL, DATA_TYPE)
);

-- Verify the new structure
SELECT 'Correct table structure created:' as MESSAGE;

SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'RAW' 
  AND TABLE_NAME = 'ETL_WATERMARKS'
ORDER BY ORDINAL_POSITION;

*/