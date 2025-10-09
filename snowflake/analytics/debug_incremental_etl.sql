-- Diagnostic Query: Check Incremental ETL Issues for Company Overview
-- Run this to debug why incremental processing is selecting the same symbols

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;

-- 1. Check if ETL_WATERMARKS table exists and show its structure
SELECT 'Checking if ETL_WATERMARKS table exists:' as MESSAGE;

-- First, check if the table exists
SELECT 
    table_name,
    table_type,
    created
FROM INFORMATION_SCHEMA.TABLES 
WHERE table_schema = 'RAW' 
  AND table_name = 'ETL_WATERMARKS';

-- Show table structure if it exists
SELECT 'ETL_WATERMARKS table structure:' as MESSAGE;

SELECT 
    column_name,
    data_type,
    is_nullable
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE table_schema = 'RAW' 
  AND table_name = 'ETL_WATERMARKS'
ORDER BY ordinal_position;

-- Try to get basic count (if table exists)
SELECT 'ETL_WATERMARKS basic stats:' as MESSAGE;

SELECT 
    COUNT(*) as total_records
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;

-- 2. Show sample records from ETL_WATERMARKS (if it exists and has data)
SELECT 'Sample ETL_WATERMARKS records:' as MESSAGE;

SELECT TOP 10 *
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;

-- 3. Check if ETL_WATERMARKS table exists and has the expected schema
SELECT 'Diagnosis:' as MESSAGE;

SELECT 
    CASE 
        WHEN COUNT(*) = 0 THEN 'ETL_WATERMARKS table does not exist - this is the problem!'
        ELSE CONCAT('ETL_WATERMARKS table exists with ', COUNT(*), ' records')
    END as diagnosis
FROM INFORMATION_SCHEMA.TABLES 
WHERE table_schema = 'RAW' 
  AND table_name = 'ETL_WATERMARKS';

-- 4. Show what tables DO exist in RAW schema
SELECT 'Tables that exist in RAW schema:' as MESSAGE;

SELECT 
    table_name,
    table_type,
    row_count
FROM INFORMATION_SCHEMA.TABLES 
WHERE table_schema = 'RAW'
ORDER BY table_name;

-- 5. Conclusion
SELECT 'Likely Issue:' as MESSAGE;

SELECT 
    'The ETL_WATERMARKS table probably does not exist' as issue,
    'This means incremental ETL is not working because there is no place to store processing status' as explanation,
    'Solution: Create the ETL_WATERMARKS table or check if it has a different name' as solution;