-- Check ETL Watermark Table Updates for Company Overview
-- Run this to verify that company overview processing is updating the watermark table

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;

-- 1. Check if ETL_WATERMARKS table exists and has data
SELECT 'ETL Watermarks table status:' as MESSAGE;

SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN data_type = 'company_overview' THEN 1 END) as company_overview_records,
    MIN(last_updated) as earliest_update,
    MAX(last_updated) as latest_update
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS;

-- 2. Show recent company overview watermark entries
SELECT 'Recent company overview watermarks:' as MESSAGE;

SELECT 
    symbol,
    data_type,
    last_processed_date,
    processing_status,
    last_updated,
    error_message
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE data_type = 'company_overview'
ORDER BY last_updated DESC
LIMIT 10;

-- 3. Check watermark status distribution
SELECT 'Company overview processing status summary:' as MESSAGE;

SELECT 
    processing_status,
    COUNT(*) as count,
    MIN(last_updated) as earliest,
    MAX(last_updated) as latest
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE data_type = 'company_overview'
GROUP BY processing_status
ORDER BY count DESC;

-- 4. Check for symbols processed today
SELECT 'Symbols processed today:' as MESSAGE;

SELECT 
    symbol,
    processing_status,
    last_processed_date,
    last_updated
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE data_type = 'company_overview'
  AND DATE(last_updated) = CURRENT_DATE()
ORDER BY last_updated DESC
LIMIT 15;

-- 5. Check for any recent errors
SELECT 'Recent errors (if any):' as MESSAGE;

SELECT 
    symbol,
    processing_status,
    error_message,
    last_updated
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE data_type = 'company_overview'
  AND processing_status = 'failed'
  AND last_updated >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY last_updated DESC
LIMIT 10;

-- 6. Compare with actual company overview data
SELECT 'Comparison with company overview table:' as MESSAGE;

SELECT 
    'Symbols in COMPANY_OVERVIEW table' as source,
    COUNT(DISTINCT symbol) as symbol_count
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW

UNION ALL

SELECT 
    'Symbols in ETL_WATERMARKS (company_overview)' as source,
    COUNT(DISTINCT symbol) as symbol_count
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE data_type = 'company_overview';