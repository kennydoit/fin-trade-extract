-- Snowflake Credit Usage Analysis for fin-trade-extract
-- This query helps understand where credits are being consumed

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. WAREHOUSE CREDIT USAGE BY HOUR (Last 7 days)
-- =============================================================================
SELECT 
    DATE_TRUNC('HOUR', start_time) as hour,
    warehouse_name,
    SUM(credits_used) as total_credits,
    COUNT(*) as query_count,
    AVG(credits_used) as avg_credits_per_query,
    SUM(total_elapsed_time)/1000 as total_seconds,
    AVG(total_elapsed_time)/1000 as avg_seconds_per_query
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND warehouse_name = 'FIN_TRADE_WH'
  AND credits_used > 0
GROUP BY 1, 2
ORDER BY hour DESC, total_credits DESC;

-- =============================================================================
-- 2. TOP EXPENSIVE QUERIES (Last 7 days)
-- =============================================================================
SELECT 
    query_id,
    DATE_TRUNC('minute', start_time) as execution_time,
    user_name,
    warehouse_name,
    credits_used,
    total_elapsed_time/1000 as duration_seconds,
    bytes_scanned,
    bytes_written,
    query_type,
    LEFT(query_text, 100) as query_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND warehouse_name = 'FIN_TRADE_WH'
  AND credits_used > 0
ORDER BY credits_used DESC
LIMIT 20;

-- =============================================================================
-- 3. CREDIT USAGE BY TABLE/OPERATION TYPE
-- =============================================================================
SELECT 
    CASE 
        WHEN UPPER(query_text) LIKE '%COMPANY_OVERVIEW%' THEN 'Company Overview'
        WHEN UPPER(query_text) LIKE '%TIME_SERIES%' THEN 'Time Series'
        WHEN UPPER(query_text) LIKE '%LISTING_STATUS%' THEN 'Listing Status'
        WHEN UPPER(query_text) LIKE '%COPY INTO%' THEN 'Data Loading (COPY INTO)'
        WHEN UPPER(query_text) LIKE '%CREATE%TABLE%' THEN 'Table Creation'
        WHEN UPPER(query_text) LIKE '%INSERT%' THEN 'Data Insert'
        WHEN UPPER(query_text) LIKE '%SELECT%' THEN 'Data Query'
        ELSE 'Other'
    END as operation_type,
    COUNT(*) as query_count,
    SUM(credits_used) as total_credits,
    AVG(credits_used) as avg_credits_per_query,
    SUM(total_elapsed_time)/1000 as total_seconds,
    SUM(bytes_scanned) as total_bytes_scanned,
    SUM(bytes_written) as total_bytes_written
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND warehouse_name = 'FIN_TRADE_WH'
  AND credits_used > 0
GROUP BY 1
ORDER BY total_credits DESC;

-- =============================================================================
-- 4. DATA LOADING OPERATIONS ANALYSIS
-- =============================================================================
SELECT 
    DATE_TRUNC('day', start_time) as load_date,
    CASE 
        WHEN UPPER(query_text) LIKE '%COMPANY_OVERVIEW%' THEN 'Company Overview'
        WHEN UPPER(query_text) LIKE '%TIME_SERIES%' THEN 'Time Series'
        ELSE 'Other'
    END as data_type,
    COUNT(*) as load_operations,
    SUM(credits_used) as total_credits,
    AVG(credits_used) as avg_credits_per_load,
    SUM(bytes_written) as total_bytes_loaded,
    AVG(bytes_written) as avg_bytes_per_load
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND warehouse_name = 'FIN_TRADE_WH'
  AND (UPPER(query_text) LIKE '%COPY INTO%' OR UPPER(query_text) LIKE '%INSERT%')
  AND credits_used > 0
GROUP BY 1, 2
ORDER BY load_date DESC, total_credits DESC;

-- =============================================================================
-- 5. WAREHOUSE AUTO-SUSPEND/RESUME ANALYSIS
-- =============================================================================
SELECT 
    DATE_TRUNC('hour', start_time) as hour,
    warehouse_name,
    COUNT(CASE WHEN query_text LIKE '%resumed%' THEN 1 END) as resumes,
    COUNT(CASE WHEN query_text LIKE '%suspended%' THEN 1 END) as suspends,
    SUM(credits_used) as credits_during_hour
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND warehouse_name = 'FIN_TRADE_WH'
GROUP BY 1, 2
HAVING resumes > 0 OR suspends > 0 OR credits_during_hour > 0
ORDER BY hour DESC;

-- =============================================================================
-- 6. TABLE SIZE AND CREDIT CORRELATION
-- =============================================================================
SELECT 
    table_schema,
    table_name,
    row_count,
    bytes as table_size_bytes,
    bytes / (1024*1024*1024) as table_size_gb,
    -- Estimated credits based on recent operations
    (SELECT SUM(credits_used) 
     FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY qh
     WHERE qh.start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
       AND UPPER(qh.query_text) LIKE '%' || tables.table_name || '%'
       AND qh.credits_used > 0
    ) as recent_credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES tables
WHERE table_catalog = 'FIN_TRADE_EXTRACT'
  AND table_schema = 'RAW'
  AND table_type = 'BASE TABLE'
ORDER BY table_size_gb DESC;

-- =============================================================================
-- 7. DAILY CREDIT SUMMARY WITH COST ESTIMATES
-- =============================================================================
SELECT 
    DATE_TRUNC('day', start_time) as date,
    warehouse_name,
    COUNT(*) as total_queries,
    SUM(credits_used) as total_credits,
    -- Assuming $2 per credit (adjust based on your Snowflake pricing)
    SUM(credits_used) * 2 as estimated_cost_usd,
    SUM(total_elapsed_time)/1000/3600 as total_compute_hours,
    SUM(bytes_scanned)/(1024*1024*1024) as total_gb_scanned,
    SUM(bytes_written)/(1024*1024*1024) as total_gb_written
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE start_time >= DATEADD('day', -30, CURRENT_TIMESTAMP())
  AND warehouse_name = 'FIN_TRADE_WH'
  AND credits_used > 0
GROUP BY 1, 2
ORDER BY date DESC;

-- =============================================================================
-- 8. MOST EXPENSIVE INDIVIDUAL OPERATIONS (Detailed)
-- =============================================================================
SELECT 
    query_id,
    start_time,
    end_time,
    user_name,
    credits_used,
    total_elapsed_time/1000 as duration_seconds,
    bytes_scanned/(1024*1024) as mb_scanned,
    bytes_written/(1024*1024) as mb_written,
    partitions_scanned,
    partitions_total,
    query_type,
    CASE 
        WHEN UPPER(query_text) LIKE '%COMPANY_OVERVIEW%' THEN 'Company Overview'
        WHEN UPPER(query_text) LIKE '%TIME_SERIES%' THEN 'Time Series'
        ELSE 'Other'
    END as data_type,
    LEFT(query_text, 200) as query_preview
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND warehouse_name = 'FIN_TRADE_WH'
  AND credits_used > 0.1  -- Focus on operations that used significant credits
ORDER BY credits_used DESC
LIMIT 50;

-- =============================================================================
-- 9. CREDIT EFFICIENCY METRICS
-- =============================================================================
SELECT 
    'Company Overview Operations' as operation_category,
    COUNT(*) as operations,
    SUM(credits_used) as total_credits,
    AVG(credits_used) as avg_credits_per_op,
    SUM(bytes_written)/(1024*1024*1024) as total_gb_processed,
    SUM(credits_used) / NULLIF(SUM(bytes_written)/(1024*1024*1024), 0) as credits_per_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND warehouse_name = 'FIN_TRADE_WH'
  AND UPPER(query_text) LIKE '%COMPANY_OVERVIEW%'
  AND credits_used > 0

UNION ALL

SELECT 
    'Time Series Operations' as operation_category,
    COUNT(*) as operations,
    SUM(credits_used) as total_credits,
    AVG(credits_used) as avg_credits_per_op,
    SUM(bytes_written)/(1024*1024*1024) as total_gb_processed,
    SUM(credits_used) / NULLIF(SUM(bytes_written)/(1024*1024*1024), 0) as credits_per_gb
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
WHERE start_time >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND warehouse_name = 'FIN_TRADE_WH'
  AND UPPER(query_text) LIKE '%TIME_SERIES%'
  AND credits_used > 0

ORDER BY total_credits DESC;

-- =============================================================================
-- SUMMARY: Key Questions This Analysis Answers
-- =============================================================================
/*
1. Query #1: Which hours had the highest credit usage?
2. Query #2: What were the most expensive individual operations?
3. Query #3: How do credits break down by operation type (overview vs time series)?
4. Query #4: What's the credit cost per data loading operation?
5. Query #5: How often is the warehouse resuming/suspending (idle costs)?
6. Query #6: Which tables are largest and correlate with high credit usage?
7. Query #7: What's the daily credit burn rate and estimated cost?
8. Query #8: Detailed breakdown of the most expensive operations
9. Query #9: Credit efficiency - how many credits per GB processed?

Expected Insights:
- Company overview operations should use more credits due to wider tables (58+ columns)
- Time series operations should be more efficient (fewer columns)
- Data loading (COPY INTO) operations may show the biggest differences
- Large table scans will show high credit usage
*/