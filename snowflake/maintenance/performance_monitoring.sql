-- ============================================================================
-- Performance and Cost Monitoring for fin-trade-extract Pipeline
-- Monitor warehouse usage, query performance, and Snowpipe costs
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;

-- ============================================================================
-- WAREHOUSE USAGE AND COST MONITORING
-- ============================================================================

-- Warehouse credit usage over the last 7 days
SELECT 
    'Warehouse Credit Usage (Last 7 Days)' AS METRIC_NAME,
    DATE_TRUNC('day', START_TIME) AS DATE,
    WAREHOUSE_NAME,
    SUM(CREDITS_USED) AS DAILY_CREDITS,
    SUM(CREDITS_USED) * 2.0 AS ESTIMATED_DAILY_COST_USD  -- Assuming $2/credit for X-Small
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME = 'FIN_TRADE_WH'
GROUP BY DATE, WAREHOUSE_NAME
ORDER BY DATE DESC;

-- Warehouse utilization patterns
SELECT 
    'Warehouse Utilization Patterns' AS METRIC_NAME,
    DATE_TRUNC('hour', START_TIME) AS HOUR,
    AVG(AVG_RUNNING) AS AVG_RUNNING_QUERIES,
    AVG(AVG_QUEUED_LOAD) AS AVG_QUEUED_LOAD,
    SUM(CREDITS_USED) AS HOURLY_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME = 'FIN_TRADE_WH'
GROUP BY HOUR
ORDER BY HOUR DESC
LIMIT 24;  -- Last 24 hours

-- ============================================================================
-- SNOWPIPE USAGE AND COST MONITORING
-- ============================================================================

-- Snowpipe credit usage and file processing
SELECT 
    'Snowpipe Usage (Last 7 Days)' AS METRIC_NAME,
    DATE_TRUNC('day', START_TIME) AS DATE,
    PIPE_NAME,
    SUM(CREDITS_USED) AS DAILY_CREDITS,
    SUM(BYTES_INSERTED) AS BYTES_PROCESSED,
    SUM(FILES_INSERTED) AS FILES_PROCESSED,
    AVG(AVG_FILE_SIZE_BYTES) AS AVG_FILE_SIZE_BYTES,
    SUM(CREDITS_USED) * 2.0 AS ESTIMATED_DAILY_COST_USD  -- Snowpipe pricing
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND PIPE_NAME LIKE 'FIN_TRADE_EXTRACT.RAW.%'
GROUP BY DATE, PIPE_NAME
ORDER BY DATE DESC, PIPE_NAME;

-- Snowpipe error monitoring
SELECT 
    'Snowpipe Errors (Last 7 Days)' AS METRIC_NAME,
    DATE_TRUNC('day', LAST_RECEIVED_MESSAGE_TIMESTAMP) AS DATE,
    PIPE_NAME,
    ERROR_TYPE,
    COUNT(*) AS ERROR_COUNT,
    ARRAY_AGG(DISTINCT ERROR_MESSAGE) AS ERROR_MESSAGES
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE LAST_RECEIVED_MESSAGE_TIMESTAMP >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND PIPE_NAME LIKE 'FIN_TRADE_EXTRACT.RAW.%'
  AND STATUS = 'LOAD_FAILED'
GROUP BY DATE, PIPE_NAME, ERROR_TYPE
ORDER BY DATE DESC, ERROR_COUNT DESC;

-- ============================================================================
-- QUERY PERFORMANCE MONITORING
-- ============================================================================

-- Top 10 longest running queries (last 24 hours)
SELECT 
    'Longest Running Queries (Last 24 Hours)' AS METRIC_NAME,
    QUERY_ID,
    USER_NAME,
    WAREHOUSE_NAME,
    EXECUTION_TIME / 1000 AS EXECUTION_TIME_SECONDS,
    TOTAL_ELAPSED_TIME / 1000 AS TOTAL_ELAPSED_TIME_SECONDS,
    QUERY_TYPE,
    LEFT(QUERY_TEXT, 100) || '...' AS QUERY_PREVIEW
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME = 'FIN_TRADE_WH'
  AND EXECUTION_STATUS = 'SUCCESS'
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 10;

-- Query patterns and frequency
SELECT 
    'Query Patterns (Last 7 Days)' AS METRIC_NAME,
    DATE_TRUNC('day', START_TIME) AS DATE,
    QUERY_TYPE,
    COUNT(*) AS QUERY_COUNT,
    AVG(EXECUTION_TIME / 1000) AS AVG_EXECUTION_TIME_SECONDS,
    SUM(CREDITS_USED_CLOUD_SERVICES) AS CLOUD_SERVICES_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME = 'FIN_TRADE_WH'
GROUP BY DATE, QUERY_TYPE
ORDER BY DATE DESC, QUERY_COUNT DESC;

-- ============================================================================
-- STORAGE USAGE AND COST MONITORING
-- ============================================================================

-- Database storage usage
SELECT 
    'Database Storage Usage' AS METRIC_NAME,
    DATABASE_NAME,
    SCHEMA_NAME,
    TABLE_NAME,
    ACTIVE_BYTES / (1024*1024*1024) AS ACTIVE_GB,
    TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_GB,
    FAILSAFE_BYTES / (1024*1024*1024) AS FAILSAFE_GB,
    (ACTIVE_BYTES + TIME_TRAVEL_BYTES + FAILSAFE_BYTES) / (1024*1024*1024) AS TOTAL_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE DATABASE_NAME = 'FIN_TRADE_EXTRACT'
  AND SCHEMA_NAME IN ('RAW', 'PROCESSED', 'ANALYTICS')
ORDER BY TOTAL_GB DESC;

-- Storage costs over time
SELECT 
    'Storage Costs (Last 30 Days)' AS METRIC_NAME,
    DATE_TRUNC('day', USAGE_DATE) AS DATE,
    STORAGE_BYTES / (1024*1024*1024) AS STORAGE_GB,
    STAGE_BYTES / (1024*1024*1024) AS STAGE_GB,
    FAILSAFE_BYTES / (1024*1024*1024) AS FAILSAFE_GB,
    -- Approximate storage cost ($40/TB/month = ~$0.00137/GB/day)
    (STORAGE_BYTES / (1024*1024*1024)) * 0.00137 AS ESTIMATED_STORAGE_COST_USD
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
WHERE USAGE_DATE >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY DATE DESC;

-- ============================================================================
-- DATA INGESTION MONITORING
-- ============================================================================

-- Data loading patterns by table
SELECT 
    'Data Loading Patterns (Last 7 Days)' AS METRIC_NAME,
    DATE_TRUNC('day', LAST_LOAD_TIME) AS DATE,
    TABLE_NAME,
    SUM(ROW_COUNT) AS ROWS_LOADED,
    SUM(FILE_SIZE) / (1024*1024) AS MB_LOADED,
    COUNT(DISTINCT FILE_NAME) AS FILES_LOADED,
    COUNT(CASE WHEN STATUS = 'LOADED' THEN 1 END) AS SUCCESSFUL_LOADS,
    COUNT(CASE WHEN STATUS = 'LOAD_FAILED' THEN 1 END) AS FAILED_LOADS
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE LAST_LOAD_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND TABLE_NAME LIKE 'FIN_TRADE_EXTRACT.RAW.%'
GROUP BY DATE, TABLE_NAME
ORDER BY DATE DESC, MB_LOADED DESC;

-- File processing efficiency
SELECT 
    'File Processing Efficiency' AS METRIC_NAME,
    PIPE_NAME,
    AVG(FILE_SIZE) / (1024*1024) AS AVG_FILE_SIZE_MB,
    AVG(ROW_COUNT) AS AVG_ROWS_PER_FILE,
    SUM(FILE_SIZE) / SUM(ROW_COUNT) AS AVG_BYTES_PER_ROW,
    COUNT(*) AS TOTAL_FILES_PROCESSED
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE LAST_LOAD_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND PIPE_NAME LIKE 'FIN_TRADE_EXTRACT.RAW.%'
  AND STATUS = 'LOADED'
GROUP BY PIPE_NAME
ORDER BY TOTAL_FILES_PROCESSED DESC;

-- ============================================================================
-- COST SUMMARY DASHBOARD
-- ============================================================================

-- Total estimated costs (last 7 days)
WITH warehouse_costs AS (
    SELECT SUM(CREDITS_USED) * 2.0 AS warehouse_cost
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
      AND WAREHOUSE_NAME = 'FIN_TRADE_WH'
),
snowpipe_costs AS (
    SELECT SUM(CREDITS_USED) * 2.0 AS snowpipe_cost
    FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
    WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
      AND PIPE_NAME LIKE 'FIN_TRADE_EXTRACT.RAW.%'
),
storage_costs AS (
    SELECT AVG(STORAGE_BYTES / (1024*1024*1024)) * 0.00137 * 7 AS weekly_storage_cost
    FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE
    WHERE USAGE_DATE >= DATEADD('day', -7, CURRENT_DATE())
)
SELECT 
    'Cost Summary (Last 7 Days)' AS METRIC_NAME,
    ROUND(COALESCE(wc.warehouse_cost, 0), 2) AS WAREHOUSE_COST_USD,
    ROUND(COALESCE(sc.snowpipe_cost, 0), 2) AS SNOWPIPE_COST_USD,
    ROUND(COALESCE(st.weekly_storage_cost, 0), 2) AS STORAGE_COST_USD,
    ROUND(COALESCE(wc.warehouse_cost, 0) + COALESCE(sc.snowpipe_cost, 0) + COALESCE(st.weekly_storage_cost, 0), 2) AS TOTAL_WEEKLY_COST_USD,
    ROUND((COALESCE(wc.warehouse_cost, 0) + COALESCE(sc.snowpipe_cost, 0) + COALESCE(st.weekly_storage_cost, 0)) * 52/12, 2) AS ESTIMATED_MONTHLY_COST_USD
FROM warehouse_costs wc
CROSS JOIN snowpipe_costs sc
CROSS JOIN storage_costs st;

-- ============================================================================
-- PERFORMANCE RECOMMENDATIONS
-- ============================================================================

-- Tables that might benefit from clustering
SELECT 
    'Clustering Recommendations' AS METRIC_NAME,
    TABLE_NAME,
    ROW_COUNT,
    BYTES / (1024*1024*1024) AS SIZE_GB,
    CLUSTERING_KEY,
    CASE 
        WHEN CLUSTERING_KEY IS NULL AND ROW_COUNT > 1000000 THEN 'Consider adding clustering key'
        WHEN ROW_COUNT < 100000 THEN 'Too small for clustering'
        ELSE 'Clustering configured'
    END AS RECOMMENDATION
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
WHERE TABLE_SCHEMA = 'RAW'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY ROW_COUNT DESC;

-- Queries that might benefit from optimization
SELECT 
    'Query Optimization Opportunities' AS METRIC_NAME,
    QUERY_TYPE,
    COUNT(*) AS QUERY_COUNT,
    AVG(EXECUTION_TIME / 1000) AS AVG_EXECUTION_TIME_SECONDS,
    MAX(EXECUTION_TIME / 1000) AS MAX_EXECUTION_TIME_SECONDS,
    CASE 
        WHEN AVG(EXECUTION_TIME / 1000) > 60 THEN 'Review for optimization'
        WHEN MAX(EXECUTION_TIME / 1000) > 300 THEN 'Some queries are very slow'
        ELSE 'Performance looks good'
    END AS RECOMMENDATION
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
  AND WAREHOUSE_NAME = 'FIN_TRADE_WH'
  AND EXECUTION_STATUS = 'SUCCESS'
GROUP BY QUERY_TYPE
ORDER BY AVG_EXECUTION_TIME_SECONDS DESC;