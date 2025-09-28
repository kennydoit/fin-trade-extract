-- ============================================================================
-- Snowflake Pipeline Debug Script
-- Run this step by step to identify data loading issues
-- ============================================================================

-- Set context
USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;
USE SCHEMA RAW;

-- ============================================================================
-- Step 1: Check basic setup
-- ============================================================================
SELECT 'STEP 1: Basic Setup Check' AS debug_step;

-- Verify tables exist
SHOW TABLES IN SCHEMA RAW;

-- Check table structure
DESC TABLE OVERVIEW;

-- Check if table is empty
SELECT COUNT(*) AS row_count FROM OVERVIEW;

-- ============================================================================
-- Step 2: Check external stages
-- ============================================================================
SELECT 'STEP 2: External Stage Check' AS debug_step;

-- Verify stages exist
SHOW STAGES IN SCHEMA RAW;

-- List files in the overview stage
LIST @OVERVIEW_STAGE;

-- Try to list files with more details
LIST @OVERVIEW_STAGE PATTERN='*.csv';

-- ============================================================================
-- Step 3: Check Snowpipes
-- ============================================================================
SELECT 'STEP 3: Snowpipe Check' AS debug_step;

-- Show all pipes
SHOW PIPES IN SCHEMA RAW;

-- Check pipe status (use correct syntax)
SELECT SYSTEM$PIPE_STATUS('RAW.OVERVIEW_PIPE') AS overview_pipe_status;

-- Get pipe details
DESC PIPE OVERVIEW_PIPE;

-- ============================================================================
-- Step 4: Check copy history (use correct schema)
-- ============================================================================
SELECT 'STEP 4: Copy History Check' AS debug_step;

-- Check copy history from account level (not database level)
SELECT 
    TABLE_NAME,
    PIPE_NAME,
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    ERROR_LIMIT,
    STATUS,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY 
WHERE TABLE_NAME = 'OVERVIEW' 
    AND TABLE_SCHEMA = 'RAW'
    AND TABLE_CATALOG = 'FIN_TRADE_EXTRACT'
ORDER BY LAST_LOAD_TIME DESC 
LIMIT 10;

-- Alternative: Check load history
SELECT 
    SCHEMA_NAME,
    TABLE_NAME,
    FILE_NAME,
    ROW_COUNT,
    ERROR_COUNT,
    STATUS,
    LAST_LOAD_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY 
WHERE TABLE_NAME = 'OVERVIEW'
    AND SCHEMA_NAME = 'RAW'
ORDER BY LAST_LOAD_TIME DESC 
LIMIT 10;

-- ============================================================================
-- Step 5: Test manual data loading
-- ============================================================================
SELECT 'STEP 5: Manual Load Test' AS debug_step;

-- Try to manually copy data from stage (if files exist)
-- This will help identify if the issue is with pipes or the data itself
-- COPY INTO OVERVIEW 
-- FROM @OVERVIEW_STAGE 
-- FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
-- ON_ERROR = 'CONTINUE';

-- ============================================================================
-- Step 6: Check S3 bucket directly
-- ============================================================================
SELECT 'STEP 6: S3 Integration Check' AS debug_step;

-- Check storage integration
DESC STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION;

-- ============================================================================
-- Step 7: Pipe refresh (if needed)
-- ============================================================================
SELECT 'STEP 7: Pipe Refresh Commands' AS debug_step;

-- If pipes are stuck, you can refresh them manually:
-- ALTER PIPE OVERVIEW_PIPE REFRESH;

-- ============================================================================
-- Summary queries to run
-- ============================================================================
SELECT 'SUMMARY: Key Information to Check' AS debug_step;

SELECT 'Check these items:' AS instruction,
       '1. Are there files in @OVERVIEW_STAGE?' AS check1,
       '2. What does SYSTEM$PIPE_STATUS show?' AS check2,
       '3. Any errors in COPY_HISTORY?' AS check3,
       '4. Is storage integration working?' AS check4;