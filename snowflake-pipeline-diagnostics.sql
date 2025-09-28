-- ============================================================================
-- Comprehensive Snowflake Pipeline Diagnostics
-- Debug why real data isn't appearing in Snowflake despite successful Lambda
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: Current Table Status
-- ============================================================================

SELECT 'CURRENT TABLE STATUS' as step;

-- Check what's currently in the table
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    COUNT(CASE WHEN NAME IS NOT NULL AND TRIM(NAME) != '' THEN 1 END) as records_with_names,
    COUNT(CASE WHEN MARKET_CAPITALIZATION IS NOT NULL THEN 1 END) as records_with_market_cap,
    MIN(LOADED_AT) as earliest_load,
    MAX(LOADED_AT) as latest_load
FROM OVERVIEW;

-- Show recent records
SELECT 'RECENT RECORDS' as step;
SELECT SYMBOL, NAME, MARKET_CAPITALIZATION, LOADED_AT 
FROM OVERVIEW 
ORDER BY LOADED_AT DESC 
LIMIT 10;

-- ============================================================================
-- STEP 2: Snowpipe Status Check
-- ============================================================================

SELECT 'SNOWPIPE STATUS' as step;

-- Check if pipe is running
SELECT SYSTEM$PIPE_STATUS('OVERVIEW_PIPE') as pipe_status;

-- Check pipe definition
DESC PIPE OVERVIEW_PIPE;

-- ============================================================================
-- STEP 3: Check External Stage
-- ============================================================================

SELECT 'EXTERNAL STAGE CHECK' as step;

-- List files in stage (should include our new real data file)
LIST @OVERVIEW_STAGE;

-- Look specifically for our test file
LIST @OVERVIEW_STAGE PATTERN='*real-api-test*';

-- ============================================================================
-- STEP 4: Check Copy History for Loading Issues
-- ============================================================================

SELECT 'COPY HISTORY - RECENT ACTIVITY' as step;

-- Check recent loading attempts
SELECT 
    FILE_NAME,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    STATUS,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY 
WHERE TABLE_NAME = 'OVERVIEW' 
    AND TABLE_SCHEMA = 'RAW'
ORDER BY LAST_LOAD_TIME DESC 
LIMIT 10;

-- Check for specific errors
SELECT 'LOADING ERRORS' as step;
SELECT 
    FILE_NAME,
    STATUS,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY 
WHERE TABLE_NAME = 'OVERVIEW' 
    AND TABLE_SCHEMA = 'RAW'
    AND STATUS != 'LOADED'
ORDER BY LAST_LOAD_TIME DESC;

-- ============================================================================
-- STEP 5: Manual Load Test
-- ============================================================================

SELECT 'MANUAL LOAD TEST' as step;

-- Try to manually load from stage to see what happens
COPY INTO OVERVIEW 
FROM @OVERVIEW_STAGE 
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
PATTERN = '.*real-api-test.*'
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;

-- Check if manual load worked
SELECT 'POST MANUAL LOAD CHECK' as step;
SELECT COUNT(*) as records_after_manual_load FROM OVERVIEW;

-- Show any new records
SELECT 'NEWEST RECORDS' as step;
SELECT SYMBOL, NAME, MARKET_CAPITALIZATION, LOADED_AT 
FROM OVERVIEW 
WHERE LOADED_AT > DATEADD(MINUTE, -10, CURRENT_TIMESTAMP())
ORDER BY LOADED_AT DESC;

-- ============================================================================
-- STEP 6: File Format and Stage Validation
-- ============================================================================

SELECT 'FILE FORMAT CHECK' as step;
SHOW FILE FORMATS IN SCHEMA RAW;

SELECT 'STAGE VALIDATION' as step;
SHOW STAGES IN SCHEMA RAW;

-- ============================================================================
-- STEP 7: Permissions and Configuration Check
-- ============================================================================

SELECT 'STORAGE INTEGRATION CHECK' as step;
DESC STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION;

-- ============================================================================
-- DIAGNOSTIC SUMMARY
-- ============================================================================

SELECT 'DIAGNOSTIC SUMMARY' as step;

-- Summary of findings
SELECT 
    'Pipeline Component' as component,
    'Status' as status,
    'Notes' as notes
    
UNION ALL

SELECT 
    'Lambda Function',
    'WORKING',
    'Successfully creating CSV files with real data'
    
UNION ALL

SELECT 
    'S3 Bucket',
    'WORKING', 
    'Files uploading correctly - real-api-test file is 1823 bytes'
    
UNION ALL

SELECT 
    'External Stage',
    CASE WHEN EXISTS(SELECT 1 FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE "name" LIKE '%real-api-test%') 
         THEN 'WORKING' ELSE 'ISSUE' END,
    'Check if files visible in @OVERVIEW_STAGE'
    
UNION ALL

SELECT 
    'Snowpipe',
    CASE WHEN SYSTEM$PIPE_STATUS('OVERVIEW_PIPE') LIKE '%running%' 
         THEN 'RUNNING' ELSE 'CHECK_NEEDED' END,
    'Automatic data loading from S3'
    
UNION ALL

SELECT 
    'Data Loading',
    CASE WHEN (SELECT COUNT(*) FROM OVERVIEW WHERE NAME IS NOT NULL AND TRIM(NAME) != '') > 0 
         THEN 'SUCCESS' ELSE 'PENDING' END,
    'Real financial data in table';

-- ============================================================================
-- NEXT STEPS RECOMMENDATIONS
-- ============================================================================

SELECT 'NEXT STEPS' as step;

SELECT 
    'If manual COPY worked but Snowpipe didnt:' as scenario,
    'Check S3 bucket notifications and Snowpipe notification channel' as action
    
UNION ALL

SELECT 
    'If manual COPY failed:',
    'Check file format, stage configuration, or table schema mismatch'
    
UNION ALL

SELECT 
    'If files not visible in stage:',
    'Check storage integration permissions and S3 access'
    
UNION ALL

SELECT 
    'If everything looks good but no data:',
    'Snowpipe might need 1-5 minutes to process new files';