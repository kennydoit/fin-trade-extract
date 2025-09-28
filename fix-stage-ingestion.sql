-- ============================================================================
-- SNOWFLAKE STAGE INGESTION FIX
-- Fix the stage reference issue and load all pending S3 CSV files
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: VERIFY STAGE EXISTS AND CHECK FILES
-- ============================================================================

-- Show all stages to confirm OVERVIEW_STAGE exists
SHOW STAGES IN SCHEMA RAW;

-- List files in the overview stage to see what's available
LIST @OVERVIEW_STAGE;

-- Check current record count
SELECT COUNT(*) as current_record_count FROM RAW.OVERVIEW;

-- ============================================================================
-- STEP 2: LOAD ALL PENDING FILES FROM TODAY (2025-09-28)
-- ============================================================================

-- Load all the files we created today that contain the 500+ companies
COPY INTO RAW.OVERVIEW
FROM @OVERVIEW_STAGE
PATTERN = '.*overview_20250928.*\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- STEP 3: VERIFY DATA WAS LOADED
-- ============================================================================

-- Check total record count now
SELECT COUNT(*) as total_records_after_load FROM RAW.OVERVIEW;

-- Check unique companies loaded
SELECT COUNT(DISTINCT SYMBOL) as unique_companies FROM RAW.OVERVIEW;

-- Show the most recent records to verify they loaded correctly
SELECT SYMBOL, NAME, SECTOR, MARKET_CAPITALIZATION, LOAD_TIMESTAMP
FROM RAW.OVERVIEW 
WHERE LOAD_TIMESTAMP >= CURRENT_DATE()
ORDER BY LOAD_TIMESTAMP DESC 
LIMIT 20;

-- ============================================================================
-- STEP 4: CREATE ALIAS STAGE (OPTIONAL - FOR FUTURE COMPATIBILITY)
-- ============================================================================

-- Create an alias stage called S3_STAGE that points to the same location
-- This ensures future scripts that reference S3_STAGE will work
CREATE OR REPLACE STAGE S3_STAGE
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
URL = 's3://fin-trade-craft-landing/'
FILE_FORMAT = CSV_FORMAT
COMMENT = 'Alias stage for backward compatibility with S3_STAGE references';

-- ============================================================================
-- STEP 5: CHECK SNOWPIPE STATUS AND REFRESH
-- ============================================================================

-- Check if Snowpipe exists and is running
SHOW PIPES IN SCHEMA RAW;

-- If the pipe exists, refresh it to pick up any remaining files
-- ALTER PIPE RAW.OVERVIEW_PIPE REFRESH;

-- ============================================================================
-- STEP 6: FINAL VERIFICATION
-- ============================================================================

-- Show summary of all loaded data
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    MIN(LOAD_TIMESTAMP) as earliest_load,
    MAX(LOAD_TIMESTAMP) as latest_load,
    COUNT(CASE WHEN LOAD_TIMESTAMP >= CURRENT_DATE() THEN 1 END) as records_loaded_today
FROM RAW.OVERVIEW;

-- Show breakdown by sector for the newly loaded companies
SELECT 
    SECTOR,
    COUNT(*) as company_count,
    COUNT(CASE WHEN LOAD_TIMESTAMP >= CURRENT_DATE() THEN 1 END) as loaded_today
FROM RAW.OVERVIEW 
GROUP BY SECTOR 
ORDER BY company_count DESC;