-- ============================================================================
-- SNOWFLAKE DATA INGESTION FIX
-- Force load the S3 CSV files that haven't been automatically ingested
-- ============================================================================

-- First, let's check what we currently have
SELECT COUNT(*) as current_record_count FROM RAW.OVERVIEW;

-- Check the most recent records
SELECT SYMBOL, LOAD_TIMESTAMP, BATCH_ID, SOURCE_FILE_NAME 
FROM RAW.OVERVIEW 
ORDER BY LOAD_TIMESTAMP DESC 
LIMIT 10;

-- ============================================================================
-- MANUAL DATA LOADING FROM S3 FILES
-- Load the CSV files that Snowpipe missed
-- ============================================================================

-- Load the large batch files directly
COPY INTO RAW.OVERVIEW
FROM @RAW.S3_STAGE/overview/
PATTERN = '.*overview_20250928_035238.*\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO RAW.OVERVIEW  
FROM @RAW.S3_STAGE/overview/
PATTERN = '.*overview_20250928_035431.*\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO RAW.OVERVIEW
FROM @RAW.S3_STAGE/overview/
PATTERN = '.*overview_20250928_035537.*\.csv' 
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

COPY INTO RAW.OVERVIEW
FROM @RAW.S3_STAGE/overview/
PATTERN = '.*overview_20250928_035702.*\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load today's files too
COPY INTO RAW.OVERVIEW
FROM @RAW.S3_STAGE/overview/
PATTERN = '.*overview_20250928_124.*\.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- ============================================================================
-- VERIFY THE DATA LOADED
-- ============================================================================

-- Check total record count now
SELECT COUNT(*) as total_records FROM RAW.OVERVIEW;

-- Check the newest records
SELECT SYMBOL, NAME, SECTOR, MARKET_CAPITALIZATION, LOAD_TIMESTAMP
FROM RAW.OVERVIEW 
ORDER BY LOAD_TIMESTAMP DESC 
LIMIT 20;

-- Check unique companies
SELECT COUNT(DISTINCT SYMBOL) as unique_companies FROM RAW.OVERVIEW;

-- ============================================================================
-- CHECK SNOWPIPE STATUS (Diagnostic)
-- ============================================================================

-- Check if Snowpipe is running
SHOW PIPES IN SCHEMA RAW;

-- Check pipe history 
SELECT *
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
  DATE_RANGE_START => DATEADD('day',-1,CURRENT_DATE()),
  DATE_RANGE_END => CURRENT_DATE(),
  PIPE_NAME => 'RAW.OVERVIEW_PIPE'
));

-- ============================================================================
-- REFRESH SNOWPIPE (Force it to check for new files)
-- ============================================================================

-- Force Snowpipe to refresh and check for new files
ALTER PIPE RAW.OVERVIEW_PIPE REFRESH;

-- Wait a moment, then check if more files were loaded
-- SELECT COUNT(*) FROM RAW.OVERVIEW;