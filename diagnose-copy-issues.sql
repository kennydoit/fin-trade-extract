-- ============================================================================
-- SNOWFLAKE DIAGNOSTIC AND DIRECT LOAD
-- Diagnose why COPY INTO isn't working and try alternative approaches
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: COMPREHENSIVE DIAGNOSTICS
-- ============================================================================

-- Check current state
SELECT COUNT(*) as current_records FROM RAW.OVERVIEW;

-- Show stages and their configurations
SHOW STAGES IN SCHEMA RAW;

-- Describe the OVERVIEW_STAGE to see its exact configuration
DESC STAGE OVERVIEW_STAGE;

-- List files in the stage (this should show all our CSV files)
LIST @OVERVIEW_STAGE;

-- ============================================================================
-- STEP 2: TEST FILE ACCESS - SAMPLE ONE FILE
-- ============================================================================

-- Try to select raw data from one specific file to see if we can access it
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
FROM @OVERVIEW_STAGE/overview_20250928_035238_aad23b30.csv
LIMIT 5;

-- ============================================================================
-- STEP 3: CHECK COPY INTO HISTORY
-- ============================================================================

-- Check the copy history to see what happened with our previous attempts
SELECT 
    FILE_NAME,
    STATUS,
    ERROR_COUNT,
    ERROR_LIMIT,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUMBER,
    FIRST_ERROR_CHARACTER_POS,
    FIRST_ERROR_COLUMN_NAME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'FIN_TRADE_EXTRACT.RAW.OVERVIEW',
    START_TIME => DATEADD(hours, -2, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC;

-- ============================================================================
-- STEP 4: TRY DIFFERENT FILE FORMAT
-- ============================================================================

-- Create a more flexible file format
CREATE OR REPLACE FILE FORMAT FLEXIBLE_CSV_FORMAT
TYPE = 'CSV'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
REPLACE_INVALID_CHARACTERS = TRUE
EMPTY_FIELD_AS_NULL = TRUE
NULL_IF = ('', 'NULL', 'null', 'None', 'N/A')
ESCAPE_UNENCLOSED_FIELD = NONE
COMMENT = 'More flexible CSV format for troubleshooting';

-- Try loading with the flexible format
COPY INTO RAW.OVERVIEW
FROM @OVERVIEW_STAGE
FILES = ('overview_20250928_035238_aad23b30.csv')
FILE_FORMAT = FLEXIBLE_CSV_FORMAT
ON_ERROR = 'CONTINUE'
VALIDATION_MODE = 'RETURN_ERRORS';

-- ============================================================================
-- STEP 5: CREATE TEMPORARY STAGE WITH DIFFERENT CONFIG
-- ============================================================================

-- Create a new stage with minimal configuration to test
CREATE OR REPLACE STAGE TEMP_OVERVIEW_STAGE
URL = 's3://fin-trade-craft-landing/overview/'
STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
FILE_FORMAT = (
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
);

-- List files in the new stage
LIST @TEMP_OVERVIEW_STAGE;

-- Try loading from the new stage
COPY INTO RAW.OVERVIEW
FROM @TEMP_OVERVIEW_STAGE
FILES = ('overview_20250928_035238_aad23b30.csv')
ON_ERROR = 'CONTINUE'
VALIDATION_MODE = 'RETURN_ERRORS';

-- ============================================================================
-- STEP 6: VALIDATION MODE TEST
-- ============================================================================

-- Run in validation mode to see what errors we get without actually loading
COPY INTO RAW.OVERVIEW
FROM @OVERVIEW_STAGE
FILES = ('overview_20250928_035238_aad23b30.csv')
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
VALIDATION_MODE = 'RETURN_ALL_ERRORS';

-- ============================================================================
-- STEP 7: CHECK TABLE STRUCTURE
-- ============================================================================

-- Describe the OVERVIEW table to make sure columns match
DESC TABLE RAW.OVERVIEW;

-- ============================================================================
-- STEP 8: MANUAL INSPECTION OF FILE CONTENT
-- ============================================================================

-- Look at the actual content structure of the file
SELECT 
    $1 as col1,
    $2 as col2, 
    $3 as col3,
    $4 as col4,
    $5 as col5,
    $6 as col6,
    $7 as col7,
    $8 as col8,
    $9 as col9,
    $10 as col10,
    $11 as col11,
    $12 as col12
FROM @OVERVIEW_STAGE/overview_20250928_035238_aad23b30.csv
LIMIT 10;