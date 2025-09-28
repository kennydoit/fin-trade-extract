-- ============================================================================
-- TARGETED SNOWFLAKE DATA INGESTION
-- Load specific files that we know exist in S3
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- Check current record count
SELECT COUNT(*) as current_record_count FROM RAW.OVERVIEW;

-- ============================================================================
-- STEP 1: TEST STAGE CONNECTIVITY
-- ============================================================================

-- List files in the stage to verify connectivity
LIST @OVERVIEW_STAGE;

-- ============================================================================
-- STEP 2: LOAD SPECIFIC LARGE FILES FROM TODAY
-- ============================================================================

-- Load the large files one by one to ensure they get processed
-- File 1: overview_20250928_035238_aad23b30.csv (55KB - ~46 companies)
COPY INTO RAW.OVERVIEW
FROM @OVERVIEW_STAGE
FILES = ('overview_20250928_035238_aad23b30.csv')
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- Check progress
SELECT COUNT(*) as records_after_file1 FROM RAW.OVERVIEW;

-- File 2: overview_20250928_035431_559ca0e6.csv (106KB - ~47 companies)
COPY INTO RAW.OVERVIEW
FROM @OVERVIEW_STAGE  
FILES = ('overview_20250928_035431_559ca0e6.csv')
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- Check progress
SELECT COUNT(*) as records_after_file2 FROM RAW.OVERVIEW;

-- File 3: overview_20250928_035537_6cbdb789.csv (57KB - ~49 companies)
COPY INTO RAW.OVERVIEW
FROM @OVERVIEW_STAGE
FILES = ('overview_20250928_035537_6cbdb789.csv')
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- File 4: overview_20250928_035702_49a938d1.csv (113KB - another large batch)
COPY INTO RAW.OVERVIEW
FROM @OVERVIEW_STAGE
FILES = ('overview_20250928_035702_49a938d1.csv')
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- File 5: overview_20250928_125259_5c81f779.csv (60KB)
COPY INTO RAW.OVERVIEW
FROM @OVERVIEW_STAGE
FILES = ('overview_20250928_125259_5c81f779.csv')
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- File 6: overview_20250928_125427_6e8be71b.csv (54KB)
COPY INTO RAW.OVERVIEW
FROM @OVERVIEW_STAGE
FILES = ('overview_20250928_125427_6e8be71b.csv')
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- File 7: overview_20250928_125458_6d0eb07b.csv (56KB)
COPY INTO RAW.OVERVIEW
FROM @OVERVIEW_STAGE
FILES = ('overview_20250928_125458_6d0eb07b.csv')
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- File 8: overview_20250928_125529_aca3aa9e.csv (59KB)
COPY INTO RAW.OVERVIEW
FROM @OVERVIEW_STAGE
FILES = ('overview_20250928_125529_aca3aa9e.csv')
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- STEP 3: VERIFY ALL DATA LOADED
-- ============================================================================

-- Final record count
SELECT COUNT(*) as total_records_final FROM RAW.OVERVIEW;

-- Check unique companies
SELECT COUNT(DISTINCT SYMBOL) as unique_companies FROM RAW.OVERVIEW;

-- Show sample of newly loaded data
SELECT SYMBOL, NAME, SECTOR, MARKET_CAPITALIZATION, LOAD_TIMESTAMP
FROM RAW.OVERVIEW 
ORDER BY LOAD_TIMESTAMP DESC 
LIMIT 25;

-- Summary by sector
SELECT 
    SECTOR,
    COUNT(*) as company_count
FROM RAW.OVERVIEW 
GROUP BY SECTOR 
ORDER BY company_count DESC;

-- ============================================================================
-- STEP 4: CHECK FOR DUPLICATES
-- ============================================================================

-- Check if any symbols are duplicated (they shouldn't be)
SELECT 
    SYMBOL, 
    COUNT(*) as duplicate_count
FROM RAW.OVERVIEW 
GROUP BY SYMBOL
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;