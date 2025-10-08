-- DEBUG VERSION: Load company overview CSV files to see actual structure
-- This version focuses on getting ANY data loaded first, then we can optimize

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ACCOUNTADMIN;

-- 1) Create stage if needed
CREATE STAGE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGE
  URL='s3://fin-trade-craft-landing/company_overview/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION;

-- 2) Create simple staging table - ALL VARCHAR to avoid type issues
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING (
  col1 VARCHAR(500),
  col2 VARCHAR(500),
  col3 VARCHAR(500),
  col4 VARCHAR(500),
  col5 VARCHAR(500),
  col6 VARCHAR(500),
  col7 VARCHAR(500),
  col8 VARCHAR(500),
  col9 VARCHAR(500),
  col10 VARCHAR(500),
  col11 VARCHAR(500),
  col12 VARCHAR(500),
  col13 VARCHAR(500),
  col14 VARCHAR(500),
  col15 VARCHAR(500),
  col16 VARCHAR(500),
  col17 VARCHAR(500),
  col18 VARCHAR(500),
  col19 VARCHAR(500),
  col20 VARCHAR(500),
  col21 VARCHAR(500),
  col22 VARCHAR(500),
  col23 VARCHAR(500),
  col24 VARCHAR(500),
  col25 VARCHAR(500),
  col26 VARCHAR(500),
  col27 VARCHAR(500),
  col28 VARCHAR(500),
  col29 VARCHAR(500),
  col30 VARCHAR(500),
  col31 VARCHAR(500),
  col32 VARCHAR(500),
  col33 VARCHAR(500),
  col34 VARCHAR(500),
  col35 VARCHAR(500),
  col36 VARCHAR(500),
  col37 VARCHAR(500),
  col38 VARCHAR(500),
  col39 VARCHAR(500),
  col40 VARCHAR(500),
  source_file VARCHAR(500)
);

-- Debug: Check what files are in the stage
SELECT 'Files in S3 stage:' as MESSAGE;
LIST @COMPANY_OVERVIEW_STAGE;

-- Debug: Check if RAW_CSV_FORMAT exists
SHOW FILE FORMATS LIKE 'RAW_CSV_FORMAT';

-- Try loading with minimal columns first
COPY INTO FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
FROM (
  SELECT 
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
    $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
    $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
    $31, $32, $33, $34, $35, $36, $37, $38, $39, $40,
    METADATA$FILENAME
  FROM @COMPANY_OVERVIEW_STAGE
)
FILE_FORMAT = (
  TYPE = 'CSV',
  FIELD_DELIMITER = ',',
  RECORD_DELIMITER = '\n',
  SKIP_HEADER = 1,
  FIELD_OPTIONALLY_ENCLOSED_BY = '"',
  ESCAPE_CHAR = '\\',
  NULL_IF = ('', 'NULL', 'null', 'None'),
  EMPTY_FIELD_AS_NULL = TRUE,
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
PATTERN = 'overview_.*\\.csv'
ON_ERROR = CONTINUE;

-- Debug: Check load results
SELECT 'Load Results:' as MESSAGE;

SELECT 
    'Total rows loaded:' as METRIC,
    COUNT(*) as VALUE
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING;

SELECT 
    'Files processed:' as METRIC,
    COUNT(DISTINCT source_file) as VALUE
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING;

-- Show files and row counts
SELECT 
    source_file,
    COUNT(*) as rows_per_file
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
GROUP BY source_file
ORDER BY source_file;

-- Show first few columns from first row to understand structure
SELECT 'First 10 columns from first row:' as MESSAGE;
SELECT 
    col1, col2, col3, col4, col5, 
    col6, col7, col8, col9, col10,
    source_file
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
LIMIT 3;

-- Show columns 11-20
SELECT 'Columns 11-20 from first row:' as MESSAGE;
SELECT 
    col11, col12, col13, col14, col15,
    col16, col17, col18, col19, col20,
    source_file
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
LIMIT 3;

-- Show columns 21-30
SELECT 'Columns 21-30 from first row:' as MESSAGE;
SELECT 
    col21, col22, col23, col24, col25,
    col26, col27, col28, col29, col30,
    source_file
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
LIMIT 3;

-- Show columns 31-40
SELECT 'Columns 31-40 from first row:' as MESSAGE;
SELECT 
    col31, col32, col33, col34, col35,
    col36, col37, col38, col39, col40,
    source_file
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
LIMIT 3;

-- Look for any null patterns
SELECT 'Null pattern analysis:' as MESSAGE;
SELECT 
    'Non-null columns in first row:' as analysis,
    CASE WHEN col1 IS NOT NULL THEN 'col1' END,
    CASE WHEN col2 IS NOT NULL THEN 'col2' END,
    CASE WHEN col3 IS NOT NULL THEN 'col3' END,
    CASE WHEN col4 IS NOT NULL THEN 'col4' END,
    CASE WHEN col5 IS NOT NULL THEN 'col5' END,
    CASE WHEN col6 IS NOT NULL THEN 'col6' END,
    CASE WHEN col7 IS NOT NULL THEN 'col7' END,
    CASE WHEN col8 IS NOT NULL THEN 'col8' END,
    CASE WHEN col9 IS NOT NULL THEN 'col9' END,
    CASE WHEN col10 IS NOT NULL THEN 'col10' END
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
LIMIT 1;

-- NO DELETE STATEMENTS - KEEP ALL DATA FOR DEBUGGING

SELECT 'Debug staging table created successfully! Check the output above to understand the CSV structure.' as COMPLETION_MESSAGE;

-- Do NOT drop the staging table
SELECT 'Staging table preserved for debugging' as PRESERVATION_MESSAGE;