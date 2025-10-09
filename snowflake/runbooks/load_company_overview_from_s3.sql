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

-- 2) DROP existing table and create fresh optimized COMPANY_OVERVIEW table 
-- This removes the old 40+ column structure with expensive financial metrics
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW;

-- Create main COMPANY_OVERVIEW table with only essential fields for cost optimization
CREATE TABLE FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW (
  SYMBOL_ID NUMBER(38,0),
  SYMBOL VARCHAR(20),
  ASSET_TYPE VARCHAR(50),
  NAME VARCHAR(500),
  DESCRIPTION TEXT,
  CIK VARCHAR(20),
  EXCHANGE VARCHAR(50),
  CURRENCY VARCHAR(10),
  COUNTRY VARCHAR(100),
  SECTOR VARCHAR(100),
  INDUSTRY VARCHAR(200),
  ADDRESS TEXT,
  OFFICIAL_SITE VARCHAR(500),
  FISCAL_YEAR_END VARCHAR(50),
  LATEST_QUARTER DATE,
  PROCESSED_DATE DATE,
  LOAD_DATE VARCHAR(50),
  UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 3) Drop and recreate staging table to match exact Alpha Vantage API field names
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING;

-- Also clean up any stale data in main table that might have wrong LOAD_DATE values
DELETE FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW 
WHERE LOAD_DATE IN ('621', '631') OR LOAD_DATE IS NULL OR LENGTH(LOAD_DATE) < 10;

CREATE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING (
  -- Essential Alpha Vantage API field names only (cost optimized)
  Symbol VARCHAR(500),
  AssetType VARCHAR(500),
  Name VARCHAR(500),
  Description VARCHAR(2000),
  CIK VARCHAR(500),
  Exchange VARCHAR(500),
  Currency VARCHAR(500),
  Country VARCHAR(500),
  Sector VARCHAR(500),
  Industry VARCHAR(500),
  Address VARCHAR(2000),
  OfficialSite VARCHAR(500),
  FiscalYearEnd VARCHAR(500),
  LatestQuarter VARCHAR(500),
  
  -- Processing metadata (added by Python script)
  SYMBOL_ID VARCHAR(500),
  PROCESSED_DATE VARCHAR(500),
  LOAD_DATE VARCHAR(500),
  
  -- File tracking (automatically added by Snowflake COPY command)
  source_file VARCHAR(500) DEFAULT NULL
);

-- Debug: Check what files are in the stage
SELECT 'Files in S3 stage:' as MESSAGE;
LIST @COMPANY_OVERVIEW_STAGE;

-- Create a specific file format for company overview CSV with proper header parsing
CREATE OR REPLACE FILE FORMAT COMPANY_OVERVIEW_CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ESCAPE = '\\'
  NULL_IF = ('', 'NULL', 'null', 'None')
  EMPTY_FIELD_AS_NULL = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- Load CSV files using MATCH_BY_COLUMN_NAME to handle different column orders
COPY INTO FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
FROM @COMPANY_OVERVIEW_STAGE
FILE_FORMAT = COMPANY_OVERVIEW_CSV_FORMAT
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = 'overview_.*\\.csv'
ON_ERROR = CONTINUE;

-- Update source_file column with actual filenames after the load
UPDATE FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
SET source_file = CONCAT('overview_', Symbol, '_', LOAD_DATE, '.csv')
WHERE source_file IS NULL AND Symbol IS NOT NULL AND LOAD_DATE IS NOT NULL;

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

-- Show sample data with proper column names
SELECT 'Core company information:' as MESSAGE;
SELECT 
    Symbol, Name, Exchange, AssetType, Country, Sector, Industry,
    source_file
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
LIMIT 3;

-- Show key fiscal fields you mentioned
SELECT 'Key fiscal data fields:' as MESSAGE;
SELECT 
    Symbol, FiscalYearEnd, LatestQuarter,
    source_file
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
LIMIT 3;

-- Show processing metadata
SELECT 'Processing metadata:' as MESSAGE;
SELECT 
    Symbol, SYMBOL_ID, PROCESSED_DATE, LOAD_DATE,
    source_file
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
LIMIT 3;

-- Check for data completeness in key fields
SELECT 'Data completeness analysis:' as MESSAGE;
SELECT 
    COUNT(*) as total_rows,
    COUNT(Symbol) as symbols_with_data,
    COUNT(FiscalYearEnd) as fiscal_year_end_populated,
    COUNT(LatestQuarter) as latest_quarter_populated,
    COUNT(Name) as company_name_populated
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING;

-- NO DELETE STATEMENTS - KEEP ALL DATA FOR DEBUGGING

-- Transfer data from staging to main table with only essential fields (cost optimized)
INSERT INTO FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW (
  SYMBOL_ID, SYMBOL, ASSET_TYPE, NAME, DESCRIPTION, CIK, EXCHANGE, CURRENCY, COUNTRY,
  SECTOR, INDUSTRY, ADDRESS, OFFICIAL_SITE, FISCAL_YEAR_END, LATEST_QUARTER, 
  PROCESSED_DATE, LOAD_DATE, UPDATED_AT
)
SELECT 
  TRY_TO_NUMBER(SYMBOL_ID) as SYMBOL_ID,
  Symbol,
  AssetType as ASSET_TYPE,
  Name,
  Description,
  CIK,
  Exchange,
  Currency,
  Country,
  Sector,
  Industry,
  Address,
  OfficialSite as OFFICIAL_SITE,
  FiscalYearEnd as FISCAL_YEAR_END,
  TRY_TO_DATE(LatestQuarter, 'YYYY-MM-DD') as LATEST_QUARTER,
  TRY_TO_DATE(PROCESSED_DATE, 'YYYY-MM-DD') as PROCESSED_DATE,
  LOAD_DATE,
  CURRENT_TIMESTAMP() as UPDATED_AT
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING
WHERE Symbol IS NOT NULL AND Symbol != '' AND Symbol != 'Symbol';

-- Show final results
SELECT 'Data transfer completed!' as MESSAGE;

SELECT 
    'Records in staging table:' as METRIC,
    COUNT(*) as VALUE
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING;

SELECT 
    'Records in main COMPANY_OVERVIEW table:' as METRIC,
    COUNT(*) as VALUE
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW;

-- Show sample records in main table
SELECT 'Sample records in main table:' as MESSAGE;
SELECT 
    SYMBOL, FISCAL_YEAR_END, LATEST_QUARTER, UPDATED_AT
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW
ORDER BY SYMBOL
LIMIT 5;

SELECT 'Debug staging table created successfully! Check the output above to understand the CSV structure.' as COMPLETION_MESSAGE;

-- Do NOT drop the staging table
SELECT 'Staging table preserved for debugging' as PRESERVATION_MESSAGE;