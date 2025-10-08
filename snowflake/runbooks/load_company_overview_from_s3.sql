-- Load/merge Alpha Vantage COMPANY_OVERVIEW from S3 into RAW.COMPANY_OVERVIEW
-- Automatically processes all CSV files in the company_overview/ prefix

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ACCOUNTADMIN;

-- 1) Create stage if needed
CREATE STAGE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGE
  URL='s3://fin-trade-craft-landing/company_overview/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION;

-- 2) Create main table if needed
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW (
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
  MARKET_CAPITALIZATION NUMBER,
  EBITDA NUMBER,
  PE_RATIO NUMBER,
  PEG_RATIO NUMBER,
  BOOK_VALUE NUMBER,
  DIVIDEND_PER_SHARE NUMBER,
  DIVIDEND_YIELD NUMBER,
  EPS NUMBER,
  REVENUE_PER_SHARE_TTM NUMBER,
  PROFIT_MARGIN NUMBER,
  OPERATING_MARGIN_TTM NUMBER,
  RETURN_ON_ASSETS_TTM NUMBER,
  RETURN_ON_EQUITY_TTM NUMBER,
  REVENUE_TTM NUMBER,
  GROSS_PROFIT_TTM NUMBER,
  DILUTED_EPS_TTM NUMBER,
  QUARTERLY_EARNINGS_GROWTH_YOY NUMBER,
  QUARTERLY_REVENUE_GROWTH_YOY NUMBER,
  ANALYST_TARGET_PRICE NUMBER,
  TRAILING_PE NUMBER,
  FORWARD_PE NUMBER,
  PRICE_TO_SALES_RATIO_TTM NUMBER,
  PRICE_TO_BOOK_RATIO NUMBER,
  EV_TO_REVENUE NUMBER,
  EV_TO_EBITDA NUMBER,
  BETA NUMBER,
  WEEK_52_HIGH NUMBER,
  WEEK_52_LOW NUMBER,
  DAY_50_MOVING_AVERAGE NUMBER,
  DAY_200_MOVING_AVERAGE NUMBER,
  SHARES_OUTSTANDING NUMBER,
  DIVIDEND_DATE DATE,
  EX_DIVIDEND_DATE DATE,
  LATEST_QUARTER DATE,
  PROCESSED_DATE DATE,
  LOAD_DATE VARCHAR(50),
  UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- 3) Create simple staging table like listing status - capture any CSV columns
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING (
  col1 VARCHAR,
  col2 VARCHAR,
  col3 VARCHAR,
  col4 VARCHAR,
  col5 VARCHAR,
  col6 VARCHAR,
  col7 VARCHAR,
  col8 VARCHAR,
  col9 VARCHAR,
  col10 VARCHAR,
  col11 VARCHAR,
  col12 VARCHAR,
  col13 VARCHAR,
  col14 VARCHAR,
  col15 VARCHAR,
  col16 VARCHAR,
  col17 VARCHAR,
  col18 VARCHAR,
  col19 VARCHAR,
  col20 VARCHAR,
  col21 VARCHAR,
  col22 VARCHAR,
  col23 VARCHAR,
  col24 VARCHAR,
  col25 VARCHAR,
  col26 VARCHAR,
  col27 VARCHAR,
  col28 VARCHAR,
  col29 VARCHAR,
  col30 VARCHAR,
  col31 VARCHAR,
  col32 VARCHAR,
  col33 VARCHAR,
  col34 VARCHAR,
  col35 VARCHAR,
  col36 VARCHAR,
  col37 VARCHAR,
  col38 VARCHAR,
  col39 VARCHAR,
  col40 VARCHAR,
  col41 VARCHAR,
  col42 VARCHAR,
  col43 VARCHAR,
  col44 VARCHAR,
  col45 VARCHAR,
  col46 VARCHAR,
  col47 VARCHAR,
  col48 VARCHAR,
  col49 VARCHAR,
  col50 VARCHAR,
  col51 VARCHAR,
  col52 VARCHAR,
  col53 VARCHAR,
  col54 VARCHAR,
  col55 VARCHAR,
  col56 VARCHAR,
  col57 VARCHAR,
  col58 VARCHAR,
  col59 VARCHAR,
  col60 VARCHAR,
  source_file VARCHAR
);

-- Debug: Check what files are in the stage
LIST @COMPANY_OVERVIEW_STAGE;

-- Load ALL CSV files using same pattern as listing status
COPY INTO FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, METADATA$FILENAME
  FROM @COMPANY_OVERVIEW_STAGE
)
FILE_FORMAT = (FORMAT_NAME = FIN_TRADE_EXTRACT.RAW.RAW_CSV_FORMAT)
PATTERN = 'overview_.*\\.csv'
ON_ERROR = CONTINUE;

-- Debug: Check how many rows were loaded from each file
SELECT 
    source_file,
    COUNT(*) as rows_loaded 
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
GROUP BY source_file;

SELECT COUNT(*) as total_rows_loaded FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING;

-- Debug: Show sample of loaded data
SELECT col1, col2, col3, col4, col5, source_file 
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
LIMIT 5;

-- Remove bad rows (assuming symbol is in col2 based on typical CSV structure)
DELETE FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING WHERE col2 IS NULL OR col2 = '';

-- Debug: Check for duplicate data across files (using col2 as identifier)
SELECT 
    col2 as identifier,
    COUNT(*) as occurrence_count,
    COUNT(DISTINCT source_file) as file_count,
    LISTAGG(DISTINCT source_file, ', ') as files
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
GROUP BY col2
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 10;

-- Create deduplicated staging data (using col2 as identifier)
DELETE FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
WHERE (col2, source_file) NOT IN (
  SELECT col2, MIN(source_file) 
  FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
  GROUP BY col2
);

-- Debug: Check staging data after deduplication
SELECT COUNT(*) as rows_after_dedup FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING;

-- Debug: Show all non-null columns from first row to understand structure
SELECT 'Column mapping analysis:' as MESSAGE;

-- Show first 20 columns from first record
SELECT 
  'First 20 columns:' as section,
  col1, col2, col3, col4, col5, col6, col7, col8, col9, col10,
  col11, col12, col13, col14, col15, col16, col17, col18, col19, col20
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
LIMIT 1;

-- Show columns 21-40 from first record  
SELECT 
  'Columns 21-40:' as section,
  col21, col22, col23, col24, col25, col26, col27, col28, col29, col30,
  col31, col32, col33, col34, col35, col36, col37, col38, col39, col40
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
LIMIT 1;

-- Show columns 41-60 from first record
SELECT 
  'Columns 41-60:' as section,
  col41, col42, col43, col44, col45, col46, col47, col48, col49, col50,
  col51, col52, col53, col54, col55, col56, col57, col58, col59, col60
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
LIMIT 1;

-- For now, don't insert into main table until we understand the column mapping
SELECT 'Check the debug output above to identify which columns contain Symbol, FiscalYearEnd, LatestQuarter, etc.' as NEXT_STEP;

-- Verify results and show summary
SELECT 'COMPANY OVERVIEW LOADING SUMMARY:' as MESSAGE;

SELECT 
    'Records in staging table:' as METRIC,
    COUNT(*) as VALUE
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING;

SELECT 
    'Records in main table:' as METRIC,
    COUNT(*) as VALUE
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW;

SELECT 
    'Files processed:' as METRIC,
    COUNT(DISTINCT source_file) as VALUE
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING;

-- Show which symbols were loaded
SELECT 
    'Symbols loaded:' as MESSAGE,
    col1 as SYMBOL_LOADED,  -- Assuming col1 is the symbol
    source_file
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING
ORDER BY col1;