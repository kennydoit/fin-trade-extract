-- ============================================================================
-- CHECK TABLE STRUCTURE AND CSV COMPATIBILITY
-- Compare CSV columns with table structure to identify the issue
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- ============================================================================
-- STEP 1: EXAMINE TABLE STRUCTURE
-- ============================================================================

-- Show the exact structure of the OVERVIEW table
DESC TABLE RAW.OVERVIEW;

-- Count columns in the table
SELECT COUNT(*) as table_column_count 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'RAW' 
AND TABLE_NAME = 'OVERVIEW';

-- Show all column names in order
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'RAW' 
AND TABLE_NAME = 'OVERVIEW'
ORDER BY ORDINAL_POSITION;

-- ============================================================================
-- STEP 2: EXAMINE CSV FILE STRUCTURE 
-- ============================================================================

-- Look at the raw file structure (first few rows)
SELECT 
    $1 as col1_symbol_id,
    $2 as col2_symbol,
    $3 as col3_asset_type,
    $4 as col4_name,
    $5 as col5_description,
    $6 as col6_cik,
    $7 as col7_exchange,
    $8 as col8_currency,
    $9 as col9_country,
    $10 as col10_sector,
    $11 as col11_industry,
    $12 as col12_address
FROM @OVERVIEW_STAGE/overview_20250928_035238_aad23b30.csv
LIMIT 3;

-- Count total columns in CSV by looking at metadata
SELECT 
    METADATA$FILENAME as filename,
    COUNT(*) as csv_rows
FROM @OVERVIEW_STAGE/overview_20250928_035238_aad23b30.csv
GROUP BY METADATA$FILENAME;

-- ============================================================================
-- STEP 3: TRY EXPLICIT COLUMN MAPPING
-- ============================================================================

-- Try loading with explicit column mapping to see if that works
COPY INTO RAW.OVERVIEW (
    SYMBOL_ID, SYMBOL, ASSET_TYPE, NAME, DESCRIPTION, CIK, EXCHANGE, 
    CURRENCY, COUNTRY, SECTOR, INDUSTRY, ADDRESS, OFFICIAL_SITE, 
    FISCAL_YEAR_END, MARKET_CAPITALIZATION, EBITDA, PE_RATIO, PEG_RATIO, 
    BOOK_VALUE, DIVIDEND_PER_SHARE, DIVIDEND_YIELD, EPS, REVENUE_PER_SHARE_TTM, 
    PROFIT_MARGIN, OPERATING_MARGIN_TTM, RETURN_ON_ASSETS_TTM, RETURN_ON_EQUITY_TTM, 
    REVENUE_TTM, GROSS_PROFIT_TTM, DILUTED_EPS_TTM, QUARTERLY_EARNINGS_GROWTH_YOY, 
    QUARTERLY_REVENUE_GROWTH_YOY, ANALYST_TARGET_PRICE, TRAILING_PE, FORWARD_PE, 
    PRICE_TO_SALES_RATIO_TTM, PRICE_TO_BOOK_RATIO, EV_TO_REVENUE, EV_TO_EBITDA, 
    BETA, WEEK_52_HIGH, WEEK_52_LOW, DAY_50_MOVING_AVERAGE, DAY_200_MOVING_AVERAGE, 
    SHARES_OUTSTANDING, DIVIDEND_DATE, EX_DIVIDEND_DATE, API_RESPONSE_STATUS, 
    CREATED_AT, UPDATED_AT
)
FROM @OVERVIEW_STAGE
FILES = ('overview_20250928_035238_aad23b30.csv')
FILE_FORMAT = (
    TYPE = 'CSV' 
    SKIP_HEADER = 1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';

-- Check if any records were loaded
SELECT COUNT(*) as records_after_explicit_mapping FROM RAW.OVERVIEW;

-- ============================================================================
-- STEP 4: VALIDATION MODE WITH DETAILED ERRORS
-- ============================================================================

-- Run in validation mode to get detailed error information
COPY INTO RAW.OVERVIEW
FROM @OVERVIEW_STAGE
FILES = ('overview_20250928_035238_aad23b30.csv')
FILE_FORMAT = (
    TYPE = 'CSV' 
    SKIP_HEADER = 1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
VALIDATION_MODE = 'RETURN_ALL_ERRORS';

-- ============================================================================
-- STEP 5: CHECK COPY HISTORY FOR ERRORS
-- ============================================================================

-- Check detailed copy history
SELECT 
    FILE_NAME,
    STATUS,
    ROW_COUNT,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    FIRST_ERROR_LINE_NUMBER,
    FIRST_ERROR_COLUMN_NAME,
    LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'FIN_TRADE_EXTRACT.RAW.OVERVIEW',
    START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC
LIMIT 10;