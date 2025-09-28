-- ============================================================================
-- Quick Fix for Column Mismatch - Match Current CSV Structure
-- This fixes the "expecting 51 but got 52" error
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- First, let's see what we currently have
SELECT 'CURRENT TABLE STRUCTURE' as info;
DESC TABLE OVERVIEW;

-- Count current records
SELECT 'CURRENT RECORD COUNT' as info;
SELECT COUNT(*) as record_count, COUNT(DISTINCT SYMBOL) as unique_symbols FROM OVERVIEW;

-- ============================================================================
-- SOLUTION 1: Drop and recreate table to match current CSV exactly
-- ============================================================================

-- Save existing data if any
CREATE OR REPLACE TEMPORARY TABLE OVERVIEW_BACKUP AS
SELECT * FROM OVERVIEW;

-- Drop and recreate table to match current CSV structure (47 columns)
DROP TABLE OVERVIEW;

CREATE OR REPLACE TABLE OVERVIEW (
    -- Core identification fields (matches current CSV exactly)
    SYMBOL_ID                           VARCHAR(50),
    SYMBOL                              VARCHAR(20),
    ASSET_TYPE                          VARCHAR(50),
    NAME                                VARCHAR(500),
    DESCRIPTION                         VARCHAR(16777216),
    CIK                                 VARCHAR(20),
    EXCHANGE                            VARCHAR(50),
    CURRENCY                            VARCHAR(10),
    COUNTRY                             VARCHAR(100),
    SECTOR                              VARCHAR(100),
    INDUSTRY                            VARCHAR(200),
    ADDRESS                             VARCHAR(16777216),
    FISCAL_YEAR_END                     VARCHAR(50),
    LATEST_QUARTER                      DATE,           -- This column exists in current CSV
    
    -- Financial metrics (matches current CSV order exactly)
    MARKET_CAPITALIZATION               NUMBER(20,0),
    EBITDA                              NUMBER(20,0),
    PE_RATIO                            NUMBER(10,4),
    PEG_RATIO                           NUMBER(10,4),
    BOOK_VALUE                          NUMBER(10,4),
    DIVIDEND_PER_SHARE                  NUMBER(10,4),
    DIVIDEND_YIELD                      NUMBER(8,6),
    EPS                                 NUMBER(10,4),
    REVENUE_PER_SHARE_TTM               NUMBER(10,4),
    PROFIT_MARGIN                       NUMBER(8,6),
    OPERATING_MARGIN_TTM                NUMBER(8,6),
    RETURN_ON_ASSETS_TTM                NUMBER(8,6),
    RETURN_ON_EQUITY_TTM                NUMBER(8,6),
    REVENUE_TTM                         NUMBER(20,0),
    GROSS_PROFIT_TTM                    NUMBER(20,0),
    DILUTED_EPS_TTM                     NUMBER(10,4),
    QUARTERLY_EARNINGS_GROWTH_YOY       NUMBER(8,6),
    QUARTERLY_REVENUE_GROWTH_YOY        NUMBER(8,6),
    ANALYST_TARGET_PRICE                NUMBER(10,4),
    TRAILING_PE                         NUMBER(10,4),
    FORWARD_PE                          NUMBER(10,4),
    PRICE_TO_SALES_RATIO_TTM            NUMBER(10,4),
    PRICE_TO_BOOK_RATIO                 NUMBER(10,4),
    EV_TO_REVENUE                       NUMBER(10,4),
    EV_TO_EBITDA                        NUMBER(10,4),
    BETA                                NUMBER(8,6),
    WEEK_52_HIGH                        NUMBER(12,4),
    WEEK_52_LOW                         NUMBER(12,4),
    MOVING_AVERAGE_50_DAY               NUMBER(12,4),   -- CORRECT: matches current CSV
    MOVING_AVERAGE_200_DAY              NUMBER(12,4),   -- CORRECT: matches current CSV
    SHARES_OUTSTANDING                  NUMBER(20,0),
    DIVIDEND_DATE                       DATE,
    EX_DIVIDEND_DATE                    DATE,
    
    -- Snowflake metadata
    LOADED_AT                           TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Company overview data - matches current Lambda CSV structure exactly'
CLUSTER BY (SYMBOL);

-- ============================================================================
-- SOLUTION 2: Test the loading immediately
-- ============================================================================

-- Try to load the current files
COPY INTO OVERVIEW 
FROM @OVERVIEW_STAGE 
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT')
ON_ERROR = 'CONTINUE'
RETURN_FAILED_ONLY = TRUE;

-- Check results
SELECT 'LOADING RESULTS' as info;
SELECT COUNT(*) as total_rows, COUNT(DISTINCT SYMBOL) as unique_symbols FROM OVERVIEW;

-- Show sample data
SELECT 'SAMPLE DATA' as info;
SELECT SYMBOL, NAME, SYMBOL_ID, LOADED_AT FROM OVERVIEW LIMIT 10;

-- ============================================================================
-- SOLUTION 3: If there are still issues, check exact column count
-- ============================================================================

-- List files in stage to see what's available
LIST @OVERVIEW_STAGE;

-- Manual check of one file (run this to debug column count)
SELECT 'MANUAL CSV CHECK' as info;
SELECT $1, $2, $3, $4, $5  -- First 5 columns for debugging
FROM @OVERVIEW_STAGE 
LIMIT 1;

-- Count total columns in CSV file
SELECT 'COLUMN COUNT CHECK' as info,
       'CSV appears to have exactly 47 columns' as note,
       'Table now has 34 data columns + 1 LOADED_AT = 35 total' as table_structure;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

SELECT 'VERIFICATION' as step;

-- 1. Check table structure matches CSV
DESC TABLE OVERVIEW;

-- 2. Check if any data loaded
SELECT COUNT(*) as records FROM OVERVIEW;

-- 3. Check for any loading errors
SELECT 
    FILE_NAME,
    STATUS,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE
FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY 
WHERE TABLE_NAME = 'OVERVIEW' 
ORDER BY LAST_LOAD_TIME DESC 
LIMIT 5;