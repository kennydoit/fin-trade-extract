-- Load Alpha Vantage TIME_SERIES_DAILY_ADJUSTED from S3 using proper staging pattern
-- This follows the same proven approach as listing_status but adapted for time series data

DROP STAGE IF EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGE;

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ACCOUNTADMIN;

-- Variables (will be replaced by workflow if needed)
SET LOAD_DATE = '20251005';

-- 1) Create external stage pointing to S3 time series folder with file format
CREATE STAGE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGE
  URL='s3://fin-trade-craft-landing/time_series_daily_adjusted/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
  FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  );

-- 2) Ensure main table exists with proper structure
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED (
  symbol VARCHAR(20) NOT NULL,
  date DATE NOT NULL,
  open NUMBER(15,4),
  high NUMBER(15,4),
  low NUMBER(15,4),
  close NUMBER(15,4),
  adjusted_close NUMBER(15,4),
  volume NUMBER(20,0),
  dividend_amount NUMBER(15,6),
  split_coefficient NUMBER(10,6),
  load_date DATE,
  PRIMARY KEY (symbol, date)
);

-- 3) Create staging table (transient for performance)
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING (
  symbol VARCHAR(20),
  date DATE,
  open NUMBER(15,4),
  high NUMBER(15,4),
  low NUMBER(15,4),
  close NUMBER(15,4),
  adjusted_close NUMBER(15,4),
  volume NUMBER(20,0),
  dividend_amount NUMBER(15,6),
  split_coefficient NUMBER(10,6),
  load_date DATE,
  source_filename VARCHAR(500)  -- Track which file each row came from
);

-- Debug: Check what files are available in the stage
LIST @TIME_SERIES_STAGE;

-- Debug: Check stage configuration
DESCRIBE STAGE TIME_SERIES_STAGE;

-- Debug: Try to list with pattern
LIST @TIME_SERIES_STAGE PATTERN = '*.csv';

-- Debug: Preview the first few rows from files to understand structure
SELECT 
    $1 as timestamp_raw,
    $2 as open_raw,
    $3 as high_raw,
    $4 as low_raw,
    $5 as close_raw,
    $6 as adjusted_close_raw,
    $7 as volume_raw,
    $8 as dividend_amount_raw,
    $9 as split_coefficient_raw,
    METADATA$FILENAME as source_file,
    METADATA$FILE_ROW_NUMBER as row_number
FROM @TIME_SERIES_STAGE
LIMIT 10;

-- 4) First create a temporary staging table to hold raw string data
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_RAW_STAGING (
  timestamp_raw VARCHAR(50),
  open_raw VARCHAR(50),
  high_raw VARCHAR(50),
  low_raw VARCHAR(50),
  close_raw VARCHAR(50),
  adjusted_close_raw VARCHAR(50),
  volume_raw VARCHAR(50),
  dividend_amount_raw VARCHAR(50),
  split_coefficient_raw VARCHAR(50),
  source_filename VARCHAR(500)
);

-- 5) Load raw data from S3 into temporary staging table (simple COPY)
COPY INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_RAW_STAGING (
    timestamp_raw, 
    open_raw, 
    high_raw, 
    low_raw, 
    close_raw, 
    adjusted_close_raw, 
    volume_raw, 
    dividend_amount_raw, 
    split_coefficient_raw,
    source_filename
)
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, METADATA$FILENAME
    FROM @TIME_SERIES_STAGE
)
PATTERN = '*.csv'
ON_ERROR = CONTINUE;

-- Debug: Check raw data load
SELECT COUNT(*) as raw_rows_loaded FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_RAW_STAGING;
SELECT source_filename, COUNT(*) FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_RAW_STAGING GROUP BY source_filename;

-- 6) Transform and load into proper staging table with data type conversions
INSERT INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING (
    date, 
    open, 
    high, 
    low, 
    close, 
    adjusted_close, 
    volume, 
    dividend_amount, 
    split_coefficient,
    source_filename
)
SELECT 
    TRY_TO_DATE(timestamp_raw, 'YYYY-MM-DD') as date,
    TRY_TO_NUMBER(open_raw, 15, 4) as open,
    TRY_TO_NUMBER(high_raw, 15, 4) as high,
    TRY_TO_NUMBER(low_raw, 15, 4) as low,
    TRY_TO_NUMBER(close_raw, 15, 4) as close,
    TRY_TO_NUMBER(adjusted_close_raw, 15, 4) as adjusted_close,
    TRY_TO_NUMBER(volume_raw, 20, 0) as volume,
    TRY_TO_NUMBER(dividend_amount_raw, 15, 6) as dividend_amount,
    TRY_TO_NUMBER(split_coefficient_raw, 10, 6) as split_coefficient,
    source_filename
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_RAW_STAGING
WHERE timestamp_raw IS NOT NULL 
  AND timestamp_raw != 'timestamp'  -- Skip header row if it gets through

-- Debug: Check transformed data load
SELECT COUNT(*) as transformed_rows_loaded FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING;
SELECT 
    source_filename,
    COUNT(*) as row_count,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
GROUP BY source_filename 
ORDER BY source_filename;

-- 7) Extract symbol from filename and set load_date
UPDATE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
SET symbol = REGEXP_SUBSTR(source_filename, 'time_series_daily_adjusted_([A-Z0-9]+)_', 1, 1, 'e', 1)
WHERE symbol IS NULL;

UPDATE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
SET load_date = TO_DATE($LOAD_DATE, 'YYYYMMDD');

-- Debug: Check symbol extraction and data quality
SELECT 
    symbol,
    COUNT(*) as row_count,
    COUNT(CASE WHEN date IS NULL THEN 1 END) as null_dates,
    COUNT(CASE WHEN close IS NULL THEN 1 END) as null_closes,
    COUNT(CASE WHEN volume IS NULL THEN 1 END) as null_volumes,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
GROUP BY symbol 
ORDER BY symbol;

-- 8) Data quality validation - remove bad records
DELETE FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
WHERE symbol IS NULL 
   OR date IS NULL 
   OR close IS NULL
   OR close <= 0
   OR volume < 0;

-- Debug: Check data after cleanup
SELECT 
    'After cleanup' as stage,
    symbol,
    COUNT(*) as clean_row_count,
    AVG(close) as avg_close_price,
    AVG(volume) as avg_volume
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
GROUP BY symbol 
ORDER BY symbol;

-- 9) Merge staging data into final table (upsert pattern)
MERGE INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED tgt
USING FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING src
ON tgt.symbol = src.symbol AND tgt.date = src.date
WHEN MATCHED THEN UPDATE SET
  open = src.open,
  high = src.high,
  low = src.low,
  close = src.close,
  adjusted_close = src.adjusted_close,
  volume = src.volume,
  dividend_amount = src.dividend_amount,
  split_coefficient = src.split_coefficient,
  load_date = src.load_date
WHEN NOT MATCHED THEN INSERT (
  symbol, date, open, high, low, close, adjusted_close, 
  volume, dividend_amount, split_coefficient, load_date
) VALUES (
  src.symbol, src.date, src.open, src.high, src.low, src.close, 
  src.adjusted_close, src.volume, src.dividend_amount, 
  src.split_coefficient, src.load_date
);

-- 10) Final verification and reporting
SELECT 
    'Final table summary' as report_type,
    symbol,
    COUNT(*) as total_records,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    MAX(load_date) as last_load_date
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED
GROUP BY symbol
ORDER BY symbol;

-- Summary metrics
SELECT 
    COUNT(DISTINCT symbol) as unique_symbols,
    COUNT(*) as total_records,
    MIN(date) as earliest_market_date,
    MAX(date) as latest_market_date,
    COUNT(DISTINCT load_date) as load_dates
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED;

-- Clean up temporary raw staging table
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_RAW_STAGING;