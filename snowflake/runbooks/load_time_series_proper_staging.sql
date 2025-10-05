-- Load Alpha Vantage TIME_SERIES_DAILY_ADJUSTED from S3 using proper staging pattern
-- This follows the same proven approach as listing_status but adapted for time series data


USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ACCOUNTADMIN;

DROP STAGE IF EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGE;
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING;
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED;


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

-- Debug: Check what files are available in the stage
LIST @TIME_SERIES_STAGE;

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



-- Debug: Check stage configuration
DESCRIBE STAGE TIME_SERIES_STAGE;

-- Debug: Try to list with pattern (regex format)
LIST @TIME_SERIES_STAGE PATTERN = '.*\.csv';

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

-- 4) Load data directly into staging table (simplified 2-table approach)
COPY INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING (
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
FROM (
    SELECT 
        TO_DATE($1, 'YYYY-MM-DD'),
        $2::NUMBER(15,4),
        $3::NUMBER(15,4),
        $4::NUMBER(15,4),
        $5::NUMBER(15,4),
        $6::NUMBER(15,4),
        $7::NUMBER(20,0),
        $8::NUMBER(15,6),
        $9::NUMBER(10,6),
        METADATA$FILENAME
    FROM @TIME_SERIES_STAGE
)
PATTERN = '.*\.csv'
ON_ERROR = CONTINUE;

-- Debug: Check staging data load
SELECT COUNT(*) as staging_rows_loaded FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING;
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

