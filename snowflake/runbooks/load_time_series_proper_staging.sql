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

-- 2) Ensure main table exists with proper structure (matching schema/02_tables.sql)
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED (
    SYMBOL_ID         NUMBER(38,0) NOT NULL,
    SYMBOL            VARCHAR(20) NOT NULL,
    DATE              DATE NOT NULL,
    OPEN              NUMBER(15,4),
    HIGH              NUMBER(15,4),
    LOW               NUMBER(15,4),
    CLOSE             NUMBER(15,4),
    ADJUSTED_CLOSE    NUMBER(15,4),
    VOLUME            NUMBER(20,0),
    DIVIDEND_AMOUNT   NUMBER(15,6),
    SPLIT_COEFFICIENT NUMBER(10,6),
    CREATED_AT        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT UK_TIME_SERIES_SYMBOL_DATE UNIQUE (SYMBOL_ID, DATE)
)
COMMENT = 'Daily adjusted time series data from Alpha Vantage API'
CLUSTER BY (DATE, SYMBOL_ID);

-- 3) Create staging table (transient for performance) 
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING (
  symbol VARCHAR(20),
  symbol_id NUMBER(38,0),  -- Will be populated by lookup
  date DATE,
  open NUMBER(15,4),
  high NUMBER(15,4),
  low NUMBER(15,4),
  close NUMBER(15,4),
  adjusted_close NUMBER(15,4),
  volume NUMBER(20,0),
  dividend_amount NUMBER(15,6),
  split_coefficient NUMBER(10,6),
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

-- 5) Extract symbol from filename
UPDATE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
SET symbol = REGEXP_SUBSTR(source_filename, 'time_series_daily_adjusted_([A-Z0-9]+)_', 1, 1, 'e', 1)
WHERE symbol IS NULL;

-- 6) Lookup symbol_id from LISTING_STATUS table (like the proper schema design)
UPDATE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING staging
SET symbol_id = (
    SELECT ls.SYMBOL_ID 
    FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS ls 
    WHERE ls.SYMBOL = staging.symbol
)
WHERE staging.symbol_id IS NULL;

-- Debug: Check symbol extraction and symbol_id lookup
SELECT 
    symbol,
    symbol_id,
    COUNT(*) as row_count,
    COUNT(CASE WHEN date IS NULL THEN 1 END) as null_dates,
    COUNT(CASE WHEN close IS NULL THEN 1 END) as null_closes,
    COUNT(CASE WHEN volume IS NULL THEN 1 END) as null_volumes,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
GROUP BY symbol, symbol_id 
ORDER BY symbol;

-- 7) Remove duplicates (keep only the most recent file's data for each symbol+date)
-- This handles the case where multiple CSV files contain same symbol data
DELETE FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
WHERE (symbol, date, source_filename) IN (
    SELECT symbol, date, source_filename
    FROM (
        SELECT 
            symbol, 
            date, 
            source_filename,
            ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY source_filename DESC) as rn
        FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING
    ) 
    WHERE rn > 1
);

-- 8) Data quality validation - remove bad records  
DELETE FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
WHERE symbol IS NULL 
   OR symbol_id IS NULL
   OR date IS NULL 
   OR close IS NULL
   OR close <= 0
   OR volume < 0;

-- Debug: Check data after cleanup and deduplication
SELECT 
    'After cleanup' as stage,
    symbol,
    symbol_id,
    COUNT(*) as clean_row_count,
    AVG(close) as avg_close_price,
    AVG(volume) as avg_volume
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
GROUP BY symbol, symbol_id
ORDER BY symbol;

-- 9) Merge staging data into final table using symbol_id (proper foreign key relationship)
MERGE INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED tgt
USING FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING src
ON tgt.symbol_id = src.symbol_id AND tgt.date = src.date
WHEN MATCHED THEN UPDATE SET
  symbol = src.symbol,
  open = src.open,
  high = src.high,
  low = src.low,
  close = src.close,
  adjusted_close = src.adjusted_close,
  volume = src.volume,
  dividend_amount = src.dividend_amount,
  split_coefficient = src.split_coefficient,
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  symbol_id, symbol, date, open, high, low, close, adjusted_close, 
  volume, dividend_amount, split_coefficient, created_at, updated_at
) VALUES (
  src.symbol_id, src.symbol, src.date, src.open, src.high, src.low, src.close, 
  src.adjusted_close, src.volume, src.dividend_amount, 
  src.split_coefficient, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- 10) Final verification and reporting  
SELECT 
    'Final table summary' as report_type,
    symbol_id,
    symbol,
    COUNT(*) as total_records,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    MAX(updated_at) as last_update
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED
GROUP BY symbol_id, symbol
ORDER BY symbol;

-- Summary metrics
SELECT 
    COUNT(DISTINCT symbol_id) as unique_symbol_ids,
    COUNT(DISTINCT symbol) as unique_symbols,  
    COUNT(*) as total_records,
    MIN(date) as earliest_market_date,
    MAX(date) as latest_market_date
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED;

