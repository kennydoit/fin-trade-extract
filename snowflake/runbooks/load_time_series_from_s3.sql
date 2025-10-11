-- ============================================================================
-- Load Time Series Data from S3 Stage - Simple Pattern with Calculated Symbol ID
-- *** RECOMMENDED APPROACH - USE THIS FILE ***
-- 
-- Features:
-- - Calculated SYMBOL_ID column (hash-based, consistent with symbol)
-- - Full historical data (20+ years from Alpha Vantage)
-- - Simple SYMBOL VARCHAR + DATE unique constraint
-- - Proper duplicate handling and data quality checks
-- ============================================================================
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- FOR TESTING ONLY: Clean up any existing objects
-- DROP STAGE IF EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGE;
-- DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING;
-- DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED;

-- 1) Create external stage pointing to S3 time series folder
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

-- 2) List files in stage to verify content
LIST @TIME_SERIES_STAGE;

-- 3) Create main table matching current listing_status pattern (no SYMBOL_ID)
-- Force drop and recreate to ensure proper schema
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED;
CREATE TABLE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED (
    SYMBOL_ID         NUMBER(38,0),
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
    LOAD_DATE         DATE DEFAULT CURRENT_DATE(),
    
    -- Constraints
    CONSTRAINT UK_TIME_SERIES_SYMBOL_DATE UNIQUE (SYMBOL, DATE)
)
COMMENT = 'Daily adjusted time series data from Alpha Vantage API'
CLUSTER BY (DATE, SYMBOL);

-- 4) Create staging table (transient for performance) 
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING (
  symbol VARCHAR(20),
  symbol_id NUMBER(38,0),  -- Will be calculated from symbol
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

-- 5) Load data directly into staging table
COPY INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING (
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
SELECT COUNT(*) as staging_rows_loaded FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING;

-- 6) Extract symbol from filename
-- Filename pattern: time_series_daily_adjusted/SYMBOL_TIMESTAMP.csv (e.g., AAA_20251011_123739.csv, AA-W_20251011_124558.csv)
-- Symbols can contain: letters, numbers, dots (BRK.B), hyphens (AA-W), underscores
UPDATE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING 
SET symbol = REGEXP_SUBSTR(source_filename, '([A-Z0-9._-]+)_[0-9]{8}_[0-9]{6}\\.csv', 1, 1, 'e', 1)
WHERE symbol IS NULL;

-- Debug: Check symbol extraction
SELECT 
    symbol,
    COUNT(*) as row_count,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    source_filename
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING 
GROUP BY symbol, source_filename
ORDER BY symbol;

-- 7) Calculate symbol_id using same logic as listing_status (hash-based approach)
UPDATE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING 
SET symbol_id = ABS(HASH(symbol)) % 1000000000  -- Generate consistent ID from symbol
WHERE symbol IS NOT NULL;

-- Debug: Check symbol_id calculation
SELECT 
    symbol,
    symbol_id,
    COUNT(*) as row_count
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING 
GROUP BY symbol, symbol_id
ORDER BY symbol;

-- 8) Remove duplicates (keep most recent file's data for each symbol+date)
DELETE FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING 
WHERE (symbol, date, source_filename) IN (
    SELECT symbol, date, source_filename
    FROM (
        SELECT 
            symbol, 
            date, 
            source_filename,
            ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY source_filename DESC) as rn
        FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING
    ) 
    WHERE rn > 1
);

-- 9) Data quality validation - remove bad records  
DELETE FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING 
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
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING 
GROUP BY symbol, symbol_id
ORDER BY symbol;

-- 10) Load from staging to final table with calculated symbol_id
MERGE INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED AS target
USING (
    SELECT 
        staging.symbol_id,
        staging.symbol,
        staging.date,
        staging.open,
        staging.high,
        staging.low,
        staging.close,
        staging.adjusted_close,
        staging.volume,
        staging.dividend_amount,
        staging.split_coefficient,
        CURRENT_DATE() as load_date
    FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING staging
    WHERE staging.symbol IS NOT NULL
      AND staging.symbol_id IS NOT NULL
      AND staging.date IS NOT NULL
) AS source
ON target.SYMBOL = source.symbol 
   AND target.DATE = source.date
WHEN NOT MATCHED THEN
    INSERT (SYMBOL_ID, SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, ADJUSTED_CLOSE, VOLUME, DIVIDEND_AMOUNT, SPLIT_COEFFICIENT, LOAD_DATE)
    VALUES (source.symbol_id, source.symbol, source.date, source.open, source.high, source.low, source.close, source.adjusted_close, source.volume, source.dividend_amount, source.split_coefficient, source.load_date);

-- 11) Final validation and reporting
SELECT 
    symbol,
    COUNT(*) as total_rows,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    load_date
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED 
GROUP BY symbol, load_date
ORDER BY symbol;

-- Show summary stats
SELECT 
    COUNT(DISTINCT symbol) as unique_symbols,
    COUNT(*) as total_rows,
    MIN(date) as earliest_date,
    MAX(date) as latest_date
FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED;

-- Cleanup staging table
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING;

SELECT 'Time series data loading completed successfully!' as status;