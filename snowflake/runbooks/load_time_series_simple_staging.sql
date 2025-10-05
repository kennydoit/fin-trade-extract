-- ============================================================================
-- Load Time Series Data from S3 Stage - Simple Pattern (matching listing_status)
-- ============================================================================
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

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
UPDATE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGING 
SET symbol = REGEXP_SUBSTR(source_filename, 'time_series_daily_adjusted_([A-Z0-9]+)_', 1, 1, 'e', 1)
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

-- 7) Load from staging to final table (simple pattern matching listing_status)
MERGE INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED AS target
USING (
    SELECT 
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
      AND staging.date IS NOT NULL
) AS source
ON target.SYMBOL = source.symbol 
   AND target.DATE = source.date
WHEN NOT MATCHED THEN
    INSERT (SYMBOL, DATE, OPEN, HIGH, LOW, CLOSE, ADJUSTED_CLOSE, VOLUME, DIVIDEND_AMOUNT, SPLIT_COEFFICIENT, LOAD_DATE)
    VALUES (source.symbol, source.date, source.open, source.high, source.low, source.close, source.adjusted_close, source.volume, source.dividend_amount, source.split_coefficient, source.load_date);

-- Debug: Final validation
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