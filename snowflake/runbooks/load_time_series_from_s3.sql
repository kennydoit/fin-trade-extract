-- Load/merge Alpha Vantage TIME_SERIES_DAILY_ADJUSTED from S3 into RAW.TIME_SERIES_DAILY_ADJUSTED
-- Set LOAD_DATE to the YYYYMMDD date of the file and SYMBOL to the stock symbol

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

SET LOAD_DATE = '20251003';
SET SYMBOL = 'AAPL';
SET S3_PREFIX = 'time_series_daily_adjusted/';
SET FILE_NAME = 'time_series_daily_adjusted_' || $SYMBOL || '_' || $LOAD_DATE || '.csv';

-- Debug: Check what files are in the stage
LIST @TIME_SERIES_STAGE;

-- 1) Create stage if needed
CREATE STAGE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGE
  URL='s3://fin-trade-craft-landing/time_series_daily_adjusted/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION;

-- 2) Create table if needed (run the schema file first)
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

-- 3) Create file format if needed
CREATE FILE FORMAT IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 1
  NULL_IF = ('NULL', 'null', '')
  EMPTY_FIELD_AS_NULL = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- 4) Load into staging table
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
  load_date DATE
);

-- Debug: Try to read the file directly
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10 
FROM @TIME_SERIES_STAGE
(FILE_FORMAT => FIN_TRADE_EXTRACT.RAW.TIME_SERIES_CSV_FORMAT) 
PATTERN = '.*time_series_daily_adjusted_' || $SYMBOL || '_' || $LOAD_DATE || '\\.csv'
LIMIT 5;

-- Copy data from S3 into staging table
COPY INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING (
  date, open, high, low, close, adjusted_close, volume, dividend_amount, split_coefficient
)
FROM (
  SELECT 
    TO_DATE($1, 'YYYY-MM-DD'),  -- timestamp -> date
    $2::NUMBER(15,4),           -- open
    $3::NUMBER(15,4),           -- high  
    $4::NUMBER(15,4),           -- low
    $5::NUMBER(15,4),           -- close
    $6::NUMBER(15,4),           -- adjusted_close
    $7::NUMBER(20,0),           -- volume
    $8::NUMBER(15,6),           -- dividend_amount
    $9::NUMBER(10,6)            -- split_coefficient
  FROM @TIME_SERIES_STAGE
)
FILE_FORMAT = FIN_TRADE_EXTRACT.RAW.TIME_SERIES_CSV_FORMAT
PATTERN = '.*time_series_daily_adjusted_' || $SYMBOL || '_' || $LOAD_DATE || '\\.csv'
ON_ERROR = 'CONTINUE';

-- Debug: Check how many rows were loaded
SELECT COUNT(*) as rows_loaded FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING;

-- Add metadata to staging data
UPDATE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
SET 
  symbol = $SYMBOL,
  load_date = TO_DATE($LOAD_DATE, 'YYYYMMDD');

-- Debug: Check staging data before cleanup
SELECT COUNT(*) as rows_before_cleanup FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING;
SELECT * FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING LIMIT 5;

-- Remove bad rows (if any)
DELETE FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
WHERE symbol IS NULL OR date IS NULL OR close IS NULL;

-- Debug: Check staging data after cleanup
SELECT COUNT(*) as rows_after_cleanup FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING;
SELECT * FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING LIMIT 10;

-- 5) Merge into final table (upsert by symbol + date)
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
  src.symbol, src.date, src.open, src.high, src.low, src.close, src.adjusted_close,
  src.volume, src.dividend_amount, src.split_coefficient, src.load_date
);

-- 6) Verify results
SELECT COUNT(*) AS total_records FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED 
WHERE symbol = $SYMBOL;

SELECT COUNT(*) AS records_for_load_date FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED 
WHERE symbol = $SYMBOL AND load_date = TO_DATE($LOAD_DATE, 'YYYYMMDD');

SELECT * FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED 
WHERE symbol = $SYMBOL 
ORDER BY date DESC 
LIMIT 10;