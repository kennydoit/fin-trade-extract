-- Load Alpha Vantage TIME_SERIES_DAILY_ADJUSTED from S3 - Simple Version

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ACCOUNTADMIN;

SET LOAD_DATE = '20251004';

CREATE STAGE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGE URL='s3://fin-trade-craft-landing/time_series_daily_adjusted/' STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION;

CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED (symbol VARCHAR(20) NOT NULL, date DATE NOT NULL, open NUMBER(15,4), high NUMBER(15,4), low NUMBER(15,4), close NUMBER(15,4), adjusted_close NUMBER(15,4), volume NUMBER(20,0), dividend_amount NUMBER(15,6), split_coefficient NUMBER(10,6), load_date DATE, PRIMARY KEY (symbol, date));

CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING (symbol VARCHAR(20), date DATE, open NUMBER(15,4), high NUMBER(15,4), low NUMBER(15,4), close NUMBER(15,4), adjusted_close NUMBER(15,4), volume NUMBER(20,0), dividend_amount NUMBER(15,6), split_coefficient NUMBER(10,6), load_date DATE);

-- Debug: Check what files are in the stage
LIST @TIME_SERIES_STAGE;

-- Load ALL CSV files from the time series stage (supports multiple symbols)
COPY INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING (date, open, high, low, close, adjusted_close, volume, dividend_amount, split_coefficient)
FROM @TIME_SERIES_STAGE
FILE_FORMAT = (FORMAT_NAME = FIN_TRADE_EXTRACT.RAW.RAW_CSV_FORMAT)
PATTERN = '.*\.csv'
ON_ERROR = CONTINUE;

-- Debug: Check how many rows were loaded
SELECT COUNT(*) as rows_loaded FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING;

UPDATE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING SET load_date = TO_DATE($LOAD_DATE, 'YYYYMMDD');

-- Extract symbol from filename metadata (Snowflake automatically tracks source file)
UPDATE FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING 
SET symbol = REGEXP_SUBSTR(METADATA$FILENAME, 'time_series_daily_adjusted_([A-Z0-9]+)_', 1, 1, 'e', 1)
WHERE symbol IS NULL;

SELECT COUNT(*) as rows_loaded FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING;

DELETE FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING WHERE symbol IS NULL OR date IS NULL OR close IS NULL;

-- Debug: Check data before cleanup
SELECT COUNT(*) as rows_before_cleanup FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING;
SELECT DISTINCT symbol FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING;

-- Remove bad rows
DELETE FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING WHERE symbol IS NULL OR date IS NULL OR close IS NULL;

-- Debug: Check data after cleanup  
SELECT COUNT(*) as rows_after_cleanup FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING;

MERGE INTO FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED tgt USING FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED_STAGING src ON tgt.symbol = src.symbol AND tgt.date = src.date WHEN MATCHED THEN UPDATE SET open = src.open, high = src.high, low = src.low, close = src.close, adjusted_close = src.adjusted_close, volume = src.volume, dividend_amount = src.dividend_amount, split_coefficient = src.split_coefficient, load_date = src.load_date WHEN NOT MATCHED THEN INSERT (symbol, date, open, high, low, close, adjusted_close, volume, dividend_amount, split_coefficient, load_date) VALUES (src.symbol, src.date, src.open, src.high, src.low, src.close, src.adjusted_close, src.volume, src.dividend_amount, src.split_coefficient, src.load_date);

SELECT COUNT(*) AS total_records FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED;
SELECT DISTINCT symbol FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED ORDER BY symbol;
