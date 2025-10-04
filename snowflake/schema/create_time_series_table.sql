-- Create Snowflake table for Alpha Vantage TIME_SERIES_DAILY_ADJUSTED data
-- Adapted from Postgres schema with Snowflake-appropriate data types

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Create stage for time series data if needed
CREATE STAGE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_STAGE
  URL='s3://fin-trade-craft-landing/time_series_daily_adjusted/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION;

-- Create main time series table with Snowflake data types
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED (
  symbol VARCHAR(20) NOT NULL,
  date DATE NOT NULL,
  open NUMBER(15,4),
  high NUMBER(15,4),
  low NUMBER(15,4),
  close NUMBER(15,4),
  adjusted_close NUMBER(15,4),
  volume NUMBER(20,0),           -- BIGINT equivalent
  dividend_amount NUMBER(15,6),
  split_coefficient NUMBER(10,6),
  load_date DATE,               -- Track which batch this data came from
  PRIMARY KEY (symbol, date)    -- Ensure one record per symbol and date
);

-- Create file format for time series CSV data
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