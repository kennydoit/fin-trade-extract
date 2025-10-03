-- Load/merge Alpha Vantage LISTING_STATUS from S3 into RAW.LISTING_STATUS
-- Set LOAD_DATE to the YYYYMMDD date of the file

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ACCOUNTADMIN;

SET LOAD_DATE = '20251002';
SET S3_PREFIX = 'listing_status/';
SET FILE_NAME = 'listing_status_' || $LOAD_DATE || '.csv';

-- 1) Create stage if needed
CREATE STAGE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGE
  URL='s3://fin-trade-craft-landing/listing_status/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION;


-- 2) Create table if needed
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.LISTING_STATUS (
  symbol VARCHAR,
  name VARCHAR,
  exchange VARCHAR,
  assetType VARCHAR,
  ipoDate VARCHAR,
  delistingDate VARCHAR,
  status VARCHAR,
  load_date DATE
);


-- 3) Load into staging table
CREATE OR REPLACE TABLE FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING AS
SELECT * FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS WHERE 1=0;

-- Use a named file format for compatibility
COPY INTO FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING
FROM @LISTING_STATUS_STAGE
FILE_FORMAT = (FORMAT_NAME = FIN_TRADE_EXTRACT.RAW.RAW_CSV_FORMAT)
FILES = ('listing_status_20251002.csv')
ON_ERROR = CONTINUE;


-- COPY INTO FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING
-- FROM @LISTING_STATUS_STAGE/$(S3_PREFIX)$(FILE_NAME)
-- FILE_FORMAT = (FORMAT_NAME = FIN_TRADE_EXTRACT.RAW.RAW_CSV_FORMAT)
-- ON_ERROR = CONTINUE;

-- Add load_date to staging data
ALTER TABLE FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING ADD COLUMN load_date DATE;
UPDATE FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING SET load_date = TO_DATE($LOAD_DATE, 'YYYYMMDD');

-- Remove bad rows
DELETE FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING WHERE symbol IS NULL OR symbol = '#NAME?';

SELECT * FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING LIMIT 10;

-- 4) Merge into final table (upsert by symbol)
MERGE INTO FIN_TRADE_EXTRACT.RAW.LISTING_STATUS tgt
USING FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING src
ON UPPER(TRIM(tgt.symbol)) = UPPER(TRIM(src.symbol))
WHEN MATCHED THEN UPDATE SET
  name = src.name,
  exchange = src.exchange,
  assetType = src.assetType,
  ipoDate = src.ipoDate,
  delistingDate = src.delistingDate,
  status = src.status,
  load_date = src.load_date
WHEN NOT MATCHED THEN INSERT (
  symbol, name, exchange, assetType, ipoDate, delistingDate, status, load_date
) VALUES (
  src.symbol, src.name, src.exchange, src.assetType, src.ipoDate, src.delistingDate, src.status, src.load_date
);

-- 5) Verify
SELECT COUNT(*) AS total_symbols FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS;
SELECT * FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS ORDER BY symbol LIMIT 20;
