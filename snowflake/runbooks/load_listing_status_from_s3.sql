-- Load/merge Alpha Vantage LISTING_STATUS from S3 into RAW.LISTING_STATUS
-- Set LOAD_DATE to the YYYYMMDD date of the file

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;

SET LOAD_DATE = '20251001';
SET S3_PREFIX = 'listing_status/';
SET FILE_NAME = 'listing_status_' || $LOAD_DATE || '.csv';

-- 1) Create stage if needed
CREATE STAGE IF NOT EXISTS LISTING_STATUS_STAGE
  URL='s3://fin-trade-craft-landing/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION;

-- 2) Create table if needed
CREATE TABLE IF NOT EXISTS RAW.LISTING_STATUS (
  symbol VARCHAR,
  name VARCHAR,
  exchange VARCHAR,
  assetType VARCHAR,
  ipoDate VARCHAR,
  delistingDate VARCHAR,
  status VARCHAR
);
-- 3) Load into staging table
CREATE OR REPLACE TABLE RAW.LISTING_STATUS_STAGING AS
SELECT * FROM RAW.LISTING_STATUS WHERE 1=0;

COPY INTO RAW.LISTING_STATUS_STAGING
FROM @LISTING_STATUS_STAGE/$S3_PREFIX$FILE_NAME
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
ON_ERROR = 'CONTINUE';

-- Remove bad rows
DELETE FROM RAW.LISTING_STATUS_STAGING WHERE symbol IS NULL OR symbol = '#NAME?';

SELECT * FROM RAW.LISTING_STATUS_STAGING LIMIT 10;

-- 4) Merge into final table (upsert by symbol)
MERGE INTO RAW.LISTING_STATUS tgt
USING RAW.LISTING_STATUS_STAGING src
ON tgt.symbol = src.symbol
WHEN MATCHED THEN UPDATE SET
  name = src.name,
  exchange = src.exchange,
  assetType = src.assetType,
  ipoDate = src.ipoDate,
  delistingDate = src.delistingDate,
  status = src.status
WHEN NOT MATCHED THEN INSERT (
  symbol, name, exchange, assetType, ipoDate, delistingDate, status
) VALUES (
  src.symbol, src.name, src.exchange, src.assetType, src.ipoDate, src.delistingDate, src.status
);

-- 5) Verify
SELECT COUNT(*) AS total_symbols FROM RAW.LISTING_STATUS;
SELECT * FROM RAW.LISTING_STATUS ORDER BY symbol LIMIT 20;