-- Load/merge Alpha Vantage LISTING_STATUS (both active and delisted) from S3 into RAW.LISTING_STATUS
-- Automatically processes all CSV files in the listing_status/ prefix

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ACCOUNTADMIN;

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

-- 3) Load both active and delisted files into staging table
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING (
  symbol VARCHAR,
  name VARCHAR,
  exchange VARCHAR,
  assetType VARCHAR,
  ipoDate VARCHAR,
  delistingDate VARCHAR,
  status VARCHAR,
  load_date DATE,
  source_file VARCHAR
);

-- Debug: Check what files are in the stage
-- LIST @LISTING_STATUS_STAGE;

-- Load ALL CSV files (both active and delisted automatically)
COPY INTO FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING (symbol, name, exchange, assetType, ipoDate, delistingDate, status, source_file)
FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7, METADATA$FILENAME
  FROM @LISTING_STATUS_STAGE
)
FILE_FORMAT = (FORMAT_NAME = FIN_TRADE_EXTRACT.RAW.RAW_CSV_FORMAT)
PATTERN = '.*\\.csv'
ON_ERROR = CONTINUE;
-- Note: PARALLEL parameter not supported for external stages (S3)
-- Snowflake automatically parallelizes based on number of files

-- Add load_date to staging data (use current date)
UPDATE FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING SET load_date = CURRENT_DATE();

-- Remove bad rows
DELETE FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING WHERE symbol IS NULL OR symbol = '#NAME?';


-- Create deduplicated staging data
DELETE FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING 
WHERE (symbol, source_file) NOT IN (
  SELECT symbol, MIN(source_file) 
  FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING 
  GROUP BY symbol
);


-- Simple merge without complex subqueries
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

