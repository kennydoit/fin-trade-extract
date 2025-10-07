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
LIST @LISTING_STATUS_STAGE;

-- Load ALL CSV files (both active and delisted automatically)
COPY INTO FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING (symbol, name, exchange, assetType, ipoDate, delistingDate, status, source_file)
FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7, METADATA$FILENAME
  FROM @LISTING_STATUS_STAGE
)
FILE_FORMAT = (FORMAT_NAME = FIN_TRADE_EXTRACT.RAW.RAW_CSV_FORMAT)
PATTERN = '.*\\.csv'
ON_ERROR = CONTINUE;

-- Debug: Check how many rows were loaded from each file
SELECT 
    source_file,
    COUNT(*) as rows_loaded 
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING 
GROUP BY source_file;

SELECT COUNT(*) as total_rows_loaded FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING;

-- Add load_date to staging data (use current date)
UPDATE FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING SET load_date = CURRENT_DATE();

-- Remove bad rows
DELETE FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING WHERE symbol IS NULL OR symbol = '#NAME?';

-- Debug: Check for duplicate symbols across files
SELECT 
    symbol,
    COUNT(*) as occurrence_count,
    COUNT(DISTINCT source_file) as file_count,
    LISTAGG(DISTINCT source_file, ', ') as files
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING 
GROUP BY symbol
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC
LIMIT 10;

-- Create deduplicated staging data
DELETE FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING 
WHERE (symbol, source_file) NOT IN (
  SELECT symbol, MIN(source_file) 
  FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING 
  GROUP BY symbol
);

-- Debug: Check staging data after deduplication
SELECT COUNT(*) as rows_after_dedup FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING;

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

-- Verify final results
SELECT COUNT(*) AS total_symbols FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS;

-- Check distribution by status
SELECT 
    status,
    COUNT(*) as symbol_count
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS 
GROUP BY status
ORDER BY symbol_count DESC;

-- Check for delisted symbols count
SELECT 
    'DELISTED SYMBOLS' as category,
    COUNT(*) as count
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS 
WHERE delistingDate IS NOT NULL AND delistingDate != '';

-- Check for active symbols count
SELECT 
    'ACTIVE SYMBOLS' as category,
    COUNT(*) as count
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS 
WHERE (delistingDate IS NULL OR delistingDate = '') AND status = 'Active';

-- Sample active records
SELECT 'ACTIVE SAMPLES' as type, symbol, name, exchange, status, ipoDate, delistingDate, load_date
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS 
WHERE status = 'Active' AND (delistingDate IS NULL OR delistingDate = '')
ORDER BY symbol LIMIT 10;

-- Sample delisted records
SELECT 'DELISTED SAMPLES' as type, symbol, name, exchange, status, ipoDate, delistingDate, load_date
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS 
WHERE delistingDate IS NOT NULL AND delistingDate != ''
ORDER BY symbol LIMIT 10;
