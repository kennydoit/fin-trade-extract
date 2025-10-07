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
  source_file VARCHAR  -- Track which file the record came from
);

-- Debug: Check what files are in the stage
LIST @LISTING_STATUS_STAGE;

-- Debug: Preview active file
SELECT $1, $2, $3, $4, $5, $6, $7 
FROM @LISTING_STATUS_STAGE
(FILE_FORMAT => FIN_TRADE_EXTRACT.RAW.RAW_CSV_FORMAT)
WHERE METADATA$FILENAME LIKE '%active%'
LIMIT 5;

-- Debug: Preview delisted file
SELECT $1, $2, $3, $4, $5, $6, $7 
FROM @LISTING_STATUS_STAGE
(FILE_FORMAT => FIN_TRADE_EXTRACT.RAW.RAW_CSV_FORMAT)
WHERE METADATA$FILENAME LIKE '%delisted%'
LIMIT 5;

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

-- Debug: Check staging data before cleanup
SELECT COUNT(*) as rows_before_cleanup FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING;
SELECT 
    source_file,
    symbol,
    name,
    exchange,
    status,
    ipoDate,
    delistingDate
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING 
LIMIT 10;

-- Remove bad rows
DELETE FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING WHERE symbol IS NULL OR symbol = '#NAME?';

-- Debug: Check staging data after cleanup
SELECT COUNT(*) as rows_after_cleanup FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING;
SELECT 
    source_file,
    COUNT(*) as clean_rows
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING 
GROUP BY source_file;

-- Show sample records from active stocks
SELECT 
    'ACTIVE STOCKS SAMPLE' as record_type,
    symbol, name, exchange, status, ipoDate, delistingDate
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING 
WHERE source_file LIKE '%active%'
LIMIT 5;

-- Show sample records from delisted stocks
SELECT 
    'DELISTED STOCKS SAMPLE' as record_type,
    symbol, name, exchange, status, ipoDate, delistingDate
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS_STAGING 
WHERE source_file LIKE '%delisted%'
LIMIT 5;

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

-- 5) Verify final results
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
