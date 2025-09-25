-- ============================================================================
-- Snowflake Database Setup for fin-trade-extract Pipeline
-- ============================================================================

-- Create database and schema
CREATE DATABASE IF NOT EXISTS FIN_TRADE_EXTRACT;
USE DATABASE FIN_TRADE_EXTRACT;

-- Create schemas for different data layers
CREATE SCHEMA IF NOT EXISTS RAW;           -- Raw data from S3
CREATE SCHEMA IF NOT EXISTS PROCESSED;     -- Cleaned and transformed data
CREATE SCHEMA IF NOT EXISTS ANALYTICS;     -- Business intelligence views

-- Create compute warehouse (cost-optimized)
CREATE WAREHOUSE IF NOT EXISTS FIN_TRADE_WH
WITH 
    WAREHOUSE_SIZE = 'X-SMALL'              -- 1 credit per hour
    AUTO_SUSPEND = 60                       -- Suspend after 1 minute
    AUTO_RESUME = TRUE                      -- Auto-resume on query
    INITIALLY_SUSPENDED = TRUE              -- Start suspended
    MIN_CLUSTER_COUNT = 1                   -- Minimum clusters
    MAX_CLUSTER_COUNT = 1                   -- Maximum clusters (no auto-scaling)
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Warehouse for fin-trade-extract data pipeline - cost optimized';

-- Set context
USE WAREHOUSE FIN_TRADE_WH;
USE SCHEMA RAW;

-- Create file formats for different data types
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
TYPE = 'PARQUET'
COMPRESSION = 'SNAPPY'
COMMENT = 'File format for Lambda-generated Parquet files';

CREATE OR REPLACE FILE FORMAT JSON_FORMAT
TYPE = 'JSON'
COMPRESSION = 'GZIP'
COMMENT = 'File format for Lambda-generated JSON files';

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
TYPE = 'CSV'
COMPRESSION = 'GZIP'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
REPLACE_INVALID_CHARACTERS = TRUE
COMMENT = 'File format for CSV data files';

-- Show created objects
SHOW DATABASES LIKE 'FIN_TRADE_EXTRACT';
SHOW SCHEMAS IN DATABASE FIN_TRADE_EXTRACT;
SHOW WAREHOUSES LIKE 'FIN_TRADE_WH';
SHOW FILE FORMATS IN SCHEMA RAW;