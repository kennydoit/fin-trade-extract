Purpose:
The purpose of this task is to improve the watermarking process and standardize
incremental ETL for all symbol-driven data sources.

All alpha vantage ETLs must start with the watermarking table, which will be
the sole table responsible for feeding into the incremental ETL process.

The listing_status table is the seed table that builds the watermarking table.

Here's how it will work.

1. Build initial watermarking table:
   - Create a separate workflow that creates the watermarkig table from listing_status. 
   - Create calculated SYMBOL_ID
   - FIRST_FISCAL_DATE and LAST_FISCAL_DATE are null
   - TABLE_NAME = 'WATERMARK'
   - Primary Keys: SYMBOL_ID, TABLE_NAME --> no duplicates allowed. Upsert only
   - There should be approximately 20k records including listed and delisted stocks

2. Build watermarks for each data source. 
   Since the watermarking table will drive the ETL process, there needs to be watermarking
   records for each source
Running any ALPHA VANTAGE extraction for the first time.
   For example, let's say we are running TIME_SERIES_DAILY_ADJUSTED for the first time
   Step 1: Add the 20k records with table_name 'WATERMARK' 
   Step 2: Change 'WATERMARK' to 'TIME_SERIES_DAILY_ADJUSTED' for the new records
   Step 3: Eliminate out of scope records. This would apply to fundamentals. 
           For fundamentals, we would delete any row that was an ETF or delisted stock
   The resulting table would have eligible symbols
   We now have a watermark table that con feed into the 



# WATERMARKING TABLE COLUMNS:

TABLE_NAME                  VARCHAR(100) NOT NULL,     -- Target table name (e.g., 'BALANCE_SHEET', 'TIME_SERIES_DAILY_ADJUSTED')
SYMBOL_ID                   NUMBER(38,0) NOT NULL,     -- Hash-based symbol identifier for consistent joins

-- Symbol Reference
SYMBOL                      VARCHAR(20) NOT NULL,      -- Actual symbol for debugging and reference

EXCHANGE                    VARCHAR(64)                -- From listing_status
ASSET_TYPE                  VARCHAR(64)                -- From listing_status
STATUS                      VARCHAR(12)                -- From listing_status

-- Listing Information  
IPO_DATE                    DATE,                      -- IPO date from listing status
DELISTING_DATE              DATE,                      -- Delisting date from listing status

-- Processing Tracking
FIRST_FISCAL_DATE           DATE,                      -- First fiscal/data date available (earliest time series data point)
LAST_FISCAL_DATE            DATE,                      -- Last fiscal/data date available in the data
LAST_SUCCESSFUL_RUN         TIMESTAMP_NTZ,             -- Last successful processing timestamp
CONSECUTIVE_FAILURES        NUMBER(5,0) DEFAULT 0,     -- Count of consecutive processing failures

-- Audit Fields
CREATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
UPDATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),

