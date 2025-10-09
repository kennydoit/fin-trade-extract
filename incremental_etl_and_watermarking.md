Purpose:
The purpose of this task is to improve the watermarking process and standardize
incremental ETL for all symbol-driven data sources.

All alpha vantage ETLs must start with the watermarking table, which will be
the sole table responsible for feeding into the incremental ETL process.

The listing_status table is the seed table that builds the watermarking table.

Here's how it will work.

1. Build initial watermarking table:
   - Table to be called FIN_TRADE_EXTRACT.RAW.ETL_WATERMARK
   - Create a separate workflow that creates the watermarkig table from listing_status. 
   - Create calculated SYMBOL_ID
   - FIRST_FISCAL_DATE and LAST_FISCAL_DATE are null
   - TABLE_NAME = 'WATERMARK'
   - API_ELIGIBLE = 'NO'
   - Primary Keys: SYMBOL_ID, TABLE_NAME --> no duplicates allowed. Upsert only
   - There should be approximately 20k records including listed and delisted stocks

3. Build watermarks for each data source. 
   Since the watermarking table will drive the ETL process, there needs to be watermarking
   records for each source.

   A. Build watermarking entries for TIME_SERIES_DAILY_ADJUSTED
      Step 1: Copy the 20k records with table_name 'WATERMARK' 
      Step 2: Change 'WATERMARK' to 'TIME_SERIES_DAILY_ADJUSTED' for the new records
      Step 3: Change API_ELIGIBLE to 'YES'
      We now have a watermark table that can feed into the TIME_SERIES_DAILY_ADJUSTED ETL
      The Incremental ETL process can only attempt to process records where API_ELIGIBLE = 'YES'
   B. Build watermarking entries for COMPANY_OVERVIEW
      Step 1: Copy the 20k records with table_name 'WATERMARK'
      Step 2: Change 'WATERMARK' to 'COMPANY_OVERVIEW' for the new records
      Step 3: Change API_ELIGIBLE to 'YES'
      Step 4: if ASSET_TYPE is not for a common stock, then set API_ELIGIBLE to 'NO'
   C. Build watermarking entries for CASH_FLOW
      Step 1: Copy the 20k records with table_name 'WATERMARK' 
      Step 2: Change 'WATERMARK' to 'CASH_FLOW' for the new records
      Step 3: Change API_ELIGIBLE to 'YES'
      Step 4: if ASSET_TYPE is not for a common stock, then set API_ELIGIBLE to 'NO'
      Step 5: If stock is not Active, then set API_ELIGIBLE to 'NO'
   D. Build watermarking entries for BALANCE_SHEET
      Step 1: Copy the 20k records with table_name 'WATERMARK' 
      Step 2: Change 'WATERMARK' to 'BALANCE_SHEET' for the new records
      Step 3: Change API_ELIGIBLE to 'YES'
      Step 4: if ASSET_TYPE is not for a common stock, then set API_ELIGIBLE to 'NO'
      Step 5: If stock is not Active, then set API_ELIGIBLE to 'NO'
   D. Build watermarking entries for INCOME_STATEMENT
      Step 1: Copy the 20k records with table_name 'WATERMARK' 
      Step 2: Change 'WATERMARK' to 'BALANCE_SHEET' for the new records
      Step 3: Change API_ELIGIBLE to 'YES'
      Step 4: if ASSET_TYPE is not for a common stock, then set API_ELIGIBLE to 'NO'
      Step 5: If stock is not Active, then set API_ELIGIBLE to 'NO'
   E. Build watermarking entries for INSIDER_TRANSACTIONS
      Step 1: Copy the 20k records with table_name 'WATERMARK' 
      Step 2: Change 'WATERMARK' to 'INSIDER_TRANSACTIONS' for the new records
      Step 3: Change API_ELIGIBLE to 'YES'
      Step 4: if ASSET_TYPE is not for a common stock, then set API_ELIGIBLE to 'NO'
      Step 5: If stock is not Active, then set API_ELIGIBLE to 'NO'
   E. Build watermarking entries for EARNINGS_CALL_TRANSCRIPT
      Step 1: Copy the 20k records with table_name 'WATERMARK' 
      Step 2: Change 'WATERMARK' to 'EARNINGS_CALL_TRANSCRIPT' for the new records
      Step 3: Change API_ELIGIBLE to 'YES'
      Step 4: if ASSET_TYPE is not for a common stock, then set API_ELIGIBLE to 'NO'
      Step 5: If stock is not Active, then set API_ELIGIBLE to 'NO'
   
Once watermarking table is created for all symbol-driven API calls, we can run first ETL:
Here is how the first ETL will work:
   A. First ETL: TIME_SERIES_DAILY_ADJUSTED
      Step 1: There will be 3 ETLs for Time Series Daily Adjusted.
              1. NYSE
              2. NASDAQ
              3. ETF
      Step 1: Create listing of SYMBOLS to pull that can be used for batch processing. 
              Ignore records where API_ELIGIBLE = 'NO'
              Include Active and Inactive Symbols
      Step 2: Run API extractions for all remaining symbols
      Step 3: Load all data into Snowflake
      Step 4: Once data are loaded, update ETL_WATERMARK table. 
              Step 4 must be an independent function that can be called any time to update ETL_WATERMARK.
              This is important in case the process fails or if we want to retro-activley update ETL_WATERMARK 
              Update ETL_WATERMARK values:
                  FIRST_FISCAL_DATE - minimum value of date field
                  LAST_FISCAL_DATE - maximum value of date field
                  LAST_SUCCESSFUL_RUN - current date-time stamp
                  CREATED_AT - current date-time stamp for initial load 
                  UPDATED_AT - current date-time stamp

    

# WATERMARKING TABLE COLUMNS:

TABLE_NAME                  VARCHAR(100) NOT NULL,     -- Target table name (e.g., 'BALANCE_SHEET', 'TIME_SERIES_DAILY_ADJUSTED')
SYMBOL_ID                   NUMBER(38,0) NOT NULL,     -- Hash-based symbol identifier for consistent joins

-- Symbol Reference
SYMBOL                      VARCHAR(20) NOT NULL,      -- Actual symbol for debugging and reference

EXCHANGE                    VARCHAR(64)                -- From listing_status
ASSET_TYPE                  VARCHAR(64)                -- From listing_status
STATUS                      VARCHAR(12)                -- From listing_status
API_ELIGIBLE                VARCHAR(3)                 -- can be 'YES' or 'NO'

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

