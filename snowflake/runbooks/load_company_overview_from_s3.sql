-- ============================================================================
-- Load Company Overview Data from S3 into Snowflake
-- 
-- This script loads company overview data extracted by fetch_company_overview_bulk.py
-- into the RAW.COMPANY_OVERVIEW table. It processes files organized by date and
-- performs intelligent deduplication and merging.
--
-- The script handles the new company overview data structure with proper typing
-- and handles the active common stock focus of the new extractor.
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- 1) Create/Update Company Overview Staging Table
-- ============================================================================

CREATE OR REPLACE TABLE RAW.COMPANY_OVERVIEW_STAGING (
    -- Core Identifiers
    SYMBOL_ID                           NUMBER(38,0),
    SYMBOL                              VARCHAR(20),
    
    -- Company Information
    ASSET_TYPE                          VARCHAR(50),
    NAME                                VARCHAR(500),
    DESCRIPTION                         TEXT,
    CIK                                 VARCHAR(20),
    EXCHANGE                            VARCHAR(50),
    CURRENCY                            VARCHAR(10),
    COUNTRY                             VARCHAR(100),
    SECTOR                              VARCHAR(100),
    INDUSTRY                            VARCHAR(200),
    ADDRESS                             TEXT,
    OFFICIAL_SITE                       VARCHAR(500),
    FISCAL_YEAR_END                     VARCHAR(50),
    
    -- Financial Metrics (as strings for flexible parsing)
    MARKET_CAPITALIZATION               VARCHAR(50),
    EBITDA                              VARCHAR(50),
    PE_RATIO                            VARCHAR(50),
    PEG_RATIO                           VARCHAR(50),
    BOOK_VALUE                          VARCHAR(50),
    DIVIDEND_PER_SHARE                  VARCHAR(50),
    DIVIDEND_YIELD                      VARCHAR(50),
    EPS                                 VARCHAR(50),
    REVENUE_PER_SHARE_TTM               VARCHAR(50),
    PROFIT_MARGIN                       VARCHAR(50),
    OPERATING_MARGIN_TTM                VARCHAR(50),
    RETURN_ON_ASSETS_TTM                VARCHAR(50),
    RETURN_ON_EQUITY_TTM                VARCHAR(50),
    REVENUE_TTM                         VARCHAR(50),
    GROSS_PROFIT_TTM                    VARCHAR(50),
    DILUTED_EPS_TTM                     VARCHAR(50),
    QUARTERLY_EARNINGS_GROWTH_YOY       VARCHAR(50),
    QUARTERLY_REVENUE_GROWTH_YOY        VARCHAR(50),
    ANALYST_TARGET_PRICE                VARCHAR(50),
    TRAILING_PE                         VARCHAR(50),
    FORWARD_PE                          VARCHAR(50),
    PRICE_TO_SALES_RATIO_TTM            VARCHAR(50),
    PRICE_TO_BOOK_RATIO                 VARCHAR(50),
    EV_TO_REVENUE                       VARCHAR(50),
    EV_TO_EBITDA                        VARCHAR(50),
    BETA                                VARCHAR(50),
    WEEK_52_HIGH                        VARCHAR(50),
    WEEK_52_LOW                         VARCHAR(50),
    DAY_50_MOVING_AVERAGE               VARCHAR(50),
    DAY_200_MOVING_AVERAGE              VARCHAR(50),
    SHARES_OUTSTANDING                  VARCHAR(50),
    DIVIDEND_DATE                       VARCHAR(50),
    EX_DIVIDEND_DATE                    VARCHAR(50),
    LATEST_QUARTER                      VARCHAR(50),
    
    -- Processing Metadata
    PROCESSED_DATE                      VARCHAR(50),
    LOAD_DATE                           VARCHAR(50)
)
COMMENT = 'Staging table for company overview data from Alpha Vantage OVERVIEW function';

-- ============================================================================
-- 2) Create Company Overview Stage if it doesn't exist
-- ============================================================================

-- Create external stage pointing to S3 company overview folder
CREATE STAGE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGE
  URL='s3://fin-trade-craft-landing/company_overview/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION;

-- ============================================================================
-- 3) Copy Recent Company Overview Files from S3
-- ============================================================================

-- Copy all company overview files from the last 7 days
-- Files are organized as: company_overview/YYYY/MM/DD/overview_SYMBOL_LOADDATE.csv
COPY INTO RAW.COMPANY_OVERVIEW_STAGING
FROM @COMPANY_OVERVIEW_STAGE
PATTERN = '.*company_overview/[0-9]{4}/[0-9]{2}/[0-9]{2}/overview_.*\\.csv'
FILE_FORMAT = (FORMAT_NAME = FIN_TRADE_EXTRACT.RAW.RAW_CSV_FORMAT)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- Show load results
SELECT 'Files loaded into staging:' as MESSAGE;
SELECT COUNT(*) as RECORDS_LOADED FROM RAW.COMPANY_OVERVIEW_STAGING;
SELECT LOAD_DATE, COUNT(*) as RECORD_COUNT 
FROM RAW.COMPANY_OVERVIEW_STAGING 
GROUP BY LOAD_DATE 
ORDER BY LOAD_DATE DESC;

-- ============================================================================
-- 4) Merge Company Overview Data into Final Table
-- ============================================================================

-- Merge staging data into the main OVERVIEW table (or COMPANY_OVERVIEW if it exists)
-- This handles deduplication and keeps the most recent data per symbol
MERGE INTO RAW.OVERVIEW AS tgt
USING (
    WITH ranked_staging AS (
        SELECT
            *,
            -- Parse timestamps for ranking
            TRY_TO_TIMESTAMP_NTZ(PROCESSED_DATE) AS PROCESSED_DATE_TS,
            -- Rank by most recent processing date, then load date
            ROW_NUMBER() OVER (
                PARTITION BY SYMBOL
                ORDER BY 
                    TRY_TO_TIMESTAMP_NTZ(PROCESSED_DATE) DESC NULLS LAST,
                    LOAD_DATE DESC NULLS LAST,
                    TRY_TO_TIMESTAMP_NTZ(LATEST_QUARTER) DESC NULLS LAST
            ) AS rn
        FROM RAW.COMPANY_OVERVIEW_STAGING
        WHERE SYMBOL IS NOT NULL 
          AND TRIM(SYMBOL) != ''
          AND SYMBOL_ID IS NOT NULL
    )
    SELECT
        -- Core identifiers
        SYMBOL_ID,
        SYMBOL,
        ASSET_TYPE,
        NAME,
        DESCRIPTION,
        CIK,
        EXCHANGE,
        CURRENCY,
        COUNTRY,
        SECTOR,
        INDUSTRY,
        ADDRESS,
        OFFICIAL_SITE,
        FISCAL_YEAR_END,
        
        -- Convert numeric fields with proper handling
        TRY_TO_NUMBER(NULLIF(TRIM(MARKET_CAPITALIZATION), '')) AS MARKET_CAPITALIZATION,
        TRY_TO_NUMBER(NULLIF(TRIM(EBITDA), '')) AS EBITDA,
        TRY_TO_NUMBER(NULLIF(TRIM(PE_RATIO), '')) AS PE_RATIO,
        TRY_TO_NUMBER(NULLIF(TRIM(PEG_RATIO), '')) AS PEG_RATIO,
        TRY_TO_NUMBER(NULLIF(TRIM(BOOK_VALUE), '')) AS BOOK_VALUE,
        TRY_TO_NUMBER(NULLIF(TRIM(DIVIDEND_PER_SHARE), '')) AS DIVIDEND_PER_SHARE,
        TRY_TO_NUMBER(NULLIF(TRIM(DIVIDEND_YIELD), '')) AS DIVIDEND_YIELD,
        TRY_TO_NUMBER(NULLIF(TRIM(EPS), '')) AS EPS,
        TRY_TO_NUMBER(NULLIF(TRIM(REVENUE_PER_SHARE_TTM), '')) AS REVENUE_PER_SHARE_TTM,
        TRY_TO_NUMBER(NULLIF(TRIM(PROFIT_MARGIN), '')) AS PROFIT_MARGIN,
        TRY_TO_NUMBER(NULLIF(TRIM(OPERATING_MARGIN_TTM), '')) AS OPERATING_MARGIN_TTM,
        TRY_TO_NUMBER(NULLIF(TRIM(RETURN_ON_ASSETS_TTM), '')) AS RETURN_ON_ASSETS_TTM,
        TRY_TO_NUMBER(NULLIF(TRIM(RETURN_ON_EQUITY_TTM), '')) AS RETURN_ON_EQUITY_TTM,
        TRY_TO_NUMBER(NULLIF(TRIM(REVENUE_TTM), '')) AS REVENUE_TTM,
        TRY_TO_NUMBER(NULLIF(TRIM(GROSS_PROFIT_TTM), '')) AS GROSS_PROFIT_TTM,
        TRY_TO_NUMBER(NULLIF(TRIM(DILUTED_EPS_TTM), '')) AS DILUTED_EPS_TTM,
        TRY_TO_NUMBER(NULLIF(TRIM(QUARTERLY_EARNINGS_GROWTH_YOY), '')) AS QUARTERLY_EARNINGS_GROWTH_YOY,
        TRY_TO_NUMBER(NULLIF(TRIM(QUARTERLY_REVENUE_GROWTH_YOY), '')) AS QUARTERLY_REVENUE_GROWTH_YOY,
        TRY_TO_NUMBER(NULLIF(TRIM(ANALYST_TARGET_PRICE), '')) AS ANALYST_TARGET_PRICE,
        TRY_TO_NUMBER(NULLIF(TRIM(TRAILING_PE), '')) AS TRAILING_PE,
        TRY_TO_NUMBER(NULLIF(TRIM(FORWARD_PE), '')) AS FORWARD_PE,
        TRY_TO_NUMBER(NULLIF(TRIM(PRICE_TO_SALES_RATIO_TTM), '')) AS PRICE_TO_SALES_RATIO_TTM,
        TRY_TO_NUMBER(NULLIF(TRIM(PRICE_TO_BOOK_RATIO), '')) AS PRICE_TO_BOOK_RATIO,
        TRY_TO_NUMBER(NULLIF(TRIM(EV_TO_REVENUE), '')) AS EV_TO_REVENUE,
        TRY_TO_NUMBER(NULLIF(TRIM(EV_TO_EBITDA), '')) AS EV_TO_EBITDA,
        TRY_TO_NUMBER(NULLIF(TRIM(BETA), '')) AS BETA,
        TRY_TO_NUMBER(NULLIF(TRIM(WEEK_52_HIGH), '')) AS WEEK_52_HIGH,
        TRY_TO_NUMBER(NULLIF(TRIM(WEEK_52_LOW), '')) AS WEEK_52_LOW,
        TRY_TO_NUMBER(NULLIF(TRIM(DAY_50_MOVING_AVERAGE), '')) AS DAY_50_MOVING_AVERAGE,
        TRY_TO_NUMBER(NULLIF(TRIM(DAY_200_MOVING_AVERAGE), '')) AS DAY_200_MOVING_AVERAGE,
        TRY_TO_NUMBER(NULLIF(TRIM(SHARES_OUTSTANDING), '')) AS SHARES_OUTSTANDING,
        
        -- Convert date fields
        TRY_TO_DATE(NULLIF(TRIM(DIVIDEND_DATE), '')) AS DIVIDEND_DATE,
        TRY_TO_DATE(NULLIF(TRIM(EX_DIVIDEND_DATE), '')) AS EX_DIVIDEND_DATE,
        TRY_TO_DATE(NULLIF(TRIM(LATEST_QUARTER), '')) AS LATEST_QUARTER,
        
        -- Processing metadata
        PROCESSED_DATE_TS,
        LOAD_DATE,
        CURRENT_TIMESTAMP() AS UPDATED_AT
    FROM ranked_staging
    WHERE rn = 1
) AS src
ON tgt.SYMBOL_ID = src.SYMBOL_ID
WHEN MATCHED THEN UPDATE SET
    tgt.SYMBOL = src.SYMBOL,
    tgt.ASSET_TYPE = src.ASSET_TYPE,
    tgt.NAME = src.NAME,
    tgt.DESCRIPTION = src.DESCRIPTION,
    tgt.CIK = src.CIK,
    tgt.EXCHANGE = src.EXCHANGE,
    tgt.CURRENCY = src.CURRENCY,
    tgt.COUNTRY = src.COUNTRY,
    tgt.SECTOR = src.SECTOR,
    tgt.INDUSTRY = src.INDUSTRY,
    tgt.ADDRESS = src.ADDRESS,
    tgt.OFFICIALSITE = src.OFFICIAL_SITE,
    tgt.FISCALYEAREND = src.FISCAL_YEAR_END,
    tgt.MARKETCAPITALIZATION = src.MARKET_CAPITALIZATION,
    tgt.EBITDA = src.EBITDA,
    tgt.PERATIO = src.PE_RATIO,
    tgt.PEGRATIO = src.PEG_RATIO,
    tgt.BOOKVALUE = src.BOOK_VALUE,
    tgt.DIVIDENDPERSHARE = src.DIVIDEND_PER_SHARE,
    tgt.DIVIDENDYIELD = src.DIVIDEND_YIELD,
    tgt.EPS = src.EPS,
    tgt.REVENUEPERSHARETTM = src.REVENUE_PER_SHARE_TTM,
    tgt.PROFITMARGIN = src.PROFIT_MARGIN,
    tgt.OPERATINGMARGINTTM = src.OPERATING_MARGIN_TTM,
    tgt.RETURNONASSETSTTM = src.RETURN_ON_ASSETS_TTM,
    tgt.RETURNONEQUITYTTM = src.RETURN_ON_EQUITY_TTM,
    tgt.REVENUETTM = src.REVENUE_TTM,
    tgt.GROSSPROFITTTM = src.GROSS_PROFIT_TTM,
    tgt.DILUTEDEPSTTM = src.DILUTED_EPS_TTM,
    tgt.QUARTERLYEARNINGSGROWTHYOY = src.QUARTERLY_EARNINGS_GROWTH_YOY,
    tgt.QUARTERLYREVENUEGROWTHYOY = src.QUARTERLY_REVENUE_GROWTH_YOY,
    tgt.ANALYSTTARGETPRICE = src.ANALYST_TARGET_PRICE,
    tgt.TRAILINGPE = src.TRAILING_PE,
    tgt.FORWARDPE = src.FORWARD_PE,
    tgt.PRICETOSALESRATIOTTM = src.PRICE_TO_SALES_RATIO_TTM,
    tgt.PRICETOBOOKRATIO = src.PRICE_TO_BOOK_RATIO,
    tgt.EVTOREVENUE = src.EV_TO_REVENUE,
    tgt.EVTOEBITDA = src.EV_TO_EBITDA,
    tgt.BETA = src.BETA,
    tgt."52WEEKHIGH" = src.WEEK_52_HIGH,
    tgt."52WEEKLOW" = src.WEEK_52_LOW,
    tgt."50DAYMOVINGAVERAGE" = src.DAY_50_MOVING_AVERAGE,
    tgt."200DAYMOVINGAVERAGE" = src.DAY_200_MOVING_AVERAGE,
    tgt.SHARESOUTSTANDING = src.SHARES_OUTSTANDING,
    tgt.DIVIDENDDATE = src.DIVIDEND_DATE,
    tgt.EXDIVIDENDDATE = src.EX_DIVIDEND_DATE,
    tgt.UPDATED_AT = src.UPDATED_AT,
    tgt.BATCH_ID = 'AUTO_OVERVIEW_ETL',
    tgt.SOURCE_FILE_NAME = src.LOAD_DATE
WHEN NOT MATCHED THEN INSERT (
    SYMBOL_ID, SYMBOL, ASSET_TYPE, NAME, DESCRIPTION, CIK, EXCHANGE,
    CURRENCY, COUNTRY, SECTOR, INDUSTRY, ADDRESS, OFFICIALSITE,
    FISCALYEAREND, MARKETCAPITALIZATION, EBITDA, PERATIO, PEGRATIO,
    BOOKVALUE, DIVIDENDPERSHARE, DIVIDENDYIELD, EPS, REVENUEPERSHARETTM,
    PROFITMARGIN, OPERATINGMARGINTTM, RETURNONASSETSTTM, RETURNONEQUITYTTM,
    REVENUETTM, GROSSPROFITTTM, DILUTEDEPSTTM, QUARTERLYEARNINGSGROWTHYOY,
    QUARTERLYREVENUEGROWTHYOY, ANALYSTTARGETPRICE, TRAILINGPE, FORWARDPE,
    PRICETOSALESRATIOTTM, PRICETOBOOKRATIO, EVTOREVENUE, EVTOEBITDA,
    BETA, "52WEEKHIGH", "52WEEKLOW", "50DAYMOVINGAVERAGE", "200DAYMOVINGAVERAGE",
    SHARESOUTSTANDING, DIVIDENDDATE, EXDIVIDENDDATE, CREATED_AT, UPDATED_AT,
    BATCH_ID, SOURCE_FILE_NAME
) VALUES (
    src.SYMBOL_ID, src.SYMBOL, src.ASSET_TYPE, src.NAME, src.DESCRIPTION, src.CIK, src.EXCHANGE,
    src.CURRENCY, src.COUNTRY, src.SECTOR, src.INDUSTRY, src.ADDRESS, src.OFFICIAL_SITE,
    src.FISCAL_YEAR_END, src.MARKET_CAPITALIZATION, src.EBITDA, src.PE_RATIO, src.PEG_RATIO,
    src.BOOK_VALUE, src.DIVIDEND_PER_SHARE, src.DIVIDEND_YIELD, src.EPS, src.REVENUE_PER_SHARE_TTM,
    src.PROFIT_MARGIN, src.OPERATING_MARGIN_TTM, src.RETURN_ON_ASSETS_TTM, src.RETURN_ON_EQUITY_TTM,
    src.REVENUE_TTM, src.GROSS_PROFIT_TTM, src.DILUTED_EPS_TTM, src.QUARTERLY_EARNINGS_GROWTH_YOY,
    src.QUARTERLY_REVENUE_GROWTH_YOY, src.ANALYST_TARGET_PRICE, src.TRAILING_PE, src.FORWARD_PE,
    src.PRICE_TO_SALES_RATIO_TTM, src.PRICE_TO_BOOK_RATIO, src.EV_TO_REVENUE, src.EV_TO_EBITDA,
    src.BETA, src.WEEK_52_HIGH, src.WEEK_52_LOW, src.DAY_50_MOVING_AVERAGE, src.DAY_200_MOVING_AVERAGE,
    src.SHARES_OUTSTANDING, src.DIVIDEND_DATE, src.EX_DIVIDEND_DATE, src.PROCESSED_DATE_TS, src.UPDATED_AT,
    'AUTO_OVERVIEW_ETL', src.LOAD_DATE
);

-- ============================================================================
-- 5) Update ETL Watermarks
-- ============================================================================

-- Update watermarks for successfully processed symbols
MERGE INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS AS w
USING (
    SELECT DISTINCT
        'COMPANY_OVERVIEW' as TABLE_NAME,
        SYMBOL_ID,
        SYMBOL,
        TRY_TO_DATE(LATEST_QUARTER) as FISCAL_DATE,
        CURRENT_TIMESTAMP() as SUCCESSFUL_RUN
    FROM RAW.COMPANY_OVERVIEW_STAGING
    WHERE SYMBOL IS NOT NULL AND SYMBOL_ID IS NOT NULL
) AS src
ON w.TABLE_NAME = src.TABLE_NAME AND w.SYMBOL_ID = src.SYMBOL_ID
WHEN MATCHED THEN UPDATE SET
    w.LAST_FISCAL_DATE = COALESCE(src.FISCAL_DATE, w.LAST_FISCAL_DATE),
    w.LAST_SUCCESSFUL_RUN = src.SUCCESSFUL_RUN,
    w.CONSECUTIVE_FAILURES = 0,
    w.UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    TABLE_NAME, SYMBOL_ID, SYMBOL, LAST_FISCAL_DATE, LAST_SUCCESSFUL_RUN, 
    CONSECUTIVE_FAILURES, CREATED_AT, UPDATED_AT
) VALUES (
    src.TABLE_NAME, src.SYMBOL_ID, src.SYMBOL, src.FISCAL_DATE, src.SUCCESSFUL_RUN,
    0, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
);

-- ============================================================================
-- 6) Verification and Cleanup
-- ============================================================================

-- Show processing results
SELECT 'Company Overview Loading Results:' as MESSAGE;

SELECT 
    'Records in OVERVIEW table after merge:' as METRIC,
    COUNT(*) as VALUE
FROM RAW.OVERVIEW;

SELECT 
    'New/Updated records from this load:' as METRIC,
    COUNT(*) as VALUE
FROM RAW.OVERVIEW
WHERE BATCH_ID = 'AUTO_OVERVIEW_ETL'
  AND (CREATED_AT >= CURRENT_TIMESTAMP() - INTERVAL '1 hour' 
       OR UPDATED_AT >= CURRENT_TIMESTAMP() - INTERVAL '1 hour');

-- Show sample of recent records
SELECT 'Sample of recently loaded overview data:' as MESSAGE;
SELECT 
    SYMBOL,
    NAME,
    SECTOR,
    INDUSTRY,
    EXCHANGE,
    MARKETCAPITALIZATION,
    PERATIO,
    UPDATED_AT
FROM RAW.OVERVIEW
WHERE BATCH_ID = 'AUTO_OVERVIEW_ETL'
  AND UPDATED_AT >= CURRENT_TIMESTAMP() - INTERVAL '1 hour'
ORDER BY UPDATED_AT DESC
LIMIT 10;

-- Watermark verification
SELECT 'ETL Watermarks updated:' as MESSAGE;
SELECT 
    COUNT(*) as WATERMARK_RECORDS_UPDATED
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'COMPANY_OVERVIEW'
  AND UPDATED_AT >= CURRENT_TIMESTAMP() - INTERVAL '1 hour';

-- Clean up staging table
TRUNCATE TABLE RAW.COMPANY_OVERVIEW_STAGING;

SELECT '✅ Company overview data loading completed successfully!' as COMPLETION_STATUS;