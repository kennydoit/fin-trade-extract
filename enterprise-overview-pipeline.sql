-- ============================================================================
-- Enterprise Data Pipeline with Staging, Watermarking, and Deduplication
-- Implements proper data engineering patterns for the OVERVIEW pipeline
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;

-- ============================================================================
-- 1. STAGING/LANDING TABLES 
-- ============================================================================

-- Create staging schema for raw ingestion
CREATE SCHEMA IF NOT EXISTS STAGING;
USE SCHEMA STAGING;

-- Staging table - exact copy of CSV structure for initial load
CREATE OR REPLACE TABLE OVERVIEW_STAGING (
    -- Core identification fields (matches actual CSV from Lambda)
    SYMBOL_ID                           VARCHAR(50),
    SYMBOL                              VARCHAR(20),
    ASSET_TYPE                          VARCHAR(50),
    NAME                                VARCHAR(500),
    DESCRIPTION                         VARCHAR(16777216),
    CIK                                 VARCHAR(20),
    EXCHANGE                            VARCHAR(50),
    CURRENCY                            VARCHAR(10),
    COUNTRY                             VARCHAR(100),
    SECTOR                              VARCHAR(100),
    INDUSTRY                            VARCHAR(200),
    ADDRESS                             VARCHAR(16777216),
    OFFICIAL_SITE                       VARCHAR(500),
    FISCAL_YEAR_END                     VARCHAR(50),
    
    -- Financial metrics
    MARKET_CAPITALIZATION               NUMBER(20,0),
    EBITDA                              NUMBER(20,0),
    PE_RATIO                            NUMBER(10,4),
    PEG_RATIO                           NUMBER(10,4),
    BOOK_VALUE                          NUMBER(10,4),
    DIVIDEND_PER_SHARE                  NUMBER(10,4),
    DIVIDEND_YIELD                      NUMBER(8,6),
    EPS                                 NUMBER(10,4),
    REVENUE_PER_SHARE_TTM               NUMBER(10,4),
    PROFIT_MARGIN                       NUMBER(8,6),
    OPERATING_MARGIN_TTM                NUMBER(8,6),
    RETURN_ON_ASSETS_TTM                NUMBER(8,6),
    RETURN_ON_EQUITY_TTM                NUMBER(8,6),
    REVENUE_TTM                         NUMBER(20,0),
    GROSS_PROFIT_TTM                    NUMBER(20,0),
    DILUTED_EPS_TTM                     NUMBER(10,4),
    QUARTERLY_EARNINGS_GROWTH_YOY       NUMBER(8,6),
    QUARTERLY_REVENUE_GROWTH_YOY        NUMBER(8,6),
    ANALYST_TARGET_PRICE                NUMBER(10,4),
    TRAILING_PE                         NUMBER(10,4),
    FORWARD_PE                          NUMBER(10,4),
    PRICE_TO_SALES_RATIO_TTM            NUMBER(10,4),
    PRICE_TO_BOOK_RATIO                 NUMBER(10,4),
    EV_TO_REVENUE                       NUMBER(10,4),
    EV_TO_EBITDA                        NUMBER(10,4),
    BETA                                NUMBER(8,6),
    WEEK_52_HIGH                        NUMBER(12,4),
    WEEK_52_LOW                         NUMBER(12,4),
    DAY_50_MOVING_AVERAGE               NUMBER(12,4),
    DAY_200_MOVING_AVERAGE              NUMBER(12,4),
    SHARES_OUTSTANDING                  NUMBER(20,0),
    DIVIDEND_DATE                       DATE,
    EX_DIVIDEND_DATE                    DATE,
    
    -- Lambda metadata
    API_RESPONSE_STATUS                 VARCHAR(50),
    CREATED_AT                          TIMESTAMP,
    UPDATED_AT                          TIMESTAMP,
    
    -- Staging metadata
    FILE_NAME                           VARCHAR(500),    -- Track source file
    INGESTED_AT                         TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID                            VARCHAR(100)     -- Track processing batch
)
COMMENT = 'Staging table for OVERVIEW data - raw CSV ingestion with metadata'
CLUSTER BY (SYMBOL, INGESTED_AT);

-- ============================================================================
-- 2. WATERMARKING TABLE
-- ============================================================================

-- Table to track processing watermarks
CREATE OR REPLACE TABLE DATA_WATERMARKS (
    TABLE_NAME                          VARCHAR(100),
    LAST_PROCESSED_FILE                 VARCHAR(500),
    LAST_PROCESSED_TIMESTAMP            TIMESTAMP_LTZ,
    LAST_SUCCESSFUL_BATCH_ID            VARCHAR(100),
    RECORDS_PROCESSED                   NUMBER,
    CREATED_AT                          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Watermark tracking for all data pipeline tables';

-- Initialize watermark for OVERVIEW
INSERT INTO DATA_WATERMARKS (TABLE_NAME, LAST_PROCESSED_TIMESTAMP, RECORDS_PROCESSED)
VALUES ('OVERVIEW', '1900-01-01'::TIMESTAMP_LTZ, 0);

-- ============================================================================
-- 3. ENHANCED PRODUCTION TABLE
-- ============================================================================

USE SCHEMA RAW;

-- Enhanced production table with better keys and constraints
CREATE OR REPLACE TABLE OVERVIEW (
    -- Primary key and versioning
    OVERVIEW_KEY                        VARCHAR(100) NOT NULL,  -- SYMBOL + '_' + CREATED_DATE
    SYMBOL                              VARCHAR(20) NOT NULL,
    
    -- Core identification fields
    SYMBOL_ID                           VARCHAR(50),
    ASSET_TYPE                          VARCHAR(50),
    NAME                                VARCHAR(500),
    DESCRIPTION                         VARCHAR(16777216),
    CIK                                 VARCHAR(20),
    EXCHANGE                            VARCHAR(50),
    CURRENCY                            VARCHAR(10),
    COUNTRY                             VARCHAR(100),
    SECTOR                              VARCHAR(100),
    INDUSTRY                            VARCHAR(200),
    ADDRESS                             VARCHAR(16777216),
    OFFICIAL_SITE                       VARCHAR(500),
    FISCAL_YEAR_END                     VARCHAR(50),
    
    -- Financial metrics
    MARKET_CAPITALIZATION               NUMBER(20,0),
    EBITDA                              NUMBER(20,0),
    PE_RATIO                            NUMBER(10,4),
    PEG_RATIO                           NUMBER(10,4),
    BOOK_VALUE                          NUMBER(10,4),
    DIVIDEND_PER_SHARE                  NUMBER(10,4),
    DIVIDEND_YIELD                      NUMBER(8,6),
    EPS                                 NUMBER(10,4),
    REVENUE_PER_SHARE_TTM               NUMBER(10,4),
    PROFIT_MARGIN                       NUMBER(8,6),
    OPERATING_MARGIN_TTM                NUMBER(8,6),
    RETURN_ON_ASSETS_TTM                NUMBER(8,6),
    RETURN_ON_EQUITY_TTM                NUMBER(8,6),
    REVENUE_TTM                         NUMBER(20,0),
    GROSS_PROFIT_TTM                    NUMBER(20,0),
    DILUTED_EPS_TTM                     NUMBER(10,4),
    QUARTERLY_EARNINGS_GROWTH_YOY       NUMBER(8,6),
    QUARTERLY_REVENUE_GROWTH_YOY        NUMBER(8,6),
    ANALYST_TARGET_PRICE                NUMBER(10,4),
    TRAILING_PE                         NUMBER(10,4),
    FORWARD_PE                          NUMBER(10,4),
    PRICE_TO_SALES_RATIO_TTM            NUMBER(10,4),
    PRICE_TO_BOOK_RATIO                 NUMBER(10,4),
    EV_TO_REVENUE                       NUMBER(10,4),
    EV_TO_EBITDA                        NUMBER(10,4),
    BETA                                NUMBER(8,6),
    WEEK_52_HIGH                        NUMBER(12,4),
    WEEK_52_LOW                         NUMBER(12,4),
    DAY_50_MOVING_AVERAGE               NUMBER(12,4),
    DAY_200_MOVING_AVERAGE              NUMBER(12,4),
    SHARES_OUTSTANDING                  NUMBER(20,0),
    DIVIDEND_DATE                       DATE,
    EX_DIVIDEND_DATE                    DATE,
    
    -- Source tracking
    API_RESPONSE_STATUS                 VARCHAR(50),
    SOURCE_CREATED_AT                   TIMESTAMP,
    SOURCE_UPDATED_AT                   TIMESTAMP,
    SOURCE_FILE_NAME                    VARCHAR(500),
    BATCH_ID                            VARCHAR(100),
    
    -- Data quality flags
    IS_CURRENT_RECORD                   BOOLEAN DEFAULT TRUE,
    DATA_QUALITY_SCORE                  NUMBER(3,2),    -- 0.00 to 1.00
    
    -- Pipeline metadata
    INGESTED_AT                         TIMESTAMP_LTZ,
    PROCESSED_AT                        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    PIPELINE_VERSION                    VARCHAR(20) DEFAULT '1.0'
)
COMMENT = 'Production OVERVIEW table with deduplication and versioning'
CLUSTER BY (SYMBOL, PROCESSED_AT);

-- Add primary key constraint
ALTER TABLE OVERVIEW ADD CONSTRAINT PK_OVERVIEW PRIMARY KEY (OVERVIEW_KEY);

-- ============================================================================
-- 4. DEDUPLICATION AND PROCESSING STORED PROCEDURES
-- ============================================================================

-- Procedure to process staging data to production
CREATE OR REPLACE PROCEDURE SP_PROCESS_OVERVIEW_STAGING(BATCH_ID VARCHAR)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Get current watermark
    var watermark_query = `
        SELECT LAST_PROCESSED_TIMESTAMP, LAST_SUCCESSFUL_BATCH_ID 
        FROM STAGING.DATA_WATERMARKS 
        WHERE TABLE_NAME = 'OVERVIEW'
    `;
    var watermark_stmt = snowflake.createStatement({sqlText: watermark_query});
    var watermark_result = watermark_stmt.execute();
    watermark_result.next();
    var last_processed = watermark_result.getColumnValue(1);
    var last_batch = watermark_result.getColumnValue(2);
    
    // Process new records from staging
    var process_query = `
        INSERT INTO RAW.OVERVIEW (
            OVERVIEW_KEY, SYMBOL, SYMBOL_ID, ASSET_TYPE, NAME, DESCRIPTION,
            CIK, EXCHANGE, CURRENCY, COUNTRY, SECTOR, INDUSTRY, ADDRESS,
            OFFICIAL_SITE, FISCAL_YEAR_END, MARKET_CAPITALIZATION, EBITDA,
            PE_RATIO, PEG_RATIO, BOOK_VALUE, DIVIDEND_PER_SHARE, DIVIDEND_YIELD,
            EPS, REVENUE_PER_SHARE_TTM, PROFIT_MARGIN, OPERATING_MARGIN_TTM,
            RETURN_ON_ASSETS_TTM, RETURN_ON_EQUITY_TTM, REVENUE_TTM, GROSS_PROFIT_TTM,
            DILUTED_EPS_TTM, QUARTERLY_EARNINGS_GROWTH_YOY, QUARTERLY_REVENUE_GROWTH_YOY,
            ANALYST_TARGET_PRICE, TRAILING_PE, FORWARD_PE, PRICE_TO_SALES_RATIO_TTM,
            PRICE_TO_BOOK_RATIO, EV_TO_REVENUE, EV_TO_EBITDA, BETA, WEEK_52_HIGH,
            WEEK_52_LOW, DAY_50_MOVING_AVERAGE, DAY_200_MOVING_AVERAGE, SHARES_OUTSTANDING,
            DIVIDEND_DATE, EX_DIVIDEND_DATE, API_RESPONSE_STATUS, SOURCE_CREATED_AT,
            SOURCE_UPDATED_AT, SOURCE_FILE_NAME, BATCH_ID, INGESTED_AT,
            DATA_QUALITY_SCORE, IS_CURRENT_RECORD
        )
        SELECT 
            SYMBOL || '_' || TO_VARCHAR(CREATED_AT, 'YYYY-MM-DD') as OVERVIEW_KEY,
            SYMBOL, SYMBOL_ID, ASSET_TYPE, NAME, DESCRIPTION,
            CIK, EXCHANGE, CURRENCY, COUNTRY, SECTOR, INDUSTRY, ADDRESS,
            OFFICIAL_SITE, FISCAL_YEAR_END, MARKET_CAPITALIZATION, EBITDA,
            PE_RATIO, PEG_RATIO, BOOK_VALUE, DIVIDEND_PER_SHARE, DIVIDEND_YIELD,
            EPS, REVENUE_PER_SHARE_TTM, PROFIT_MARGIN, OPERATING_MARGIN_TTM,
            RETURN_ON_ASSETS_TTM, RETURN_ON_EQUITY_TTM, REVENUE_TTM, GROSS_PROFIT_TTM,
            DILUTED_EPS_TTM, QUARTERLY_EARNINGS_GROWTH_YOY, QUARTERLY_REVENUE_GROWTH_YOY,
            ANALYST_TARGET_PRICE, TRAILING_PE, FORWARD_PE, PRICE_TO_SALES_RATIO_TTM,
            PRICE_TO_BOOK_RATIO, EV_TO_REVENUE, EV_TO_EBITDA, BETA, WEEK_52_HIGH,
            WEEK_52_LOW, DAY_50_MOVING_AVERAGE, DAY_200_MOVING_AVERAGE, SHARES_OUTSTANDING,
            DIVIDEND_DATE, EX_DIVIDEND_DATE, API_RESPONSE_STATUS, CREATED_AT,
            UPDATED_AT, FILE_NAME, BATCH_ID, INGESTED_AT,
            CASE 
                WHEN API_RESPONSE_STATUS = 'success' AND NAME IS NOT NULL THEN 1.0
                WHEN API_RESPONSE_STATUS = 'success' THEN 0.7
                ELSE 0.3
            END as DATA_QUALITY_SCORE,
            TRUE as IS_CURRENT_RECORD
        FROM STAGING.OVERVIEW_STAGING s
        WHERE s.INGESTED_AT > '` + last_processed + `'
            AND s.BATCH_ID = '` + BATCH_ID + `'
            AND NOT EXISTS (
                SELECT 1 FROM RAW.OVERVIEW p 
                WHERE p.SYMBOL = s.SYMBOL 
                AND p.SOURCE_CREATED_AT = s.CREATED_AT
            )
    `;
    
    var process_stmt = snowflake.createStatement({sqlText: process_query});
    var process_result = process_stmt.execute();
    
    // Update watermark
    var update_watermark = `
        UPDATE STAGING.DATA_WATERMARKS 
        SET LAST_PROCESSED_TIMESTAMP = CURRENT_TIMESTAMP(),
            LAST_SUCCESSFUL_BATCH_ID = '` + BATCH_ID + `',
            UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE TABLE_NAME = 'OVERVIEW'
    `;
    
    var watermark_update_stmt = snowflake.createStatement({sqlText: update_watermark});
    watermark_update_stmt.execute();
    
    return "Processing completed for batch: " + BATCH_ID;
$$;

-- Show created objects
SHOW TABLES IN SCHEMA STAGING;
SHOW TABLES IN SCHEMA RAW;
SHOW PROCEDURES;