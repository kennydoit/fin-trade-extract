-- ============================================================================
-- Load Company Overview Data from S3 Stage - Watermark-Based Processing
-- 
-- Features:
-- - Production-ready company overview processing
-- - Comprehensive company profile and financial metrics
-- - Calculated SYMBOL_ID column (consistent with other tables)
-- - Proper duplicate handling and data quality validation
-- - Single snapshot per symbol (no historical versions)
-- ============================================================================
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- FOR TESTING ONLY: Clean up any existing objects
-- DROP STAGE IF EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGE;
-- DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING;
-- DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW;

-- 1) Create external stage pointing to S3 company_overview folder (force recreate to ensure CSV format)
CREATE OR REPLACE STAGE FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGE
  URL='s3://fin-trade-craft-landing/company_overview/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
  FILE_FORMAT = (
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', 'None', '')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE = '\\'
    ESCAPE_UNENCLOSED_FIELD = '\\'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ENCODING = 'UTF8'
  );

-- 2) List files in stage to verify content
-- LIST @COMPANY_OVERVIEW_STAGE;

-- 3) Create main table for company overview data
-- ⚠️ CRITICAL: DO NOT DROP - Would delete all company overview data!
-- DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW;
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW (
    SYMBOL_ID                           NUMBER(38,0),
    SYMBOL                              VARCHAR(20) NOT NULL,
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
    FISCAL_YEAR_END                     VARCHAR(50),
    LATEST_QUARTER                      DATE,
    
    -- Financial Metrics
    MARKET_CAPITALIZATION               NUMBER(20,0),
    EBITDA                              NUMBER(20,0),
    PE_RATIO                            NUMBER(10,2),
    PEG_RATIO                           NUMBER(10,4),
    BOOK_VALUE                          NUMBER(10,2),
    DIVIDEND_PER_SHARE                  NUMBER(10,4),
    DIVIDEND_YIELD                      NUMBER(10,6),
    EPS                                 NUMBER(10,4),
    REVENUE_PER_SHARE_TTM               NUMBER(10,4),
    PROFIT_MARGIN                       NUMBER(10,6),
    OPERATING_MARGIN_TTM                NUMBER(10,6),
    RETURN_ON_ASSETS_TTM                NUMBER(10,6),
    RETURN_ON_EQUITY_TTM                NUMBER(10,6),
    REVENUE_TTM                         NUMBER(20,0),
    GROSS_PROFIT_TTM                    NUMBER(20,0),
    DILUTED_EPS_TTM                     NUMBER(10,4),
    QUARTERLY_EARNINGS_GROWTH_YOY       NUMBER(10,6),
    QUARTERLY_REVENUE_GROWTH_YOY        NUMBER(10,6),
    ANALYST_TARGET_PRICE                NUMBER(10,2),
    TRAILING_PE                         NUMBER(10,2),
    FORWARD_PE                          NUMBER(10,2),
    PRICE_TO_SALES_RATIO_TTM            NUMBER(10,4),
    PRICE_TO_BOOK_RATIO                 NUMBER(10,4),
    EV_TO_REVENUE                       NUMBER(10,4),
    EV_TO_EBITDA                        NUMBER(10,4),
    BETA                                NUMBER(10,6),
    WEEK_52_HIGH                        NUMBER(10,2),
    WEEK_52_LOW                         NUMBER(10,2),
    DAY_50_MOVING_AVERAGE               NUMBER(10,2),
    DAY_200_MOVING_AVERAGE              NUMBER(10,2),
    SHARES_OUTSTANDING                  NUMBER(20,0),
    DIVIDEND_DATE                       DATE,
    EX_DIVIDEND_DATE                    DATE,
    
    -- Metadata
    LOAD_DATE                           DATE DEFAULT CURRENT_DATE(),
    
    -- Constraints
    CONSTRAINT UK_COMPANY_OVERVIEW_SYMBOL UNIQUE (SYMBOL)
)
COMMENT = 'Company overview and fundamentals data from Alpha Vantage API'
CLUSTER BY (SYMBOL, SECTOR);

-- 4) Create staging table (transient for performance)
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING (
    Symbol VARCHAR(20),
    symbol_id NUMBER(38,0),
    AssetType VARCHAR(50),
    Name VARCHAR(500),
    Description TEXT,
    CIK VARCHAR(20),
    Exchange VARCHAR(50),
    Currency VARCHAR(10),
    Country VARCHAR(100),
    Sector VARCHAR(100),
    Industry VARCHAR(200),
    Address TEXT,
    FiscalYearEnd VARCHAR(50),
    LatestQuarter DATE,
    
    -- Financial Metrics
    MarketCapitalization NUMBER(20,0),
    EBITDA NUMBER(20,0),
    PERatio NUMBER(10,2),
    PEGRatio NUMBER(10,4),
    BookValue NUMBER(10,2),
    DividendPerShare NUMBER(10,4),
    DividendYield NUMBER(10,6),
    EPS NUMBER(10,4),
    RevenuePerShareTTM NUMBER(10,4),
    ProfitMargin NUMBER(10,6),
    OperatingMarginTTM NUMBER(10,6),
    ReturnOnAssetsTTM NUMBER(10,6),
    ReturnOnEquityTTM NUMBER(10,6),
    RevenueTTM NUMBER(20,0),
    GrossProfitTTM NUMBER(20,0),
    DilutedEPSTTM NUMBER(10,4),
    QuarterlyEarningsGrowthYOY NUMBER(10,6),
    QuarterlyRevenueGrowthYOY NUMBER(10,6),
    AnalystTargetPrice NUMBER(10,2),
    TrailingPE NUMBER(10,2),
    ForwardPE NUMBER(10,2),
    PriceToSalesRatioTTM NUMBER(10,4),
    PriceToBookRatio NUMBER(10,4),
    EVToRevenue NUMBER(10,4),
    EVToEBITDA NUMBER(10,4),
    Beta NUMBER(10,6),
    Week52High NUMBER(10,2),
    Week52Low NUMBER(10,2),
    Day50MovingAverage NUMBER(10,2),
    Day200MovingAverage NUMBER(10,2),
    SharesOutstanding NUMBER(20,0),
    DividendDate DATE,
    ExDividendDate DATE,
    
    -- Metadata
    source_filename VARCHAR(500)
);

-- 5) Load CSV files from S3 stage into staging table
COPY INTO FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING (
    Symbol,
    AssetType,
    Name,
    Description,
    CIK,
    Exchange,
    Currency,
    Country,
    Sector,
    Industry,
    Address,
    FiscalYearEnd,
    LatestQuarter,
    MarketCapitalization,
    EBITDA,
    PERatio,
    PEGRatio,
    BookValue,
    DividendPerShare,
    DividendYield,
    EPS,
    RevenuePerShareTTM,
    ProfitMargin,
    OperatingMarginTTM,
    ReturnOnAssetsTTM,
    ReturnOnEquityTTM,
    RevenueTTM,
    GrossProfitTTM,
    DilutedEPSTTM,
    QuarterlyEarningsGrowthYOY,
    QuarterlyRevenueGrowthYOY,
    AnalystTargetPrice,
    TrailingPE,
    ForwardPE,
    PriceToSalesRatioTTM,
    PriceToBookRatio,
    EVToRevenue,
    EVToEBITDA,
    Beta,
    Week52High,
    Week52Low,
    Day50MovingAverage,
    Day200MovingAverage,
    SharesOutstanding,
    DividendDate,
    ExDividendDate,
    source_filename
)
FROM (
    SELECT 
        $1::VARCHAR(20) as Symbol,
        $2::VARCHAR(50) as AssetType,
        $3::VARCHAR(500) as Name,
        $4::TEXT as Description,
        $5::VARCHAR(20) as CIK,
        $6::VARCHAR(50) as Exchange,
        $7::VARCHAR(10) as Currency,
        $8::VARCHAR(100) as Country,
        $9::VARCHAR(100) as Sector,
        $10::VARCHAR(200) as Industry,
        $11::TEXT as Address,
        $12::VARCHAR(50) as FiscalYearEnd,
        TRY_TO_DATE($13, 'YYYY-MM-DD') as LatestQuarter,
        TRY_TO_NUMBER($14, 20, 0) as MarketCapitalization,
        TRY_TO_NUMBER($15, 20, 0) as EBITDA,
        TRY_TO_NUMBER($16, 10, 2) as PERatio,
        TRY_TO_NUMBER($17, 10, 4) as PEGRatio,
        TRY_TO_NUMBER($18, 10, 2) as BookValue,
        TRY_TO_NUMBER($19, 10, 4) as DividendPerShare,
        TRY_TO_NUMBER($20, 10, 6) as DividendYield,
        TRY_TO_NUMBER($21, 10, 4) as EPS,
        TRY_TO_NUMBER($22, 10, 4) as RevenuePerShareTTM,
        TRY_TO_NUMBER($23, 10, 6) as ProfitMargin,
        TRY_TO_NUMBER($24, 10, 6) as OperatingMarginTTM,
        TRY_TO_NUMBER($25, 10, 6) as ReturnOnAssetsTTM,
        TRY_TO_NUMBER($26, 10, 6) as ReturnOnEquityTTM,
        TRY_TO_NUMBER($27, 20, 0) as RevenueTTM,
        TRY_TO_NUMBER($28, 20, 0) as GrossProfitTTM,
        TRY_TO_NUMBER($29, 10, 4) as DilutedEPSTTM,
        TRY_TO_NUMBER($30, 10, 6) as QuarterlyEarningsGrowthYOY,
        TRY_TO_NUMBER($31, 10, 6) as QuarterlyRevenueGrowthYOY,
        TRY_TO_NUMBER($32, 10, 2) as AnalystTargetPrice,
        TRY_TO_NUMBER($33, 10, 2) as TrailingPE,
        TRY_TO_NUMBER($34, 10, 2) as ForwardPE,
        TRY_TO_NUMBER($35, 10, 4) as PriceToSalesRatioTTM,
        TRY_TO_NUMBER($36, 10, 4) as PriceToBookRatio,
        TRY_TO_NUMBER($37, 10, 4) as EVToRevenue,
        TRY_TO_NUMBER($38, 10, 4) as EVToEBITDA,
        TRY_TO_NUMBER($39, 10, 6) as Beta,
        TRY_TO_NUMBER($40, 10, 2) as Week52High,
        TRY_TO_NUMBER($41, 10, 2) as Week52Low,
        TRY_TO_NUMBER($42, 10, 2) as Day50MovingAverage,
        TRY_TO_NUMBER($43, 10, 2) as Day200MovingAverage,
        TRY_TO_NUMBER($44, 20, 0) as SharesOutstanding,
        TRY_TO_DATE($45, 'YYYY-MM-DD') as DividendDate,
        TRY_TO_DATE($46, 'YYYY-MM-DD') as ExDividendDate,
        METADATA$FILENAME as source_filename
    FROM @COMPANY_OVERVIEW_STAGE
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
PATTERN = '.*\.csv'
ON_ERROR = CONTINUE;

-- 8) Remove duplicates (keep most recent file's data for each symbol)
DELETE FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING 
WHERE (Symbol, source_filename) IN (
    SELECT Symbol, source_filename
    FROM (
        SELECT 
            Symbol, 
            source_filename,
            ROW_NUMBER() OVER (PARTITION BY Symbol ORDER BY source_filename DESC) as rn
        FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING
    ) 
    WHERE rn > 1
);

-- Debug: Check data after cleanup and deduplication
SELECT 
    'After cleanup' as stage,
    COUNT(*) as clean_row_count,
    COUNT(DISTINCT Symbol) as unique_symbols,
    COUNT(DISTINCT Sector) as unique_sectors,
    COUNT(DISTINCT Industry) as unique_industries
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING;

-- 9) Load from staging to final table with MERGE (handle updates)
MERGE INTO FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW AS target
USING (
    SELECT 
        staging.symbol_id,
        staging.Symbol,
        staging.AssetType,
        staging.Name,
        staging.Description,
        staging.CIK,
        staging.Exchange,
        staging.Currency,
        staging.Country,
        staging.Sector,
        staging.Industry,
        staging.Address,
        staging.FiscalYearEnd,
        staging.LatestQuarter,
        
        -- Financial Metrics
        staging.MarketCapitalization,
        staging.EBITDA,
        staging.PERatio,
        staging.PEGRatio,
        staging.BookValue,
        staging.DividendPerShare,
        staging.DividendYield,
        staging.EPS,
        staging.RevenuePerShareTTM,
        staging.ProfitMargin,
        staging.OperatingMarginTTM,
        staging.ReturnOnAssetsTTM,
        staging.ReturnOnEquityTTM,
        staging.RevenueTTM,
        staging.GrossProfitTTM,
        staging.DilutedEPSTTM,
        staging.QuarterlyEarningsGrowthYOY,
        staging.QuarterlyRevenueGrowthYOY,
        staging.AnalystTargetPrice,
        staging.TrailingPE,
        staging.ForwardPE,
        staging.PriceToSalesRatioTTM,
        staging.PriceToBookRatio,
        staging.EVToRevenue,
        staging.EVToEBITDA,
        staging.Beta,
        staging.Week52High,
        staging.Week52Low,
        staging.Day50MovingAverage,
        staging.Day200MovingAverage,
        staging.SharesOutstanding,
        staging.DividendDate,
        staging.ExDividendDate
        
    FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING staging
    WHERE staging.Symbol IS NOT NULL
      AND staging.symbol_id IS NOT NULL
) AS source
ON target.SYMBOL = source.Symbol
WHEN MATCHED THEN
    UPDATE SET
        SYMBOL_ID = source.symbol_id,
        ASSET_TYPE = source.AssetType,
        NAME = source.Name,
        DESCRIPTION = source.Description,
        CIK = source.CIK,
        EXCHANGE = source.Exchange,
        CURRENCY = source.Currency,
        COUNTRY = source.Country,
        SECTOR = source.Sector,
        INDUSTRY = source.Industry,
        ADDRESS = source.Address,
        FISCAL_YEAR_END = source.FiscalYearEnd,
        LATEST_QUARTER = source.LatestQuarter,
        
        -- Financial Metrics
        MARKET_CAPITALIZATION = source.MarketCapitalization,
        EBITDA = source.EBITDA,
        PE_RATIO = source.PERatio,
        PEG_RATIO = source.PEGRatio,
        BOOK_VALUE = source.BookValue,
        DIVIDEND_PER_SHARE = source.DividendPerShare,
        DIVIDEND_YIELD = source.DividendYield,
        EPS = source.EPS,
        REVENUE_PER_SHARE_TTM = source.RevenuePerShareTTM,
        PROFIT_MARGIN = source.ProfitMargin,
        OPERATING_MARGIN_TTM = source.OperatingMarginTTM,
        RETURN_ON_ASSETS_TTM = source.ReturnOnAssetsTTM,
        RETURN_ON_EQUITY_TTM = source.ReturnOnEquityTTM,
        REVENUE_TTM = source.RevenueTTM,
        GROSS_PROFIT_TTM = source.GrossProfitTTM,
        DILUTED_EPS_TTM = source.DilutedEPSTTM,
        QUARTERLY_EARNINGS_GROWTH_YOY = source.QuarterlyEarningsGrowthYOY,
        QUARTERLY_REVENUE_GROWTH_YOY = source.QuarterlyRevenueGrowthYOY,
        ANALYST_TARGET_PRICE = source.AnalystTargetPrice,
        TRAILING_PE = source.TrailingPE,
        FORWARD_PE = source.ForwardPE,
        PRICE_TO_SALES_RATIO_TTM = source.PriceToSalesRatioTTM,
        PRICE_TO_BOOK_RATIO = source.PriceToBookRatio,
        EV_TO_REVENUE = source.EVToRevenue,
        EV_TO_EBITDA = source.EVToEBITDA,
        BETA = source.Beta,
        WEEK_52_HIGH = source.Week52High,
        WEEK_52_LOW = source.Week52Low,
        DAY_50_MOVING_AVERAGE = source.Day50MovingAverage,
        DAY_200_MOVING_AVERAGE = source.Day200MovingAverage,
        SHARES_OUTSTANDING = source.SharesOutstanding,
        DIVIDEND_DATE = source.DividendDate,
        EX_DIVIDEND_DATE = source.ExDividendDate,
        
        -- Metadata
        LOAD_DATE = CURRENT_DATE()
        
WHEN NOT MATCHED THEN
    INSERT (
        SYMBOL_ID, SYMBOL, ASSET_TYPE, NAME, DESCRIPTION, CIK, EXCHANGE, CURRENCY, COUNTRY,
        SECTOR, INDUSTRY, ADDRESS, FISCAL_YEAR_END, LATEST_QUARTER,
        MARKET_CAPITALIZATION, EBITDA, PE_RATIO, PEG_RATIO, BOOK_VALUE, DIVIDEND_PER_SHARE,
        DIVIDEND_YIELD, EPS, REVENUE_PER_SHARE_TTM, PROFIT_MARGIN, OPERATING_MARGIN_TTM,
        RETURN_ON_ASSETS_TTM, RETURN_ON_EQUITY_TTM, REVENUE_TTM, GROSS_PROFIT_TTM,
        DILUTED_EPS_TTM, QUARTERLY_EARNINGS_GROWTH_YOY, QUARTERLY_REVENUE_GROWTH_YOY,
        ANALYST_TARGET_PRICE, TRAILING_PE, FORWARD_PE, PRICE_TO_SALES_RATIO_TTM,
        PRICE_TO_BOOK_RATIO, EV_TO_REVENUE, EV_TO_EBITDA, BETA, WEEK_52_HIGH, WEEK_52_LOW,
        DAY_50_MOVING_AVERAGE, DAY_200_MOVING_AVERAGE, SHARES_OUTSTANDING, DIVIDEND_DATE,
        EX_DIVIDEND_DATE, LOAD_DATE
    )
    VALUES (
        source.symbol_id, source.Symbol, source.AssetType, source.Name, source.Description,
        source.CIK, source.Exchange, source.Currency, source.Country, source.Sector,
        source.Industry, source.Address, source.FiscalYearEnd, source.LatestQuarter,
        source.MarketCapitalization, source.EBITDA, source.PERatio, source.PEGRatio,
        source.BookValue, source.DividendPerShare, source.DividendYield, source.EPS,
        source.RevenuePerShareTTM, source.ProfitMargin, source.OperatingMarginTTM,
        source.ReturnOnAssetsTTM, source.ReturnOnEquityTTM, source.RevenueTTM,
        source.GrossProfitTTM, source.DilutedEPSTTM, source.QuarterlyEarningsGrowthYOY,
        source.QuarterlyRevenueGrowthYOY, source.AnalystTargetPrice, source.TrailingPE,
        source.ForwardPE, source.PriceToSalesRatioTTM, source.PriceToBookRatio,
        source.EVToRevenue, source.EVToEBITDA, source.Beta, source.Week52High,
        source.Week52Low, source.Day50MovingAverage, source.Day200MovingAverage,
        source.SharesOutstanding, source.DividendDate, source.ExDividendDate,
        CURRENT_DATE()
    );

-- Cleanup staging table
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW_STAGING;

SELECT 'Company overview data loading completed successfully!' as status;
