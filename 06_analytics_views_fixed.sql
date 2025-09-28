-- ============================================================================
-- ANALYTICS LAYER: Business-Ready Views and KPIs (FIXED VERSION)
-- Creates analytical views using watermarked data for business intelligence
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE WAREHOUSE FIN_TRADE_WH;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- STEP 1: Current Financial Snapshot (Latest Data) - WITHOUT moving averages
-- ============================================================================

CREATE OR REPLACE VIEW CURRENT_FINANCIAL_SNAPSHOT AS
WITH latest_data AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY SYMBOL ORDER BY LOAD_TIMESTAMP DESC) as rn
    FROM FIN_TRADE_EXTRACT.RAW.OVERVIEW
)
SELECT 
    SYMBOL,
    NAME as COMPANY_NAME,
    SECTOR,
    INDUSTRY,
    EXCHANGE,
    COUNTRY,
    
    -- Valuation Metrics
    ROUND(MARKET_CAPITALIZATION / 1000000000, 2) as MARKET_CAP_BILLIONS,
    PE_RATIO,
    PRICE_TO_BOOK_RATIO,
    PRICE_TO_SALES_RATIO_TTM,
    EV_TO_EBITDA,
    
    -- Profitability Metrics
    PROFIT_MARGIN * 100 as PROFIT_MARGIN_PCT,
    OPERATING_MARGIN_TTM * 100 as OPERATING_MARGIN_PCT,
    RETURN_ON_ASSETS_TTM * 100 as ROA_PCT,
    RETURN_ON_EQUITY_TTM * 100 as ROE_PCT,
    
    -- Growth Metrics
    QUARTERLY_EARNINGS_GROWTH_YOY * 100 as QUARTERLY_EARNINGS_GROWTH_PCT,
    QUARTERLY_REVENUE_GROWTH_YOY * 100 as QUARTERLY_REVENUE_GROWTH_PCT,
    
    -- Dividend Information
    DIVIDEND_YIELD * 100 as DIVIDEND_YIELD_PCT,
    DIVIDEND_PER_SHARE,
    DIVIDEND_DATE,
    
    -- Trading Metrics
    BETA,
    WEEK_52_HIGH,
    WEEK_52_LOW,
    -- Note: Moving averages temporarily removed until column names verified
    
    -- Data Quality
    LOAD_TIMESTAMP as LAST_UPDATED,
    BATCH_ID,
    DATEDIFF('hour', LOAD_TIMESTAMP, CURRENT_TIMESTAMP) as HOURS_SINCE_UPDATE
    
FROM latest_data
WHERE rn = 1
AND NAME IS NOT NULL;

-- ============================================================================
-- STEP 2: Market Cap Rankings and Classifications
-- ============================================================================

CREATE OR REPLACE VIEW MARKET_CAP_RANKINGS AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY MARKET_CAP_BILLIONS DESC) as RANK,
    SYMBOL,
    COMPANY_NAME,
    SECTOR,
    MARKET_CAP_BILLIONS,
    
    -- Market Cap Classification
    CASE 
        WHEN MARKET_CAP_BILLIONS >= 200 THEN 'MEGA_CAP'
        WHEN MARKET_CAP_BILLIONS >= 10 THEN 'LARGE_CAP'
        WHEN MARKET_CAP_BILLIONS >= 2 THEN 'MID_CAP'
        WHEN MARKET_CAP_BILLIONS >= 0.3 THEN 'SMALL_CAP'
        ELSE 'MICRO_CAP'
    END as CAP_CLASSIFICATION,
    
    PE_RATIO,
    ROE_PCT,
    DIVIDEND_YIELD_PCT,
    LAST_UPDATED
    
FROM CURRENT_FINANCIAL_SNAPSHOT
WHERE MARKET_CAP_BILLIONS IS NOT NULL
ORDER BY RANK;

-- ============================================================================
-- STEP 3: Sector Performance Analysis
-- ============================================================================

CREATE OR REPLACE VIEW SECTOR_PERFORMANCE AS
SELECT 
    SECTOR,
    COUNT(*) as COMPANY_COUNT,
    
    -- Market Cap Metrics
    ROUND(SUM(MARKET_CAP_BILLIONS), 2) as TOTAL_MARKET_CAP_BILLIONS,
    ROUND(AVG(MARKET_CAP_BILLIONS), 2) as AVG_MARKET_CAP_BILLIONS,
    ROUND(MEDIAN(MARKET_CAP_BILLIONS), 2) as MEDIAN_MARKET_CAP_BILLIONS,
    
    -- Valuation Metrics
    ROUND(AVG(PE_RATIO), 2) as AVG_PE_RATIO,
    ROUND(MEDIAN(PE_RATIO), 2) as MEDIAN_PE_RATIO,
    
    -- Profitability Metrics
    ROUND(AVG(PROFIT_MARGIN_PCT), 2) as AVG_PROFIT_MARGIN_PCT,
    ROUND(AVG(ROE_PCT), 2) as AVG_ROE_PCT,
    ROUND(AVG(ROA_PCT), 2) as AVG_ROA_PCT,
    
    -- Growth Metrics
    ROUND(AVG(QUARTERLY_EARNINGS_GROWTH_PCT), 2) as AVG_EARNINGS_GROWTH_PCT,
    ROUND(AVG(QUARTERLY_REVENUE_GROWTH_PCT), 2) as AVG_REVENUE_GROWTH_PCT,
    
    -- Dividend Metrics
    ROUND(AVG(DIVIDEND_YIELD_PCT), 2) as AVG_DIVIDEND_YIELD_PCT,
    COUNT(CASE WHEN DIVIDEND_YIELD_PCT > 0 THEN 1 END) as DIVIDEND_PAYING_COMPANIES,
    
    -- Risk Metrics
    ROUND(AVG(BETA), 2) as AVG_BETA,
    
    MAX(LAST_UPDATED) as LAST_SECTOR_UPDATE
    
FROM CURRENT_FINANCIAL_SNAPSHOT
WHERE SECTOR IS NOT NULL
GROUP BY SECTOR
ORDER BY TOTAL_MARKET_CAP_BILLIONS DESC;

-- ============================================================================
-- STEP 4: Investment Screening Views
-- ============================================================================

-- High Quality Dividend Stocks
CREATE OR REPLACE VIEW HIGH_QUALITY_DIVIDEND_STOCKS AS
SELECT 
    m.RANK,
    m.SYMBOL,
    m.COMPANY_NAME,
    m.SECTOR,
    m.MARKET_CAP_BILLIONS,
    m.DIVIDEND_YIELD_PCT,
    m.PE_RATIO,
    c.ROE_PCT,
    c.PROFIT_MARGIN_PCT,
    c.BETA,
    m.LAST_UPDATED
FROM MARKET_CAP_RANKINGS m
JOIN CURRENT_FINANCIAL_SNAPSHOT c ON m.SYMBOL = c.SYMBOL
WHERE m.DIVIDEND_YIELD_PCT >= 2.0  -- At least 2% dividend yield
  AND m.PE_RATIO BETWEEN 5 AND 25  -- Reasonable valuation
  AND c.ROE_PCT >= 10               -- Strong return on equity
  AND c.PROFIT_MARGIN_PCT >= 5      -- Profitable companies
  AND m.MARKET_CAP_BILLIONS >= 1    -- Mid cap and above
ORDER BY m.DIVIDEND_YIELD_PCT DESC, m.MARKET_CAP_BILLIONS DESC;

-- Growth Stocks (High Growth, Higher Risk)
CREATE OR REPLACE VIEW HIGH_GROWTH_STOCKS AS
SELECT 
    m.RANK,
    m.SYMBOL,
    m.COMPANY_NAME,
    m.SECTOR,
    m.MARKET_CAP_BILLIONS,
    c.QUARTERLY_EARNINGS_GROWTH_PCT,
    c.QUARTERLY_REVENUE_GROWTH_PCT,
    m.PE_RATIO,
    c.ROE_PCT,
    c.BETA,
    m.LAST_UPDATED
FROM MARKET_CAP_RANKINGS m
JOIN CURRENT_FINANCIAL_SNAPSHOT c ON m.SYMBOL = c.SYMBOL
WHERE (c.QUARTERLY_EARNINGS_GROWTH_PCT >= 15 OR c.QUARTERLY_REVENUE_GROWTH_PCT >= 10)
  AND m.MARKET_CAP_BILLIONS >= 1
  AND m.PE_RATIO IS NOT NULL
ORDER BY c.QUARTERLY_EARNINGS_GROWTH_PCT DESC NULLS LAST;

-- Value Stocks (Low PE, Strong Fundamentals)
CREATE OR REPLACE VIEW VALUE_STOCKS AS
SELECT 
    m.RANK,
    m.SYMBOL,
    m.COMPANY_NAME,
    m.SECTOR,
    m.MARKET_CAP_BILLIONS,
    m.PE_RATIO,
    c.PRICE_TO_BOOK_RATIO,
    c.ROE_PCT,
    c.ROA_PCT,
    c.PROFIT_MARGIN_PCT,
    m.LAST_UPDATED
FROM MARKET_CAP_RANKINGS m
JOIN CURRENT_FINANCIAL_SNAPSHOT c ON m.SYMBOL = c.SYMBOL
WHERE m.PE_RATIO BETWEEN 5 AND 15        -- Low to moderate PE
  AND c.PRICE_TO_BOOK_RATIO BETWEEN 0.5 AND 2  -- Reasonable P/B ratio
  AND c.ROE_PCT >= 8                      -- Decent returns
  AND c.PROFIT_MARGIN_PCT >= 3            -- Profitable
  AND m.MARKET_CAP_BILLIONS >= 2          -- Large enough companies
ORDER BY m.PE_RATIO ASC, c.ROE_PCT DESC;

-- ============================================================================
-- STEP 5: Business KPI Summary
-- ============================================================================

CREATE OR REPLACE VIEW BUSINESS_KPI_SUMMARY AS
SELECT 
    'PORTFOLIO_OVERVIEW' as KPI_CATEGORY,
    COUNT(DISTINCT SYMBOL) as TOTAL_COMPANIES_TRACKED,
    COUNT(DISTINCT SECTOR) as SECTORS_COVERED,
    COUNT(DISTINCT EXCHANGE) as EXCHANGES_COVERED,
    ROUND(SUM(MARKET_CAP_BILLIONS), 1) as TOTAL_MARKET_CAP_BILLIONS,
    ROUND(AVG(MARKET_CAP_BILLIONS), 1) as AVG_MARKET_CAP_BILLIONS,
    ROUND(AVG(PE_RATIO), 1) as AVG_PE_RATIO,
    ROUND(AVG(DIVIDEND_YIELD_PCT), 2) as AVG_DIVIDEND_YIELD_PCT,
    MAX(LAST_UPDATED) as LAST_DATA_UPDATE
FROM CURRENT_FINANCIAL_SNAPSHOT

UNION ALL

SELECT 
    'HIGH_VALUE_OPPORTUNITIES' as KPI_CATEGORY,
    (SELECT COUNT(*) FROM HIGH_QUALITY_DIVIDEND_STOCKS) as DIVIDEND_OPPORTUNITIES,
    (SELECT COUNT(*) FROM HIGH_GROWTH_STOCKS) as GROWTH_OPPORTUNITIES,  
    (SELECT COUNT(*) FROM VALUE_STOCKS) as VALUE_OPPORTUNITIES,
    (SELECT ROUND(AVG(MARKET_CAP_BILLIONS), 1) FROM HIGH_QUALITY_DIVIDEND_STOCKS) as AVG_DIVIDEND_STOCK_SIZE,
    (SELECT ROUND(AVG(DIVIDEND_YIELD_PCT), 2) FROM HIGH_QUALITY_DIVIDEND_STOCKS) as AVG_DIVIDEND_YIELD,
    (SELECT ROUND(AVG(PE_RATIO), 1) FROM VALUE_STOCKS) as AVG_VALUE_STOCK_PE,
    NULL as AVG_DIVIDEND_YIELD_PCT,
    CURRENT_TIMESTAMP as LAST_DATA_UPDATE;

SELECT 'ANALYTICS LAYER COMPLETE (without moving averages)' as status;
SELECT 'Run check-column-names.sql to verify moving average column names' as next_step;