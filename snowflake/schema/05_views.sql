-- ============================================================================
-- Snowflake Analytical Views and Transformations
-- Business Intelligence layer for fin-trade-extract data
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- CORE BUSINESS VIEWS
-- ============================================================================

-- Company master view with latest overview data
CREATE OR REPLACE VIEW COMPANIES AS
SELECT 
    ls.SYMBOL_ID,
    ls.SYMBOL,
    ls.NAME AS COMPANY_NAME,
    ls.EXCHANGE,
    ls.ASSET_TYPE,
    ls.IPO_DATE,
    ls.STATUS,
    o.SECTOR,
    o.INDUSTRY,
    o.COUNTRY,
    o.MARKET_CAPITALIZATION,
    o.REVENUE_TTM,
    o.NET_INCOME,
    o.PE_RATIO,
    o.DIVIDEND_YIELD,
    o.BETA,
    o.WEEK_52_HIGH,
    o.WEEK_52_LOW,
    ls.CREATED_AT,
    GREATEST(ls.UPDATED_AT, COALESCE(o.UPDATED_AT, ls.UPDATED_AT)) AS LAST_UPDATED
FROM RAW.LISTING_STATUS ls
LEFT JOIN RAW.OVERVIEW o ON ls.SYMBOL_ID = o.SYMBOL_ID
WHERE ls.STATE = 'active'
ORDER BY ls.SYMBOL;

-- Latest stock prices view
CREATE OR REPLACE VIEW LATEST_STOCK_PRICES AS
SELECT 
    ts.SYMBOL_ID,
    ts.SYMBOL,
    c.COMPANY_NAME,
    ts.DATE AS PRICE_DATE,
    ts.OPEN,
    ts.HIGH,
    ts.LOW,
    ts.CLOSE,
    ts.ADJUSTED_CLOSE,
    ts.VOLUME,
    ROW_NUMBER() OVER (PARTITION BY ts.SYMBOL_ID ORDER BY ts.DATE DESC) AS RECENCY_RANK
FROM RAW.TIME_SERIES_DAILY_ADJUSTED ts
JOIN COMPANIES c ON ts.SYMBOL_ID = c.SYMBOL_ID
QUALIFY RECENCY_RANK = 1  -- Only the most recent price for each symbol
ORDER BY ts.SYMBOL;

-- Financial summary view combining key metrics
CREATE OR REPLACE VIEW FINANCIAL_SUMMARY AS
SELECT 
    c.SYMBOL_ID,
    c.SYMBOL,
    c.COMPANY_NAME,
    c.SECTOR,
    c.INDUSTRY,
    c.MARKET_CAPITALIZATION,
    
    -- Latest price data
    lp.PRICE_DATE,
    lp.CLOSE AS CURRENT_PRICE,
    lp.VOLUME AS CURRENT_VOLUME,
    
    -- Fundamental ratios
    c.PE_RATIO,
    c.DIVIDEND_YIELD,
    c.BETA,
    
    -- Revenue and profitability (TTM)
    c.REVENUE_TTM,
    
    -- Latest income statement data (most recent fiscal period)
    is_latest.FISCAL_DATE_ENDING AS LATEST_FISCAL_DATE,
    is_latest.TOTAL_REVENUE AS LATEST_REVENUE,
    is_latest.NET_INCOME AS LATEST_NET_INCOME,
    is_latest.GROSS_PROFIT AS LATEST_GROSS_PROFIT,
    is_latest.OPERATING_INCOME AS LATEST_OPERATING_INCOME,
    
    -- Calculated margins
    CASE 
        WHEN is_latest.TOTAL_REVENUE > 0 
        THEN ROUND((is_latest.NET_INCOME::FLOAT / is_latest.TOTAL_REVENUE::FLOAT) * 100, 2) 
        ELSE NULL 
    END AS NET_MARGIN_PERCENT,
    
    CASE 
        WHEN is_latest.TOTAL_REVENUE > 0 
        THEN ROUND((is_latest.GROSS_PROFIT::FLOAT / is_latest.TOTAL_REVENUE::FLOAT) * 100, 2) 
        ELSE NULL 
    END AS GROSS_MARGIN_PERCENT

FROM COMPANIES c
LEFT JOIN LATEST_STOCK_PRICES lp ON c.SYMBOL_ID = lp.SYMBOL_ID
LEFT JOIN (
    SELECT 
        SYMBOL_ID,
        FISCAL_DATE_ENDING,
        TOTAL_REVENUE,
        NET_INCOME,
        GROSS_PROFIT,
        OPERATING_INCOME,
        ROW_NUMBER() OVER (PARTITION BY SYMBOL_ID ORDER BY FISCAL_DATE_ENDING DESC) as rn
    FROM RAW.INCOME_STATEMENT
    WHERE FISCAL_DATE_ENDING IS NOT NULL
) is_latest ON c.SYMBOL_ID = is_latest.SYMBOL_ID AND is_latest.rn = 1
ORDER BY c.MARKET_CAPITALIZATION DESC NULLS LAST;

-- ============================================================================
-- SECTOR AND INDUSTRY ANALYSIS VIEWS
-- ============================================================================

-- Sector performance summary
CREATE OR REPLACE VIEW SECTOR_PERFORMANCE AS
SELECT 
    SECTOR,
    COUNT(*) AS COMPANY_COUNT,
    AVG(MARKET_CAPITALIZATION) AS AVG_MARKET_CAP,
    MEDIAN(MARKET_CAPITALIZATION) AS MEDIAN_MARKET_CAP,
    SUM(MARKET_CAPITALIZATION) AS TOTAL_MARKET_CAP,
    AVG(PE_RATIO) AS AVG_PE_RATIO,
    AVG(DIVIDEND_YIELD) AS AVG_DIVIDEND_YIELD,
    AVG(BETA) AS AVG_BETA,
    AVG(NET_MARGIN_PERCENT) AS AVG_NET_MARGIN,
    AVG(GROSS_MARGIN_PERCENT) AS AVG_GROSS_MARGIN
FROM FINANCIAL_SUMMARY
WHERE SECTOR IS NOT NULL
GROUP BY SECTOR
ORDER BY TOTAL_MARKET_CAP DESC NULLS LAST;

-- Industry performance summary
CREATE OR REPLACE VIEW INDUSTRY_PERFORMANCE AS
SELECT 
    SECTOR,
    INDUSTRY,
    COUNT(*) AS COMPANY_COUNT,
    AVG(MARKET_CAPITALIZATION) AS AVG_MARKET_CAP,
    SUM(MARKET_CAPITALIZATION) AS TOTAL_MARKET_CAP,
    AVG(PE_RATIO) AS AVG_PE_RATIO,
    AVG(DIVIDEND_YIELD) AS AVG_DIVIDEND_YIELD,
    AVG(NET_MARGIN_PERCENT) AS AVG_NET_MARGIN
FROM FINANCIAL_SUMMARY
WHERE INDUSTRY IS NOT NULL
GROUP BY SECTOR, INDUSTRY
ORDER BY TOTAL_MARKET_CAP DESC NULLS LAST;

-- ============================================================================
-- TIME SERIES ANALYSIS VIEWS
-- ============================================================================

-- Stock price performance (30, 90, 365 day returns)
CREATE OR REPLACE VIEW STOCK_PERFORMANCE AS
SELECT 
    current_price.SYMBOL_ID,
    current_price.SYMBOL,
    c.COMPANY_NAME,
    c.SECTOR,
    
    -- Current price
    current_price.CLOSE AS CURRENT_PRICE,
    current_price.DATE AS CURRENT_DATE,
    
    -- Historical prices
    price_30d.CLOSE AS PRICE_30D_AGO,
    price_90d.CLOSE AS PRICE_90D_AGO,
    price_365d.CLOSE AS PRICE_365D_AGO,
    
    -- Returns calculation
    CASE 
        WHEN price_30d.CLOSE > 0 
        THEN ROUND(((current_price.CLOSE - price_30d.CLOSE) / price_30d.CLOSE) * 100, 2) 
        ELSE NULL 
    END AS RETURN_30D_PERCENT,
    
    CASE 
        WHEN price_90d.CLOSE > 0 
        THEN ROUND(((current_price.CLOSE - price_90d.CLOSE) / price_90d.CLOSE) * 100, 2) 
        ELSE NULL 
    END AS RETURN_90D_PERCENT,
    
    CASE 
        WHEN price_365d.CLOSE > 0 
        THEN ROUND(((current_price.CLOSE - price_365d.CLOSE) / price_365d.CLOSE) * 100, 2) 
        ELSE NULL 
    END AS RETURN_365D_PERCENT,
    
    -- Volatility (standard deviation of daily returns over 30 days)
    vol_30d.VOLATILITY_30D

FROM LATEST_STOCK_PRICES current_price
JOIN COMPANIES c ON current_price.SYMBOL_ID = c.SYMBOL_ID
LEFT JOIN RAW.TIME_SERIES_DAILY_ADJUSTED price_30d 
    ON current_price.SYMBOL_ID = price_30d.SYMBOL_ID 
    AND price_30d.DATE = DATEADD('day', -30, current_price.DATE)
LEFT JOIN RAW.TIME_SERIES_DAILY_ADJUSTED price_90d 
    ON current_price.SYMBOL_ID = price_90d.SYMBOL_ID 
    AND price_90d.DATE = DATEADD('day', -90, current_price.DATE)
LEFT JOIN RAW.TIME_SERIES_DAILY_ADJUSTED price_365d 
    ON current_price.SYMBOL_ID = price_365d.SYMBOL_ID 
    AND price_365d.DATE = DATEADD('day', -365, current_price.DATE)
LEFT JOIN (
    -- Calculate 30-day volatility
    SELECT 
        SYMBOL_ID,
        STDDEV(daily_return) * SQRT(252) AS VOLATILITY_30D  -- Annualized volatility
    FROM (
        SELECT 
            SYMBOL_ID,
            DATE,
            LN(CLOSE / LAG(CLOSE) OVER (PARTITION BY SYMBOL_ID ORDER BY DATE)) AS daily_return
        FROM RAW.TIME_SERIES_DAILY_ADJUSTED
        WHERE DATE >= DATEADD('day', -30, CURRENT_DATE())
    )
    GROUP BY SYMBOL_ID
) vol_30d ON current_price.SYMBOL_ID = vol_30d.SYMBOL_ID
ORDER BY current_price.SYMBOL;

-- ============================================================================
-- INSIDER TRADING ANALYSIS VIEWS
-- ============================================================================

-- Insider trading summary
CREATE OR REPLACE VIEW INSIDER_TRADING_SUMMARY AS
SELECT 
    it.SYMBOL_ID,
    it.SYMBOL,
    c.COMPANY_NAME,
    COUNT(*) AS TOTAL_TRANSACTIONS,
    COUNT(CASE WHEN it.ACQUISITION_OR_DISPOSAL = 'A' THEN 1 END) AS ACQUISITIONS,
    COUNT(CASE WHEN it.ACQUISITION_OR_DISPOSAL = 'D' THEN 1 END) AS DISPOSALS,
    SUM(CASE WHEN it.ACQUISITION_OR_DISPOSAL = 'A' THEN it.SHARES ELSE 0 END) AS TOTAL_SHARES_ACQUIRED,
    SUM(CASE WHEN it.ACQUISITION_OR_DISPOSAL = 'D' THEN it.SHARES ELSE 0 END) AS TOTAL_SHARES_DISPOSED,
    SUM(CASE WHEN it.ACQUISITION_OR_DISPOSAL = 'A' THEN it.SHARES * it.SHARE_PRICE ELSE 0 END) AS TOTAL_VALUE_ACQUIRED,
    SUM(CASE WHEN it.ACQUISITION_OR_DISPOSAL = 'D' THEN it.SHARES * it.SHARE_PRICE ELSE 0 END) AS TOTAL_VALUE_DISPOSED,
    MAX(it.TRANSACTION_DATE) AS LATEST_TRANSACTION_DATE
FROM RAW.INSIDER_TRANSACTIONS it
JOIN COMPANIES c ON it.SYMBOL_ID = c.SYMBOL_ID
WHERE it.TRANSACTION_DATE >= DATEADD('year', -1, CURRENT_DATE())  -- Last 1 year
GROUP BY it.SYMBOL_ID, it.SYMBOL, c.COMPANY_NAME
ORDER BY TOTAL_VALUE_ACQUIRED DESC;

-- ============================================================================
-- DATA QUALITY AND MONITORING VIEWS
-- ============================================================================

-- Data freshness monitoring
CREATE OR REPLACE VIEW DATA_FRESHNESS AS
SELECT 
    'OVERVIEW' AS DATA_TYPE,
    COUNT(*) AS RECORD_COUNT,
    MAX(UPDATED_AT) AS LAST_UPDATE,
    DATEDIFF('hour', MAX(UPDATED_AT), CURRENT_TIMESTAMP()) AS HOURS_SINCE_UPDATE
FROM RAW.OVERVIEW
WHERE API_RESPONSE_STATUS = 'success'

UNION ALL

SELECT 
    'TIME_SERIES' AS DATA_TYPE,
    COUNT(*) AS RECORD_COUNT,
    MAX(UPDATED_AT) AS LAST_UPDATE,
    DATEDIFF('hour', MAX(UPDATED_AT), CURRENT_TIMESTAMP()) AS HOURS_SINCE_UPDATE
FROM RAW.TIME_SERIES_DAILY_ADJUSTED

UNION ALL

SELECT 
    'INCOME_STATEMENT' AS DATA_TYPE,
    COUNT(*) AS RECORD_COUNT,
    MAX(UPDATED_AT) AS LAST_UPDATE,
    DATEDIFF('hour', MAX(UPDATED_AT), CURRENT_TIMESTAMP()) AS HOURS_SINCE_UPDATE
FROM RAW.INCOME_STATEMENT
WHERE API_RESPONSE_STATUS = 'success'

UNION ALL

SELECT 
    'BALANCE_SHEET' AS DATA_TYPE,
    COUNT(*) AS RECORD_COUNT,
    MAX(UPDATED_AT) AS LAST_UPDATE,
    DATEDIFF('hour', MAX(UPDATED_AT), CURRENT_TIMESTAMP()) AS HOURS_SINCE_UPDATE
FROM RAW.BALANCE_SHEET
WHERE API_RESPONSE_STATUS = 'success'

UNION ALL

SELECT 
    'CASH_FLOW' AS DATA_TYPE,
    COUNT(*) AS RECORD_COUNT,
    MAX(UPDATED_AT) AS LAST_UPDATE,
    DATEDIFF('hour', MAX(UPDATED_AT), CURRENT_TIMESTAMP()) AS HOURS_SINCE_UPDATE
FROM RAW.CASH_FLOW
WHERE API_RESPONSE_STATUS = 'success'

ORDER BY DATA_TYPE;

-- Pipeline health monitoring
CREATE OR REPLACE VIEW PIPELINE_HEALTH AS
SELECT 
    'Lambda Extractions' AS COMPONENT,
    CASE 
        WHEN MAX(hours_since_update) <= 24 THEN 'Healthy'
        WHEN MAX(hours_since_update) <= 72 THEN 'Warning'
        ELSE 'Critical'
    END AS STATUS,
    MAX(hours_since_update) AS MAX_HOURS_SINCE_UPDATE,
    SUM(record_count) AS TOTAL_RECORDS
FROM DATA_FRESHNESS

UNION ALL

SELECT 
    'Snowpipe Ingestion' AS COMPONENT,
    CASE 
        WHEN COUNT(*) > 0 THEN 'Healthy'
        ELSE 'No Data'
    END AS STATUS,
    NULL AS MAX_HOURS_SINCE_UPDATE,
    NULL AS TOTAL_RECORDS
FROM RAW.OVERVIEW
WHERE CREATED_AT >= DATEADD('day', -1, CURRENT_TIMESTAMP());

-- ============================================================================
-- SHOW CREATED VIEWS
-- ============================================================================

SHOW VIEWS IN SCHEMA ANALYTICS;

-- Show view details
SELECT 
    TABLE_NAME AS VIEW_NAME,
    COMMENT
FROM INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'ANALYTICS'
ORDER BY TABLE_NAME;