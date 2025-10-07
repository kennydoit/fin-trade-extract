-- ============================================================================
-- Comprehensive Data Coverage and Watermarking System
-- 
-- This creates a centralized view of:
-- - All symbols from listing status
-- - Last available data date per symbol per data type
-- - Last successful API pull timestamp 
-- - API request success/failure counts
-- - Data freshness and completeness metrics
-- ============================================================================
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- 1) Enhanced ETL Watermark Table
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS (
    SYMBOL                      VARCHAR(20) NOT NULL,
    DATA_TYPE                   VARCHAR(50) NOT NULL,
    
    -- Data Coverage
    FIRST_DATA_DATE             DATE,           -- Earliest data point available
    LAST_DATA_DATE              DATE,           -- Latest data point available  
    TOTAL_RECORDS               NUMBER(10,0),   -- Total records in database
    
    -- ETL Tracking
    LAST_SUCCESSFUL_PULL        TIMESTAMP_NTZ,  -- Last successful API call
    LAST_ATTEMPTED_PULL         TIMESTAMP_NTZ,  -- Last API attempt (success or fail)
    LAST_UPDATE_CHECK           TIMESTAMP_NTZ,  -- Last incremental check
    
    -- API Request Metrics
    TOTAL_API_REQUESTS          NUMBER(10,0) DEFAULT 0,
    SUCCESSFUL_API_REQUESTS     NUMBER(10,0) DEFAULT 0,
    FAILED_API_REQUESTS         NUMBER(10,0) DEFAULT 0,
    CONSECUTIVE_FAILURES        NUMBER(5,0) DEFAULT 0,
    
    -- Data Quality Metrics
    DATA_QUALITY_SCORE          NUMBER(5,3),    -- 0.000-1.000 quality score
    COMPLETENESS_PCT            NUMBER(5,2),    -- % of expected data present
    STALENESS_DAYS              NUMBER(5,0),    -- Days since last update
    
    -- Status and Metadata
    STATUS                      VARCHAR(20) DEFAULT 'ACTIVE',  -- ACTIVE, STALE, FAILED, DELISTED
    ERROR_MESSAGE               VARCHAR(1000),  -- Last error if failed
    PROCESSING_MODE             VARCHAR(20),    -- incremental, full_refresh, etc.
    
    -- Timestamps
    CREATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT PK_ETL_WATERMARKS PRIMARY KEY (SYMBOL, DATA_TYPE)
)
COMMENT = 'Centralized ETL watermarking and data coverage tracking'
CLUSTER BY (DATA_TYPE, STATUS, LAST_DATA_DATE);

-- 2) Comprehensive Data Coverage View
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.ANALYTICS.DATA_COVERAGE_DASHBOARD AS
WITH symbol_universe AS (
    SELECT DISTINCT
        symbol,
        exchange,
        assetType,
        status,
        name
    FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS
    WHERE symbol IS NOT NULL
),

time_series_coverage AS (
    SELECT 
        symbol,
        MIN(date) as first_date,
        MAX(date) as last_date,
        COUNT(*) as total_records,
        DATEDIFF('day', MAX(date), CURRENT_DATE()) as staleness_days
    FROM FIN_TRADE_EXTRACT.RAW.TIME_SERIES_DAILY_ADJUSTED
    GROUP BY symbol
),

balance_sheet_coverage AS (
    SELECT 
        symbol,
        MIN(fiscal_date_ending) as first_date,
        MAX(fiscal_date_ending) as last_date,
        COUNT(*) as total_records,
        DATEDIFF('day', MAX(fiscal_date_ending), CURRENT_DATE()) as staleness_days
    FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET
    GROUP BY symbol
),

etl_status AS (
    SELECT 
        symbol,
        data_type,
        last_processed_at,
        success,
        error_message
    FROM FIN_TRADE_EXTRACT.RAW.LATEST_ETL_STATUS
)

SELECT 
    -- Symbol Information
    u.symbol,
    u.exchange,
    u.assetType,
    u.status as listing_status,
    u.name as company_name,
    
    -- Time Series Coverage
    ts.first_date as ts_first_date,
    ts.last_date as ts_last_date,
    ts.total_records as ts_total_records,
    ts.staleness_days as ts_staleness_days,
    CASE 
        WHEN ts.staleness_days IS NULL THEN 'NO_DATA'
        WHEN ts.staleness_days <= 7 THEN 'CURRENT' 
        WHEN ts.staleness_days <= 30 THEN 'STALE'
        ELSE 'VERY_STALE'
    END as ts_freshness,
    
    -- Balance Sheet Coverage  
    bs.first_date as bs_first_date,
    bs.last_date as bs_last_date,
    bs.total_records as bs_total_records,
    bs.staleness_days as bs_staleness_days,
    CASE 
        WHEN bs.staleness_days IS NULL THEN 'NO_DATA'
        WHEN bs.staleness_days <= 90 THEN 'CURRENT'
        WHEN bs.staleness_days <= 180 THEN 'STALE' 
        ELSE 'VERY_STALE'
    END as bs_freshness,
    
    -- ETL Processing Status
    etl_ts.last_processed_at as ts_last_processed,
    etl_ts.success as ts_last_success,
    etl_ts.error_message as ts_last_error,
    
    etl_bs.last_processed_at as bs_last_processed,
    etl_bs.success as bs_last_success,
    etl_bs.error_message as bs_last_error,
    
    -- Overall Data Availability
    CASE 
        WHEN ts.symbol IS NOT NULL AND bs.symbol IS NOT NULL THEN 'BOTH'
        WHEN ts.symbol IS NOT NULL THEN 'TIME_SERIES_ONLY'
        WHEN bs.symbol IS NOT NULL THEN 'BALANCE_SHEET_ONLY'
        ELSE 'NO_DATA'
    END as data_availability,
    
    -- Metadata
    CURRENT_TIMESTAMP() as report_generated_at

FROM symbol_universe u
LEFT JOIN time_series_coverage ts ON u.symbol = ts.symbol
LEFT JOIN balance_sheet_coverage bs ON u.symbol = bs.symbol  
LEFT JOIN etl_status etl_ts ON u.symbol = etl_ts.symbol AND etl_ts.data_type = 'TIME_SERIES_DAILY_ADJUSTED'
LEFT JOIN etl_status etl_bs ON u.symbol = etl_bs.symbol AND etl_bs.data_type = 'BALANCE_SHEET'
ORDER BY u.symbol;

-- 3) API Request Failure Analysis View
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.ANALYTICS.API_FAILURE_ANALYSIS AS
SELECT 
    data_type,
    COUNT(*) as total_symbols,
    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_pulls,
    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as failed_pulls,
    ROUND(SUM(CASE WHEN success THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate_pct,
    
    -- Common error patterns
    COUNT(CASE WHEN error_message LIKE '%rate limit%' THEN 1 END) as rate_limit_errors,
    COUNT(CASE WHEN error_message LIKE '%timeout%' THEN 1 END) as timeout_errors,
    COUNT(CASE WHEN error_message LIKE '%not found%' THEN 1 END) as not_found_errors,
    
    MIN(last_processed_at) as earliest_attempt,
    MAX(last_processed_at) as latest_attempt
    
FROM FIN_TRADE_EXTRACT.RAW.INCREMENTAL_ETL_STATUS
GROUP BY data_type
ORDER BY data_type;

-- 4) Stale Data Alert View  
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.ANALYTICS.STALE_DATA_ALERTS AS
WITH data_staleness AS (
    SELECT 
        symbol,
        exchange,
        assetType,
        ts_last_date,
        ts_staleness_days,
        bs_last_date, 
        bs_staleness_days,
        data_availability
    FROM FIN_TRADE_EXTRACT.ANALYTICS.DATA_COVERAGE_DASHBOARD
)
SELECT 
    symbol,
    exchange,
    assetType,
    
    -- Time Series Alerts
    CASE 
        WHEN ts_staleness_days > 7 AND assetType IN ('Stock', 'ETF') THEN 'TIME_SERIES_STALE'
        WHEN ts_staleness_days IS NULL AND assetType IN ('Stock', 'ETF') THEN 'TIME_SERIES_MISSING'
    END as ts_alert,
    
    -- Balance Sheet Alerts  
    CASE 
        WHEN bs_staleness_days > 120 AND assetType = 'Stock' THEN 'BALANCE_SHEET_STALE'
        WHEN bs_staleness_days IS NULL AND assetType = 'Stock' THEN 'BALANCE_SHEET_MISSING'
    END as bs_alert,
    
    ts_staleness_days,
    bs_staleness_days,
    ts_last_date,
    bs_last_date
    
FROM data_staleness
WHERE (ts_staleness_days > 7 OR ts_staleness_days IS NULL OR 
       bs_staleness_days > 120 OR bs_staleness_days IS NULL)
  AND assetType IN ('Stock', 'ETF')
ORDER BY ts_staleness_days DESC NULLS LAST, bs_staleness_days DESC NULLS LAST;

-- 5) Data Coverage Summary
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.ANALYTICS.DATA_COVERAGE_SUMMARY AS
SELECT 
    'Time Series' as data_type,
    COUNT(DISTINCT symbol) as total_symbols_with_data,
    MIN(ts_first_date) as earliest_data_date,
    MAX(ts_last_date) as latest_data_date,
    AVG(ts_staleness_days) as avg_staleness_days,
    SUM(CASE WHEN ts_freshness = 'CURRENT' THEN 1 ELSE 0 END) as current_symbols,
    SUM(CASE WHEN ts_freshness = 'STALE' THEN 1 ELSE 0 END) as stale_symbols,
    SUM(CASE WHEN ts_freshness = 'VERY_STALE' THEN 1 ELSE 0 END) as very_stale_symbols
FROM FIN_TRADE_EXTRACT.ANALYTICS.DATA_COVERAGE_DASHBOARD
WHERE ts_first_date IS NOT NULL

UNION ALL

SELECT 
    'Balance Sheet' as data_type,
    COUNT(DISTINCT symbol) as total_symbols_with_data,
    MIN(bs_first_date) as earliest_data_date,
    MAX(bs_last_date) as latest_data_date,
    AVG(bs_staleness_days) as avg_staleness_days,
    SUM(CASE WHEN bs_freshness = 'CURRENT' THEN 1 ELSE 0 END) as current_symbols,
    SUM(CASE WHEN bs_freshness = 'STALE' THEN 1 ELSE 0 END) as stale_symbols,
    SUM(CASE WHEN bs_freshness = 'VERY_STALE' THEN 1 ELSE 0 END) as very_stale_symbols
FROM FIN_TRADE_EXTRACT.ANALYTICS.DATA_COVERAGE_DASHBOARD
WHERE bs_first_date IS NOT NULL;

SELECT 'Comprehensive data coverage and watermarking system created successfully!' as status;