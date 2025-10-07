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

-- 1) Enhanced ETL Watermark Table with Listing Dates
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS (
    TABLE_NAME                  VARCHAR(100) NOT NULL,     -- Target table name (e.g., 'BALANCE_SHEET', 'TIME_SERIES_DAILY_ADJUSTED')
    SYMBOL_ID                   NUMBER(38,0) NOT NULL,     -- Hash-based symbol identifier
    SYMBOL                      VARCHAR(20) NOT NULL,      -- Actual symbol for reference
    IPO_DATE                    DATE,                      -- IPO date from listing status
    DELISTING_DATE              DATE,                      -- Delisting date from listing status  
    LAST_FISCAL_DATE            DATE,                      -- Last fiscal/data date available
    LAST_SUCCESSFUL_RUN         TIMESTAMP_NTZ,             -- Last successful processing timestamp
    CONSECUTIVE_FAILURES        NUMBER(5,0) DEFAULT 0,     -- Count of consecutive failures
    CREATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT PK_ETL_WATERMARKS PRIMARY KEY (TABLE_NAME, SYMBOL_ID)
)
COMMENT = 'Enhanced ETL watermarking table with listing dates for comprehensive tracking'
CLUSTER BY (TABLE_NAME, LAST_SUCCESSFUL_RUN);

-- 1a) Initialize Watermarks from Listing Status
INSERT INTO FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
    (TABLE_NAME, SYMBOL_ID, SYMBOL, IPO_DATE, DELISTING_DATE, CREATED_AT, UPDATED_AT)
SELECT DISTINCT
    'LISTING_STATUS' as TABLE_NAME,
    ABS(HASH(ls.symbol)) % 1000000000 as SYMBOL_ID,
    ls.symbol as SYMBOL,
    TRY_TO_DATE(ls.ipoDate, 'YYYY-MM-DD') as IPO_DATE,
    TRY_TO_DATE(ls.delistingDate, 'YYYY-MM-DD') as DELISTING_DATE,
    CURRENT_TIMESTAMP() as CREATED_AT,
    CURRENT_TIMESTAMP() as UPDATED_AT
FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS ls
WHERE ls.symbol IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w 
      WHERE w.TABLE_NAME = 'LISTING_STATUS' 
        AND w.SYMBOL_ID = ABS(HASH(ls.symbol)) % 1000000000
  );

-- 2) Enhanced Data Coverage View with Listing Dates
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.ANALYTICS.DATA_COVERAGE_DASHBOARD AS
WITH symbol_universe AS (
    SELECT 
        symbol,
        ABS(HASH(symbol)) % 1000000000 as symbol_id,
        exchange,
        assetType,
        status,
        name,
        TRY_TO_DATE(ipoDate, 'YYYY-MM-DD') as ipo_date,
        TRY_TO_DATE(delistingDate, 'YYYY-MM-DD') as delisting_date
    FROM FIN_TRADE_EXTRACT.RAW.LISTING_STATUS
    WHERE symbol IS NOT NULL
),

time_series_coverage AS (
    SELECT 
        symbol,
        ABS(HASH(symbol)) % 1000000000 as symbol_id,
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
        ABS(HASH(symbol)) % 1000000000 as symbol_id,
        MIN(fiscal_date_ending) as first_date,
        MAX(fiscal_date_ending) as last_date,
        COUNT(*) as total_records,
        DATEDIFF('day', MAX(fiscal_date_ending), CURRENT_DATE()) as staleness_days
    FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET
    GROUP BY symbol
),

watermark_status AS (
    SELECT 
        w.table_name,
        w.symbol_id,
        w.symbol,
        w.ipo_date,
        w.delisting_date,
        w.last_fiscal_date,
        w.last_successful_run,
        w.consecutive_failures,
        w.created_at,
        w.updated_at
    FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS w
)

SELECT 
    -- Symbol Information
    u.symbol,
    u.symbol_id,
    u.exchange,
    u.assetType,
    u.status as listing_status,
    u.name as company_name,
    
    -- Listing Dates
    COALESCE(wm_ls.ipo_date, u.ipo_date) as ipo_date,
    COALESCE(wm_ls.delisting_date, u.delisting_date) as delisting_date,
    DATEDIFF('day', COALESCE(wm_ls.ipo_date, u.ipo_date), CURRENT_DATE()) as days_since_ipo,
    CASE 
        WHEN COALESCE(wm_ls.delisting_date, u.delisting_date) IS NOT NULL THEN 'DELISTED'
        WHEN u.status = 'Active' THEN 'ACTIVE'
        ELSE u.status
    END as current_status,
    
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
    
    -- Watermark Status
    wm_ts.last_successful_run as ts_last_successful_run,
    wm_ts.consecutive_failures as ts_consecutive_failures,
    wm_bs.last_successful_run as bs_last_successful_run,
    wm_bs.consecutive_failures as bs_consecutive_failures,
    
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
LEFT JOIN time_series_coverage ts ON u.symbol_id = ts.symbol_id
LEFT JOIN balance_sheet_coverage bs ON u.symbol_id = bs.symbol_id  
LEFT JOIN watermark_status wm_ls ON u.symbol_id = wm_ls.symbol_id AND wm_ls.table_name = 'LISTING_STATUS'
LEFT JOIN watermark_status wm_ts ON u.symbol_id = wm_ts.symbol_id AND wm_ts.table_name = 'TIME_SERIES_DAILY_ADJUSTED'
LEFT JOIN watermark_status wm_bs ON u.symbol_id = wm_bs.symbol_id AND wm_bs.table_name = 'BALANCE_SHEET'
ORDER BY u.symbol;

-- 3) Processing Status Analytics
CREATE OR REPLACE VIEW FIN_TRADE_EXTRACT.ANALYTICS.ETL_PROCESSING_STATS AS
WITH processing_summary AS (
    SELECT 
        table_name,
        COUNT(*) as total_symbols,
        COUNT(CASE WHEN consecutive_failures = 0 THEN 1 END) as successful_symbols,
        COUNT(CASE WHEN consecutive_failures > 0 THEN 1 END) as failed_symbols,
        MAX(last_successful_run) as most_recent_run,
        MIN(last_successful_run) as oldest_run,
        AVG(consecutive_failures) as avg_consecutive_failures,
        MAX(consecutive_failures) as max_consecutive_failures
    FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
    GROUP BY table_name
),

failure_distribution AS (
    SELECT 
        table_name,
        consecutive_failures,
        COUNT(*) as symbols_count
    FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
    WHERE consecutive_failures > 0
    GROUP BY table_name, consecutive_failures
)

SELECT 
    ps.table_name,
    ps.total_symbols,
    ps.successful_symbols,
    ps.failed_symbols,
    ROUND((ps.successful_symbols::FLOAT / ps.total_symbols * 100), 2) as success_rate_pct,
    ps.most_recent_run,
    ps.oldest_run,
    DATEDIFF('hour', ps.most_recent_run, CURRENT_TIMESTAMP()) as hours_since_last_run,
    ps.avg_consecutive_failures,
    ps.max_consecutive_failures,
    
    -- Failure distribution summary
    STRING_AGG(fd.consecutive_failures || ' failures: ' || fd.symbols_count || ' symbols', ', ') 
        WITHIN GROUP (ORDER BY fd.consecutive_failures) as failure_distribution,
        
    CURRENT_TIMESTAMP() as report_generated_at
    
FROM processing_summary ps
LEFT JOIN failure_distribution fd ON ps.table_name = fd.table_name
GROUP BY ps.table_name, ps.total_symbols, ps.successful_symbols, ps.failed_symbols,
         ps.most_recent_run, ps.oldest_run, ps.avg_consecutive_failures, ps.max_consecutive_failures
ORDER BY ps.table_name;

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