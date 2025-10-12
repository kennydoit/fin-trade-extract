-- ============================================================================
-- Load Balance Sheet Data from S3 Stage - Fundamentals Processing
-- 
-- Features:
-- - Handles both annual and quarterly balance sheet reports
-- - Comprehensive balance sheet metrics (assets, liabilities, equity)
-- - Calculated SYMBOL_ID column (consistent with other tables)
-- - Proper duplicate handling and data quality validation
-- - Support for multiple report periods per symbol
-- ============================================================================
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- FOR TESTING ONLY: Clean up any existing objects
-- DROP STAGE IF EXISTS FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGE;
-- DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING;
-- DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET;

-- 1) Create external stage pointing to S3 balance sheet folder (force recreate to ensure CSV format)
CREATE OR REPLACE STAGE FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGE
  URL='s3://fin-trade-craft-landing/balance_sheet/'
  STORAGE_INTEGRATION = FIN_TRADE_S3_INTEGRATION
  FILE_FORMAT = (
    TYPE = 'CSV'
    COMPRESSION = 'AUTO'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    EMPTY_FIELD_AS_NULL = TRUE
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ENCODING = 'UTF8'
  );

-- 2) List files in stage to verify content
LIST @BALANCE_SHEET_STAGE;

-- 3) Create main table for balance sheet data
-- ⚠️ CRITICAL: DO NOT DROP - Would delete all historical balance sheet data!
-- DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET;
CREATE TABLE IF NOT EXISTS FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET (
    SYMBOL_ID                                   NUMBER(38,0),
    SYMBOL                                      VARCHAR(20) NOT NULL,
    REPORT_TYPE                                 VARCHAR(20) NOT NULL, -- 'annual' or 'quarterly'
    FISCAL_DATE_ENDING                          DATE NOT NULL,
    REPORTED_CURRENCY                           VARCHAR(10) DEFAULT 'USD',
    
    -- ASSETS
    TOTAL_ASSETS                                NUMBER(20,2),
    CURRENT_ASSETS                              NUMBER(20,2),
    CASH_AND_EQUIVALENTS                        NUMBER(20,2),
    CASH_AND_SHORT_TERM_INVESTMENTS             NUMBER(20,2),
    INVENTORY                                   NUMBER(20,2),
    CURRENT_NET_RECEIVABLES                     NUMBER(20,2),
    NON_CURRENT_ASSETS                          NUMBER(20,2),
    PROPERTY_PLANT_EQUIPMENT                    NUMBER(20,2),
    ACCUMULATED_DEPRECIATION_AMORTIZATION_PPE   NUMBER(20,2),
    INTANGIBLE_ASSETS                           NUMBER(20,2),
    INTANGIBLE_ASSETS_EXCLUDING_GOODWILL        NUMBER(20,2),
    GOODWILL                                    NUMBER(20,2),
    INVESTMENTS                                 NUMBER(20,2),
    LONG_TERM_INVESTMENTS                       NUMBER(20,2),
    SHORT_TERM_INVESTMENTS                      NUMBER(20,2),
    OTHER_CURRENT_ASSETS                        NUMBER(20,2),
    OTHER_NON_CURRENT_ASSETS                    NUMBER(20,2),
    
    -- LIABILITIES
    TOTAL_LIABILITIES                           NUMBER(20,2),
    CURRENT_LIABILITIES                         NUMBER(20,2),
    CURRENT_ACCOUNTS_PAYABLE                    NUMBER(20,2),
    DEFERRED_REVENUE                            NUMBER(20,2),
    CURRENT_DEBT                                NUMBER(20,2),
    SHORT_TERM_DEBT                             NUMBER(20,2),
    NON_CURRENT_LIABILITIES                     NUMBER(20,2),
    CAPITAL_LEASE_OBLIGATIONS                   NUMBER(20,2),
    LONG_TERM_DEBT                              NUMBER(20,2),
    CURRENT_LONG_TERM_DEBT                      NUMBER(20,2),
    LONG_TERM_DEBT_NONCURRENT                   NUMBER(20,2),
    SHORT_LONG_TERM_DEBT_TOTAL                  NUMBER(20,2),
    OTHER_CURRENT_LIABILITIES                   NUMBER(20,2),
    OTHER_NON_CURRENT_LIABILITIES               NUMBER(20,2),
    
    -- EQUITY
    TOTAL_SHAREHOLDER_EQUITY                    NUMBER(20,2),
    TREASURY_STOCK                              NUMBER(20,2),
    RETAINED_EARNINGS                           NUMBER(20,2),
    COMMON_STOCK                                NUMBER(20,2),
    COMMON_STOCK_SHARES_OUTSTANDING             NUMBER(20,0),
    
    -- METADATA
    CREATED_AT                                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT UK_BALANCE_SHEET_SYMBOL_TYPE_DATE UNIQUE (SYMBOL, REPORT_TYPE, FISCAL_DATE_ENDING)
)
COMMENT = 'Balance sheet fundamentals data from Alpha Vantage API'
CLUSTER BY (FISCAL_DATE_ENDING, SYMBOL, REPORT_TYPE);

-- 4) Create staging table (transient for performance)
CREATE OR REPLACE TRANSIENT TABLE FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING (
    symbol VARCHAR(20),
    symbol_id NUMBER(38,0),
    report_type VARCHAR(20),
    fiscal_date_ending DATE,
    reported_currency VARCHAR(10),
    
    -- Assets
    total_assets NUMBER(20,2),
    current_assets NUMBER(20,2), 
    cash_and_equivalents NUMBER(20,2),
    cash_and_short_term_investments NUMBER(20,2),
    inventory NUMBER(20,2),
    current_net_receivables NUMBER(20,2),
    non_current_assets NUMBER(20,2),
    property_plant_equipment NUMBER(20,2),
    accumulated_depreciation_amortization_ppe NUMBER(20,2),
    intangible_assets NUMBER(20,2),
    intangible_assets_excluding_goodwill NUMBER(20,2),
    goodwill NUMBER(20,2),
    investments NUMBER(20,2),
    long_term_investments NUMBER(20,2),
    short_term_investments NUMBER(20,2),
    other_current_assets NUMBER(20,2),
    other_non_current_assets NUMBER(20,2),
    
    -- Liabilities
    total_liabilities NUMBER(20,2),
    current_liabilities NUMBER(20,2),
    current_accounts_payable NUMBER(20,2),
    deferred_revenue NUMBER(20,2),
    current_debt NUMBER(20,2),
    short_term_debt NUMBER(20,2),
    non_current_liabilities NUMBER(20,2),
    capital_lease_obligations NUMBER(20,2),
    long_term_debt NUMBER(20,2),
    current_long_term_debt NUMBER(20,2),
    long_term_debt_noncurrent NUMBER(20,2),
    short_long_term_debt_total NUMBER(20,2),
    other_current_liabilities NUMBER(20,2),
    other_non_current_liabilities NUMBER(20,2),
    
    -- Equity
    total_shareholder_equity NUMBER(20,2),
    treasury_stock NUMBER(20,2),
    retained_earnings NUMBER(20,2),
    common_stock NUMBER(20,2),
    common_stock_shares_outstanding NUMBER(20,0),
    
    -- Metadata
    source_filename VARCHAR(500)
);

-- 5) Load CSV files from S3 stage into staging table
COPY INTO FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING (
    symbol,
    fiscal_date_ending,
    report_type,
    reported_currency,
    total_assets,
    current_assets,
    cash_and_equivalents,
    cash_and_short_term_investments,
    inventory,
    current_net_receivables,
    non_current_assets,
    property_plant_equipment,
    accumulated_depreciation_amortization_ppe,
    intangible_assets,
    goodwill,
    investments,
    long_term_investments,
    short_term_investments,
    other_current_assets,
    other_non_current_assets,
    total_liabilities,
    current_liabilities,
    current_accounts_payable,
    deferred_revenue,
    current_debt,
    short_term_debt,
    non_current_liabilities,
    capital_lease_obligations,
    long_term_debt,
    current_long_term_debt,
    long_term_debt_noncurrent,
    other_current_liabilities,
    other_non_current_liabilities,
    total_shareholder_equity,
    treasury_stock,
    retained_earnings,
    common_stock,
    common_stock_shares_outstanding,
    source_filename
)
FROM (
    SELECT 
        $1::VARCHAR(20) as symbol,
        TRY_TO_DATE($2, 'YYYY-MM-DD') as fiscal_date_ending,
        $3::VARCHAR(20) as period_type,
        $4::VARCHAR(10) as reported_currency,
        TRY_TO_NUMBER($5, 20, 2) as total_assets,
        TRY_TO_NUMBER($6, 20, 2) as total_current_assets,
        TRY_TO_NUMBER($7, 20, 2) as cash_and_cash_equivalents,
        TRY_TO_NUMBER($8, 20, 2) as cash_and_short_term_investments,
        TRY_TO_NUMBER($9, 20, 2) as inventory,
        TRY_TO_NUMBER($10, 20, 2) as current_net_receivables,
        TRY_TO_NUMBER($11, 20, 2) as total_non_current_assets,
        TRY_TO_NUMBER($12, 20, 2) as property_plant_equipment,
        TRY_TO_NUMBER($13, 20, 2) as accumulated_depreciation_amortization_ppe,
        TRY_TO_NUMBER($14, 20, 2) as intangible_assets,
        TRY_TO_NUMBER($15, 20, 2) as goodwill,
        TRY_TO_NUMBER($16, 20, 2) as investments,
        TRY_TO_NUMBER($17, 20, 2) as long_term_investments,
        TRY_TO_NUMBER($18, 20, 2) as short_term_investments,
        TRY_TO_NUMBER($19, 20, 2) as other_current_assets,
        TRY_TO_NUMBER($20, 20, 2) as other_non_current_assets,
        TRY_TO_NUMBER($21, 20, 2) as total_liabilities,
        TRY_TO_NUMBER($22, 20, 2) as total_current_liabilities,
        TRY_TO_NUMBER($23, 20, 2) as current_accounts_payable,
        TRY_TO_NUMBER($24, 20, 2) as deferred_revenue,
        TRY_TO_NUMBER($25, 20, 2) as current_debt,
        TRY_TO_NUMBER($26, 20, 2) as short_term_debt,
        TRY_TO_NUMBER($27, 20, 2) as total_non_current_liabilities,
        TRY_TO_NUMBER($28, 20, 2) as capital_lease_obligations,
        TRY_TO_NUMBER($29, 20, 2) as long_term_debt,
        TRY_TO_NUMBER($30, 20, 2) as current_long_term_debt,
        TRY_TO_NUMBER($31, 20, 2) as long_term_debt_noncurrent,
        TRY_TO_NUMBER($32, 20, 2) as other_current_liabilities,
        TRY_TO_NUMBER($33, 20, 2) as other_non_current_liabilities,
        TRY_TO_NUMBER($34, 20, 2) as total_shareholder_equity,
        TRY_TO_NUMBER($35, 20, 2) as treasury_stock,
        TRY_TO_NUMBER($36, 20, 2) as retained_earnings,
        TRY_TO_NUMBER($37, 20, 2) as common_stock,
        TRY_TO_NUMBER($38, 20, 0) as common_stock_shares_outstanding,
        METADATA$FILENAME as source_filename
    FROM @BALANCE_SHEET_STAGE
)
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
PATTERN = '.*\.csv'
PARALLEL = 16
ON_ERROR = CONTINUE;

-- Debug: Check staging data load
SELECT COUNT(*) as staging_rows_loaded FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING;

-- 6) Calculate symbol_id using same logic as other tables (hash-based approach)
UPDATE FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING 
SET symbol_id = ABS(HASH(symbol)) % 1000000000
WHERE symbol IS NOT NULL;

-- Debug: Check symbol_id calculation and data distribution
SELECT 
    symbol,
    report_type,
    symbol_id,
    COUNT(*) as row_count,
    MIN(fiscal_date_ending) as earliest_date,
    MAX(fiscal_date_ending) as latest_date
FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING 
GROUP BY symbol, report_type, symbol_id
ORDER BY symbol, report_type;

-- 7) Data quality validation - remove bad records
DELETE FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING 
WHERE symbol IS NULL 
   OR symbol_id IS NULL
   OR fiscal_date_ending IS NULL
   OR report_type IS NULL
   OR report_type NOT IN ('annual', 'quarterly');

-- 8) Remove duplicates (keep most recent file's data for each symbol+report_type+date)
DELETE FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING 
WHERE (symbol, report_type, fiscal_date_ending, source_filename) IN (
    SELECT symbol, report_type, fiscal_date_ending, source_filename
    FROM (
        SELECT 
            symbol, 
            report_type,
            fiscal_date_ending,
            source_filename,
            ROW_NUMBER() OVER (PARTITION BY symbol, report_type, fiscal_date_ending ORDER BY source_filename DESC) as rn
        FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING
    ) 
    WHERE rn > 1
);

-- Debug: Check data after cleanup and deduplication
SELECT 
    'After cleanup' as stage,
    symbol,
    report_type,
    COUNT(*) as clean_row_count,
    MIN(fiscal_date_ending) as earliest_date,
    MAX(fiscal_date_ending) as latest_date,
    AVG(total_assets) as avg_total_assets
FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING 
GROUP BY symbol, report_type
ORDER BY symbol, report_type;

-- 9) Load from staging to final table with MERGE (handle updates)
MERGE INTO FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET AS target
USING (
    SELECT 
        staging.symbol_id,
        staging.symbol,
        staging.report_type,
        staging.fiscal_date_ending,
        staging.reported_currency,
        
        -- Assets
        staging.total_assets,
        staging.current_assets,
        staging.cash_and_equivalents,
        staging.cash_and_short_term_investments,
        staging.inventory,
        staging.current_net_receivables,
        staging.non_current_assets,
        staging.property_plant_equipment,
        staging.accumulated_depreciation_amortization_ppe,
        staging.intangible_assets,
        staging.intangible_assets_excluding_goodwill,
        staging.goodwill,
        staging.investments,
        staging.long_term_investments,
        staging.short_term_investments,
        staging.other_current_assets,
        staging.other_non_current_assets,
        
        -- Liabilities
        staging.total_liabilities,
        staging.current_liabilities,
        staging.current_accounts_payable,
        staging.deferred_revenue,
        staging.current_debt,
        staging.short_term_debt,
        staging.non_current_liabilities,
        staging.capital_lease_obligations,
        staging.long_term_debt,
        staging.current_long_term_debt,
        staging.long_term_debt_noncurrent,
        staging.short_long_term_debt_total,
        staging.other_current_liabilities,
        staging.other_non_current_liabilities,
        
        -- Equity
        staging.total_shareholder_equity,
        staging.treasury_stock,
        staging.retained_earnings,
        staging.common_stock,
        staging.common_stock_shares_outstanding
        
    FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING staging
    WHERE staging.symbol IS NOT NULL
      AND staging.symbol_id IS NOT NULL
      AND staging.fiscal_date_ending IS NOT NULL
      AND staging.report_type IS NOT NULL
) AS source
ON target.SYMBOL = source.symbol 
   AND target.REPORT_TYPE = source.report_type
   AND target.FISCAL_DATE_ENDING = source.fiscal_date_ending
WHEN MATCHED THEN
    UPDATE SET
        SYMBOL_ID = source.symbol_id,
        REPORTED_CURRENCY = source.reported_currency,
        
        -- Assets
        TOTAL_ASSETS = source.total_assets,
        CURRENT_ASSETS = source.current_assets,
        CASH_AND_EQUIVALENTS = source.cash_and_equivalents,
        CASH_AND_SHORT_TERM_INVESTMENTS = source.cash_and_short_term_investments,
        INVENTORY = source.inventory,
        CURRENT_NET_RECEIVABLES = source.current_net_receivables,
        NON_CURRENT_ASSETS = source.non_current_assets,
        PROPERTY_PLANT_EQUIPMENT = source.property_plant_equipment,
        ACCUMULATED_DEPRECIATION_AMORTIZATION_PPE = source.accumulated_depreciation_amortization_ppe,
        INTANGIBLE_ASSETS = source.intangible_assets,
        INTANGIBLE_ASSETS_EXCLUDING_GOODWILL = source.intangible_assets_excluding_goodwill,
        GOODWILL = source.goodwill,
        INVESTMENTS = source.investments,
        LONG_TERM_INVESTMENTS = source.long_term_investments,
        SHORT_TERM_INVESTMENTS = source.short_term_investments,
        OTHER_CURRENT_ASSETS = source.other_current_assets,
        OTHER_NON_CURRENT_ASSETS = source.other_non_current_assets,
        
        -- Liabilities
        TOTAL_LIABILITIES = source.total_liabilities,
        CURRENT_LIABILITIES = source.current_liabilities,
        CURRENT_ACCOUNTS_PAYABLE = source.current_accounts_payable,
        DEFERRED_REVENUE = source.deferred_revenue,
        CURRENT_DEBT = source.current_debt,
        SHORT_TERM_DEBT = source.short_term_debt,
        NON_CURRENT_LIABILITIES = source.non_current_liabilities,
        CAPITAL_LEASE_OBLIGATIONS = source.capital_lease_obligations,
        LONG_TERM_DEBT = source.long_term_debt,
        CURRENT_LONG_TERM_DEBT = source.current_long_term_debt,
        LONG_TERM_DEBT_NONCURRENT = source.long_term_debt_noncurrent,
        SHORT_LONG_TERM_DEBT_TOTAL = source.short_long_term_debt_total,
        OTHER_CURRENT_LIABILITIES = source.other_current_liabilities,
        OTHER_NON_CURRENT_LIABILITIES = source.other_non_current_liabilities,
        
        -- Equity
        TOTAL_SHAREHOLDER_EQUITY = source.total_shareholder_equity,
        TREASURY_STOCK = source.treasury_stock,
        RETAINED_EARNINGS = source.retained_earnings,
        COMMON_STOCK = source.common_stock,
        COMMON_STOCK_SHARES_OUTSTANDING = source.common_stock_shares_outstanding
        
WHEN NOT MATCHED THEN
    INSERT (
        SYMBOL_ID, SYMBOL, REPORT_TYPE, FISCAL_DATE_ENDING, REPORTED_CURRENCY,
        TOTAL_ASSETS, CURRENT_ASSETS, CASH_AND_EQUIVALENTS, CASH_AND_SHORT_TERM_INVESTMENTS,
        INVENTORY, CURRENT_NET_RECEIVABLES, NON_CURRENT_ASSETS, PROPERTY_PLANT_EQUIPMENT,
        ACCUMULATED_DEPRECIATION_AMORTIZATION_PPE, INTANGIBLE_ASSETS, 
        INTANGIBLE_ASSETS_EXCLUDING_GOODWILL, GOODWILL, INVESTMENTS, LONG_TERM_INVESTMENTS,
        SHORT_TERM_INVESTMENTS, OTHER_CURRENT_ASSETS, OTHER_NON_CURRENT_ASSETS,
        TOTAL_LIABILITIES, CURRENT_LIABILITIES, CURRENT_ACCOUNTS_PAYABLE, DEFERRED_REVENUE,
        CURRENT_DEBT, SHORT_TERM_DEBT, NON_CURRENT_LIABILITIES, CAPITAL_LEASE_OBLIGATIONS,
        LONG_TERM_DEBT, CURRENT_LONG_TERM_DEBT, LONG_TERM_DEBT_NONCURRENT,
        SHORT_LONG_TERM_DEBT_TOTAL, OTHER_CURRENT_LIABILITIES, OTHER_NON_CURRENT_LIABILITIES,
        TOTAL_SHAREHOLDER_EQUITY, TREASURY_STOCK, RETAINED_EARNINGS, COMMON_STOCK,
        COMMON_STOCK_SHARES_OUTSTANDING
    )
    VALUES (
        source.symbol_id, source.symbol, source.report_type, source.fiscal_date_ending, 
        source.reported_currency, source.total_assets, source.current_assets, 
        source.cash_and_equivalents, source.cash_and_short_term_investments, source.inventory,
        source.current_net_receivables, source.non_current_assets, source.property_plant_equipment,
        source.accumulated_depreciation_amortization_ppe, source.intangible_assets,
        source.intangible_assets_excluding_goodwill, source.goodwill, source.investments,
        source.long_term_investments, source.short_term_investments, source.other_current_assets,
        source.other_non_current_assets, source.total_liabilities, source.current_liabilities,
        source.current_accounts_payable, source.deferred_revenue, source.current_debt,
        source.short_term_debt, source.non_current_liabilities, source.capital_lease_obligations,
        source.long_term_debt, source.current_long_term_debt, source.long_term_debt_noncurrent,
        source.short_long_term_debt_total, source.other_current_liabilities,
        source.other_non_current_liabilities, source.total_shareholder_equity, source.treasury_stock,
        source.retained_earnings, source.common_stock, source.common_stock_shares_outstanding
    );

-- 10) Final validation and reporting
SELECT 
    symbol,
    report_type,
    COUNT(*) as total_reports,
    MIN(fiscal_date_ending) as earliest_period,
    MAX(fiscal_date_ending) as latest_period,
    AVG(total_assets) as avg_total_assets,
    AVG(total_liabilities) as avg_total_liabilities,
    AVG(total_shareholder_equity) as avg_shareholder_equity
FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET 
GROUP BY symbol, report_type
ORDER BY symbol, report_type;

-- Show summary stats
SELECT 
    report_type,
    COUNT(DISTINCT symbol) as unique_symbols,
    COUNT(*) as total_reports,
    MIN(fiscal_date_ending) as earliest_period,
    MAX(fiscal_date_ending) as latest_period,
    AVG(total_assets) as avg_total_assets,
    AVG(total_shareholder_equity) as avg_shareholder_equity
FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET
GROUP BY report_type
ORDER BY report_type;

-- Show overall summary
SELECT 
    'Balance Sheet Load Summary' as summary,
    COUNT(DISTINCT symbol) as unique_symbols,
    COUNT(*) as total_reports,
    COUNT(CASE WHEN report_type = 'annual' THEN 1 END) as annual_reports,
    COUNT(CASE WHEN report_type = 'quarterly' THEN 1 END) as quarterly_reports,
    MIN(fiscal_date_ending) as earliest_period,
    MAX(fiscal_date_ending) as latest_period
FROM FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET;

-- Cleanup staging table
DROP TABLE IF EXISTS FIN_TRADE_EXTRACT.RAW.BALANCE_SHEET_STAGING;

SELECT 'Balance sheet data loading completed successfully!' as status;