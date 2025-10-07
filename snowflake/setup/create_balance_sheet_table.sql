-- ============================================================================
-- Create BALANCE_SHEET table structure for fundamentals processing
-- Run this before the first balance sheet workflow execution
-- ============================================================================
USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;
USE WAREHOUSE FIN_TRADE_WH;
USE ROLE ETL_ROLE;

-- Create main table for balance sheet data (matches load_balance_sheet_from_s3.sql)
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
    LOAD_DATE                                   VARCHAR(50),
    FETCHED_AT                                  TIMESTAMP_NTZ,
    CREATED_AT                                  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT UK_BALANCE_SHEET_SYMBOL_TYPE_DATE UNIQUE (SYMBOL, REPORT_TYPE, FISCAL_DATE_ENDING)
)
COMMENT = 'Balance sheet fundamentals data from Alpha Vantage API'
CLUSTER BY (FISCAL_DATE_ENDING, SYMBOL, REPORT_TYPE);

SELECT 'BALANCE_SHEET table created successfully!' as status;