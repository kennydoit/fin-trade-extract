-- ============================================================================
-- Snowflake Table Definitions for fin-trade-extract Pipeline
-- Converted from PostgreSQL schema with Snowflake optimizations
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- ============================================================================
-- CORE SYMBOL METADATA
-- ============================================================================

-- Table for storing symbol metadata and listing status
CREATE OR REPLACE TABLE LISTING_STATUS (
    SYMBOL_ID       NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    SYMBOL          VARCHAR(20) NOT NULL,
    NAME            VARCHAR(500),
    EXCHANGE        VARCHAR(50),
    ASSET_TYPE      VARCHAR(50),
    IPO_DATE        DATE,
    DELISTING_DATE  DATE,
    STATUS          VARCHAR(20),
    STATE           VARCHAR(20) DEFAULT 'active',
    CREATED_AT      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Snowflake-specific constraints
    CONSTRAINT UK_LISTING_STATUS_SYMBOL UNIQUE (SYMBOL)
)
COMMENT = 'Master table for all stock symbols and their metadata'
CLUSTER BY (SYMBOL);  -- Cluster by symbol for faster lookups

-- ============================================================================
-- COMPANY FUNDAMENTALS
-- ============================================================================

-- Table for storing company overview information
CREATE OR REPLACE TABLE OVERVIEW (
    OVERVIEW_ID     NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    SYMBOL_ID       NUMBER(38,0) NOT NULL,
    SYMBOL          VARCHAR(20) NOT NULL,
    ASSET_TYPE      VARCHAR(50),
    NAME            VARCHAR(500),
    DESCRIPTION     VARCHAR(16777216),  -- Large text field in Snowflake
    CIK             VARCHAR(20),
    EXCHANGE        VARCHAR(50),
    CURRENCY        VARCHAR(10),
    COUNTRY         VARCHAR(100),
    SECTOR          VARCHAR(100),
    INDUSTRY        VARCHAR(200),
    ADDRESS         VARCHAR(16777216),
    OFFICIAL_SITE   VARCHAR(255),
    FISCAL_YEAR_END VARCHAR(20),
    MARKET_CAPITALIZATION NUMBER(20,0),
    EBITDA          NUMBER(20,0),
    PE_RATIO        NUMBER(10,4),
    PEG_RATIO       NUMBER(10,4),
    BOOK_VALUE      NUMBER(10,4),
    DIVIDEND_PER_SHARE NUMBER(10,4),
    DIVIDEND_YIELD  NUMBER(8,6),
    EPS             NUMBER(10,4),
    REVENUE_PER_SHARE_TTM NUMBER(10,4),
    PROFIT_MARGIN   NUMBER(8,6),
    OPERATING_MARGIN_TTM NUMBER(8,6),
    RETURN_ON_ASSETS_TTM NUMBER(8,6),
    RETURN_ON_EQUITY_TTM NUMBER(8,6),
    REVENUE_TTM     NUMBER(20,0),
    GROSS_PROFIT_TTM NUMBER(20,0),
    DILUTED_EPS_TTM NUMBER(10,4),
    QUARTERLY_EARNINGS_GROWTH_YOY NUMBER(8,6),
    QUARTERLY_REVENUE_GROWTH_YOY NUMBER(8,6),
    ANALYST_TARGET_PRICE NUMBER(10,4),
    TRAILING_PE     NUMBER(10,4),
    FORWARD_PE      NUMBER(10,4),
    PRICE_TO_SALES_RATIO_TTM NUMBER(10,4),
    PRICE_TO_BOOK_RATIO NUMBER(10,4),
    EV_TO_REVENUE   NUMBER(10,4),
    EV_TO_EBITDA    NUMBER(10,4),
    BETA            NUMBER(8,6),
    WEEK_52_HIGH    NUMBER(12,4),
    WEEK_52_LOW     NUMBER(12,4),
    DAY_50_MOVING_AVERAGE NUMBER(12,4),
    DAY_200_MOVING_AVERAGE NUMBER(12,4),
    SHARES_OUTSTANDING NUMBER(20,0),
    DIVIDEND_DATE   DATE,
    EX_DIVIDEND_DATE DATE,
    API_RESPONSE_STATUS VARCHAR(20),
    CREATED_AT      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Snowflake-specific constraints
    CONSTRAINT UK_OVERVIEW_SYMBOL_ID UNIQUE (SYMBOL_ID)
)
COMMENT = 'Company overview and key financial metrics from Alpha Vantage'
CLUSTER BY (SYMBOL_ID, SYMBOL);

-- Table for storing income statement data
CREATE OR REPLACE TABLE INCOME_STATEMENT (
    SYMBOL_ID                           NUMBER(38,0) NOT NULL,
    SYMBOL                              VARCHAR(20) NOT NULL,
    FISCAL_DATE_ENDING                  DATE,
    REPORT_TYPE                         VARCHAR(10) NOT NULL,
    REPORTED_CURRENCY                   VARCHAR(10),
    GROSS_PROFIT                        NUMBER(20,0),
    TOTAL_REVENUE                       NUMBER(20,0),
    COST_OF_REVENUE                     NUMBER(20,0),
    COST_OF_GOODS_AND_SERVICES_SOLD     NUMBER(20,0),
    OPERATING_INCOME                    NUMBER(20,0),
    SELLING_GENERAL_AND_ADMINISTRATIVE  NUMBER(20,0),
    RESEARCH_AND_DEVELOPMENT            NUMBER(20,0),
    OPERATING_EXPENSES                  NUMBER(20,0),
    INVESTMENT_INCOME_NET               NUMBER(20,0),
    NET_INTEREST_INCOME                 NUMBER(20,0),
    INTEREST_INCOME                     NUMBER(20,0),
    INTEREST_EXPENSE                    NUMBER(20,0),
    NON_INTEREST_INCOME                 NUMBER(20,0),
    OTHER_NON_OPERATING_INCOME          NUMBER(20,0),
    DEPRECIATION                        NUMBER(20,0),
    DEPRECIATION_AND_AMORTIZATION       NUMBER(20,0),
    INCOME_BEFORE_TAX                   NUMBER(20,0),
    INCOME_TAX_EXPENSE                  NUMBER(20,0),
    INTEREST_AND_DEBT_EXPENSE           NUMBER(20,0),
    NET_INCOME_FROM_CONTINUING_OPERATIONS NUMBER(20,0),
    COMPREHENSIVE_INCOME_NET_OF_TAX     NUMBER(20,0),
    EBIT                                NUMBER(20,0),
    EBITDA                              NUMBER(20,0),
    NET_INCOME                          NUMBER(20,0),
    API_RESPONSE_STATUS                 VARCHAR(20),
    CREATED_AT                          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Income statement data from Alpha Vantage API'
CLUSTER BY (SYMBOL_ID, FISCAL_DATE_ENDING);

-- Table for storing balance sheet data
CREATE OR REPLACE TABLE BALANCE_SHEET (
    SYMBOL_ID                               NUMBER(38,0) NOT NULL,
    SYMBOL                                  VARCHAR(20) NOT NULL,
    FISCAL_DATE_ENDING                      DATE,
    REPORT_TYPE                             VARCHAR(10) NOT NULL,
    REPORTED_CURRENCY                       VARCHAR(10),
    TOTAL_ASSETS                            NUMBER(20,0),
    TOTAL_CURRENT_ASSETS                    NUMBER(20,0),
    CASH_AND_CASH_EQUIVALENTS_AT_CARRYING_VALUE NUMBER(20,0),
    CASH_AND_SHORT_TERM_INVESTMENTS         NUMBER(20,0),
    INVENTORY                               NUMBER(20,0),
    CURRENT_NET_RECEIVABLES                 NUMBER(20,0),
    TOTAL_NON_CURRENT_ASSETS                NUMBER(20,0),
    PROPERTY_PLANT_EQUIPMENT                NUMBER(20,0),
    ACCUMULATED_DEPRECIATION_AMORTIZATION_PPE NUMBER(20,0),
    INTANGIBLE_ASSETS                       NUMBER(20,0),
    INTANGIBLE_ASSETS_EXCLUDING_GOODWILL    NUMBER(20,0),
    GOODWILL                                NUMBER(20,0),
    INVESTMENTS                             NUMBER(20,0),
    LONG_TERM_INVESTMENTS                   NUMBER(20,0),
    SHORT_TERM_INVESTMENTS                  NUMBER(20,0),
    OTHER_CURRENT_ASSETS                    NUMBER(20,0),
    OTHER_NON_CURRENT_ASSETS                NUMBER(20,0),
    TOTAL_LIABILITIES                       NUMBER(20,0),
    TOTAL_CURRENT_LIABILITIES               NUMBER(20,0),
    CURRENT_ACCOUNTS_PAYABLE                NUMBER(20,0),
    DEFERRED_REVENUE                        NUMBER(20,0),
    CURRENT_DEBT                            NUMBER(20,0),
    SHORT_TERM_DEBT                         NUMBER(20,0),
    TOTAL_NON_CURRENT_LIABILITIES           NUMBER(20,0),
    CAPITAL_LEASE_OBLIGATIONS               NUMBER(20,0),
    LONG_TERM_DEBT                          NUMBER(20,0),
    CURRENT_LONG_TERM_DEBT                  NUMBER(20,0),
    LONG_TERM_DEBT_NONCURRENT               NUMBER(20,0),
    SHORT_LONG_TERM_DEBT_TOTAL              NUMBER(20,0),
    OTHER_CURRENT_LIABILITIES               NUMBER(20,0),
    OTHER_NON_CURRENT_LIABILITIES           NUMBER(20,0),
    TOTAL_SHAREHOLDER_EQUITY                NUMBER(20,0),
    TREASURY_STOCK                          NUMBER(20,0),
    RETAINED_EARNINGS                       NUMBER(20,0),
    COMMON_STOCK                            NUMBER(20,0),
    COMMON_STOCK_SHARES_OUTSTANDING         NUMBER(20,0),
    API_RESPONSE_STATUS                     VARCHAR(20),
    CREATED_AT                              TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                              TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Balance sheet data from Alpha Vantage API'
CLUSTER BY (SYMBOL_ID, FISCAL_DATE_ENDING);

-- Table for storing cash flow data
CREATE OR REPLACE TABLE CASH_FLOW (
    SYMBOL_ID                                       NUMBER(38,0) NOT NULL,
    SYMBOL                                          VARCHAR(20) NOT NULL,
    FISCAL_DATE_ENDING                              DATE,
    REPORT_TYPE                                     VARCHAR(10) NOT NULL,
    REPORTED_CURRENCY                               VARCHAR(10),
    OPERATING_CASHFLOW                              NUMBER(20,0),
    PAYMENTS_FOR_OPERATING_ACTIVITIES               NUMBER(20,0),
    PROCEEDS_FROM_OPERATING_ACTIVITIES              NUMBER(20,0),
    CHANGE_IN_OPERATING_LIABILITIES                 NUMBER(20,0),
    CHANGE_IN_OPERATING_ASSETS                      NUMBER(20,0),
    DEPRECIATION_DEPLETION_AND_AMORTIZATION         NUMBER(20,0),
    CAPITAL_EXPENDITURES                            NUMBER(20,0),
    CHANGE_IN_RECEIVABLES                           NUMBER(20,0),
    CHANGE_IN_INVENTORY                             NUMBER(20,0),
    PROFIT_LOSS                                     NUMBER(20,0),
    CASHFLOW_FROM_INVESTMENT                        NUMBER(20,0),
    CASHFLOW_FROM_FINANCING                         NUMBER(20,0),
    PROCEEDS_FROM_REPAYMENTS_OF_SHORT_TERM_DEBT     NUMBER(20,0),
    PAYMENTS_FOR_REPURCHASE_OF_COMMON_STOCK         NUMBER(20,0),
    PAYMENTS_FOR_REPURCHASE_OF_EQUITY               NUMBER(20,0),
    PAYMENTS_FOR_REPURCHASE_OF_PREFERRED_STOCK      NUMBER(20,0),
    DIVIDEND_PAYOUT                                 NUMBER(20,0),
    DIVIDEND_PAYOUT_COMMON_STOCK                    NUMBER(20,0),
    DIVIDEND_PAYOUT_PREFERRED_STOCK                 NUMBER(20,0),
    PROCEEDS_FROM_ISSUANCE_OF_COMMON_STOCK          NUMBER(20,0),
    PROCEEDS_FROM_ISSUANCE_OF_LONG_TERM_DEBT_AND_CAPITAL_SECURITIES_NET NUMBER(20,0),
    PROCEEDS_FROM_ISSUANCE_OF_PREFERRED_STOCK       NUMBER(20,0),
    PROCEEDS_FROM_REPURCHASE_OF_EQUITY              NUMBER(20,0),
    PROCEEDS_FROM_SALE_OF_TREASURY_STOCK            NUMBER(20,0),
    CHANGE_IN_CASH_AND_CASH_EQUIVALENTS             NUMBER(20,0),
    CHANGE_IN_EXCHANGE_RATE                         NUMBER(20,0),
    NET_INCOME                                      NUMBER(20,0),
    API_RESPONSE_STATUS                             VARCHAR(20),
    CREATED_AT                                      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT                                      TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Cash flow statement data from Alpha Vantage API'
CLUSTER BY (SYMBOL_ID, FISCAL_DATE_ENDING);

-- ============================================================================
-- TIME SERIES DATA
-- ============================================================================

-- Table for storing time series adjusted data
CREATE OR REPLACE TABLE TIME_SERIES_DAILY_ADJUSTED (
    SYMBOL_ID         NUMBER(38,0) NOT NULL,
    SYMBOL            VARCHAR(20) NOT NULL,
    DATE              DATE NOT NULL,
    OPEN              NUMBER(15,4),
    HIGH              NUMBER(15,4),
    LOW               NUMBER(15,4),
    CLOSE             NUMBER(15,4),
    ADJUSTED_CLOSE    NUMBER(15,4),
    VOLUME            NUMBER(20,0),
    DIVIDEND_AMOUNT   NUMBER(15,6),
    SPLIT_COEFFICIENT NUMBER(10,6),
    CREATED_AT        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT UK_TIME_SERIES_SYMBOL_DATE UNIQUE (SYMBOL_ID, DATE)
)
COMMENT = 'Daily adjusted time series data from Alpha Vantage API'
CLUSTER BY (DATE, SYMBOL_ID);  -- Cluster by date first for time-series queries

-- ============================================================================
-- COMMODITIES AND ECONOMIC DATA
-- ============================================================================

-- Table for storing commodities data
CREATE OR REPLACE TABLE COMMODITIES (
    COMMODITY_ID          NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    COMMODITY_NAME        VARCHAR(100) NOT NULL,
    FUNCTION_NAME         VARCHAR(50) NOT NULL,
    DATE                  DATE,
    INTERVAL              VARCHAR(10) NOT NULL,
    UNIT                  VARCHAR(50),
    VALUE                 NUMBER(15,6),
    NAME                  VARCHAR(500),
    API_RESPONSE_STATUS   VARCHAR(20),
    CREATED_AT            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT UK_COMMODITIES_NAME_DATE_INTERVAL UNIQUE (COMMODITY_NAME, DATE, INTERVAL)
)
COMMENT = 'Commodities data from Alpha Vantage API'
CLUSTER BY (DATE, COMMODITY_NAME);

-- Table for storing economic indicators data
CREATE OR REPLACE TABLE ECONOMIC_INDICATORS (
    ECONOMIC_INDICATOR_ID          NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    ECONOMIC_INDICATOR_NAME        VARCHAR(100) NOT NULL,
    FUNCTION_NAME         VARCHAR(50) NOT NULL,
    MATURITY              VARCHAR(20),
    DATE                  DATE,
    INTERVAL              VARCHAR(15) NOT NULL,
    UNIT                  VARCHAR(50),
    VALUE                 NUMBER(15,6),
    NAME                  VARCHAR(500),
    API_RESPONSE_STATUS   VARCHAR(20),
    CREATED_AT            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT            TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT UK_ECONOMIC_INDICATORS_UNIQUE UNIQUE (ECONOMIC_INDICATOR_NAME, FUNCTION_NAME, MATURITY, DATE, INTERVAL)
)
COMMENT = 'Economic indicators data from Alpha Vantage API'
CLUSTER BY (DATE, ECONOMIC_INDICATOR_NAME);

-- ============================================================================
-- INSIDER TRANSACTIONS AND EARNINGS
-- ============================================================================

-- Table for storing insider transactions data
CREATE OR REPLACE TABLE INSIDER_TRANSACTIONS (
    TRANSACTION_ID      NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    SYMBOL_ID           NUMBER(38,0) NOT NULL,
    SYMBOL              VARCHAR(20) NOT NULL,
    TRANSACTION_DATE    DATE NOT NULL,
    EXECUTIVE           VARCHAR(500) NOT NULL,
    EXECUTIVE_TITLE     VARCHAR(500),
    SECURITY_TYPE       VARCHAR(100),
    ACQUISITION_OR_DISPOSAL VARCHAR(1),
    SHARES              NUMBER(20,4),
    SHARE_PRICE         NUMBER(20,4),
    API_RESPONSE_STATUS VARCHAR(20) DEFAULT 'pass',
    CREATED_AT          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT UK_INSIDER_TRANSACTIONS_UNIQUE UNIQUE (SYMBOL_ID, TRANSACTION_DATE, EXECUTIVE, SECURITY_TYPE, ACQUISITION_OR_DISPOSAL, SHARES, SHARE_PRICE)
)
COMMENT = 'Insider transactions data from Alpha Vantage API'
CLUSTER BY (SYMBOL_ID, TRANSACTION_DATE);

-- Table for storing earnings call transcripts data
CREATE OR REPLACE TABLE EARNINGS_CALL_TRANSCRIPTS (
    TRANSCRIPT_ID       NUMBER(38,0) IDENTITY(1,1) PRIMARY KEY,
    SYMBOL_ID           NUMBER(38,0) NOT NULL,
    SYMBOL              VARCHAR(20) NOT NULL,
    QUARTER             VARCHAR(10) NOT NULL,
    SPEAKER             VARCHAR(500) NOT NULL,
    TITLE               VARCHAR(500),
    CONTENT             VARCHAR(16777216) NOT NULL,  -- Large text field
    CONTENT_HASH        VARCHAR(32) NOT NULL,
    SENTIMENT           NUMBER(5,3),
    API_RESPONSE_STATUS VARCHAR(20) DEFAULT 'pass',
    CREATED_AT          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT          TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Constraints
    CONSTRAINT UK_EARNINGS_CALL_TRANSCRIPTS_UNIQUE UNIQUE (SYMBOL_ID, QUARTER, SPEAKER, CONTENT_HASH)
)
COMMENT = 'Earnings call transcripts data from Alpha Vantage API'
CLUSTER BY (SYMBOL_ID, QUARTER);

-- ============================================================================
-- SHOW CREATED TABLES
-- ============================================================================

SHOW TABLES IN SCHEMA RAW;

-- Show table details
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES,
    CLUSTERING_KEY,
    COMMENT
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'RAW' 
    AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;