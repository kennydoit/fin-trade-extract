-- ============================================================================
-- Snowpipe Configuration for Automatic CSV Loading
-- Sets up automatic data loading from S3 CSV files to Snowflake tables
-- ============================================================================

USE DATABASE FIN_TRADE_EXTRACT;
USE SCHEMA RAW;

-- ============================================================================
-- OVERVIEW DATA SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE OVERVIEW_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO OVERVIEW
FROM @OVERVIEW_STAGE
PATTERN = '.*overview_.*\\.csv'
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
ON_ERROR = 'CONTINUE';  -- Skip bad records and continue

-- ============================================================================
-- TIME SERIES DATA SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE TIME_SERIES_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO TIME_SERIES_DAILY_ADJUSTED (
    SYMBOL_ID,
    DATE,
    OPEN,
    HIGH,
    LOW,
    CLOSE,
    ADJUSTED_CLOSE,
    VOLUME,
    DIVIDEND_AMOUNT,
    SPLIT_COEFFICIENT
)
FROM @TIME_SERIES_STAGE
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- INCOME STATEMENT SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE INCOME_STATEMENT_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO INCOME_STATEMENT (
    SYMBOL_ID,
    FISCAL_DATE_ENDING,
    REPORTED_CURRENCY,
    GROSS_PROFIT,
    TOTAL_REVENUE,
    COST_OF_REVENUE,
    COST_OF_GOODS_AND_SERVICES_SOLD,
    OPERATING_INCOME,
    SELLING_GENERAL_AND_ADMINISTRATIVE,
    RESEARCH_AND_DEVELOPMENT,
    OPERATING_EXPENSES,
    INVESTMENT_INCOME_NET,
    NET_INTEREST_INCOME,
    INTEREST_INCOME,
    INTEREST_EXPENSE,
    NON_INTEREST_INCOME,
    OTHER_NON_OPERATING_INCOME,
    DEPRECIATION,
    DEPRECIATION_AND_AMORTIZATION,
    INCOME_BEFORE_TAX,
    INCOME_TAX_EXPENSE,
    INTEREST_AND_DEBT_EXPENSE,
    NET_INCOME_FROM_CONTINUING_OPERATIONS,
    COMPREHENSIVE_INCOME_NET_OF_TAX,
    EBIT,
    EBITDA,
    NET_INCOME
)
FROM @INCOME_STATEMENT_STAGE
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- BALANCE SHEET SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE BALANCE_SHEET_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO BALANCE_SHEET (
    SYMBOL_ID,
    FISCAL_DATE_ENDING,
    REPORTED_CURRENCY,
    TOTAL_ASSETS,
    TOTAL_CURRENT_ASSETS,
    CASH_AND_CASH_EQUIVALENTS_AT_CARRYING_VALUE,
    CASH_AND_SHORT_TERM_INVESTMENTS,
    INVENTORY,
    CURRENT_NET_RECEIVABLES,
    TOTAL_NON_CURRENT_ASSETS,
    PROPERTY_PLANT_EQUIPMENT,
    ACCUMULATED_DEPRECIATION_AMORTIZATION_PPE,
    INTANGIBLE_ASSETS,
    INTANGIBLE_ASSETS_EXCLUDING_GOODWILL,
    GOODWILL,
    INVESTMENTS,
    LONG_TERM_INVESTMENTS,
    SHORT_TERM_INVESTMENTS,
    OTHER_CURRENT_ASSETS,
    OTHER_NON_CURRENT_ASSETS,
    TOTAL_LIABILITIES,
    TOTAL_CURRENT_LIABILITIES,
    CURRENT_ACCOUNTS_PAYABLE,
    DEFERRED_REVENUE,
    CURRENT_DEBT,
    SHORT_TERM_DEBT,
    TOTAL_NON_CURRENT_LIABILITIES,
    CAPITAL_LEASE_OBLIGATIONS,
    LONG_TERM_DEBT,
    CURRENT_LONG_TERM_DEBT,
    LONG_TERM_DEBT_NONCURRENT,
    SHORT_LONG_TERM_DEBT_TOTAL,
    OTHER_CURRENT_LIABILITIES,
    OTHER_NON_CURRENT_LIABILITIES,
    TOTAL_SHAREHOLDER_EQUITY,
    TREASURY_STOCK,
    RETAINED_EARNINGS,
    COMMON_STOCK,
    COMMON_STOCK_SHARES_OUTSTANDING
)
FROM @BALANCE_SHEET_STAGE
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- CASH FLOW SNOWPIPE
-- ============================================================================

CREATE OR REPLACE PIPE CASH_FLOW_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO CASH_FLOW (
    SYMBOL_ID,
    FISCAL_DATE_ENDING,
    REPORTED_CURRENCY,
    OPERATING_CASHFLOW,
    PAYMENTS_FOR_OPERATING_ACTIVITIES,
    PROCEEDS_FROM_OPERATING_ACTIVITIES,
    CHANGE_IN_OPERATING_LIABILITIES,
    CHANGE_IN_OPERATING_ASSETS,
    DEPRECIATION_DEPLETION_AND_AMORTIZATION,
    CAPITAL_EXPENDITURES,
    CHANGE_IN_RECEIVABLES,
    CHANGE_IN_INVENTORY,
    PROFIT_LOSS,
    CASHFLOW_FROM_INVESTMENT,
    CASHFLOW_FROM_FINANCING,
    PROCEEDS_FROM_REPAYMENTS_OF_SHORT_TERM_DEBT,
    PAYMENTS_FOR_REPURCHASE_OF_COMMON_STOCK,
    PAYMENTS_FOR_REPURCHASE_OF_EQUITY,
    PAYMENTS_FOR_REPURCHASE_OF_PREFERRED_STOCK,
    DIVIDEND_PAYOUT,
    DIVIDEND_PAYOUT_COMMON_STOCK,
    DIVIDEND_PAYOUT_PREFERRED_STOCK,
    PROCEEDS_FROM_ISSUANCE_OF_COMMON_STOCK,
    PROCEEDS_FROM_ISSUANCE_OF_LONG_TERM_DEBT_AND_CAPITAL_SECURITIES_NET,
    PROCEEDS_FROM_ISSUANCE_OF_PREFERRED_STOCK,
    PROCEEDS_FROM_REPAYMENTS_OF_LONG_TERM_DEBT,
    PROCEEDS_FROM_SHORT_TERM_DEBT,
    CHANGE_IN_CASH_AND_CASH_EQUIVALENTS,
    CHANGE_IN_EXCHANGE_RATE,
    NET_INCOME
)
FROM @CASH_FLOW_STAGE
FILE_FORMAT = CSV_FORMAT
ON_ERROR = 'CONTINUE';

-- ============================================================================
-- SHOW SNOWPIPE DETAILS
-- ============================================================================

-- Show created pipes
SHOW PIPES IN SCHEMA RAW;

-- Get pipe notification channels for S3 event configuration
-- Run these commands to get notification channel ARNs for each pipe:
-- DESC PIPE OVERVIEW_PIPE;
-- DESC PIPE TIME_SERIES_PIPE;
-- DESC PIPE INCOME_STATEMENT_PIPE;
-- DESC PIPE BALANCE_SHEET_PIPE;
-- DESC PIPE CASH_FLOW_PIPE;

-- The notification_channel field in the DESC output contains the SQS ARN needed for S3 notifications

-- ============================================================================
-- PIPE STATUS AND MONITORING QUERIES
-- ============================================================================

-- Check pipe status
-- SELECT SYSTEM$PIPE_STATUS('OVERVIEW_PIPE');

-- View pipe execution history
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME=>'OVERVIEW', START_TIME=> DATEADD(hours, -24, CURRENT_TIMESTAMP())));

-- Manual pipe refresh (if needed for testing)
-- ALTER PIPE OVERVIEW_PIPE REFRESH;

-- ============================================================================
-- NOTES FOR S3 EVENT NOTIFICATION SETUP
-- ============================================================================

/*
IMPORTANT: After creating the pipes, you need to configure S3 bucket event notifications:

1. Get the notification channel ARNs from the query above
2. Configure S3 bucket notifications for each path:
   - overview/ -> OVERVIEW_PIPE notification channel
   - time-series/ -> TIME_SERIES_PIPE notification channel  
   - income-statement/ -> INCOME_STATEMENT_PIPE notification channel
   - balance-sheet/ -> BALANCE_SHEET_PIPE notification channel
   - cash-flow/ -> CASH_FLOW_PIPE notification channel

3. Set up notifications for ObjectCreated events (s3:ObjectCreated:Put, s3:ObjectCreated:Post)

Example AWS CLI command:
aws s3api put-bucket-notification-configuration --bucket fin-trade-craft-landing --notification-configuration file://bucket-notifications.json

The notification configuration file will be created in the deployment directory.
*/