-- PostgreSQL schema for fin-trade-craft database
-- Migrated from SQLite schema with PostgreSQL-specific optimizations

-- Table for storing symbol metadata
CREATE TABLE IF NOT EXISTS listing_status (
    symbol_id       SERIAL PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL UNIQUE,
    name            VARCHAR(255),
    exchange        VARCHAR(50),
    asset_type      VARCHAR(50),
    ipo_date        DATE,
    delisting_date  DATE,
    status          VARCHAR(20),
    state           VARCHAR(20) DEFAULT 'active',
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW()
);

-- Create index on symbol for faster lookups (on the actual table in source schema)
CREATE INDEX IF NOT EXISTS idx_listing_status_symbol ON source.listing_status(symbol);

-- Table for storing company overview information
CREATE TABLE IF NOT EXISTS overview (
    overview_id     SERIAL PRIMARY KEY,
    symbol_id       INTEGER NOT NULL,
    symbol          VARCHAR(20) NOT NULL,    
    assettype       VARCHAR(50),
    name            VARCHAR(255),   
    description     TEXT,   
    cik             VARCHAR(20),   
    exchange        VARCHAR(50),   
    currency        VARCHAR(10),   
    country         VARCHAR(100),   
    sector          VARCHAR(100),   
    industry        VARCHAR(200),   
    address         TEXT,   
    officialsite    VARCHAR(255),   
    fiscalyearend   VARCHAR(20),
    status          VARCHAR(20),
    created_at      TIMESTAMP DEFAULT NOW(),
    updated_at      TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE,
    UNIQUE(symbol_id)  -- Ensure one overview record per symbol
);

-- Create index on symbol_id for faster joins (on actual table, not view)
-- Note: Skip if overview table conflicts with views
-- CREATE INDEX IF NOT EXISTS idx_overview_symbol_id ON overview(symbol_id);

-- Table for storing time series adjusted data
CREATE TABLE IF NOT EXISTS time_series_daily_adjusted (
    symbol_id         INTEGER NOT NULL,
    symbol            VARCHAR(20) NOT NULL,
    date              DATE NOT NULL,
    open              NUMERIC(15,4),
    high              NUMERIC(15,4),
    low               NUMERIC(15,4),
    close             NUMERIC(15,4),
    adjusted_close    NUMERIC(15,4),
    volume            BIGINT,
    dividend_amount   NUMERIC(15,6),
    split_coefficient NUMERIC(10,6),
    created_at        TIMESTAMP DEFAULT NOW(),
    updated_at        TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE,
    UNIQUE(symbol_id, date)  -- Ensure one record per symbol and date
);

-- Create indexes for time series data
CREATE INDEX IF NOT EXISTS idx_time_series_symbol_id ON time_series_daily_adjusted(symbol_id);
CREATE INDEX IF NOT EXISTS idx_time_series_date ON time_series_daily_adjusted(date);
CREATE INDEX IF NOT EXISTS idx_time_series_symbol_date ON time_series_daily_adjusted(symbol_id, date);

-- Table for storing income statement data
CREATE TABLE IF NOT EXISTS income_statement (
    symbol_id                           INTEGER NOT NULL,
    symbol                              VARCHAR(20) NOT NULL,
    fiscal_date_ending                  DATE,  -- Allow NULL for empty/error records
    report_type                         VARCHAR(10) NOT NULL CHECK (report_type IN ('annual', 'quarterly')),
    reported_currency                   VARCHAR(10),
    gross_profit                        BIGINT,
    total_revenue                       BIGINT,
    cost_of_revenue                     BIGINT,
    cost_of_goods_and_services_sold     BIGINT,
    operating_income                    BIGINT,
    selling_general_and_administrative  BIGINT,
    research_and_development            BIGINT,
    operating_expenses                  BIGINT,
    investment_income_net               BIGINT,
    net_interest_income                 BIGINT,
    interest_income                     BIGINT,
    interest_expense                    BIGINT,
    non_interest_income                 BIGINT,
    other_non_operating_income          BIGINT,
    depreciation                        BIGINT,
    depreciation_and_amortization       BIGINT,
    income_before_tax                   BIGINT,
    income_tax_expense                  BIGINT,
    interest_and_debt_expense           BIGINT,
    net_income_from_continuing_operations BIGINT,
    comprehensive_income_net_of_tax     BIGINT,
    ebit                                BIGINT,
    ebitda                              BIGINT,
    net_income                          BIGINT,
    api_response_status                 VARCHAR(20),
    created_at                          TIMESTAMP DEFAULT NOW(),
    updated_at                          TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE
    -- Note: For empty/error records, fiscal_date_ending is NULL
    -- For data records, unique constraint is on (symbol_id, report_type, fiscal_date_ending)
);

-- Create indexes for income statement
CREATE INDEX IF NOT EXISTS idx_income_statement_symbol_id ON income_statement(symbol_id);
CREATE INDEX IF NOT EXISTS idx_income_statement_fiscal_date ON income_statement(fiscal_date_ending);

CREATE TABLE IF NOT EXISTS balance_sheet (
    symbol_id                               INTEGER NOT NULL,
    symbol                                  VARCHAR(20) NOT NULL,
    fiscal_date_ending                      DATE,  -- Allow NULL for empty/error records
    report_type                             VARCHAR(10) NOT NULL CHECK (report_type IN ('annual', 'quarterly')),
    reported_currency                       VARCHAR(10),
    total_assets                            BIGINT,
    total_current_assets                    BIGINT,
    cash_and_cash_equivalents_at_carrying_value BIGINT,
    cash_and_short_term_investments         BIGINT,
    inventory                               BIGINT,
    current_net_receivables                 BIGINT,
    total_non_current_assets                BIGINT,
    property_plant_equipment                BIGINT,
    accumulated_depreciation_amortization_ppe BIGINT,
    intangible_assets                       BIGINT,
    intangible_assets_excluding_goodwill    BIGINT,
    goodwill                                BIGINT,
    investments                             BIGINT,
    long_term_investments                   BIGINT,
    short_term_investments                  BIGINT,
    other_current_assets                    BIGINT,
    other_non_current_assets                BIGINT,
    total_liabilities                       BIGINT,
    total_current_liabilities               BIGINT,
    current_accounts_payable                BIGINT,
    deferred_revenue                        BIGINT,
    current_debt                            BIGINT,
    short_term_debt                         BIGINT,
    total_non_current_liabilities           BIGINT,
    capital_lease_obligations               BIGINT,
    long_term_debt                          BIGINT,
    current_long_term_debt                  BIGINT,
    long_term_debt_noncurrent               BIGINT,
    short_long_term_debt_total              BIGINT,
    other_current_liabilities               BIGINT,
    other_non_current_liabilities           BIGINT,
    total_shareholder_equity                BIGINT,
    treasury_stock                          BIGINT,
    retained_earnings                       BIGINT,
    common_stock                            BIGINT,
    common_stock_shares_outstanding         BIGINT,
    api_response_status                     VARCHAR(20),
    created_at                              TIMESTAMP DEFAULT NOW(),
    updated_at                              TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE
    -- Note: For empty/error records, fiscal_date_ending is NULL
    -- For data records, unique constraint is on (symbol_id, report_type, fiscal_date_ending)
);

-- Create indexes for balance sheet
CREATE INDEX IF NOT EXISTS idx_balance_sheet_symbol_id ON balance_sheet(symbol_id);
CREATE INDEX IF NOT EXISTS idx_balance_sheet_fiscal_date ON balance_sheet(fiscal_date_ending);

CREATE TABLE IF NOT EXISTS cash_flow (
    symbol_id                                       INTEGER NOT NULL,
    symbol                                          VARCHAR(20) NOT NULL,
    fiscal_date_ending                              DATE,  -- Allow NULL for empty/error records
    report_type                                     VARCHAR(10) NOT NULL CHECK (report_type IN ('annual', 'quarterly')),
    reported_currency                               VARCHAR(10),
    operating_cashflow                              BIGINT,
    payments_for_operating_activities               BIGINT,
    proceeds_from_operating_activities              BIGINT,
    change_in_operating_liabilities                 BIGINT,
    change_in_operating_assets                      BIGINT,
    depreciation_depletion_and_amortization         BIGINT,
    capital_expenditures                            BIGINT,
    change_in_receivables                           BIGINT,
    change_in_inventory                             BIGINT,
    profit_loss                                     BIGINT,
    cashflow_from_investment                        BIGINT,
    cashflow_from_financing                         BIGINT,
    proceeds_from_repayments_of_short_term_debt     BIGINT,
    payments_for_repurchase_of_common_stock         BIGINT,
    payments_for_repurchase_of_equity               BIGINT,
    payments_for_repurchase_of_preferred_stock      BIGINT,
    dividend_payout                                 BIGINT,
    dividend_payout_common_stock                    BIGINT,
    dividend_payout_preferred_stock                 BIGINT,
    proceeds_from_issuance_of_common_stock          BIGINT,
    proceeds_from_issuance_of_long_term_debt_and_capital_securities_net BIGINT,
    proceeds_from_issuance_of_preferred_stock       BIGINT,
    proceeds_from_repurchase_of_equity              BIGINT,
    proceeds_from_sale_of_treasury_stock            BIGINT,
    change_in_cash_and_cash_equivalents             BIGINT,
    change_in_exchange_rate                         BIGINT,
    net_income                                      BIGINT,
    api_response_status                             VARCHAR(20),
    created_at                                      TIMESTAMP DEFAULT NOW(),
    updated_at                                      TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE
    -- Note: For empty/error records, fiscal_date_ending is NULL
    -- For data records, unique constraint is on (symbol_id, report_type, fiscal_date_ending)
);

-- Create indexes for cash flow
CREATE INDEX IF NOT EXISTS idx_cash_flow_symbol_id ON cash_flow(symbol_id);
CREATE INDEX IF NOT EXISTS idx_cash_flow_fiscal_date ON cash_flow(fiscal_date_ending);

CREATE TABLE IF NOT EXISTS commodities (
    commodity_id          SERIAL PRIMARY KEY,
    commodity_name        VARCHAR(100) NOT NULL,
    function_name         VARCHAR(50) NOT NULL,  -- API function name (WTI, BRENT, NATURAL_GAS, etc.)
    date                  DATE,           -- Allow NULL for empty/error records
    interval              VARCHAR(10) NOT NULL CHECK (interval IN ('daily', 'monthly')),
    unit                  VARCHAR(50),
    value                 NUMERIC(15,6),
    name                  VARCHAR(255),           -- Full name from API response
    api_response_status   VARCHAR(20),           -- 'data', 'empty', 'error', 'pass'
    created_at            TIMESTAMP DEFAULT NOW(),
    updated_at            TIMESTAMP DEFAULT NOW(),
    UNIQUE(commodity_name, date, interval)  -- Ensure one record per commodity, date, interval
    -- Note: For empty/error records, date is NULL
    -- For data records, unique constraint is on (commodity_name, date, interval)
);

-- Create indexes for commodities
CREATE INDEX IF NOT EXISTS idx_commodities_name ON commodities(commodity_name);
CREATE INDEX IF NOT EXISTS idx_commodities_date ON commodities(date);

CREATE TABLE IF NOT EXISTS economic_indicators (
    economic_indicator_id          SERIAL PRIMARY KEY,
    economic_indicator_name        VARCHAR(100) NOT NULL,
    function_name         VARCHAR(50) NOT NULL,  -- API function name (REAL_GDP, TREASURY_YIELD, etc.)
    maturity              VARCHAR(20),           -- For Treasury yields (3month, 2year, 5year, 7year, 10year, 30year)
    date                  DATE,           -- Allow NULL for empty/error records
    interval              VARCHAR(15) NOT NULL CHECK (interval IN ('daily', 'monthly', 'quarterly')),
    unit                  VARCHAR(50),
    value                 NUMERIC(15,6),
    name                  VARCHAR(255),           -- Full name from API response
    api_response_status   VARCHAR(20),           -- 'data', 'empty', 'error', 'pass'
    created_at            TIMESTAMP DEFAULT NOW(),
    updated_at            TIMESTAMP DEFAULT NOW(),
    UNIQUE(economic_indicator_name, function_name, maturity, date, interval)  -- Ensure one record per indicator, maturity, date, interval
    -- Note: For empty/error records, date is NULL
    -- For data records, unique constraint includes maturity for Treasury yields
);

-- Create indexes for economic indicators
CREATE INDEX IF NOT EXISTS idx_economic_indicators_name ON economic_indicators(economic_indicator_name);
CREATE INDEX IF NOT EXISTS idx_economic_indicators_date ON economic_indicators(date);

-- Create triggers to automatically update the updated_at timestamp
-- This replaces the SQLite behavior and provides automatic timestamp updates

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply the trigger to all tables with updated_at columns
-- Table for storing historical options data
CREATE TABLE IF NOT EXISTS historical_options (
    option_id           SERIAL PRIMARY KEY,
    symbol_id           INTEGER NOT NULL,
    symbol              VARCHAR(20) NOT NULL,
    contract_name       VARCHAR(50) NOT NULL,
    option_type         VARCHAR(4) NOT NULL CHECK (option_type IN ('call', 'put')),
    strike              DECIMAL(12,4) NOT NULL,
    expiration          DATE NOT NULL,
    last_trade_date     DATE NOT NULL,  -- Date of the historical data
    last_price          DECIMAL(12,4),
    mark                DECIMAL(12,4),
    bid                 DECIMAL(12,4),
    bid_size            INTEGER,
    ask                 DECIMAL(12,4),
    ask_size            INTEGER,
    volume              BIGINT,
    open_interest       BIGINT,
    implied_volatility  DECIMAL(8,6),
    delta               DECIMAL(8,6),
    gamma               DECIMAL(8,6),
    theta               DECIMAL(8,6),
    vega                DECIMAL(8,6),
    rho                 DECIMAL(8,6),
    intrinsic_value     DECIMAL(12,4),
    extrinsic_value     DECIMAL(12,4),
    updated_unix        BIGINT,
    time_value          DECIMAL(12,4),
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE,
    UNIQUE(symbol_id, contract_name, last_trade_date)  -- Prevent duplicate records
);

-- Create indexes for historical options
CREATE INDEX IF NOT EXISTS idx_historical_options_symbol_id ON historical_options(symbol_id);
CREATE INDEX IF NOT EXISTS idx_historical_options_symbol ON historical_options(symbol);
CREATE INDEX IF NOT EXISTS idx_historical_options_date ON historical_options(last_trade_date);
CREATE INDEX IF NOT EXISTS idx_historical_options_expiration ON historical_options(expiration);
CREATE INDEX IF NOT EXISTS idx_historical_options_type_strike ON historical_options(option_type, strike);
CREATE INDEX IF NOT EXISTS idx_historical_options_contract ON historical_options(contract_name);

-- Table for storing real-time options data (for future use)
CREATE TABLE IF NOT EXISTS realtime_options (
    option_id           SERIAL PRIMARY KEY,
    symbol_id           INTEGER NOT NULL,
    symbol              VARCHAR(20) NOT NULL,
    contract_name       VARCHAR(50) NOT NULL,
    option_type         VARCHAR(4) NOT NULL CHECK (option_type IN ('call', 'put')),
    strike              DECIMAL(12,4) NOT NULL,
    expiration          DATE NOT NULL,
    last_price          DECIMAL(12,4),
    mark                DECIMAL(12,4),
    bid                 DECIMAL(12,4),
    bid_size            INTEGER,
    ask                 DECIMAL(12,4),
    ask_size            INTEGER,
    volume              BIGINT,
    open_interest       BIGINT,
    implied_volatility  DECIMAL(8,6),
    delta               DECIMAL(8,6),
    gamma               DECIMAL(8,6),
    theta               DECIMAL(8,6),
    vega                DECIMAL(8,6),
    rho                 DECIMAL(8,6),
    intrinsic_value     DECIMAL(12,4),
    extrinsic_value     DECIMAL(12,4),
    updated_unix        BIGINT,
    time_value          DECIMAL(12,4),
    quote_timestamp     TIMESTAMP DEFAULT NOW(),
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE,
    UNIQUE(symbol_id, contract_name)  -- Real-time data, one record per contract
);

-- Create indexes for realtime options
CREATE INDEX IF NOT EXISTS idx_realtime_options_symbol_id ON realtime_options(symbol_id);
CREATE INDEX IF NOT EXISTS idx_realtime_options_symbol ON realtime_options(symbol);
CREATE INDEX IF NOT EXISTS idx_realtime_options_expiration ON realtime_options(expiration);
CREATE INDEX IF NOT EXISTS idx_realtime_options_type_strike ON realtime_options(option_type, strike);
CREATE INDEX IF NOT EXISTS idx_realtime_options_timestamp ON realtime_options(quote_timestamp);

-- Table for storing insider transactions data
CREATE TABLE IF NOT EXISTS insider_transactions (
    transaction_id      SERIAL PRIMARY KEY,
    symbol_id           INTEGER NOT NULL,
    symbol              VARCHAR(20) NOT NULL,
    transaction_date    DATE NOT NULL,
    executive           VARCHAR(255) NOT NULL,
    executive_title     VARCHAR(255),
    security_type       VARCHAR(100),
    acquisition_or_disposal VARCHAR(1),  -- 'A' for acquisition, 'D' for disposal
    shares              DECIMAL(20,4),
    share_price         DECIMAL(20,4),
    api_response_status VARCHAR(20) DEFAULT 'pass',
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE,
    UNIQUE(symbol_id, transaction_date, executive, security_type, acquisition_or_disposal, shares, share_price)  -- Prevent duplicate transactions
);

-- Create indexes for insider transactions
CREATE INDEX IF NOT EXISTS idx_insider_transactions_symbol_id ON insider_transactions(symbol_id);
CREATE INDEX IF NOT EXISTS idx_insider_transactions_symbol ON insider_transactions(symbol);
CREATE INDEX IF NOT EXISTS idx_insider_transactions_date ON insider_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_insider_transactions_executive ON insider_transactions(executive);
CREATE INDEX IF NOT EXISTS idx_insider_transactions_type ON insider_transactions(acquisition_or_disposal);

-- Table for storing earnings call transcripts data
CREATE TABLE IF NOT EXISTS earnings_call_transcripts (
    transcript_id       SERIAL PRIMARY KEY,
    symbol_id           INTEGER NOT NULL,
    symbol              VARCHAR(20) NOT NULL,
    quarter             VARCHAR(10) NOT NULL,  -- Format: YYYYQM (e.g., 2024Q1)
    speaker             VARCHAR(255) NOT NULL,
    title               VARCHAR(255),
    content             TEXT NOT NULL,
    content_hash        VARCHAR(32) NOT NULL,  -- MD5 hash of content for uniqueness
    sentiment           DECIMAL(5,3),  -- Sentiment score (e.g., 0.6, 0.7)
    api_response_status VARCHAR(20) DEFAULT 'pass',
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (symbol_id) REFERENCES listing_status(symbol_id) ON DELETE CASCADE,
    UNIQUE(symbol_id, quarter, speaker, content_hash)  -- Use hash instead of full content
);

-- Create indexes for earnings call transcripts
CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_symbol_id ON earnings_call_transcripts(symbol_id);
CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_symbol ON earnings_call_transcripts(symbol);
CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_quarter ON earnings_call_transcripts(quarter);
CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_speaker ON earnings_call_transcripts(speaker);
CREATE INDEX IF NOT EXISTS idx_earnings_call_transcripts_sentiment ON earnings_call_transcripts(sentiment);

-- Drop triggers if they already exist to avoid duplicate-object errors on repeated initialization
-- Skip triggers for views (listing_status, overview) as views cannot have row-level triggers
-- DROP TRIGGER IF EXISTS update_listing_status_updated_at ON listing_status;
-- DROP TRIGGER IF EXISTS update_overview_updated_at ON overview;
DROP TRIGGER IF EXISTS update_time_series_updated_at ON time_series_daily_adjusted;
DROP TRIGGER IF EXISTS update_income_statement_updated_at ON income_statement;
DROP TRIGGER IF EXISTS update_balance_sheet_updated_at ON balance_sheet;
DROP TRIGGER IF EXISTS update_cash_flow_updated_at ON cash_flow;
DROP TRIGGER IF EXISTS update_commodities_updated_at ON commodities;
DROP TRIGGER IF EXISTS update_economic_indicators_updated_at ON economic_indicators;
DROP TRIGGER IF EXISTS update_historical_options_updated_at ON historical_options;
DROP TRIGGER IF EXISTS update_realtime_options_updated_at ON realtime_options;
DROP TRIGGER IF EXISTS update_insider_transactions_updated_at ON insider_transactions;
DROP TRIGGER IF EXISTS update_earnings_call_transcripts_updated_at ON earnings_call_transcripts;

-- Recreate triggers to ensure updated_at is maintained
-- Skip triggers for views (listing_status, overview) as views cannot have row-level triggers  
-- CREATE TRIGGER update_listing_status_updated_at BEFORE UPDATE ON listing_status FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
-- CREATE TRIGGER update_overview_updated_at BEFORE UPDATE ON overview FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_time_series_updated_at BEFORE UPDATE ON time_series_daily_adjusted FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_income_statement_updated_at BEFORE UPDATE ON income_statement FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_balance_sheet_updated_at BEFORE UPDATE ON balance_sheet FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_cash_flow_updated_at BEFORE UPDATE ON cash_flow FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_commodities_updated_at BEFORE UPDATE ON commodities FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_economic_indicators_updated_at BEFORE UPDATE ON economic_indicators FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_historical_options_updated_at BEFORE UPDATE ON historical_options FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_realtime_options_updated_at BEFORE UPDATE ON realtime_options FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_insider_transactions_updated_at BEFORE UPDATE ON insider_transactions FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
CREATE TRIGGER update_earnings_call_transcripts_updated_at BEFORE UPDATE ON earnings_call_transcripts FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
