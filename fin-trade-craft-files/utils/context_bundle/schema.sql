--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5
-- Dumped by pg_dump version 17.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: extracted; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA extracted;


ALTER SCHEMA extracted OWNER TO postgres;

--
-- Name: update_updated_at_column(); Type: FUNCTION; Schema: extracted; Owner: postgres
--

CREATE FUNCTION extracted.update_updated_at_column() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;


ALTER FUNCTION extracted.update_updated_at_column() OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: balance_sheet; Type: TABLE; Schema: extracted; Owner: postgres
--

CREATE TABLE extracted.balance_sheet (
    symbol_id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    fiscal_date_ending date,
    report_type character varying(10) NOT NULL,
    reported_currency character varying(10),
    total_assets bigint,
    total_current_assets bigint,
    cash_and_cash_equivalents_at_carrying_value bigint,
    cash_and_short_term_investments bigint,
    inventory bigint,
    current_net_receivables bigint,
    total_non_current_assets bigint,
    property_plant_equipment bigint,
    accumulated_depreciation_amortization_ppe bigint,
    intangible_assets bigint,
    intangible_assets_excluding_goodwill bigint,
    goodwill bigint,
    investments bigint,
    long_term_investments bigint,
    short_term_investments bigint,
    other_current_assets bigint,
    other_non_current_assets bigint,
    total_liabilities bigint,
    total_current_liabilities bigint,
    current_accounts_payable bigint,
    deferred_revenue bigint,
    current_debt bigint,
    short_term_debt bigint,
    total_non_current_liabilities bigint,
    capital_lease_obligations bigint,
    long_term_debt bigint,
    current_long_term_debt bigint,
    long_term_debt_noncurrent bigint,
    short_long_term_debt_total bigint,
    other_current_liabilities bigint,
    other_non_current_liabilities bigint,
    total_shareholder_equity bigint,
    treasury_stock bigint,
    retained_earnings bigint,
    common_stock bigint,
    common_stock_shares_outstanding bigint,
    api_response_status character varying(20),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT balance_sheet_report_type_check CHECK (((report_type)::text = ANY ((ARRAY['annual'::character varying, 'quarterly'::character varying])::text[])))
);


ALTER TABLE extracted.balance_sheet OWNER TO postgres;

--
-- Name: cash_flow; Type: TABLE; Schema: extracted; Owner: postgres
--

CREATE TABLE extracted.cash_flow (
    symbol_id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    fiscal_date_ending date,
    report_type character varying(10) NOT NULL,
    reported_currency character varying(10),
    operating_cashflow bigint,
    payments_for_operating_activities bigint,
    proceeds_from_operating_activities bigint,
    change_in_operating_liabilities bigint,
    change_in_operating_assets bigint,
    depreciation_depletion_and_amortization bigint,
    capital_expenditures bigint,
    change_in_receivables bigint,
    change_in_inventory bigint,
    profit_loss bigint,
    cashflow_from_investment bigint,
    cashflow_from_financing bigint,
    proceeds_from_repayments_of_short_term_debt bigint,
    payments_for_repurchase_of_common_stock bigint,
    payments_for_repurchase_of_equity bigint,
    payments_for_repurchase_of_preferred_stock bigint,
    dividend_payout bigint,
    dividend_payout_common_stock bigint,
    dividend_payout_preferred_stock bigint,
    proceeds_from_issuance_of_common_stock bigint,
    proceeds_from_issuance_of_long_term_debt_and_capital_securities bigint,
    proceeds_from_issuance_of_preferred_stock bigint,
    proceeds_from_repurchase_of_equity bigint,
    proceeds_from_sale_of_treasury_stock bigint,
    change_in_cash_and_cash_equivalents bigint,
    change_in_exchange_rate bigint,
    net_income bigint,
    api_response_status character varying(20),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT cash_flow_report_type_check CHECK (((report_type)::text = ANY ((ARRAY['annual'::character varying, 'quarterly'::character varying])::text[])))
);


ALTER TABLE extracted.cash_flow OWNER TO postgres;

--
-- Name: commodities; Type: TABLE; Schema: extracted; Owner: postgres
--

CREATE TABLE extracted.commodities (
    commodity_id integer NOT NULL,
    commodity_name character varying(100) NOT NULL,
    function_name character varying(50) NOT NULL,
    date date,
    "interval" character varying(10) NOT NULL,
    unit character varying(50),
    value numeric(15,6),
    name character varying(255),
    api_response_status character varying(20),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT commodities_interval_check CHECK ((("interval")::text = ANY ((ARRAY['daily'::character varying, 'monthly'::character varying])::text[])))
);


ALTER TABLE extracted.commodities OWNER TO postgres;

--
-- Name: commodities_commodity_id_seq; Type: SEQUENCE; Schema: extracted; Owner: postgres
--

CREATE SEQUENCE extracted.commodities_commodity_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE extracted.commodities_commodity_id_seq OWNER TO postgres;

--
-- Name: commodities_commodity_id_seq; Type: SEQUENCE OWNED BY; Schema: extracted; Owner: postgres
--

ALTER SEQUENCE extracted.commodities_commodity_id_seq OWNED BY extracted.commodities.commodity_id;


--
-- Name: commodities_daily; Type: TABLE; Schema: extracted; Owner: postgres
--

CREATE TABLE extracted.commodities_daily (
    daily_commodity_id integer NOT NULL,
    commodity_name character varying(100) NOT NULL,
    function_name character varying(50) NOT NULL,
    date date NOT NULL,
    original_interval character varying(15) NOT NULL,
    updated_interval character varying(15) DEFAULT 'daily'::character varying NOT NULL,
    unit character varying(50),
    value numeric(15,6),
    name character varying(255),
    is_forward_filled boolean DEFAULT false,
    original_date date,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT commodities_daily_original_interval_check CHECK (((original_interval)::text = ANY ((ARRAY['daily'::character varying, 'monthly'::character varying, 'quarterly'::character varying])::text[])))
);


ALTER TABLE extracted.commodities_daily OWNER TO postgres;

--
-- Name: commodities_daily_daily_commodity_id_seq; Type: SEQUENCE; Schema: extracted; Owner: postgres
--

CREATE SEQUENCE extracted.commodities_daily_daily_commodity_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE extracted.commodities_daily_daily_commodity_id_seq OWNER TO postgres;

--
-- Name: commodities_daily_daily_commodity_id_seq; Type: SEQUENCE OWNED BY; Schema: extracted; Owner: postgres
--

ALTER SEQUENCE extracted.commodities_daily_daily_commodity_id_seq OWNED BY extracted.commodities_daily.daily_commodity_id;


--
-- Name: earnings_call_transcripts; Type: TABLE; Schema: extracted; Owner: postgres
--

CREATE TABLE extracted.earnings_call_transcripts (
    transcript_id integer NOT NULL,
    symbol_id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    quarter character varying(10) NOT NULL,
    speaker character varying(255) NOT NULL,
    title character varying(255),
    content text NOT NULL,
    content_hash character varying(32) NOT NULL,
    sentiment numeric(5,3),
    api_response_status character varying(20) DEFAULT 'pass'::character varying,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE extracted.earnings_call_transcripts OWNER TO postgres;

--
-- Name: earnings_call_transcripts_transcript_id_seq; Type: SEQUENCE; Schema: extracted; Owner: postgres
--

CREATE SEQUENCE extracted.earnings_call_transcripts_transcript_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE extracted.earnings_call_transcripts_transcript_id_seq OWNER TO postgres;

--
-- Name: earnings_call_transcripts_transcript_id_seq; Type: SEQUENCE OWNED BY; Schema: extracted; Owner: postgres
--

ALTER SEQUENCE extracted.earnings_call_transcripts_transcript_id_seq OWNED BY extracted.earnings_call_transcripts.transcript_id;


--
-- Name: economic_indicators; Type: TABLE; Schema: extracted; Owner: postgres
--

CREATE TABLE extracted.economic_indicators (
    economic_indicator_id integer NOT NULL,
    economic_indicator_name character varying(100) NOT NULL,
    function_name character varying(50) NOT NULL,
    maturity character varying(20),
    date date,
    "interval" character varying(15) NOT NULL,
    unit character varying(50),
    value numeric(15,6),
    name character varying(255),
    api_response_status character varying(20),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT economic_indicators_interval_check CHECK ((("interval")::text = ANY ((ARRAY['daily'::character varying, 'monthly'::character varying, 'quarterly'::character varying])::text[])))
);


ALTER TABLE extracted.economic_indicators OWNER TO postgres;

--
-- Name: economic_indicators_daily; Type: TABLE; Schema: extracted; Owner: postgres
--

CREATE TABLE extracted.economic_indicators_daily (
    daily_indicator_id integer NOT NULL,
    economic_indicator_name character varying(100) NOT NULL,
    function_name character varying(50) NOT NULL,
    maturity character varying(20),
    date date NOT NULL,
    original_interval character varying(15) NOT NULL,
    updated_interval character varying(15) DEFAULT 'daily'::character varying NOT NULL,
    unit character varying(50),
    value numeric(15,6),
    name character varying(255),
    is_forward_filled boolean DEFAULT false,
    original_date date,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT economic_indicators_daily_original_interval_check CHECK (((original_interval)::text = ANY ((ARRAY['daily'::character varying, 'monthly'::character varying, 'quarterly'::character varying])::text[])))
);


ALTER TABLE extracted.economic_indicators_daily OWNER TO postgres;

--
-- Name: economic_indicators_daily_daily_indicator_id_seq; Type: SEQUENCE; Schema: extracted; Owner: postgres
--

CREATE SEQUENCE extracted.economic_indicators_daily_daily_indicator_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE extracted.economic_indicators_daily_daily_indicator_id_seq OWNER TO postgres;

--
-- Name: economic_indicators_daily_daily_indicator_id_seq; Type: SEQUENCE OWNED BY; Schema: extracted; Owner: postgres
--

ALTER SEQUENCE extracted.economic_indicators_daily_daily_indicator_id_seq OWNED BY extracted.economic_indicators_daily.daily_indicator_id;


--
-- Name: economic_indicators_economic_indicator_id_seq; Type: SEQUENCE; Schema: extracted; Owner: postgres
--

CREATE SEQUENCE extracted.economic_indicators_economic_indicator_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE extracted.economic_indicators_economic_indicator_id_seq OWNER TO postgres;

--
-- Name: economic_indicators_economic_indicator_id_seq; Type: SEQUENCE OWNED BY; Schema: extracted; Owner: postgres
--

ALTER SEQUENCE extracted.economic_indicators_economic_indicator_id_seq OWNED BY extracted.economic_indicators.economic_indicator_id;


--
-- Name: income_statement; Type: TABLE; Schema: extracted; Owner: postgres
--

CREATE TABLE extracted.income_statement (
    symbol_id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    fiscal_date_ending date,
    report_type character varying(10) NOT NULL,
    reported_currency character varying(10),
    gross_profit bigint,
    total_revenue bigint,
    cost_of_revenue bigint,
    cost_of_goods_and_services_sold bigint,
    operating_income bigint,
    selling_general_and_administrative bigint,
    research_and_development bigint,
    operating_expenses bigint,
    investment_income_net bigint,
    net_interest_income bigint,
    interest_income bigint,
    interest_expense bigint,
    non_interest_income bigint,
    other_non_operating_income bigint,
    depreciation bigint,
    depreciation_and_amortization bigint,
    income_before_tax bigint,
    income_tax_expense bigint,
    interest_and_debt_expense bigint,
    net_income_from_continuing_operations bigint,
    comprehensive_income_net_of_tax bigint,
    ebit bigint,
    ebitda bigint,
    net_income bigint,
    api_response_status character varying(20),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now(),
    CONSTRAINT income_statement_report_type_check CHECK (((report_type)::text = ANY ((ARRAY['annual'::character varying, 'quarterly'::character varying])::text[])))
);


ALTER TABLE extracted.income_statement OWNER TO postgres;

--
-- Name: insider_transactions; Type: TABLE; Schema: extracted; Owner: postgres
--

CREATE TABLE extracted.insider_transactions (
    transaction_id integer NOT NULL,
    symbol_id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    transaction_date date,
    executive character varying(255),
    executive_title character varying(255),
    security_type character varying(100),
    acquisition_or_disposal character varying(1),
    shares numeric(20,4),
    share_price numeric(20,4),
    api_response_status character varying(20) DEFAULT 'pass'::character varying,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE extracted.insider_transactions OWNER TO postgres;

--
-- Name: insider_transactions_transaction_id_seq; Type: SEQUENCE; Schema: extracted; Owner: postgres
--

CREATE SEQUENCE extracted.insider_transactions_transaction_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE extracted.insider_transactions_transaction_id_seq OWNER TO postgres;

--
-- Name: insider_transactions_transaction_id_seq; Type: SEQUENCE OWNED BY; Schema: extracted; Owner: postgres
--

ALTER SEQUENCE extracted.insider_transactions_transaction_id_seq OWNED BY extracted.insider_transactions.transaction_id;


--
-- Name: listing_status; Type: TABLE; Schema: extracted; Owner: postgres
--

CREATE TABLE extracted.listing_status (
    symbol_id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    name character varying(255),
    exchange character varying(50),
    asset_type character varying(50),
    ipo_date date,
    delisting_date date,
    status character varying(20),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE extracted.listing_status OWNER TO postgres;

--
-- Name: listing_status_symbol_id_seq; Type: SEQUENCE; Schema: extracted; Owner: postgres
--

CREATE SEQUENCE extracted.listing_status_symbol_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE extracted.listing_status_symbol_id_seq OWNER TO postgres;

--
-- Name: listing_status_symbol_id_seq; Type: SEQUENCE OWNED BY; Schema: extracted; Owner: postgres
--

ALTER SEQUENCE extracted.listing_status_symbol_id_seq OWNED BY extracted.listing_status.symbol_id;


--
-- Name: overview; Type: TABLE; Schema: extracted; Owner: postgres
--

CREATE TABLE extracted.overview (
    overview_id integer NOT NULL,
    symbol_id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    assettype character varying(50),
    name character varying(255),
    description text,
    cik character varying(20),
    exchange character varying(50),
    currency character varying(10),
    country character varying(100),
    sector character varying(100),
    industry character varying(200),
    address text,
    officialsite character varying(255),
    fiscalyearend character varying(20),
    status character varying(20),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE extracted.overview OWNER TO postgres;

--
-- Name: overview_overview_id_seq; Type: SEQUENCE; Schema: extracted; Owner: postgres
--

CREATE SEQUENCE extracted.overview_overview_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE extracted.overview_overview_id_seq OWNER TO postgres;

--
-- Name: overview_overview_id_seq; Type: SEQUENCE OWNED BY; Schema: extracted; Owner: postgres
--

ALTER SEQUENCE extracted.overview_overview_id_seq OWNED BY extracted.overview.overview_id;


--
-- Name: time_series_daily_adjusted; Type: TABLE; Schema: extracted; Owner: postgres
--

CREATE TABLE extracted.time_series_daily_adjusted (
    symbol_id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    date date NOT NULL,
    open numeric(15,4),
    high numeric(15,4),
    low numeric(15,4),
    close numeric(15,4),
    adjusted_close numeric(15,4),
    volume bigint,
    dividend_amount numeric(15,6),
    split_coefficient numeric(10,6),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE extracted.time_series_daily_adjusted OWNER TO postgres;

--
-- Name: commodities commodity_id; Type: DEFAULT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.commodities ALTER COLUMN commodity_id SET DEFAULT nextval('extracted.commodities_commodity_id_seq'::regclass);


--
-- Name: commodities_daily daily_commodity_id; Type: DEFAULT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.commodities_daily ALTER COLUMN daily_commodity_id SET DEFAULT nextval('extracted.commodities_daily_daily_commodity_id_seq'::regclass);


--
-- Name: earnings_call_transcripts transcript_id; Type: DEFAULT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.earnings_call_transcripts ALTER COLUMN transcript_id SET DEFAULT nextval('extracted.earnings_call_transcripts_transcript_id_seq'::regclass);


--
-- Name: economic_indicators economic_indicator_id; Type: DEFAULT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.economic_indicators ALTER COLUMN economic_indicator_id SET DEFAULT nextval('extracted.economic_indicators_economic_indicator_id_seq'::regclass);


--
-- Name: economic_indicators_daily daily_indicator_id; Type: DEFAULT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.economic_indicators_daily ALTER COLUMN daily_indicator_id SET DEFAULT nextval('extracted.economic_indicators_daily_daily_indicator_id_seq'::regclass);


--
-- Name: insider_transactions transaction_id; Type: DEFAULT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.insider_transactions ALTER COLUMN transaction_id SET DEFAULT nextval('extracted.insider_transactions_transaction_id_seq'::regclass);


--
-- Name: listing_status symbol_id; Type: DEFAULT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.listing_status ALTER COLUMN symbol_id SET DEFAULT nextval('extracted.listing_status_symbol_id_seq'::regclass);


--
-- Name: overview overview_id; Type: DEFAULT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.overview ALTER COLUMN overview_id SET DEFAULT nextval('extracted.overview_overview_id_seq'::regclass);


--
-- Name: commodities commodities_commodity_name_date_interval_key; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.commodities
    ADD CONSTRAINT commodities_commodity_name_date_interval_key UNIQUE (commodity_name, date, "interval");


--
-- Name: commodities_daily commodities_daily_commodity_name_function_name_date_key; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.commodities_daily
    ADD CONSTRAINT commodities_daily_commodity_name_function_name_date_key UNIQUE (commodity_name, function_name, date);


--
-- Name: commodities_daily commodities_daily_pkey; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.commodities_daily
    ADD CONSTRAINT commodities_daily_pkey PRIMARY KEY (daily_commodity_id);


--
-- Name: commodities commodities_pkey; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.commodities
    ADD CONSTRAINT commodities_pkey PRIMARY KEY (commodity_id);


--
-- Name: earnings_call_transcripts earnings_call_transcripts_pkey; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.earnings_call_transcripts
    ADD CONSTRAINT earnings_call_transcripts_pkey PRIMARY KEY (transcript_id);


--
-- Name: earnings_call_transcripts earnings_call_transcripts_symbol_id_quarter_speaker_content_key; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.earnings_call_transcripts
    ADD CONSTRAINT earnings_call_transcripts_symbol_id_quarter_speaker_content_key UNIQUE (symbol_id, quarter, speaker, content_hash);


--
-- Name: economic_indicators_daily economic_indicators_daily_economic_indicator_name_function__key; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.economic_indicators_daily
    ADD CONSTRAINT economic_indicators_daily_economic_indicator_name_function__key UNIQUE (economic_indicator_name, function_name, maturity, date);


--
-- Name: economic_indicators_daily economic_indicators_daily_pkey; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.economic_indicators_daily
    ADD CONSTRAINT economic_indicators_daily_pkey PRIMARY KEY (daily_indicator_id);


--
-- Name: economic_indicators economic_indicators_economic_indicator_name_function_name_m_key; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.economic_indicators
    ADD CONSTRAINT economic_indicators_economic_indicator_name_function_name_m_key UNIQUE (economic_indicator_name, function_name, maturity, date, "interval");


--
-- Name: economic_indicators economic_indicators_pkey; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.economic_indicators
    ADD CONSTRAINT economic_indicators_pkey PRIMARY KEY (economic_indicator_id);


--
-- Name: insider_transactions insider_transactions_pkey; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.insider_transactions
    ADD CONSTRAINT insider_transactions_pkey PRIMARY KEY (transaction_id);


--
-- Name: insider_transactions insider_transactions_symbol_id_transaction_date_executive_s_key; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.insider_transactions
    ADD CONSTRAINT insider_transactions_symbol_id_transaction_date_executive_s_key UNIQUE (symbol_id, transaction_date, executive, security_type, acquisition_or_disposal, shares, share_price);


--
-- Name: listing_status listing_status_pkey; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.listing_status
    ADD CONSTRAINT listing_status_pkey PRIMARY KEY (symbol_id);


--
-- Name: listing_status listing_status_symbol_key; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.listing_status
    ADD CONSTRAINT listing_status_symbol_key UNIQUE (symbol);


--
-- Name: overview overview_pkey; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.overview
    ADD CONSTRAINT overview_pkey PRIMARY KEY (overview_id);


--
-- Name: overview overview_symbol_id_key; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.overview
    ADD CONSTRAINT overview_symbol_id_key UNIQUE (symbol_id);


--
-- Name: time_series_daily_adjusted time_series_daily_adjusted_symbol_id_date_key; Type: CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.time_series_daily_adjusted
    ADD CONSTRAINT time_series_daily_adjusted_symbol_id_date_key UNIQUE (symbol_id, date);


--
-- Name: idx_balance_sheet_fiscal_date; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_balance_sheet_fiscal_date ON extracted.balance_sheet USING btree (fiscal_date_ending);


--
-- Name: idx_balance_sheet_symbol_id; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_balance_sheet_symbol_id ON extracted.balance_sheet USING btree (symbol_id);


--
-- Name: idx_cash_flow_fiscal_date; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_cash_flow_fiscal_date ON extracted.cash_flow USING btree (fiscal_date_ending);


--
-- Name: idx_cash_flow_symbol_id; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_cash_flow_symbol_id ON extracted.cash_flow USING btree (symbol_id);


--
-- Name: idx_commodities_daily_date; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_commodities_daily_date ON extracted.commodities_daily USING btree (date);


--
-- Name: idx_commodities_daily_forward_filled; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_commodities_daily_forward_filled ON extracted.commodities_daily USING btree (is_forward_filled);


--
-- Name: idx_commodities_daily_interval; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_commodities_daily_interval ON extracted.commodities_daily USING btree (original_interval);


--
-- Name: idx_commodities_daily_name; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_commodities_daily_name ON extracted.commodities_daily USING btree (commodity_name);


--
-- Name: idx_commodities_date; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_commodities_date ON extracted.commodities USING btree (date);


--
-- Name: idx_commodities_name; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_commodities_name ON extracted.commodities USING btree (commodity_name);


--
-- Name: idx_earnings_call_transcripts_quarter; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_earnings_call_transcripts_quarter ON extracted.earnings_call_transcripts USING btree (quarter);


--
-- Name: idx_earnings_call_transcripts_sentiment; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_earnings_call_transcripts_sentiment ON extracted.earnings_call_transcripts USING btree (sentiment);


--
-- Name: idx_earnings_call_transcripts_speaker; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_earnings_call_transcripts_speaker ON extracted.earnings_call_transcripts USING btree (speaker);


--
-- Name: idx_earnings_call_transcripts_symbol; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_earnings_call_transcripts_symbol ON extracted.earnings_call_transcripts USING btree (symbol);


--
-- Name: idx_earnings_call_transcripts_symbol_id; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_earnings_call_transcripts_symbol_id ON extracted.earnings_call_transcripts USING btree (symbol_id);


--
-- Name: idx_economic_indicators_daily_date; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_economic_indicators_daily_date ON extracted.economic_indicators_daily USING btree (date);


--
-- Name: idx_economic_indicators_daily_forward_filled; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_economic_indicators_daily_forward_filled ON extracted.economic_indicators_daily USING btree (is_forward_filled);


--
-- Name: idx_economic_indicators_daily_interval; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_economic_indicators_daily_interval ON extracted.economic_indicators_daily USING btree (original_interval);


--
-- Name: idx_economic_indicators_daily_name; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_economic_indicators_daily_name ON extracted.economic_indicators_daily USING btree (economic_indicator_name);


--
-- Name: idx_economic_indicators_date; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_economic_indicators_date ON extracted.economic_indicators USING btree (date);


--
-- Name: idx_economic_indicators_name; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_economic_indicators_name ON extracted.economic_indicators USING btree (economic_indicator_name);


--
-- Name: idx_income_statement_fiscal_date; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_income_statement_fiscal_date ON extracted.income_statement USING btree (fiscal_date_ending);


--
-- Name: idx_income_statement_symbol_id; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_income_statement_symbol_id ON extracted.income_statement USING btree (symbol_id);


--
-- Name: idx_insider_transactions_date; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_insider_transactions_date ON extracted.insider_transactions USING btree (transaction_date);


--
-- Name: idx_insider_transactions_executive; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_insider_transactions_executive ON extracted.insider_transactions USING btree (executive);


--
-- Name: idx_insider_transactions_symbol; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_insider_transactions_symbol ON extracted.insider_transactions USING btree (symbol);


--
-- Name: idx_insider_transactions_symbol_id; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_insider_transactions_symbol_id ON extracted.insider_transactions USING btree (symbol_id);


--
-- Name: idx_insider_transactions_type; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_insider_transactions_type ON extracted.insider_transactions USING btree (acquisition_or_disposal);


--
-- Name: idx_listing_status_symbol; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_listing_status_symbol ON extracted.listing_status USING btree (symbol);


--
-- Name: idx_overview_symbol_id; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_overview_symbol_id ON extracted.overview USING btree (symbol_id);


--
-- Name: idx_time_series_date; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_time_series_date ON extracted.time_series_daily_adjusted USING btree (date);


--
-- Name: idx_time_series_symbol_date; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_time_series_symbol_date ON extracted.time_series_daily_adjusted USING btree (symbol_id, date);


--
-- Name: idx_time_series_symbol_id; Type: INDEX; Schema: extracted; Owner: postgres
--

CREATE INDEX idx_time_series_symbol_id ON extracted.time_series_daily_adjusted USING btree (symbol_id);


--
-- Name: earnings_call_transcripts update_earnings_call_transcripts_updated_at; Type: TRIGGER; Schema: extracted; Owner: postgres
--

CREATE TRIGGER update_earnings_call_transcripts_updated_at BEFORE UPDATE ON extracted.earnings_call_transcripts FOR EACH ROW EXECUTE FUNCTION extracted.update_updated_at_column();


--
-- Name: insider_transactions update_insider_transactions_updated_at; Type: TRIGGER; Schema: extracted; Owner: postgres
--

CREATE TRIGGER update_insider_transactions_updated_at BEFORE UPDATE ON extracted.insider_transactions FOR EACH ROW EXECUTE FUNCTION extracted.update_updated_at_column();


--
-- Name: listing_status update_listing_status_updated_at; Type: TRIGGER; Schema: extracted; Owner: postgres
--

CREATE TRIGGER update_listing_status_updated_at BEFORE UPDATE ON extracted.listing_status FOR EACH ROW EXECUTE FUNCTION extracted.update_updated_at_column();


--
-- Name: overview update_overview_updated_at; Type: TRIGGER; Schema: extracted; Owner: postgres
--

CREATE TRIGGER update_overview_updated_at BEFORE UPDATE ON extracted.overview FOR EACH ROW EXECUTE FUNCTION extracted.update_updated_at_column();


--
-- Name: time_series_daily_adjusted update_time_series_updated_at; Type: TRIGGER; Schema: extracted; Owner: postgres
--

CREATE TRIGGER update_time_series_updated_at BEFORE UPDATE ON extracted.time_series_daily_adjusted FOR EACH ROW EXECUTE FUNCTION extracted.update_updated_at_column();


--
-- Name: balance_sheet balance_sheet_symbol_id_fkey; Type: FK CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.balance_sheet
    ADD CONSTRAINT balance_sheet_symbol_id_fkey FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE;


--
-- Name: cash_flow cash_flow_symbol_id_fkey; Type: FK CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.cash_flow
    ADD CONSTRAINT cash_flow_symbol_id_fkey FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE;


--
-- Name: earnings_call_transcripts earnings_call_transcripts_symbol_id_fkey; Type: FK CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.earnings_call_transcripts
    ADD CONSTRAINT earnings_call_transcripts_symbol_id_fkey FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE;


--
-- Name: income_statement income_statement_symbol_id_fkey; Type: FK CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.income_statement
    ADD CONSTRAINT income_statement_symbol_id_fkey FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE;


--
-- Name: insider_transactions insider_transactions_symbol_id_fkey; Type: FK CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.insider_transactions
    ADD CONSTRAINT insider_transactions_symbol_id_fkey FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE;


--
-- Name: overview overview_symbol_id_fkey; Type: FK CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.overview
    ADD CONSTRAINT overview_symbol_id_fkey FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE;


--
-- Name: time_series_daily_adjusted time_series_daily_adjusted_symbol_id_fkey; Type: FK CONSTRAINT; Schema: extracted; Owner: postgres
--

ALTER TABLE ONLY extracted.time_series_daily_adjusted
    ADD CONSTRAINT time_series_daily_adjusted_symbol_id_fkey FOREIGN KEY (symbol_id) REFERENCES extracted.listing_status(symbol_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

