# Data Dictionary for schema `extracted`

## balance_sheet
| Column | Data Type | Nullable | Default | Description |
|--------|-----------|----------|---------|-------------|
| symbol_id | integer | NO |  |  |
| symbol | character varying | NO |  |  |
| fiscal_date_ending | date | YES |  |  |
| report_type | character varying | NO |  |  |
| reported_currency | character varying | YES |  |  |
| total_assets | bigint | YES |  |  |
| total_current_assets | bigint | YES |  |  |
| cash_and_cash_equivalents_at_carrying_value | bigint | YES |  |  |
| cash_and_short_term_investments | bigint | YES |  |  |
| inventory | bigint | YES |  |  |
| current_net_receivables | bigint | YES |  |  |
| total_non_current_assets | bigint | YES |  |  |
| property_plant_equipment | bigint | YES |  |  |
| accumulated_depreciation_amortization_ppe | bigint | YES |  |  |
| intangible_assets | bigint | YES |  |  |
| intangible_assets_excluding_goodwill | bigint | YES |  |  |
| goodwill | bigint | YES |  |  |
| investments | bigint | YES |  |  |
| long_term_investments | bigint | YES |  |  |
| short_term_investments | bigint | YES |  |  |
| other_current_assets | bigint | YES |  |  |
| other_non_current_assets | bigint | YES |  |  |
| total_liabilities | bigint | YES |  |  |
| total_current_liabilities | bigint | YES |  |  |
| current_accounts_payable | bigint | YES |  |  |
| deferred_revenue | bigint | YES |  |  |
| current_debt | bigint | YES |  |  |
| short_term_debt | bigint | YES |  |  |
| total_non_current_liabilities | bigint | YES |  |  |
| capital_lease_obligations | bigint | YES |  |  |
| long_term_debt | bigint | YES |  |  |
| current_long_term_debt | bigint | YES |  |  |
| long_term_debt_noncurrent | bigint | YES |  |  |
| short_long_term_debt_total | bigint | YES |  |  |
| other_current_liabilities | bigint | YES |  |  |
| other_non_current_liabilities | bigint | YES |  |  |
| total_shareholder_equity | bigint | YES |  |  |
| treasury_stock | bigint | YES |  |  |
| retained_earnings | bigint | YES |  |  |
| common_stock | bigint | YES |  |  |
| common_stock_shares_outstanding | bigint | YES |  |  |
| api_response_status | character varying | YES |  |  |
| created_at | timestamp without time zone | YES | now() |  |
| updated_at | timestamp without time zone | YES | now() |  |

## cash_flow
| Column | Data Type | Nullable | Default | Description |
|--------|-----------|----------|---------|-------------|
| symbol_id | integer | NO |  |  |
| symbol | character varying | NO |  |  |
| fiscal_date_ending | date | YES |  |  |
| report_type | character varying | NO |  |  |
| reported_currency | character varying | YES |  |  |
| operating_cashflow | bigint | YES |  |  |
| payments_for_operating_activities | bigint | YES |  |  |
| proceeds_from_operating_activities | bigint | YES |  |  |
| change_in_operating_liabilities | bigint | YES |  |  |
| change_in_operating_assets | bigint | YES |  |  |
| depreciation_depletion_and_amortization | bigint | YES |  |  |
| capital_expenditures | bigint | YES |  |  |
| change_in_receivables | bigint | YES |  |  |
| change_in_inventory | bigint | YES |  |  |
| profit_loss | bigint | YES |  |  |
| cashflow_from_investment | bigint | YES |  |  |
| cashflow_from_financing | bigint | YES |  |  |
| proceeds_from_repayments_of_short_term_debt | bigint | YES |  |  |
| payments_for_repurchase_of_common_stock | bigint | YES |  |  |
| payments_for_repurchase_of_equity | bigint | YES |  |  |
| payments_for_repurchase_of_preferred_stock | bigint | YES |  |  |
| dividend_payout | bigint | YES |  |  |
| dividend_payout_common_stock | bigint | YES |  |  |
| dividend_payout_preferred_stock | bigint | YES |  |  |
| proceeds_from_issuance_of_common_stock | bigint | YES |  |  |
| proceeds_from_issuance_of_long_term_debt_and_capital_securities | bigint | YES |  |  |
| proceeds_from_issuance_of_preferred_stock | bigint | YES |  |  |
| proceeds_from_repurchase_of_equity | bigint | YES |  |  |
| proceeds_from_sale_of_treasury_stock | bigint | YES |  |  |
| change_in_cash_and_cash_equivalents | bigint | YES |  |  |
| change_in_exchange_rate | bigint | YES |  |  |
| net_income | bigint | YES |  |  |
| api_response_status | character varying | YES |  |  |
| created_at | timestamp without time zone | YES | now() |  |
| updated_at | timestamp without time zone | YES | now() |  |

## commodities
| Column | Data Type | Nullable | Default | Description |
|--------|-----------|----------|---------|-------------|
| commodity_id | integer | NO | nextval('commodities_commodity_id_seq'::regclass) |  |
| commodity_name | character varying | NO |  |  |
| function_name | character varying | NO |  |  |
| date | date | YES |  |  |
| interval | character varying | NO |  |  |
| unit | character varying | YES |  |  |
| value | numeric | YES |  |  |
| name | character varying | YES |  |  |
| api_response_status | character varying | YES |  |  |
| created_at | timestamp without time zone | YES | now() |  |
| updated_at | timestamp without time zone | YES | now() |  |

## commodities_daily
| Column | Data Type | Nullable | Default | Description |
|--------|-----------|----------|---------|-------------|
| daily_commodity_id | integer | NO | nextval('commodities_daily_daily_commodity_id_seq'::regclass) |  |
| commodity_name | character varying | NO |  |  |
| function_name | character varying | NO |  |  |
| date | date | NO |  |  |
| original_interval | character varying | NO |  |  |
| updated_interval | character varying | NO | 'daily'::character varying |  |
| unit | character varying | YES |  |  |
| value | numeric | YES |  |  |
| name | character varying | YES |  |  |
| is_forward_filled | boolean | YES | false |  |
| original_date | date | YES |  |  |
| created_at | timestamp without time zone | YES | now() |  |
| updated_at | timestamp without time zone | YES | now() |  |

## earnings_call_transcripts
| Column | Data Type | Nullable | Default | Description |
|--------|-----------|----------|---------|-------------|
| transcript_id | integer | NO | nextval('earnings_call_transcripts_transcript_id_seq'::regclass) |  |
| symbol_id | integer | NO |  |  |
| symbol | character varying | NO |  |  |
| quarter | character varying | NO |  |  |
| speaker | character varying | NO |  |  |
| title | character varying | YES |  |  |
| content | text | NO |  |  |
| content_hash | character varying | NO |  |  |
| sentiment | numeric | YES |  |  |
| api_response_status | character varying | YES | 'pass'::character varying |  |
| created_at | timestamp without time zone | YES | now() |  |
| updated_at | timestamp without time zone | YES | now() |  |

## economic_indicators
| Column | Data Type | Nullable | Default | Description |
|--------|-----------|----------|---------|-------------|
| economic_indicator_id | integer | NO | nextval('economic_indicators_economic_indicator_id_seq'::regclass) |  |
| economic_indicator_name | character varying | NO |  |  |
| function_name | character varying | NO |  |  |
| maturity | character varying | YES |  |  |
| date | date | YES |  |  |
| interval | character varying | NO |  |  |
| unit | character varying | YES |  |  |
| value | numeric | YES |  |  |
| name | character varying | YES |  |  |
| api_response_status | character varying | YES |  |  |
| created_at | timestamp without time zone | YES | now() |  |
| updated_at | timestamp without time zone | YES | now() |  |

## economic_indicators_daily
| Column | Data Type | Nullable | Default | Description |
|--------|-----------|----------|---------|-------------|
| daily_indicator_id | integer | NO | nextval('economic_indicators_daily_daily_indicator_id_seq'::regclass) |  |
| economic_indicator_name | character varying | NO |  |  |
| function_name | character varying | NO |  |  |
| maturity | character varying | YES |  |  |
| date | date | NO |  |  |
| original_interval | character varying | NO |  |  |
| updated_interval | character varying | NO | 'daily'::character varying |  |
| unit | character varying | YES |  |  |
| value | numeric | YES |  |  |
| name | character varying | YES |  |  |
| is_forward_filled | boolean | YES | false |  |
| original_date | date | YES |  |  |
| created_at | timestamp without time zone | YES | now() |  |
| updated_at | timestamp without time zone | YES | now() |  |

## income_statement
| Column | Data Type | Nullable | Default | Description |
|--------|-----------|----------|---------|-------------|
| symbol_id | integer | NO |  |  |
| symbol | character varying | NO |  |  |
| fiscal_date_ending | date | YES |  |  |
| report_type | character varying | NO |  |  |
| reported_currency | character varying | YES |  |  |
| gross_profit | bigint | YES |  |  |
| total_revenue | bigint | YES |  |  |
| cost_of_revenue | bigint | YES |  |  |
| cost_of_goods_and_services_sold | bigint | YES |  |  |
| operating_income | bigint | YES |  |  |
| selling_general_and_administrative | bigint | YES |  |  |
| research_and_development | bigint | YES |  |  |
| operating_expenses | bigint | YES |  |  |
| investment_income_net | bigint | YES |  |  |
| net_interest_income | bigint | YES |  |  |
| interest_income | bigint | YES |  |  |
| interest_expense | bigint | YES |  |  |
| non_interest_income | bigint | YES |  |  |
| other_non_operating_income | bigint | YES |  |  |
| depreciation | bigint | YES |  |  |
| depreciation_and_amortization | bigint | YES |  |  |
| income_before_tax | bigint | YES |  |  |
| income_tax_expense | bigint | YES |  |  |
| interest_and_debt_expense | bigint | YES |  |  |
| net_income_from_continuing_operations | bigint | YES |  |  |
| comprehensive_income_net_of_tax | bigint | YES |  |  |
| ebit | bigint | YES |  |  |
| ebitda | bigint | YES |  |  |
| net_income | bigint | YES |  |  |
| api_response_status | character varying | YES |  |  |
| created_at | timestamp without time zone | YES | now() |  |
| updated_at | timestamp without time zone | YES | now() |  |

## insider_transactions
| Column | Data Type | Nullable | Default | Description |
|--------|-----------|----------|---------|-------------|
| transaction_id | integer | NO | nextval('insider_transactions_transaction_id_seq'::regclass) |  |
| symbol_id | integer | NO |  |  |
| symbol | character varying | NO |  |  |
| transaction_date | date | YES |  |  |
| executive | character varying | YES |  |  |
| executive_title | character varying | YES |  |  |
| security_type | character varying | YES |  |  |
| acquisition_or_disposal | character varying | YES |  |  |
| shares | numeric | YES |  |  |
| share_price | numeric | YES |  |  |
| api_response_status | character varying | YES | 'pass'::character varying |  |
| created_at | timestamp without time zone | YES | now() |  |
| updated_at | timestamp without time zone | YES | now() |  |

## listing_status
| Column | Data Type | Nullable | Default | Description |
|--------|-----------|----------|---------|-------------|
| symbol_id | integer | NO | nextval('listing_status_symbol_id_seq'::regclass) |  |
| symbol | character varying | NO |  |  |
| name | character varying | YES |  |  |
| exchange | character varying | YES |  |  |
| asset_type | character varying | YES |  |  |
| ipo_date | date | YES |  |  |
| delisting_date | date | YES |  |  |
| status | character varying | YES |  |  |
| created_at | timestamp without time zone | YES | now() |  |
| updated_at | timestamp without time zone | YES | now() |  |

## overview
| Column | Data Type | Nullable | Default | Description |
|--------|-----------|----------|---------|-------------|
| overview_id | integer | NO | nextval('overview_overview_id_seq'::regclass) |  |
| symbol_id | integer | NO |  |  |
| symbol | character varying | NO |  |  |
| assettype | character varying | YES |  |  |
| name | character varying | YES |  |  |
| description | text | YES |  |  |
| cik | character varying | YES |  |  |
| exchange | character varying | YES |  |  |
| currency | character varying | YES |  |  |
| country | character varying | YES |  |  |
| sector | character varying | YES |  |  |
| industry | character varying | YES |  |  |
| address | text | YES |  |  |
| officialsite | character varying | YES |  |  |
| fiscalyearend | character varying | YES |  |  |
| status | character varying | YES |  |  |
| created_at | timestamp without time zone | YES | now() |  |
| updated_at | timestamp without time zone | YES | now() |  |

## time_series_daily_adjusted
| Column | Data Type | Nullable | Default | Description |
|--------|-----------|----------|---------|-------------|
| symbol_id | integer | NO |  |  |
| symbol | character varying | NO |  |  |
| date | date | NO |  |  |
| open | numeric | YES |  |  |
| high | numeric | YES |  |  |
| low | numeric | YES |  |  |
| close | numeric | YES |  |  |
| adjusted_close | numeric | YES |  |  |
| volume | bigint | YES |  |  |
| dividend_amount | numeric | YES |  |  |
| split_coefficient | numeric | YES |  |  |
| created_at | timestamp without time zone | YES | now() |  |
| updated_at | timestamp without time zone | YES | now() |  |
