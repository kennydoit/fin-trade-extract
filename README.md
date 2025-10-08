# fin-trade-extract

## Overview
A comprehensive financial data ETL system using GitHub Actions to extract market data and fundamentals from Alpha Vantage, store it in AWS S3, and load it into Snowflake. The system provides intelligent incremental processing across NASDAQ, NYSE, and ETF markets with full automation and zero manual intervention.

## Architecture
- **GitHub Actions**: Fully automated ETL orchestration with scheduled execution
- **Python Scripts**: Handle API calls, S3 uploads, and Snowflake operations  
- **Incremental ETL Intelligence**: Tracks processing status and only processes symbols that need updates
- **Universe Management**: Manages symbol universes for different exchanges and asset types
- **Rate Limited Processing**: Optimized for Alpha Vantage Premium (75 calls/minute)

## ETL Workflows

### 1. Listing Status ETL (`listing_status_etl.yml`)
- **Purpose**: Fetches complete universe of tradable symbols from Alpha Vantage
- **Schedule**: Daily at 6:00 AM UTC
- **Output**: Updates `LISTING_STATUS` table in Snowflake with current market symbols
- **Script**: `fetch_listing_status_to_s3.py`

### 2. NASDAQ Time Series ETL (`bulk_time_series_nasdaq.yml`)  
- **Purpose**: Bulk processing of NASDAQ equity time series data
- **Schedule**: Daily at 7:00 AM UTC
- **Capacity**: Processes 12,000+ symbols with intelligent batching
- **Features**: Incremental processing, universe filtering, comprehensive error handling
- **Script**: `fetch_time_series_bulk.py` with `EXCHANGE_FILTER='NASDAQ'`

### 3. NYSE Time Series ETL (`bulk_time_series_nyse.yml`)
- **Purpose**: Bulk processing of NYSE equity time series data  
- **Schedule**: Daily at 8:00 AM UTC (offset to optimize API usage)
- **Capacity**: Processes NYSE universe with same intelligence as NASDAQ
- **Features**: Exchange-specific filtering, NYSE universe management
- **Script**: `fetch_time_series_bulk.py` with `EXCHANGE_FILTER='NYSE'`

### 4. ETF Time Series ETL (`bulk_time_series_etfs.yml`)
- **Purpose**: Cross-exchange ETF time series processing
- **Schedule**: Daily at 9:00 AM UTC
- **Capacity**: Processes ETFs from all exchanges (smaller batch size optimized for ETFs)
- **Features**: Asset type filtering, cross-exchange coverage, ETF-specific universes  
- **Script**: `fetch_time_series_bulk.py` with `ASSET_TYPE_FILTER='ETF'`

### 5. Balance Sheet Fundamentals ETL (`bulk_balance_sheet.yml`) 🆕
- **Purpose**: Comprehensive balance sheet fundamentals processing
- **Schedule**: Monthly on the 15th at 10:00 AM UTC (fundamentals update less frequently)
- **Capacity**: Processes ~4,000 fundamentals-eligible symbols across all exchanges
- **Features**: Annual and quarterly reports, comprehensive balance sheet metrics, fundamentals screening
- **Script**: `fetch_balance_sheet_bulk.py` with intelligent fundamentals filtering

### 6. Overview Full Universe (`overview_full_universe.yml`)
- **Purpose**: Comprehensive market overview data extraction
- **Schedule**: Weekly processing for broad market analysis
- **Features**: Full universe coverage, overview metrics calculation

## Key Features

### Incremental ETL Intelligence
- **Smart Symbol Selection**: Only processes symbols that haven't been updated recently
- **Processing Modes**: 
  - `incremental` (default): Process only symbols needing updates
  - `full_refresh`: Force reprocess all symbols  
  - `universe`: Process specific symbol universes
- **Status Tracking**: Maintains processing history and prevents duplicate work
- **Dependency Management**: Ensures prerequisites are met before processing

### Universe Management
- **NASDAQ Universes**: `nasdaq_composite`, `nasdaq_high_quality`, `nasdaq_liquid`
- **NYSE Universes**: `nyse_composite`, `nyse_high_quality`, `nyse_dividend_aristocrats`
- **ETF Universes**: `etf_all_exchanges`, `etf_high_volume`, `etf_simple`
- **Fundamentals Universes**: `fundamentals_eligible` (>$100M market cap, >$5 price)
- **Dynamic Updates**: Universes auto-update based on listing status changes

### Rate Limiting & Optimization
- **API Efficiency**: 75 calls/minute optimization across all workflows
- **Staggered Scheduling**: Workflows offset to prevent API conflicts  
- **Batch Processing**: Configurable batch sizes per workflow type
- **Progress Tracking**: Real-time processing status and completion estimates

### Fundamentals Processing 🆕
- **Balance Sheet Coverage**: 40+ comprehensive balance sheet metrics
- **Report Types**: Both annual and quarterly reports
- **Asset Categories**: Assets, liabilities, and equity with detailed breakdowns
- **Screening Intelligence**: Automatic filtering for fundamentals-eligible symbols
- **Monthly Updates**: Aligned with typical fundamentals reporting cycles

## File Structure
```
.github/workflows/           # GitHub Actions workflows
├── listing_status_etl.yml          # Listing status extraction
├── bulk_time_series_nasdaq.yml     # NASDAQ bulk processing  
├── bulk_time_series_nyse.yml       # NYSE bulk processing
├── bulk_time_series_etfs.yml       # ETF bulk processing
├── bulk_balance_sheet.yml          # Balance sheet fundamentals 🆕
└── overview_full_universe.yml      # Market overview processing

scripts/github_actions/      # ETL Python scripts
├── fetch_listing_status_to_s3.py   # Listing status fetcher
├── fetch_time_series_bulk.py       # Bulk time series processor
├── fetch_balance_sheet_bulk.py     # Balance sheet processor 🆕
├── incremental_etl.py              # Incremental ETL management
├── symbol_screener.py              # Symbol screening logic
├── universe_management.py          # Universe management
└── snowflake_run_sql_file.py       # Snowflake operations

snowflake/runbooks/          # Snowflake SQL operations  
├── load_listing_status_from_s3.sql # Listing status load
├── load_time_series_from_s3.sql    # Time series data load
└── load_balance_sheet_from_s3.sql  # Balance sheet load 🆕

utils/                       # Shared utilities
├── incremental_etl.py              # ETL status tracking
├── symbol_screener.py              # Advanced symbol filtering
└── universe_management.py          # Universe definitions

s3://fin-trade-craft-landing/
├── listing_status/                  # Listing status data
├── time_series_daily_adjusted/      # Time series data by symbol
└── balance_sheet/                   # Balance sheet fundamentals 🆕
```

## Setup

### 1. Configure GitHub Secrets
Add these secrets to your repository settings:
```
ALPHAVANTAGE_API_KEY         # Alpha Vantage Premium API key
AWS_REGION                   # AWS region (e.g., us-east-1)  
AWS_ROLE_TO_ASSUME          # AWS IAM role for S3 access
SNOWFLAKE_ACCOUNT           # Snowflake account identifier
SNOWFLAKE_USER              # Snowflake username
SNOWFLAKE_PASSWORD          # Snowflake password
SNOWFLAKE_WAREHOUSE         # Snowflake warehouse name
SNOWFLAKE_DATABASE          # Snowflake database name  
SNOWFLAKE_SCHEMA            # Snowflake schema name
```

### 2. Workflow Configuration
Each workflow accepts these parameters:
- **processing_mode**: `incremental` (default), `full_refresh`, `universe`
- **universe_name**: Target specific universe (e.g., `nasdaq_high_quality`)
- **max_symbols**: Override maximum symbols to process
- **batch_size**: Override batch size for processing

### 3. Manual Execution
Trigger workflows manually in GitHub Actions tab with custom parameters, or let them run on their automated schedules.

## Monitoring & Operations

### Workflow Status
- Monitor execution in GitHub Actions tab
- Each workflow provides detailed logging and progress tracking
- Failed runs include comprehensive error reporting and retry guidance

### Processing Intelligence  
- View processing statistics in workflow logs
- Track API usage efficiency and batch processing performance
- Monitor incremental ETL effectiveness (symbols skipped vs processed)

### Data Quality
- Automated data validation during Snowflake loads
- Schema enforcement and type checking
- Duplicate detection and handling

## Data Output

### S3 Structure
```
s3://fin-trade-craft-landing/
├── listing_status/YYYYMMDD_HHMMSS_listing_status.csv
├── time_series_daily_adjusted/
│   ├── SYMBOL_YYYYMMDD_HHMMSS_daily_adjusted.csv
│   └── [12,000+ symbol files...]
└── balance_sheet/
    ├── BALANCE_SHEET_SYMBOL_YYYYMMDD_HHMMSS.csv
    └── [~4,000 fundamentals files...]
```

### Snowflake Tables
- **LISTING_STATUS**: Complete universe of tradable symbols with metadata
- **TIME_SERIES_DAILY_ADJUSTED**: Historical daily adjusted price data
- **BALANCE_SHEET**: 🆕 Comprehensive balance sheet fundamentals (annual/quarterly)
- **ETL_WATERMARKS**: Enhanced processing status tracking with fiscal date boundaries and delisting intelligence
- **UNIVERSE_***: Managed symbol universes for different asset classes

## Balance Sheet Fundamentals Features 🆕

### Comprehensive Metrics
- **Assets**: Total assets, current/non-current breakdown, cash, inventory, PPE, intangibles, goodwill
- **Liabilities**: Total liabilities, current/non-current, debt analysis, payables, deferred revenue
- **Equity**: Shareholder equity, retained earnings, common stock, treasury stock, shares outstanding

### Report Coverage
- **Annual Reports**: Complete yearly financial position
- **Quarterly Reports**: Quarterly updates for trend analysis
- **Historical Data**: Multi-year fundamentals history for analysis
- **Currency Support**: Reported currency tracking (USD default)

### Screening Intelligence
- **Market Cap Filtering**: >$100M market cap requirement
- **Price Filtering**: >$5 share price (excludes penny stocks)
- **Exchange Coverage**: NASDAQ, NYSE, AMEX fundamentals-eligible symbols
- **Asset Type Focus**: Primarily stocks (fundamentals more relevant than ETFs)

### Processing Optimization
- **Monthly Schedule**: Aligned with fundamentals reporting cycles
- **Incremental Updates**: 30-day staleness threshold (fundamentals change less frequently)
- **Smaller Batches**: 25 symbols per batch (fundamentals API calls take longer)
- **Rate Limiting**: Optimized for Alpha Vantage Premium limits

## Contributing
- All ETL logic is GitHub Actions-based with no AWS Lambda dependencies
- Add new workflows by copying existing patterns and adjusting parameters
- Extend incremental ETL system for new data types through `incremental_etl.py`
- Test changes using manual workflow triggers before committing schedule changes

---

**Questions?** Open an issue in this repository for help with setup, configuration, or extending the system.