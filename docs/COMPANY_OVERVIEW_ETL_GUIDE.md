# Company Overview ETL Guide - Watermark-Based Processing

## Overview

This guide covers the **production-ready** Company Overview ETL workflow following the same watermark-based pattern as Balance Sheet, Cash Flow, and Income Statement ETLs.

**Key Characteristics:**
- **API Function:** `OVERVIEW` (Alpha Vantage)
- **Data Type:** Semi-static company profile and financial metrics
- **Staleness Threshold:** **365 days** (annual refresh sufficient)
- **Refresh Logic:** Only re-fetch symbols >365 days old or never fetched
- **Scheduled:** Quarterly (1st of Jan, Apr, Jul, Oct)

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Setup Instructions](#setup-instructions)
3. [Data Structure](#data-structure)
4. [Watermark Processing Logic](#watermark-processing-logic)
5. [Running the ETL](#running-the-etl)
6. [Troubleshooting](#troubleshooting)
7. [Success Criteria](#success-criteria)

---

## Prerequisites

### 1. Watermarks Must Be Initialized

```sql
-- Check if watermarks exist
SELECT COUNT(*) 
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
WHERE TABLE_NAME = 'COMPANY_OVERVIEW' AND API_ELIGIBLE = 'YES';
```

**If count = 0:**
```sql
-- Run the initialization script
-- File: snowflake/setup/templates/watermark_company_overview.sql
```

Expected output: ~10,000 total records, ~8,000 with `API_ELIGIBLE='YES'`

### 2. GitHub Secrets Configured

Ensure these secrets are set in your GitHub repository:

- `ALPHAVANTAGE_API_KEY` - Premium tier (75 calls/minute)
- `AWS_ROLE_TO_ASSUME` - IAM role for OIDC authentication
- `AWS_REGION` - AWS region (e.g., `us-east-1`)
- `SNOWFLAKE_ACCOUNT` - Snowflake account identifier
- `SNOWFLAKE_USER` - Service account username
- `SNOWFLAKE_PASSWORD` - Service account password
- `SNOWFLAKE_WAREHOUSE` - Warehouse name (e.g., `FIN_TRADE_WH`)
- `SNOWFLAKE_DATABASE` - Database name (e.g., `FIN_TRADE_EXTRACT`)
- `SNOWFLAKE_SCHEMA` - Schema name (e.g., `RAW`)

### 3. S3 Configuration

- **Bucket:** `fin-trade-craft-landing`
- **Prefix:** `company_overview/`
- **Storage Integration:** `FIN_TRADE_S3_INTEGRATION` (must exist in Snowflake)

---

## Setup Instructions

### Step 1: Initialize Watermarks

Run in Snowflake:

```sql
-- Initialize watermarks for all active stocks
-- File: snowflake/setup/templates/watermark_company_overview.sql
```

This creates records in `ETL_WATERMARKS` with:
- `TABLE_NAME = 'COMPANY_OVERVIEW'`
- `API_ELIGIBLE = 'YES'` for active common stocks
- `FIRST_FISCAL_DATE = NULL` (set during first run)
- `LAST_FISCAL_DATE = NULL` (set during first run)

### Step 2: Test with Small Batch

GitHub Actions → **Company Overview ETL - Watermark Based** → Run workflow

**Test Configuration:**
- `exchange_filter`: Leave blank (or select NYSE/NASDAQ/AMEX)
- `max_symbols`: **5** (test with 5 symbols)
- `skip_recent_hours`: Leave blank
- `batch_size`: 50

**Expected Result:**
- Duration: ~2-3 minutes
- Successful: 5 symbols
- Data loaded: 5 rows in `COMPANY_OVERVIEW` table
- Watermarks updated: 5 records with `LAST_SUCCESSFUL_RUN` set

### Step 3: Verify Test Results

```sql
-- Check loaded data
SELECT COUNT(*), COUNT(DISTINCT SECTOR), COUNT(DISTINCT INDUSTRY)
FROM FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW;

-- Check watermarks
SELECT * 
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS 
WHERE TABLE_NAME = 'COMPANY_OVERVIEW' 
  AND LAST_SUCCESSFUL_RUN IS NOT NULL
ORDER BY LAST_SUCCESSFUL_RUN DESC
LIMIT 10;

-- Verify LAST_FISCAL_DATE is set to LatestQuarter
SELECT 
    SYMBOL,
    LAST_FISCAL_DATE,
    DATEDIFF(day, LAST_FISCAL_DATE, CURRENT_DATE()) as days_old,
    CASE 
        WHEN LAST_FISCAL_DATE >= DATEADD(day, -365, CURRENT_DATE()) THEN 'FRESH - Will be skipped'
        ELSE 'STALE - Will be re-fetched'
    END as staleness_status
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'COMPANY_OVERVIEW'
  AND LAST_SUCCESSFUL_RUN IS NOT NULL
ORDER BY LAST_FISCAL_DATE DESC
LIMIT 20;
```

### Step 4: Run Full Processing

After successful test, run without limits:

- `exchange_filter`: Leave blank
- `max_symbols`: Leave blank (process all eligible symbols)
- `skip_recent_hours`: Leave blank
- `batch_size`: 50

**Expected Result:**
- Duration: ~3-5 hours for ~8,000 symbols
- Processing rate: ~25-30 symbols/minute
- Total API calls: ~8,000
- Cost: ~$2-3 Snowflake credits

### Step 5: Enable Scheduled Runs

The workflow is already scheduled:

```yaml
schedule:
  # Quarterly on the 1st of Jan, Apr, Jul, Oct at 6:00 AM UTC
  - cron: '0 6 1 1,4,7,10 *'
```

**Important:** After initial run, staleness logic kicks in:
- Only symbols with `LAST_FISCAL_DATE < DATEADD(day, -365, CURRENT_DATE())` will be processed
- Expected subsequent run cost: **~$0.10-0.50** (most symbols fresh within 365 days)
- New symbols (IPOs) will be automatically included

---

## Data Structure

### Company Overview Table Schema

**Table:** `FIN_TRADE_EXTRACT.RAW.COMPANY_OVERVIEW`

**Company Profile (14 columns):**
```sql
SYMBOL_ID                   -- Hash-based ID (ABS(HASH(SYMBOL)) % 1000000000)
SYMBOL                      -- Stock ticker (PK)
ASSET_TYPE                  -- Stock, ETF, etc.
NAME                        -- Company name
DESCRIPTION                 -- Business description (TEXT)
CIK                         -- SEC Central Index Key
EXCHANGE                    -- NYSE, NASDAQ, etc.
CURRENCY                    -- USD, CAD, etc.
COUNTRY                     -- United States, Canada, etc.
SECTOR                      -- Technology, Healthcare, etc.
INDUSTRY                    -- Software, Biotechnology, etc.
ADDRESS                     -- Corporate address (TEXT)
FISCAL_YEAR_END             -- Fiscal year end month
LATEST_QUARTER              -- Most recent fiscal quarter (DATE)
```

**Financial Metrics (32 columns):**
```sql
MARKET_CAPITALIZATION       -- Market cap in USD
EBITDA                      -- Earnings before interest, tax, depreciation, amortization
PE_RATIO                    -- Price-to-earnings ratio
PEG_RATIO                   -- Price/earnings to growth ratio
BOOK_VALUE                  -- Book value per share
DIVIDEND_PER_SHARE          -- Annual dividend per share
DIVIDEND_YIELD              -- Dividend yield percentage
EPS                         -- Earnings per share
REVENUE_PER_SHARE_TTM       -- Revenue per share (trailing twelve months)
PROFIT_MARGIN               -- Profit margin percentage
OPERATING_MARGIN_TTM        -- Operating margin TTM
RETURN_ON_ASSETS_TTM        -- ROA percentage
RETURN_ON_EQUITY_TTM        -- ROE percentage
REVENUE_TTM                 -- Total revenue TTM
GROSS_PROFIT_TTM            -- Gross profit TTM
DILUTED_EPS_TTM             -- Diluted EPS TTM
QUARTERLY_EARNINGS_GROWTH_YOY   -- Quarterly earnings growth YoY
QUARTERLY_REVENUE_GROWTH_YOY    -- Quarterly revenue growth YoY
ANALYST_TARGET_PRICE        -- Analyst consensus target price
TRAILING_PE                 -- Trailing P/E ratio
FORWARD_PE                  -- Forward P/E ratio
PRICE_TO_SALES_RATIO_TTM    -- Price-to-sales ratio
PRICE_TO_BOOK_RATIO         -- Price-to-book ratio
EV_TO_REVENUE               -- Enterprise value to revenue
EV_TO_EBITDA                -- Enterprise value to EBITDA
BETA                        -- Stock beta (volatility)
WEEK_52_HIGH                -- 52-week high price
WEEK_52_LOW                 -- 52-week low price
DAY_50_MOVING_AVERAGE       -- 50-day moving average
DAY_200_MOVING_AVERAGE      -- 200-day moving average
SHARES_OUTSTANDING          -- Total shares outstanding
DIVIDEND_DATE               -- Next dividend date
EX_DIVIDEND_DATE            -- Ex-dividend date
```

**Metadata:**
```sql
LOAD_DATE                   -- Date data was loaded (auto-generated)
```

---

## Watermark Processing Logic

### Staleness Threshold: 365 Days

**Rationale:**
- Company overview data is **semi-static** (sector, industry, description change infrequently)
- Financial metrics update quarterly, but full refresh every quarter wastes API quota
- **Annual refresh** balances data freshness with API cost efficiency

### Query Logic

```sql
-- Python script queries watermarks with this WHERE clause:
WHERE TABLE_NAME = 'COMPANY_OVERVIEW'
  AND API_ELIGIBLE = 'YES'
  AND (LAST_FISCAL_DATE IS NULL 
       OR LAST_FISCAL_DATE < DATEADD(day, -365, CURRENT_DATE()))
```

**Behavior:**
- **First Run:** All symbols with `LAST_FISCAL_DATE IS NULL` are processed
- **Subsequent Runs:** Only symbols >365 days old are processed
- **New Symbols:** Automatically included (NULL fiscal date)
- **Delisted Symbols:** Marked as `API_ELIGIBLE='DEL'` after successful extraction

### Watermark Updates

After successful extraction, watermarks are updated:

```sql
MERGE INTO ETL_WATERMARKS
SET 
    FIRST_FISCAL_DATE = COALESCE(FIRST_FISCAL_DATE, LatestQuarter),
    LAST_FISCAL_DATE = LatestQuarter,
    LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP(),
    CONSECUTIVE_FAILURES = 0,
    API_ELIGIBLE = CASE 
        WHEN DELISTING_DATE <= CURRENT_DATE() THEN 'DEL'
        ELSE API_ELIGIBLE 
    END
```

**Note:** For company overview, `FIRST_FISCAL_DATE` = `LAST_FISCAL_DATE` (single snapshot, not time series)

---

## Running the ETL

### Manual Execution

**GitHub Actions:**
1. Go to: `.github/workflows/company_overview_watermark_etl.yml`
2. Click "Run workflow"
3. Configure inputs:
   - `exchange_filter`: (optional) NYSE, NASDAQ, AMEX, or blank for all
   - `max_symbols`: (optional) For testing, limit to small number
   - `skip_recent_hours`: (optional) Skip recently processed symbols
   - `batch_size`: Default 50

### Scheduled Execution

**Quarterly runs:**
- Schedule: `0 6 1 1,4,7,10 *` (1st of Jan, Apr, Jul, Oct at 6:00 AM UTC)
- Expected symbols processed: 100-500 (only stale symbols >365 days)
- Expected duration: 10-30 minutes
- Expected cost: $0.10-0.50

### Cost Optimization Features

1. **S3 Cleanup:** Deletes old files before extraction (prevents duplicate COPY)
2. **Connection Management:** Closes Snowflake after watermark query, reopens only for updates
3. **Bulk Updates:** Single MERGE statement for all watermarks (100x faster than individual UPDATEs)
4. **Staleness Logic:** Skips 99% of symbols after initial run (365-day threshold)

---

## Troubleshooting

### Issue: No symbols to process

**Check watermarks:**
```sql
SELECT 
    COUNT(*) as total,
    COUNT(CASE WHEN LAST_FISCAL_DATE IS NULL THEN 1 END) as never_pulled,
    COUNT(CASE WHEN LAST_FISCAL_DATE < DATEADD(day, -365, CURRENT_DATE()) THEN 1 END) as stale
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'COMPANY_OVERVIEW' AND API_ELIGIBLE = 'YES';
```

**Solution:** If all counts are 0, watermarks haven't been initialized. Run `watermark_company_overview.sql`

### Issue: Watermarks not updating

**Check for errors:**
```sql
SELECT SYMBOL, CONSECUTIVE_FAILURES, UPDATED_AT
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'COMPANY_OVERVIEW' AND CONSECUTIVE_FAILURES > 0
ORDER BY CONSECUTIVE_FAILURES DESC;
```

**Solution:** Check GitHub Actions logs for API errors (rate limiting, authentication, etc.)

### Issue: Data not loading to Snowflake

**Check staging table:**
```sql
SELECT COUNT(*) FROM COMPANY_OVERVIEW_STAGING;
```

**If 0:** S3 files not copied
- Verify S3 bucket/prefix correct
- Check STORAGE_INTEGRATION permissions

**If >0 but main table empty:** MERGE failed
- Check SQL logs for errors
- Verify SYMBOL column matches between staging and main table

### Issue: API rate limit errors

**Adjust rate limiting:**
```yaml
# In workflow file, add:
env:
  API_DELAY_SECONDS: '1.0'  # Slower rate (60 calls/minute)
```

---

## Success Criteria

### After First Run (Full Universe)

```sql
-- Expected: ~8,000 records
SELECT COUNT(*) FROM COMPANY_OVERVIEW;

-- Expected: ~10-15 unique sectors
SELECT COUNT(DISTINCT SECTOR) FROM COMPANY_OVERVIEW;

-- Expected: ~100+ unique industries
SELECT COUNT(DISTINCT INDUSTRY) FROM COMPANY_OVERVIEW;

-- Expected: ~8,000 watermarks with LAST_SUCCESSFUL_RUN set
SELECT COUNT(*) 
FROM ETL_WATERMARKS 
WHERE TABLE_NAME = 'COMPANY_OVERVIEW' 
  AND LAST_SUCCESSFUL_RUN IS NOT NULL;

-- Expected: 0 symbols >365 days old (all fresh after initial run)
SELECT COUNT(*)
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'COMPANY_OVERVIEW'
  AND API_ELIGIBLE = 'YES'
  AND LAST_FISCAL_DATE < DATEADD(day, -365, CURRENT_DATE());
```

### After Second Run (Quarterly Scheduled)

```sql
-- Expected: <500 symbols processed (only new or >365 days old)
SELECT COUNT(*)
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'COMPANY_OVERVIEW'
  AND LAST_SUCCESSFUL_RUN > DATEADD(day, -1, CURRENT_TIMESTAMP());

-- Expected: Most symbols still fresh
SELECT 
    CASE 
        WHEN LAST_FISCAL_DATE >= DATEADD(day, -365, CURRENT_DATE()) THEN 'FRESH'
        ELSE 'STALE'
    END as status,
    COUNT(*) as count
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'COMPANY_OVERVIEW' AND API_ELIGIBLE = 'YES'
GROUP BY status;
```

---

## Pattern Consistency

This ETL follows the **exact same pattern** as Balance Sheet, Cash Flow, and Income Statement:

| Feature | Balance Sheet | Company Overview |
|---------|--------------|------------------|
| **Python Script** | `fetch_balance_sheet_watermark.py` | `fetch_company_overview_watermark.py` |
| **SQL Loader** | `load_balance_sheet_from_s3.sql` | `load_company_overview_from_s3_watermark.sql` |
| **Workflow** | `balance_sheet_watermark_etl.yml` | `company_overview_watermark_etl.yml` |
| **Watermark Template** | N/A (uses balance sheet pattern) | `watermark_company_overview.sql` |
| **Stage Naming** | `BALANCE_SHEET_STAGE` (external) | `COMPANY_OVERVIEW_STAGE` (external) |
| **Staging Naming** | `BALANCE_SHEET_STAGING` (transient) | `COMPANY_OVERVIEW_STAGING` (transient) |
| **Staleness Logic** | 135 days (quarterly) | **365 days (annual)** |
| **Workflow Inputs** | exchange_filter, max_symbols, skip_recent_hours, batch_size | **Same** |
| **S3 Cleanup** | ✅ Yes | ✅ Yes |
| **Connection Management** | ✅ Close/reopen | ✅ Close/reopen |
| **Bulk Updates** | ✅ Single MERGE | ✅ Single MERGE |

**Only Differences:**
1. **API Function:** `BALANCE_SHEET` vs `OVERVIEW`
2. **Staleness:** 135 days vs **365 days**
3. **Data Structure:** Balance sheet metrics vs company profile + financial metrics
4. **Schedule:** Weekly (Sundays) vs **Quarterly** (1st of Jan/Apr/Jul/Oct)

---

## Reference Files

- **Python:** `scripts/github_actions/fetch_company_overview_watermark.py`
- **SQL Loader:** `snowflake/runbooks/load_company_overview_from_s3_watermark.sql`
- **Workflow:** `.github/workflows/company_overview_watermark_etl.yml`
- **Watermark Init:** `snowflake/setup/templates/watermark_company_overview.sql`
- **Naming Conventions:** `docs/NAMING_CONVENTIONS.md`
- **ETL Operations Guide:** `ETL_OPERATIONS_GUIDE.md`
