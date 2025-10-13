# Balance Sheet ETL - Quick Start Guide

## 🎯 Overview
Complete watermark-based ETL for BALANCE_SHEET fundamentals data from Alpha Vantage API. This ETL extracts annual and quarterly balance sheet statements for all eligible stocks, using the watermark system for intelligent incremental processing.

## 📋 Important Documentation
- **Naming Conventions**: See `snowflake/NAMING_CONVENTIONS.md` for standardized object naming rules
- **ETL Development Pattern**: See `docs/ETL_DEVELOPMENT_PATTERN.md` for the 5-file standard
- **Architecture**: Understanding the stage vs staging table distinction

## 🔐 Prerequisites & Required Secrets

### GitHub Secrets Required
The workflow requires the following secrets to be configured in your GitHub repository:

**Settings → Secrets and variables → Actions → New repository secret**

| Secret Name | Description | Example Value |
|------------|-------------|---------------|
| `ALPHAVANTAGE_API_KEY` | Alpha Vantage API key | `YOUR_API_KEY_HERE` |
| `AWS_ROLE_TO_ASSUME` | AWS IAM Role ARN for OIDC | `arn:aws:iam::123456789012:role/GitHubActionsRole` |
| `AWS_REGION` | AWS region for S3 bucket | `us-east-1` |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `abc12345.us-east-1` |
| `SNOWFLAKE_USER` | Snowflake username | `ETL_USER` |
| `SNOWFLAKE_PASSWORD` | Snowflake password | `your_password` |
| `SNOWFLAKE_WAREHOUSE` | Snowflake warehouse name | `FIN_TRADE_WH` |
| `SNOWFLAKE_DATABASE` | Snowflake database name | `FIN_TRADE_EXTRACT` |
| `SNOWFLAKE_SCHEMA` | Snowflake schema name | `RAW` |

### Hardcoded Configuration
These values are configured directly in the workflow (not secrets):

| Parameter | Value | Location |
|-----------|-------|----------|
| `S3_BUCKET` | `fin-trade-craft-landing` | Workflow YAML |
| `S3_BALANCE_SHEET_PREFIX` | `balance_sheet/` | Workflow YAML |
| Python Version | `3.11` | Workflow YAML |
| Runner | `ubuntu-22.04` | Workflow YAML |
| Timeout | `360 minutes` (6 hours) | Workflow YAML |

## 📊 Data Structure

### Table: BALANCE_SHEET
Complete balance sheet data with assets, liabilities, and equity sections.

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `SYMBOL` | VARCHAR(20) | Stock ticker symbol |
| `FISCAL_DATE_ENDING` | DATE | End date of the fiscal period |
| `PERIOD_TYPE` | VARCHAR(20) | 'annual' or 'quarterly' |
| `REPORTED_CURRENCY` | VARCHAR(10) | Currency code (usually 'USD') |
| **ASSETS** | | |
| `TOTAL_ASSETS` | NUMBER(38,2) | Total assets |
| `TOTAL_CURRENT_ASSETS` | NUMBER(38,2) | Current assets |
| `CASH_AND_CASH_EQUIVALENTS` | NUMBER(38,2) | Cash and equivalents |
| `CASH_AND_SHORT_TERM_INVESTMENTS` | NUMBER(38,2) | Cash + short term investments |
| `INVENTORY` | NUMBER(38,2) | Inventory value |
| `CURRENT_NET_RECEIVABLES` | NUMBER(38,2) | Accounts receivable |
| `TOTAL_NON_CURRENT_ASSETS` | NUMBER(38,2) | Long-term assets |
| `PROPERTY_PLANT_EQUIPMENT` | NUMBER(38,2) | PP&E gross value |
| `ACCUMULATED_DEPRECIATION_AMORTIZATION_PPE` | NUMBER(38,2) | Accumulated depreciation |
| `INTANGIBLE_ASSETS` | NUMBER(38,2) | Intangible assets (including goodwill) |
| `GOODWILL` | NUMBER(38,2) | Goodwill |
| `INVESTMENTS` | NUMBER(38,2) | Total investments |
| `LONG_TERM_INVESTMENTS` | NUMBER(38,2) | Long-term investments |
| `SHORT_TERM_INVESTMENTS` | NUMBER(38,2) | Short-term investments |
| `OTHER_CURRENT_ASSETS` | NUMBER(38,2) | Other current assets |
| `OTHER_NON_CURRENT_ASSETS` | NUMBER(38,2) | Other non-current assets |
| **LIABILITIES** | | |
| `TOTAL_LIABILITIES` | NUMBER(38,2) | Total liabilities |
| `TOTAL_CURRENT_LIABILITIES` | NUMBER(38,2) | Current liabilities |
| `CURRENT_ACCOUNTS_PAYABLE` | NUMBER(38,2) | Accounts payable |
| `DEFERRED_REVENUE` | NUMBER(38,2) | Deferred/unearned revenue |
| `CURRENT_DEBT` | NUMBER(38,2) | Current portion of debt |
| `SHORT_TERM_DEBT` | NUMBER(38,2) | Short-term debt |
| `TOTAL_NON_CURRENT_LIABILITIES` | NUMBER(38,2) | Long-term liabilities |
| `CAPITAL_LEASE_OBLIGATIONS` | NUMBER(38,2) | Capital lease obligations |
| `LONG_TERM_DEBT` | NUMBER(38,2) | Total long-term debt |
| `CURRENT_LONG_TERM_DEBT` | NUMBER(38,2) | Current portion of LT debt |
| `LONG_TERM_DEBT_NONCURRENT` | NUMBER(38,2) | Non-current LT debt |
| `OTHER_CURRENT_LIABILITIES` | NUMBER(38,2) | Other current liabilities |
| `OTHER_NON_CURRENT_LIABILITIES` | NUMBER(38,2) | Other non-current liabilities |
| **EQUITY** | | |
| `TOTAL_SHAREHOLDER_EQUITY` | NUMBER(38,2) | Total shareholders' equity |
| `TREASURY_STOCK` | NUMBER(38,2) | Treasury stock |
| `RETAINED_EARNINGS` | NUMBER(38,2) | Retained earnings |
| `COMMON_STOCK` | NUMBER(38,2) | Common stock |
| `COMMON_STOCK_SHARES_OUTSTANDING` | NUMBER(38,0) | Shares outstanding |
| **METADATA** | | |
| `SYMBOL_ID` | NUMBER(38,0) | Foreign key to SYMBOL table |
| `LOAD_DATE` | TIMESTAMP_NTZ | When the record was loaded |

**Primary Key:** `(SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE)`

**Natural Key Logic:**
- A company can have one annual and one quarterly report per fiscal date
- `PERIOD_TYPE` distinguishes between annual and quarterly reports

## 🚀 Initial Setup

### Step 1: Initialize Watermarks in Snowflake
Run the watermark initialization template to create tracking records for all eligible symbols:

```sql
-- Execute this SQL file in Snowflake
-- Location: snowflake/setup/templates/watermark_balance_sheet.sql

-- This will create ETL_WATERMARKS records for:
-- - All active stocks (ASSET_TYPE='Stock' AND STATUS='Active')
-- - Set API_ELIGIBLE='YES' for active stocks
-- - Set API_ELIGIBLE='DEL' for delisted stocks
-- - Set API_ELIGIBLE='NO' for ETFs and other non-stocks

-- Expected result: ~2000-3000 watermark records created
```

**Verify initialization:**
```sql
SELECT 
    API_ELIGIBLE,
    COUNT(*) as count,
    COUNT(DISTINCT EXCHANGE) as exchanges
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET'
GROUP BY API_ELIGIBLE;

-- Expected:
-- API_ELIGIBLE='YES': ~2000-3000 (active stocks)
-- API_ELIGIBLE='NO': ~500-1000 (ETFs, etc.)
-- API_ELIGIBLE='DEL': ~100-500 (delisted stocks)
```

### Step 2: Test with Small Batch
Before running the full ETL, test with a small batch to verify everything works:

1. Go to **GitHub Actions** → **Balance Sheet ETL - Watermark Based**
2. Click **Run workflow**
3. Set parameters:
   - `exchange_filter`: NYSE
   - `max_symbols`: **10** (small test batch)
   - `skip_recent_hours`: (leave blank)
   - `batch_size`: 50
4. Click **Run workflow**
5. Monitor execution (~5-10 minutes for 10 symbols)

**Expected output:**
```
🚀 Starting watermark-based balance sheet ETL...
🏢 Exchange filter: NYSE
🔒 Symbol limit: 10 symbols (testing mode)
📋 Batch size: 50 symbols

✅ Processed 10 symbols
✅ Successful: 10
❌ Failed: 0
⏱️ Duration: 8.5 minutes
```

### Step 3: Verify Test Results in Snowflake
Check that data was loaded correctly:

```sql
-- Check total records loaded
SELECT COUNT(*) as total_records
FROM BALANCE_SHEET;
-- Expected: 20-60 records (10 symbols × 2-6 periods each)

-- Check data quality
SELECT 
    SYMBOL,
    PERIOD_TYPE,
    COUNT(*) as periods,
    MIN(FISCAL_DATE_ENDING) as earliest_date,
    MAX(FISCAL_DATE_ENDING) as latest_date
FROM BALANCE_SHEET
GROUP BY SYMBOL, PERIOD_TYPE
ORDER BY SYMBOL, PERIOD_TYPE;

-- Verify balance sheet equation (Assets = Liabilities + Equity)
SELECT 
    SYMBOL,
    FISCAL_DATE_ENDING,
    PERIOD_TYPE,
    TOTAL_ASSETS,
    TOTAL_LIABILITIES + TOTAL_SHAREHOLDER_EQUITY as sum_liab_equity,
    TOTAL_ASSETS - (TOTAL_LIABILITIES + TOTAL_SHAREHOLDER_EQUITY) as difference
FROM BALANCE_SHEET
WHERE TOTAL_ASSETS IS NOT NULL
  AND TOTAL_LIABILITIES IS NOT NULL
  AND TOTAL_SHAREHOLDER_EQUITY IS NOT NULL
LIMIT 20;
-- difference should be close to 0 (allow for rounding)
```

### Step 4: Check Watermark Updates
Verify that watermarks were updated after the test run:

```sql
SELECT 
    SYMBOL,
    FIRST_FISCAL_DATE,
    LAST_FISCAL_DATE,
    LAST_SUCCESSFUL_RUN,
    DATEDIFF('day', LAST_SUCCESSFUL_RUN, CURRENT_TIMESTAMP()) as days_since_run
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET'
  AND LAST_SUCCESSFUL_RUN IS NOT NULL
ORDER BY LAST_SUCCESSFUL_RUN DESC
LIMIT 20;
```

### Step 5: Run Full Production ETL
Once the test is successful, run the full ETL:

1. Go to **GitHub Actions** → **Balance Sheet ETL - Watermark Based**
2. Click **Run workflow**
3. Set parameters:
   - `exchange_filter`: (leave blank for all exchanges)
   - `max_symbols`: (leave blank for all eligible symbols)
   - `skip_recent_hours`: **720** (skip symbols processed in last 30 days)
   - `batch_size`: 50
4. Click **Run workflow**

**Expected duration:** 3-5 hours for ~2000-3000 symbols

## 🔄 Running the ETL

### Manual Trigger
**GitHub Actions → Balance Sheet ETL - Watermark Based → Run workflow**

### Workflow Parameters

| Parameter | Description | Recommended Value |
|-----------|-------------|-------------------|
| `exchange_filter` | Filter by exchange (NYSE, NASDAQ, AMEX) | Blank (all exchanges) |
| `max_symbols` | Limit symbols processed (for testing) | Blank (production) or 10 (testing) |
| `skip_recent_hours` | Skip symbols processed within N hours | 720 (30 days for quarterly data) |
| `batch_size` | Symbols per batch | 50 (default) |

### Scheduled Execution
The workflow can be configured with a cron schedule for quarterly execution:

```yaml
schedule:
  - cron: '0 2 15 1,4,7,10 *'  # 15th of Jan, Apr, Jul, Oct at 2 AM UTC
```

**Note:** Schedule is commented out by default. Uncomment in `.github/workflows/balance_sheet_watermark_etl.yml` to enable.

## 📈 Monitoring & Validation

### Data Quality Checks

**1. Check for Missing Required Fields:**
```sql
SELECT 
    COUNT(*) as records_with_missing_fields
FROM BALANCE_SHEET
WHERE SYMBOL IS NULL 
   OR FISCAL_DATE_ENDING IS NULL 
   OR PERIOD_TYPE IS NULL;
-- Expected: 0
```

**2. Check Balance Sheet Equation:**
```sql
SELECT 
    COUNT(*) as total_records,
    SUM(CASE 
        WHEN ABS(TOTAL_ASSETS - (TOTAL_LIABILITIES + TOTAL_SHAREHOLDER_EQUITY)) > 1.00 
        THEN 1 ELSE 0 
    END) as records_not_balanced,
    AVG(ABS(TOTAL_ASSETS - (TOTAL_LIABILITIES + TOTAL_SHAREHOLDER_EQUITY))) as avg_difference
FROM BALANCE_SHEET
WHERE TOTAL_ASSETS IS NOT NULL
  AND TOTAL_LIABILITIES IS NOT NULL
  AND TOTAL_SHAREHOLDER_EQUITY IS NOT NULL;
-- records_not_balanced should be < 1% of total
-- avg_difference should be < 0.10 (due to rounding)
```

**3. Check Date Ranges:**
```sql
SELECT 
    PERIOD_TYPE,
    MIN(FISCAL_DATE_ENDING) as earliest_date,
    MAX(FISCAL_DATE_ENDING) as latest_date,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    COUNT(*) as total_records
FROM BALANCE_SHEET
GROUP BY PERIOD_TYPE;
-- Annual: earliest should be 5-10 years ago
-- Quarterly: earliest should be 2-5 years ago
```

**4. Check for Duplicates:**
```sql
SELECT 
    SYMBOL,
    FISCAL_DATE_ENDING,
    PERIOD_TYPE,
    COUNT(*) as duplicate_count
FROM BALANCE_SHEET
GROUP BY SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE
HAVING COUNT(*) > 1;
-- Expected: 0 rows (primary key prevents duplicates)
```

**5. Check Most Recent Data:**
```sql
SELECT 
    PERIOD_TYPE,
    MAX(FISCAL_DATE_ENDING) as most_recent_date,
    DATEDIFF('day', MAX(FISCAL_DATE_ENDING), CURRENT_DATE()) as days_old
FROM BALANCE_SHEET
GROUP BY PERIOD_TYPE;
-- Quarterly: days_old should be < 90 (within last quarter)
-- Annual: days_old should be < 365 (within last year)
```

### Success Criteria

**Initial Setup (after Step 2):**
- ✅ 10 symbols processed successfully
- ✅ 20-60 balance sheet records loaded
- ✅ Watermarks updated for 10 symbols
- ✅ Balance sheet equation balanced (diff < $1.00)

**Production Run (after Step 5):**
- ✅ 2000-3000 symbols processed
- ✅ 40,000-120,000 balance sheet records loaded (20-60 per symbol)
- ✅ Watermarks updated for all successful symbols
- ✅ < 5% failure rate (API issues, no data, etc.)

**Monthly Maintenance:**
- ✅ New quarterly data loaded within 7 days of earnings release
- ✅ Watermarks updated regularly
- ✅ No stale data (symbols not updated in > 135 days investigated)

## 🔧 Troubleshooting

### Issue: No Symbols Selected for Processing

**Symptoms:**
```
📊 Selected 0 symbols to process
✅ Processing complete - no symbols to process
```

**Causes:**
1. All watermarks have `LAST_SUCCESSFUL_RUN` within `skip_recent_hours` window
2. No watermarks with `API_ELIGIBLE='YES'`
3. `exchange_filter` too restrictive (e.g., AMEX has fewer stocks)

**Solutions:**
```sql
-- Check watermark status
SELECT 
    API_ELIGIBLE,
    COUNT(*) as count,
    COUNT(CASE WHEN LAST_SUCCESSFUL_RUN IS NULL THEN 1 END) as never_run,
    COUNT(CASE WHEN DATEDIFF('hour', LAST_SUCCESSFUL_RUN, CURRENT_TIMESTAMP()) > 720 THEN 1 END) as stale
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET'
GROUP BY API_ELIGIBLE;

-- Solution: Lower skip_recent_hours or leave blank to re-process all
```

### Issue: High Failure Rate

**Symptoms:**
```
✅ Successful: 1500
❌ Failed: 500
```

**Causes:**
1. API rate limiting (free tier: 5 req/min, premium: 75 req/min)
2. Symbols with no balance sheet data (new IPOs, special situations)
3. Network issues

**Solutions:**
- Reduce `batch_size` from 50 to 25
- Upgrade to Alpha Vantage premium tier
- Retry failed symbols: Set `skip_recent_hours` to blank, `max_symbols` to blank
- Check consecutive failures:
```sql
SELECT SYMBOL, CONSECUTIVE_FAILURES, LAST_SUCCESSFUL_RUN
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'BALANCE_SHEET'
  AND CONSECUTIVE_FAILURES > 3
ORDER BY CONSECUTIVE_FAILURES DESC
LIMIT 50;
```

### Issue: Duplicate Records

**Symptoms:**
```
ERROR: Duplicate key value violates unique constraint
```

**Causes:**
1. Same symbol processed twice in one run (should not happen)
2. S3 cleanup failed (old files remain)
3. Primary key constraint not enforced

**Solutions:**
```sql
-- Check for duplicates
SELECT SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE, COUNT(*)
FROM BALANCE_SHEET
GROUP BY SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE
HAVING COUNT(*) > 1;

-- Remove duplicates (keep latest LOAD_DATE)
DELETE FROM BALANCE_SHEET
WHERE (SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE, LOAD_DATE) IN (
    SELECT SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE, LOAD_DATE
    FROM BALANCE_SHEET
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE 
        ORDER BY LOAD_DATE DESC
    ) > 1
);
```

### Issue: Watermarks Not Updating

**Symptoms:**
- ETL runs successfully but `LAST_SUCCESSFUL_RUN` not updated
- `FIRST_FISCAL_DATE` and `LAST_FISCAL_DATE` remain NULL

**Causes:**
1. Python script bulk update failed
2. Snowflake connection issues during watermark update phase
3. No data returned from API (watermark should still update)

**Solutions:**
- Check workflow logs for "Updating watermarks" section
- Manually update watermarks:
```sql
UPDATE ETL_WATERMARKS
SET LAST_SUCCESSFUL_RUN = CURRENT_TIMESTAMP()
WHERE TABLE_NAME = 'BALANCE_SHEET'
  AND SYMBOL IN ('AAPL', 'MSFT', 'GOOGL');  -- Replace with actual symbols
```

### Issue: Balance Sheet Equation Not Balanced

**Symptoms:**
```
Total Assets: 1000000.00
Total Liabilities + Equity: 999500.00
Difference: 500.00
```

**Causes:**
1. Rounding differences from API (normal if < $1.00)
2. Missing components (e.g., minority interest, preferred stock)
3. Data quality issues from source

**Solutions:**
- Small differences (< $1.00) are acceptable due to rounding
- Larger differences may indicate missing line items:
```sql
SELECT 
    SYMBOL,
    FISCAL_DATE_ENDING,
    PERIOD_TYPE,
    TOTAL_ASSETS - (TOTAL_LIABILITIES + TOTAL_SHAREHOLDER_EQUITY) as difference,
    TOTAL_ASSETS,
    TOTAL_LIABILITIES,
    TOTAL_SHAREHOLDER_EQUITY
FROM BALANCE_SHEET
WHERE ABS(TOTAL_ASSETS - (TOTAL_LIABILITIES + TOTAL_SHAREHOLDER_EQUITY)) > 1000.00
ORDER BY ABS(difference) DESC
LIMIT 20;
```

## 📁 File Locations

Following the 5-file pattern for all data sources:

1. **Python Extraction Script:** `scripts/github_actions/fetch_balance_sheet_watermark.py`
2. **SQL Load Script:** `snowflake/runbooks/load_balance_sheet_from_s3.sql`
3. **GitHub Actions Workflow:** `.github/workflows/balance_sheet_watermark_etl.yml`
4. **Watermark Template:** `snowflake/setup/templates/watermark_balance_sheet.sql`
5. **This ETL Guide:** `docs/BALANCE_SHEET_ETL_GUIDE.md`

## 📚 Related Documentation

- **ETL Development Pattern:** `docs/ETL_DEVELOPMENT_PATTERN.md` - Complete 5-file pattern documentation
- **ETL Operations Guide:** `docs/ETL_OPERATIONS_GUIDE.md` - Watermark system rules
- **Cash Flow ETL:** `docs/CASH_FLOW_ETL_GUIDE.md` - Similar fundamentals pattern
- **Income Statement ETL:** `docs/INCOME_STATEMENT_ETL_GUIDE.md` - Similar fundamentals pattern
- **Naming Conventions:** `snowflake/NAMING_CONVENTIONS.md` - Snowflake object naming

## 🔄 Maintenance Schedule

### Weekly
- Review failed extractions in workflow logs
- Check for symbols with high consecutive failures

### Monthly (After Earnings Season)
- Run ETL with `skip_recent_hours=720` to update quarterly data
- Review data quality metrics
- Validate most recent fiscal dates are current

### Quarterly
- Run full ETL with no `skip_recent_hours` to re-process all symbols
- Review and clean up stale watermarks (> 180 days old)
- Validate S3 storage usage and cleanup old files if needed

---

**Last Updated:** 2025-10-12  
**ETL Version:** Watermark-based (v2.0)  
**Pattern:** 5-file standard
