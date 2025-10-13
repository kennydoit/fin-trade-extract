# Income Statement ETL - Quick Start Guide

## 🎯 Overview
Complete watermark-based ETL for INCOME_STATEMENT data, following the proven BALANCE_SHEET workflow pattern.

## 📋 Important Documentation
- **Naming Conventions**: See `snowflake/NAMING_CONVENTIONS.md` for standardized object naming rules
- **Architecture**: Understanding the stage vs staging table distinction
- **Workflow Comparison**: See `.github/workflows/WORKFLOW_COMPARISON.md` for consistency patterns

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
| `S3_INCOME_STATEMENT_PREFIX` | `income_statement/` | Workflow YAML |
| Python Version | `3.11` | Workflow YAML |
| Runner | `ubuntu-22.04` | Workflow YAML |
| Timeout | `360 minutes` (6 hours) | Workflow YAML |

## 🏗️ Setup Instructions

### Step 1: Initialize Watermarks in Snowflake

Run the watermark initialization script to create ETL_WATERMARKS records for all symbols:

```sql
-- Execute this in Snowflake
snowflake/setup/templates/watermark_income_statement.sql
```

This will:
- Insert watermark records for all active stocks
- Set `API_ELIGIBLE = 'YES'` for active common stocks
- Set `API_ELIGIBLE = 'NO'` for non-stock assets
- Set `API_ELIGIBLE = 'DEL'` for already-delisted stocks
- Initialize all fiscal dates as NULL (will be set during first extraction)

**Expected output:**
```
✅ Income Statement Watermarks Created
- total_records: ~10,000+ (all symbols)
- eligible_for_api: ~8,000+ (active stocks only)
- not_eligible: ~2,000+ (ETFs, funds, etc.)
- delisted: varies (previously delisted stocks)
```

### Step 2: Verify Prerequisites

Ensure all GitHub secrets are configured (see table above).

### Step 3: Run Initial Test

**Test with small batch first:**

1. Go to GitHub Actions → "Income Statement ETL - Watermark Based"
2. Click "Run workflow"
3. Set parameters:
   - `exchange_filter`: Leave blank (all exchanges)
   - `max_symbols`: `5` (test with 5 symbols)
   - `skip_recent_hours`: Leave blank
   - `batch_size`: `50` (default)
4. Click "Run workflow"

**What to expect:**
- Duration: ~2-3 minutes for 5 symbols
- S3 uploads: 5 CSV files to `s3://fin-trade-craft-landing/income_statement/`
- Snowflake records: ~400-500 rows total (annual + quarterly data going back 20+ years)
- Watermarks updated: 5 symbols with FIRST_FISCAL_DATE and LAST_FISCAL_DATE set

### Step 4: Verify Results

```sql
-- Check watermark updates
SELECT 
    SYMBOL,
    FIRST_FISCAL_DATE,
    LAST_FISCAL_DATE,
    LAST_SUCCESSFUL_RUN,
    CONSECUTIVE_FAILURES
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'INCOME_STATEMENT'
  AND LAST_SUCCESSFUL_RUN IS NOT NULL
ORDER BY LAST_SUCCESSFUL_RUN DESC
LIMIT 10;

-- Check loaded data
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    MIN(FISCAL_DATE_ENDING) as earliest_date,
    MAX(FISCAL_DATE_ENDING) as latest_date,
    COUNT(CASE WHEN PERIOD_TYPE = 'annual' THEN 1 END) as annual_records,
    COUNT(CASE WHEN PERIOD_TYPE = 'quarterly' THEN 1 END) as quarterly_records
FROM FIN_TRADE_EXTRACT.RAW.INCOME_STATEMENT;

-- Sample data
SELECT 
    SYMBOL,
    FISCAL_DATE_ENDING,
    PERIOD_TYPE,
    TOTAL_REVENUE,
    GROSS_PROFIT,
    OPERATING_INCOME,
    NET_INCOME,
    EBITDA
FROM FIN_TRADE_EXTRACT.RAW.INCOME_STATEMENT
ORDER BY SYMBOL, FISCAL_DATE_ENDING DESC
LIMIT 20;
```

### Step 5: Run Full Processing

After successful test:

1. Go to GitHub Actions → "Income Statement ETL - Watermark Based"
2. Click "Run workflow"
3. Set parameters:
   - `exchange_filter`: Leave blank (or choose specific exchange)
   - `max_symbols`: Leave blank (process ALL eligible symbols)
   - `skip_recent_hours`: Leave blank
   - `batch_size`: `50` (default)
4. Click "Run workflow"

**What to expect for full run:**
- Duration: ~3-5 hours for ~8,000 symbols (depends on API rate limit)
- Processing rate: ~25-30 symbols/minute (75 API calls/minute premium tier)
- Total records: ~600,000+ (annual + quarterly data for all symbols)
- S3 costs: Minimal (~$0.01 for storage + requests)
- Snowflake costs: ~$2-3 (6 hours warehouse time at X-SMALL)

## 📊 Income Statement Data Structure

### Target Table: `FIN_TRADE_EXTRACT.RAW.INCOME_STATEMENT`

**Primary Key:** `(SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE)`

**Key Columns:**

| Category | Columns |
|----------|---------|
| **Revenue** | TOTAL_REVENUE, GROSS_PROFIT, COST_OF_REVENUE |
| **Operating** | OPERATING_INCOME, OPERATING_EXPENSES, SG&A, R&D |
| **Interest** | INTEREST_INCOME, INTEREST_EXPENSE, NET_INTEREST_INCOME |
| **Tax** | INCOME_BEFORE_TAX, INCOME_TAX_EXPENSE |
| **Net Income** | NET_INCOME, NET_INCOME_FROM_CONTINUING_OPERATIONS |
| **Metrics** | EBIT, EBITDA |
| **Depreciation** | DEPRECIATION, DEPRECIATION_AND_AMORTIZATION |
| **Metadata** | SYMBOL_ID, LOAD_DATE, PERIOD_TYPE |

### Data Coverage
- **Period Types**: Annual + Quarterly
- **History**: Up to 20+ years (company-dependent)
- **Update Frequency**: Quarterly (with 45-day filing grace period)
- **Staleness Logic**: 135 days (90-day quarter + 45-day grace)

## 🔄 Watermark-Based Processing

### How It Works

1. **First Run (All symbols with NULL LAST_FISCAL_DATE)**:
   - Processes all active stocks (~8,000 symbols)
   - Takes 3-5 hours
   - Sets FIRST_FISCAL_DATE and LAST_FISCAL_DATE for each symbol

2. **Subsequent Runs (135-day staleness check)**:
   - Only processes symbols where `LAST_FISCAL_DATE < CURRENT_DATE - 135 days`
   - Much faster (~few minutes) until new quarterly data is available
   - Saves API calls and processing time

3. **Quarterly Updates**:
   - When Q3 2025 data becomes available (late October 2025)
   - Symbols with Q2 2025 data (June 30) will be 120+ days old
   - They'll be processed again to capture new quarterly data

### Staleness Logic Benefits

**Cost Savings:**
- Prevents unnecessary API calls for symbols already up-to-date
- Reduces Snowflake warehouse time (no idle connections during extraction)
- Minimal S3 storage costs (files cleaned before each run)

**API Efficiency:**
- 135-day threshold aligns with quarterly reporting cycle
- Accounts for filing delays (45-day grace period)
- Premium tier (75 calls/minute) handles large batches efficiently

## 🛠️ Troubleshooting

### Issue: Workflow Fails with "No symbols to process"

**Cause:** All eligible symbols have been processed within the last 135 days

**Solution:** This is normal! Fundamentals don't change daily. Wait until:
- 135 days pass since last LAST_FISCAL_DATE, OR
- Use `skip_recent_hours: 0` to force reprocessing

### Issue: SQL Error "Object 'INCOME_STATEMENT_STAGE' does not exist"

**Cause:** Naming conventions not followed

**Solution:** Verify you're using:
- External Stage: `INCOME_STATEMENT_STAGE` (points to S3)
- Transient Table: `INCOME_STATEMENT_STAGING` (temp data)
- Permanent Table: `INCOME_STATEMENT` (final destination)

See `snowflake/NAMING_CONVENTIONS.md` for details.

### Issue: API Rate Limit Errors

**Cause:** Free tier (5 calls/minute) is too slow

**Solution:** Upgrade to Alpha Vantage Premium tier (75 calls/minute)
- Cost: ~$50/month
- Required for processing thousands of symbols
- Test with `max_symbols: 5` on free tier to validate setup

### Issue: Watermarks Not Updating

**Cause:** Python script error or Snowflake connection issue

**Solution:**
1. Check GitHub Actions logs for Python errors
2. Verify Snowflake credentials in secrets
3. Run diagnostic query:
   ```sql
   SELECT * FROM ETL_WATERMARKS 
   WHERE TABLE_NAME = 'INCOME_STATEMENT' 
   AND LAST_SUCCESSFUL_RUN > DATEADD(hour, -24, CURRENT_TIMESTAMP());
   ```

## 📅 Scheduled Runs

The workflow includes a cron schedule:
```yaml
schedule:
  - cron: '0 5 * * 0'  # Sundays at 5 AM UTC
```

**Recommendation:** 
- Weekly runs are sufficient (fundamentals don't change daily)
- Adjust timing to avoid conflicts with balance sheet/cash flow runs
- Monitor costs and adjust frequency as needed

## 🎯 Success Criteria

Your income statement ETL is working correctly if:

✅ Watermarks show recent LAST_SUCCESSFUL_RUN timestamps  
✅ INCOME_STATEMENT table has data for processed symbols  
✅ FIRST_FISCAL_DATE and LAST_FISCAL_DATE are set  
✅ CONSECUTIVE_FAILURES is 0 for successful symbols  
✅ Delisted symbols marked as API_ELIGIBLE = 'DEL'  
✅ Summary report shows high success rate (>95%)  
✅ Processing efficiency is ~25-30 symbols/minute  

## 📚 Related Documentation

- Main ETL Docs: `docs/`
- Naming Conventions: `snowflake/NAMING_CONVENTIONS.md`
- Workflow Comparison: `.github/workflows/WORKFLOW_COMPARISON.md`
- Balance Sheet ETL: `docs/BALANCE_SHEET_ETL_GUIDE.md` (same pattern)
- Cash Flow ETL: `docs/CASH_FLOW_ETL_GUIDE.md` (same pattern)
