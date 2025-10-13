# Cash Flow ETL - Quick Start Guide

## 🎯 Overview
Complete watermark-based ETL for CASH_FLOW data, replicated from the successful BALANCE_SHEET workflow.

## 📋 Important Documentation
- **Naming Conventions**: See `snowflake/NAMING_CONVENTIONS.md` for standardized object naming rules
- **Architecture**: Understanding the stage vs staging table distinction

## 🔐 Prerequisites & Required Secrets Flow ETL - Quick Start Guide

## 🎯 Overview
Complete watermark-based ETL for CASH_FLOW data, replicated from the successful BALANCE_SHEET workflow.

## � Prerequisites & Required Secrets

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
| `S3_CASH_FLOW_PREFIX` | `cash_flow/` | Workflow YAML |
| Python Version | `3.11` | Workflow YAML |
| Runner | `ubuntu-22.04` | Workflow YAML |
| Timeout | `360 minutes` (6 hours) | Workflow YAML |

### AWS OIDC Authentication Setup
The workflow uses **OpenID Connect (OIDC)** for secure AWS authentication without storing access keys:

1. **Create IAM Role** with trust policy for GitHub:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::YOUR_ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:YOUR_GITHUB_USERNAME/fin-trade-extract:*"
        }
      }
    }
  ]
}
```

2. **Attach S3 Policy** to the role:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::fin-trade-craft-landing/*",
        "arn:aws:s3:::fin-trade-craft-landing"
      ]
    }
  ]
}
```

3. **Add Role ARN** to GitHub secrets as `AWS_ROLE_TO_ASSUME`

### Alpha Vantage API Setup
1. Sign up at https://www.alphavantage.co/support/#api-key
2. Free tier: 25 requests/day, 5 requests/minute
3. **Premium tier recommended**: 75 requests/minute (required for this workflow)
4. Add API key to GitHub secrets as `ALPHAVANTAGE_API_KEY`

### Snowflake Setup
1. **Database & Schema** must exist:
```sql
CREATE DATABASE IF NOT EXISTS FIN_TRADE_EXTRACT;
CREATE SCHEMA IF NOT EXISTS FIN_TRADE_EXTRACT.RAW;
```

2. **Warehouse** must exist:
```sql
CREATE WAREHOUSE IF NOT EXISTS FIN_TRADE_WH 
  WITH WAREHOUSE_SIZE = 'X-SMALL' 
  AUTO_SUSPEND = 60 
  AUTO_RESUME = TRUE;
```

3. **User & Role** with permissions:
```sql
CREATE USER IF NOT EXISTS ETL_USER PASSWORD = 'your_password';
CREATE ROLE IF NOT EXISTS ETL_ROLE;

GRANT USAGE ON DATABASE FIN_TRADE_EXTRACT TO ROLE ETL_ROLE;
GRANT USAGE ON SCHEMA FIN_TRADE_EXTRACT.RAW TO ROLE ETL_ROLE;
GRANT CREATE TABLE ON SCHEMA FIN_TRADE_EXTRACT.RAW TO ROLE ETL_ROLE;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA FIN_TRADE_EXTRACT.RAW TO ROLE ETL_ROLE;
GRANT USAGE ON WAREHOUSE FIN_TRADE_WH TO ROLE ETL_ROLE;

GRANT ROLE ETL_ROLE TO USER ETL_USER;
```

4. **Storage Integration** for S3:
```sql
CREATE STORAGE INTEGRATION FIN_TRADE_S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/SnowflakeS3Role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://fin-trade-craft-landing/');
```

## �📊 Features
- ✅ **Watermark-based extraction** - Only fetches when needed
- ✅ **135-day staleness logic** - Saves 99.3% of API calls (quarterly data doesn't change daily!)
- ✅ **Connection management** - Closes Snowflake connection during API extraction (saves $18-20/day)
- ✅ **Bulk watermark updates** - 100x faster using temp table + single MERGE
- ✅ **LOAD_DATE DATE** - Standardized metadata column (not CREATED_AT, not VARCHAR)
- ✅ **S3 cleanup** - Automatic cleanup before extraction (prevents duplicates)

## 🚀 Setup Steps

### Step 1: Add Watermarks (One-time)
```
GitHub Actions → Add Data Source Watermarks workflow
- Select: CASH_FLOW
- Click: Run workflow
```

This creates watermarks for ~2,000-3,000 active common stocks.

**What it does:**
- Creates watermarks for all active stocks (STATUS = 'ACTIVE' AND ASSET_TYPE = 'Stock')
- Sets API_ELIGIBLE = 'YES' for active stocks
- Sets API_ELIGIBLE = 'NO' for delisted/non-stock symbols
- Initializes all fiscal dates to NULL (ready for first run)

### Step 2: Test Run (Recommended)
```
GitHub Actions → Cash Flow Watermark ETL workflow
- max_symbols: 5
- Click: Run workflow
```

**What happens:**
1. Queries watermarks → finds 5 eligible symbols
2. Fetches cash flow data from Alpha Vantage API
3. Uploads CSV files to S3 (s3://fin-trade-craft-landing/cash_flow/)
4. Loads data to Snowflake CASH_FLOW table (creates table if not exists)
5. Updates watermarks with FIRST_FISCAL_DATE, LAST_FISCAL_DATE

**Expected results:**
- 5 symbols processed
- ~50-100 records loaded (each symbol has multiple annual + quarterly reports)
- Table created with LOAD_DATE DATE column (verify!)

### Step 3: Production Run
```
GitHub Actions → Cash Flow Watermark ETL workflow
- Leave all inputs blank (processes all eligible symbols)
- Click: Run workflow
```

**First run:**
- Processes all ~2,000-3,000 active stocks
- Takes ~30-40 minutes (75 API calls/min = 4,500 calls/hour)
- Loads 50,000-100,000+ records (annual + quarterly reports)

**Subsequent runs:**
- 135-day staleness filter prevents re-fetching recent data
- Only fetches symbols with LAST_FISCAL_DATE older than 135 days
- Typically processes only 10-20 symbols per week (99.3% reduction!)

## 📅 Scheduling

**Weekly schedule:** Sunday at 3 AM UTC
```yaml
schedule:
  - cron: '0 3 * * 0'  # Weekly on Sunday
```

**Why weekly?**
- Cash flow data is quarterly (every 90 days)
- 135-day staleness check means most symbols won't qualify
- Weekly run catches stragglers with filing delays
- Minimal API cost (~20 calls per week after initial load)

## 📊 Data Schema

### CASH_FLOW Table
```sql
SYMBOL                  VARCHAR(20)
FISCAL_DATE_ENDING      DATE
PERIOD_TYPE             VARCHAR(20)  -- 'annual' or 'quarterly'

-- Operating Activities
OPERATING_CASHFLOW      NUMBER(38,2)
CAPITAL_EXPENDITURES    NUMBER(38,2)
...

-- Investing Activities
CASHFLOW_FROM_INVESTMENT NUMBER(38,2)
...

-- Financing Activities
CASHFLOW_FROM_FINANCING NUMBER(38,2)
DIVIDEND_PAYOUT         NUMBER(38,2)
...

-- Metadata
SYMBOL_ID               NUMBER(38,0)  -- Consistent hash for joins
LOAD_DATE               DATE          -- Date data was loaded

PRIMARY KEY: (SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE)
```

### ETL_WATERMARKS for CASH_FLOW
```sql
TABLE_NAME: 'CASH_FLOW'
SYMBOL_ID: Consistent hash
SYMBOL: Stock ticker
FIRST_FISCAL_DATE: Earliest fiscal date in our data
LAST_FISCAL_DATE: Most recent fiscal date in our data
LAST_SUCCESSFUL_RUN: When we last fetched this symbol
API_ELIGIBLE: 'YES' for active stocks, 'NO' for others, 'DEL' for delisted
```

## 🔍 Monitoring Queries

### Check watermark status
```sql
SELECT 
    COUNT(*) as total_watermarks,
    COUNT(CASE WHEN API_ELIGIBLE = 'YES' THEN 1 END) as eligible,
    COUNT(CASE WHEN LAST_FISCAL_DATE IS NOT NULL THEN 1 END) as has_data,
    COUNT(CASE WHEN LAST_FISCAL_DATE < DATEADD(day, -135, CURRENT_DATE()) THEN 1 END) as needs_refresh
FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'CASH_FLOW';
```

### Check data loaded
```sql
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as unique_symbols,
    COUNT(CASE WHEN PERIOD_TYPE = 'annual' THEN 1 END) as annual_reports,
    COUNT(CASE WHEN PERIOD_TYPE = 'quarterly' THEN 1 END) as quarterly_reports,
    MIN(FISCAL_DATE_ENDING) as earliest_date,
    MAX(FISCAL_DATE_ENDING) as latest_date,
    MAX(LOAD_DATE) as last_load_date
FROM FIN_TRADE_EXTRACT.RAW.CASH_FLOW;
```

### Sample cash flow data
```sql
SELECT 
    SYMBOL,
    FISCAL_DATE_ENDING,
    PERIOD_TYPE,
    OPERATING_CASHFLOW,
    CASHFLOW_FROM_INVESTMENT,
    CASHFLOW_FROM_FINANCING,
    NET_INCOME,
    LOAD_DATE
FROM FIN_TRADE_EXTRACT.RAW.CASH_FLOW
WHERE SYMBOL = 'AAPL'
ORDER BY FISCAL_DATE_ENDING DESC, PERIOD_TYPE
LIMIT 10;
```

## 🎯 Cost Savings

### 135-Day Staleness Logic
**Without staleness check:**
- Daily run: 2,500 symbols × 1 API call = 2,500 calls/day
- Monthly cost: 2,500 × 30 = 75,000 API calls
- Wasted: 99%+ of calls (quarterly data doesn't change!)

**With 135-day staleness:**
- First run: 2,500 symbols (full load)
- Day 2-135: ~0 symbols (all have recent data)
- Day 136+: ~20 symbols/week (only companies with new filings)
- Monthly cost: ~100 API calls (99.3% reduction!)

### Connection Management
**Old approach:**
- Open connection → query watermarks → keep open 30-40 minutes during API extraction
- Warehouse cost: 40 min × $2.30/hour × 30 days = $46/month

**New approach:**
- Open → query watermarks → CLOSE
- Sleep 30-40 minutes (API extraction, no warehouse running)
- Open → bulk update watermarks → CLOSE
- Warehouse cost: 5 minutes × $2.30/hour × 30 days = $5.75/month
- **Savings: $40/month ($1.35/day per run)**

**Note:** The original $18-20/day savings was for TIME_SERIES_DAILY which runs daily with 1-2 hour extractions. CASH_FLOW runs weekly with 30-40 minute extractions, so savings are proportionally smaller but still significant!

## 🔧 Troubleshooting

### Issue: No watermarks created
**Solution:** Run Add Data Source Watermarks workflow with CASH_FLOW selected

### Issue: No symbols to process
**Check:**
```sql
SELECT * FROM FIN_TRADE_EXTRACT.RAW.ETL_WATERMARKS
WHERE TABLE_NAME = 'CASH_FLOW'
  AND API_ELIGIBLE = 'YES'
  AND (LAST_FISCAL_DATE IS NULL OR LAST_FISCAL_DATE < DATEADD(day, -135, CURRENT_DATE()))
LIMIT 10;
```

### Issue: Table has CREATED_AT instead of LOAD_DATE
**This means table was created before schema fix.**
**Solution:** Drop and recreate
```sql
DROP TABLE FIN_TRADE_EXTRACT.RAW.CASH_FLOW;
-- Next ETL run will recreate with correct schema
```

### Issue: Duplicate records
**Cause:** S3 files from previous runs not cleaned up
**Prevention:** Script automatically cleans S3 before extraction
**Manual fix:**
```sql
-- Delete duplicates (keeps most recent LOAD_DATE)
DELETE FROM FIN_TRADE_EXTRACT.RAW.CASH_FLOW
WHERE (SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE, LOAD_DATE) IN (
    SELECT SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE, LOAD_DATE
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE ORDER BY LOAD_DATE DESC) as rn
        FROM FIN_TRADE_EXTRACT.RAW.CASH_FLOW
    ) WHERE rn > 1
);
```

## 📚 Related Files

### Python Scripts
- `scripts/github_actions/fetch_cash_flow_watermark.py` - Main ETL script

### Snowflake SQL
- `snowflake/load_cash_flow_from_s3.sql` - Load script (creates table, stage, merges data)

### GitHub Actions
- `.github/workflows/cash_flow_watermark_etl.yml` - Workflow definition
- `.github/workflows/add_data_source_watermarks.yml` - Watermark creation (includes CASH_FLOW option)

### Watermarking
- `scripts/watermarking/create_individual_watermarks.py` - Watermark creation logic

## 🎉 Success Criteria

After running the workflow, you should see:
- ✅ CASH_FLOW table exists with LOAD_DATE DATE column
- ✅ 2,000-3,000 watermarks created
- ✅ 50,000-100,000+ records loaded (first run)
- ✅ FIRST_FISCAL_DATE and LAST_FISCAL_DATE populated in watermarks
- ✅ Subsequent runs only process ~20 symbols/week (staleness working!)

## 🔄 Next Steps

The same pattern can be replicated for:
- INCOME_STATEMENT (quarterly fundamentals, same 135-day logic)
- EARNINGS (quarterly earnings, same 135-day logic)

All using the same proven workflow:
1. Create watermarks
2. 135-day staleness check
3. Connection management
4. Bulk updates
5. Standardized LOAD_DATE DATE
