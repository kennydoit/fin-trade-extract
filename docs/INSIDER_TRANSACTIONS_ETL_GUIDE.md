# Insider Transactions ETL - Quick Start Guide

## 🎯 Overview
Complete watermark-based ETL for INSIDER_TRANSACTIONS data, tracking executive/director trading activity.

**Key Difference from Fundamentals**: No staleness check - insider trading is sporadic and unpredictable.

## 📋 Important Documentation
- **Naming Conventions**: See `snowflake/NAMING_CONVENTIONS.md` for standardized object naming rules
- **ETL Operations Guide**: See `ETL_OPERATIONS_GUIDE.md` for comprehensive rules and gotchas
- **Architecture**: Understanding the watermark-based ETL system

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
| `S3_INSIDER_TRANSACTIONS_PREFIX` | `insider_transactions/` | Workflow YAML |
| Python Version | `3.11` | Workflow YAML |
| Runner | `ubuntu-22.04` | Workflow YAML |
| Timeout | `360 minutes` (6 hours) | Workflow YAML |
| Default Batch Size | `100` | Workflow YAML |
| Schedule | Monthly (1st at 4 AM UTC) | Workflow YAML |

### AWS OIDC Authentication Setup
See the Cash Flow ETL Guide or Balance Sheet ETL Guide for detailed AWS OIDC setup instructions.

## 📊 Data Structure

### Alpha Vantage API Endpoint
- **Function**: `INSIDER_TRANSACTIONS`
- **Example**: `https://www.alphavantage.co/query?function=INSIDER_TRANSACTIONS&symbol=IBM&apikey=demo`
- **Rate Limit**: 75 calls/minute (premium tier recommended)
- **Data Returned**: Executive/director buy/sell transactions

### API Response Structure
```json
{
  "data": [
    {
      "transactionDate": "2025-10-01",
      "filingDate": "2025-10-03",
      "ownerName": "John Doe",
      "ownerTitle": "Chief Executive Officer",
      "transactionType": "Sale to issuer",
      "acquistionOrDisposition": "D",
      "securityName": "Common Stock",
      "securityType": "Common Stock",
      "securitiesTransacted": "5000",
      "transactionPrice": "150.25",
      "securitiesOwned": "95000"
    }
  ]
}
```

### Snowflake Table: INSIDER_TRANSACTIONS
**Table**: `FIN_TRADE_EXTRACT.RAW.INSIDER_TRANSACTIONS`

**Columns (12 data + 2 metadata)**:

| Column | Type | Description |
|--------|------|-------------|
| `SYMBOL` | VARCHAR(20) | Stock ticker |
| `TRANSACTION_DATE` | DATE | When the transaction occurred |
| `FILING_DATE` | DATE | When the transaction was filed with SEC |
| `OWNER_NAME` | VARCHAR(255) | Name of insider (executive/director) |
| `OWNER_TITLE` | VARCHAR(255) | Title/position (CEO, CFO, Director, etc.) |
| `TRANSACTION_TYPE` | VARCHAR(100) | Type of transaction |
| `ACQUISITION_DISPOSITION` | VARCHAR(20) | 'A' (buy) or 'D' (sell) |
| `SECURITY_NAME` | VARCHAR(255) | Name of security traded |
| `SECURITY_TYPE` | VARCHAR(100) | Type of security (Common Stock, Options, etc.) |
| `SHARES` | NUMBER(38,4) | Number of shares transacted |
| `VALUE` | NUMBER(38,4) | Transaction value/price |
| `SHARES_OWNED_FOLLOWING` | NUMBER(38,4) | Shares owned after transaction |
| `SYMBOL_ID` | NUMBER(38,0) | Consistent hash for joins |
| `LOAD_DATE` | DATE | Date data was loaded into Snowflake |

**Primary Key**: `(SYMBOL, TRANSACTION_DATE, OWNER_NAME, SECURITY_TYPE)`

**Important**: Same person can have multiple transactions on same date in different securities.

## 🚀 Setup Instructions

### Step 1: Initialize Watermarks (ONE-TIME SETUP)

**Run this SQL script in Snowflake** to create watermark records:

```sql
-- File: snowflake/setup/templates/watermark_insider_transactions.sql
```

This creates `ETL_WATERMARKS` records for all active stocks. You can execute it via:
- Snowflake Web UI (Worksheets)
- SnowSQL command line
- OR use the automated workflow (recommended):
  - Go to GitHub Actions
  - Run `Add Data Source Watermarks` workflow
  - Select `INSIDER_TRANSACTIONS` from dropdown

**What it does**:
- Creates watermark entry for each symbol in `SYMBOL` table
- Sets `API_ELIGIBLE = 'YES'` for active common stocks only
- Sets `API_ELIGIBLE = 'NO'` for ETFs, delisted stocks, etc.
- Initial state: `LAST_FISCAL_DATE = NULL` (will be set on first run)

**Verify watermarks created**:
```sql
SELECT COUNT(*) FROM ETL_WATERMARKS 
WHERE TABLE_NAME = 'INSIDER_TRANSACTIONS' AND API_ELIGIBLE = 'YES';

-- Should return ~2000-3000 active stocks
```

### Step 2: Test with Small Batch

**Run the GitHub Actions workflow** with test parameters:

1. Go to **GitHub Actions** → **Insider Transactions ETL - Watermark Based**
2. Click **Run workflow**
3. Set parameters:
   - `exchange_filter`: Leave blank (or select NYSE for smaller test)
   - `max_symbols`: **10** (test with 10 symbols first)
   - `skip_recent_hours`: Leave blank
   - `batch_size`: **100** (default)

4. Click **Run workflow**

**Expected results**:
- Workflow takes ~2-3 minutes for 10 symbols
- Many symbols may have "no data" (this is NORMAL for insider transactions)
- Some symbols will have transactions (executives buying/selling)
- Check Snowflake: `SELECT COUNT(*) FROM INSIDER_TRANSACTIONS;`

### Step 3: Verify Data Quality

**Check data loaded correctly**:

```sql
-- Total transactions loaded
SELECT 
    COUNT(*) as total_transactions,
    COUNT(DISTINCT SYMBOL) as symbols_with_activity,
    MIN(TRANSACTION_DATE) as earliest_transaction,
    MAX(TRANSACTION_DATE) as latest_transaction
FROM INSIDER_TRANSACTIONS;

-- Breakdown by acquisition/disposition
SELECT 
    ACQUISITION_DISPOSITION,
    COUNT(*) as count,
    SUM(SHARES) as total_shares
FROM INSIDER_TRANSACTIONS
GROUP BY ACQUISITION_DISPOSITION;

-- Recent insider activity (last 30 days)
SELECT 
    SYMBOL,
    TRANSACTION_DATE,
    OWNER_NAME,
    OWNER_TITLE,
    ACQUISITION_DISPOSITION,
    SHARES,
    VALUE
FROM INSIDER_TRANSACTIONS
WHERE TRANSACTION_DATE >= DATEADD(day, -30, CURRENT_DATE())
ORDER BY TRANSACTION_DATE DESC
LIMIT 20;
```

### Step 4: Check Watermarks Updated

**Verify watermarks were updated after successful extraction**:

```sql
SELECT 
    SYMBOL,
    FIRST_FISCAL_DATE,     -- Earliest transaction date
    LAST_FISCAL_DATE,      -- Latest transaction date
    LAST_SUCCESSFUL_RUN,   -- When ETL last ran
    CONSECUTIVE_FAILURES
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'INSIDER_TRANSACTIONS'
  AND LAST_SUCCESSFUL_RUN IS NOT NULL
ORDER BY LAST_SUCCESSFUL_RUN DESC
LIMIT 20;
```

**Expected**:
- `FIRST_FISCAL_DATE`: Set to earliest transaction date (or current date if no transactions)
- `LAST_FISCAL_DATE`: Set to latest transaction date (or current date if no transactions)
- `LAST_SUCCESSFUL_RUN`: Set to current timestamp
- `CONSECUTIVE_FAILURES`: Reset to 0

### Step 5: Run Full Processing

**Once test is successful**, run with full parameters:

1. Go to **GitHub Actions** → **Insider Transactions ETL - Watermark Based**
2. Click **Run workflow**
3. Set parameters:
   - `exchange_filter`: Leave blank (all exchanges)
   - `max_symbols`: Leave blank (all eligible symbols)
   - `skip_recent_hours`: **720** (30 days - avoid re-processing recent runs)
   - `batch_size`: **100** (default)

4. Click **Run workflow**

**Expected**:
- Processes ~2000-3000 symbols (all active stocks)
- Takes 2-4 hours (depending on API rate limit)
- Many symbols will have no insider transactions (normal)
- Updates watermarks for all processed symbols

## 📅 Scheduling & Incremental Updates

### Automated Monthly Schedule

The workflow runs automatically on the **1st of each month at 4 AM UTC**:

```yaml
schedule:
  - cron: '0 4 1 * *'
```

**Why monthly?**
- Insider trading is sporadic and unpredictable
- Companies may have no transactions for months
- Monthly runs capture recent activity without excessive API calls

### Manual Runs

For on-demand updates:

1. **Recent Activity Update** (last 30 days):
   - `skip_recent_hours`: **720** (30 days)
   - Processes only symbols not updated in last 30 days

2. **Quarterly Refresh** (every 90 days):
   - `skip_recent_hours`: **2160** (90 days)
   - Good for quarterly reviews

3. **Force Full Refresh** (all symbols):
   - `skip_recent_hours`: Leave blank
   - Processes ALL eligible symbols (use sparingly)

## ⚠️ Important Differences from Fundamentals ETL

### NO Staleness Check

**Fundamentals** (Balance Sheet, Cash Flow, Income Statement):
- Use 135-day staleness check
- Only fetch if data is likely updated (quarterly cycle)
- Saves API quota

**Insider Transactions**:
- **NO staleness check**
- Always fetch latest data when run
- Insider trading is unpredictable (can happen anytime)

**Why no staleness?**
- A company might have 0 transactions for 6 months, then 10 in one week
- Quarterly filings are predictable, insider trading is not
- We want to capture recent activity whenever we run

### Expected "No Data" Responses

**This is NORMAL for insider transactions**:
- Many companies have no recent insider trading
- ETL will report "no data" for these symbols
- Watermarks still get updated (LAST_SUCCESSFUL_RUN set)
- This is not a failure - it's expected behavior

**Example**:
- Run on 1,000 symbols
- 800 symbols: "no data" (no insider transactions)
- 200 symbols: transactions found
- All 1,000 watermarks updated successfully

## 🔧 Troubleshooting

### Issue: "No symbols to process"

**Diagnosis**:
```sql
SELECT COUNT(*) FROM ETL_WATERMARKS 
WHERE TABLE_NAME = 'INSIDER_TRANSACTIONS' AND API_ELIGIBLE = 'YES';
```

**If count = 0**:
- Watermarks not initialized
- Run Step 1: Initialize Watermarks

**If count > 0**:
- Check `skip_recent_hours` parameter
- All symbols may have been processed recently

### Issue: All symbols return "no data"

**This is NORMAL**:
- Insider trading is sporadic
- Many companies have no recent transactions
- Check a few large-cap stocks manually:
  - AAPL, MSFT, GOOGL should have some activity
  - If those also have "no data", check API key/endpoint

**Verify API access**:
```bash
curl "https://www.alphavantage.co/query?function=INSIDER_TRANSACTIONS&symbol=IBM&apikey=YOUR_API_KEY"
```

### Issue: Rate limit errors

**Symptoms**:
- Workflow stops mid-processing
- "Rate limited" messages in logs

**Solutions**:
1. Reduce `batch_size` to 50 (slower but safer)
2. Use `max_symbols` to process in smaller chunks
3. Verify you have premium API tier (75 calls/minute)

### Issue: Duplicate transactions

**Check for duplicates**:
```sql
SELECT 
    SYMBOL, TRANSACTION_DATE, OWNER_NAME, SECURITY_TYPE,
    COUNT(*) as count
FROM INSIDER_TRANSACTIONS
GROUP BY SYMBOL, TRANSACTION_DATE, OWNER_NAME, SECURITY_TYPE
HAVING COUNT(*) > 1;
```

**Should return 0 rows** (primary key prevents duplicates)

**If duplicates exist**:
- SQL MERGE logic may have issue
- Report as bug for investigation

### Issue: Watermarks not updating

**Check if connection closed properly**:
```sql
-- Check for long-running warehouse sessions
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE WAREHOUSE_NAME = 'FIN_TRADE_WH'
  AND END_TIME IS NULL
  AND START_TIME < DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY START_TIME;
```

**If found**:
- Python script may have crashed before closing connection
- Manually close connections in Snowflake UI
- Review workflow logs for Python errors

## 📈 Data Quality Checks

### Automated Checks (in SQL load script)

The SQL loader performs these checks:

1. **Missing required fields**: SYMBOL, TRANSACTION_DATE, OWNER_NAME, SECURITY_TYPE
2. **Invalid transaction dates**: Non-parseable dates
3. **Invalid filing dates**: Non-parseable dates
4. **Invalid A/D codes**: Must be 'A' or 'D'
5. **Invalid share amounts**: Non-numeric values

**View check results** in Snowflake after load:
- Check query output from `load_insider_transactions_from_s3.sql`
- Should show 0 issues for each check

### Manual Quality Checks

**Check for reasonable data distribution**:
```sql
-- Should have mix of buys (A) and sells (D)
SELECT 
    ACQUISITION_DISPOSITION,
    COUNT(*) as count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as pct
FROM INSIDER_TRANSACTIONS
GROUP BY ACQUISITION_DISPOSITION;

-- Should have various transaction types
SELECT 
    TRANSACTION_TYPE,
    COUNT(*) as count
FROM INSIDER_TRANSACTIONS
GROUP BY TRANSACTION_TYPE
ORDER BY count DESC
LIMIT 10;

-- Should have data across time (not all recent)
SELECT 
    DATE_TRUNC('year', TRANSACTION_DATE) as year,
    COUNT(*) as transactions
FROM INSIDER_TRANSACTIONS
GROUP BY year
ORDER BY year DESC;
```

## 🎯 Success Criteria

### Initial Setup Complete When:
- ✅ Watermarks created: 2000-3000 active stocks
- ✅ Test batch (10 symbols) completes successfully
- ✅ Data loads into INSIDER_TRANSACTIONS table
- ✅ Watermarks update (LAST_SUCCESSFUL_RUN set)
- ✅ Data quality checks pass (0 issues)

### Production Runs Complete When:
- ✅ All eligible symbols processed
- ✅ Watermarks updated for successful symbols
- ✅ Failed symbols have CONSECUTIVE_FAILURES incremented
- ✅ Insider transactions loaded (may be 0 for many symbols - normal)
- ✅ S3 bucket cleaned up for next run

### Monthly Maintenance:
- ✅ Automated run completes successfully
- ✅ New insider transactions captured
- ✅ Watermarks stay current
- ✅ No rate limit errors
- ✅ Warehouse auto-suspends (costs stay low)

## 📁 File Locations

### Python Scripts
- **Extraction**: `scripts/github_actions/fetch_insider_transactions_watermark.py`

### SQL Scripts
- **Load**: `snowflake/load_insider_transactions_from_s3.sql`
- **Watermark Template**: `snowflake/setup/templates/watermark_insider_transactions.sql`

### Workflows
- **ETL Workflow**: `.github/workflows/insider_transactions_watermark_etl.yml`

### Documentation
- **This Guide**: `docs/INSIDER_TRANSACTIONS_ETL_GUIDE.md`
- **ETL Operations**: `ETL_OPERATIONS_GUIDE.md`
- **Naming Conventions**: `snowflake/NAMING_CONVENTIONS.md`

## 📞 Support & References

### Alpha Vantage API Documentation
- Insider Transactions: https://www.alphavantage.co/documentation/#insider-transactions

### Related ETL Guides
- Balance Sheet ETL (similar pattern)
- Cash Flow ETL (similar pattern)
- Income Statement ETL (similar pattern)

### Key Differences to Remember
1. **No staleness check** (insider data is unpredictable)
2. **Many "no data" responses are normal** (not all companies have transactions)
3. **Natural key includes SECURITY_TYPE** (same person, same date, different securities)
4. **Monthly schedule recommended** (vs quarterly for fundamentals)

---

**Last Updated**: October 12, 2025
