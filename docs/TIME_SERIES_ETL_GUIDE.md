# Time Series Daily Adjusted ETL - Quick Start Guide

## 🎯 Overview
Watermark-based ETL for TIME_SERIES_DAILY_ADJUSTED data from Alpha Vantage API. Extracts daily stock prices with adjustments for dividends and splits for all tradable securities (stocks AND ETFs).

## 📋 Important Documentation
- **ETL Development Pattern**: See `docs/ETL_DEVELOPMENT_PATTERN.md` for the 5-file standard
- **Naming Conventions**: See `snowflake/NAMING_CONVENTIONS.md`

## 🔐 Prerequisites

### GitHub Secrets Required
Same as other ETLs - see Balance Sheet or Cash Flow ETL guides for complete list.

### Key Configuration
| Parameter | Value |
|-----------|-------|
| `S3_BUCKET` | `fin-trade-craft-landing` |
| `S3_TIME_SERIES_PREFIX` | `time_series_daily_adjusted/` |
| Schedule | Weekly recommended |
| Staleness Check | 5 days (more frequent than fundamentals) |

## 📊 Data Structure

### Table: TIME_SERIES_DAILY_ADJUSTED

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| `SYMBOL` | VARCHAR(20) | Stock ticker symbol |
| `DATE` | DATE | Trading date |
| `OPEN` | NUMBER(18,4) | Opening price |
| `HIGH` | NUMBER(18,4) | Intraday high |
| `LOW` | NUMBER(18,4) | Intraday low |
| `CLOSE` | NUMBER(18,4) | Closing price |
| `ADJUSTED_CLOSE` | NUMBER(18,4) | Close price adjusted for splits/dividends |
| `VOLUME` | NUMBER(20,0) | Trading volume |
| `DIVIDEND_AMOUNT` | NUMBER(18,6) | Dividend amount (if any) |
| `SPLIT_COEFFICIENT` | NUMBER(18,6) | Split coefficient (1.0 = no split) |
| `SYMBOL_ID` | NUMBER(38,0) | Foreign key to SYMBOL table |
| `LOAD_DATE` | TIMESTAMP_NTZ | When record was loaded |

**Primary Key:** `(SYMBOL, DATE)`

**Important Differences from Fundamentals:**
- ✅ Includes **stocks AND ETFs** (all tradable securities)
- ✅ **Daily data** vs quarterly fundamentals
- ✅ **5-day staleness** check (not 135 days)
- ✅ Two modes: **compact** (100 days) or **full** (20+ years)

## 🚀 Initial Setup

### Step 1: Initialize Watermarks in Snowflake
```sql
-- Execute: snowflake/setup/templates/watermark_time_series.sql
-- Expected: ~3000-4000 records (stocks + ETFs)
```

**Verify initialization:**
```sql
SELECT 
    ASSET_TYPE,
    API_ELIGIBLE,
    COUNT(*) as count
FROM ETL_WATERMARKS
WHERE TABLE_NAME = 'TIME_SERIES_DAILY_ADJUSTED'
GROUP BY ASSET_TYPE, API_ELIGIBLE
ORDER BY ASSET_TYPE, API_ELIGIBLE;

-- Expected:
-- Stock, YES: ~2000-3000
-- ETF, YES: ~500-1000
-- Various, NO: ~500
-- Various, DEL: ~100-500
```

### Step 2: Test with Small Batch (Compact Mode)
1. **GitHub Actions** → **Time Series ETL** → **Run workflow**
2. Parameters:
   - `exchange_filter`: NYSE
   - `max_symbols`: **5**
   - `output_size`: **compact** (100 days of data)
   - `skip_recent_hours`: (blank)
3. Expected duration: 3-5 minutes

### Step 3: Verify Test Results
```sql
-- Check total records loaded
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT SYMBOL) as symbols,
    MIN(DATE) as earliest_date,
    MAX(DATE) as latest_date
FROM TIME_SERIES_DAILY_ADJUSTED;
-- Expected: 500-700 records (5 symbols × 100 days each)

-- Check data quality
SELECT 
    SYMBOL,
    COUNT(*) as days_of_data,
    MIN(DATE) as earliest_date,
    MAX(DATE) as latest_date,
    ROUND(AVG(VOLUME), 0) as avg_volume
FROM TIME_SERIES_DAILY_ADJUSTED
GROUP BY SYMBOL
ORDER BY SYMBOL;
```

### Step 4: Run Full Production (Compact Mode)
For regular updates (recommended):
- `exchange_filter`: (blank)
- `max_symbols`: (blank)
- `output_size`: **compact**
- `skip_recent_hours`: **168** (7 days)

Expected: 2-4 hours for ~3000-4000 symbols

### Step 5: Backfill Historical Data (Full Mode) - Optional
For complete historical data:
- `output_size`: **full**
- `max_symbols`: (blank)
- `skip_recent_hours`: (blank)

⚠️ **Warning:** Full mode takes 10-20 hours and loads millions of records!

## 🔄 Running the ETL

### Manual Trigger
**GitHub Actions → Time Series ETL → Run workflow**

### Workflow Parameters

| Parameter | Description | Recommended Value |
|-----------|-------------|-------------------|
| `exchange_filter` | Filter by exchange | Blank (all) |
| `max_symbols` | Limit symbols (testing) | Blank (production), 5 (testing) |
| `output_size` | 'compact' or 'full' | **compact** (100 days) |
| `skip_recent_hours` | Skip recent symbols | 168 (weekly), 24 (daily) |

### Recommended Schedule

```yaml
schedule:
  - cron: '0 3 * * 1'  # Weekly: Monday at 3 AM UTC
```

**For daily updates (optional):**
```yaml
schedule:
  - cron: '0 4 * * *'  # Daily at 4 AM UTC
```

## 📈 Data Quality Checks

**1. Check Recent Data Coverage:**
```sql
SELECT 
    MAX(DATE) as most_recent_date,
    DATEDIFF('day', MAX(DATE), CURRENT_DATE()) as days_old,
    COUNT(DISTINCT SYMBOL) as symbols_with_recent_data
FROM TIME_SERIES_DAILY_ADJUSTED
WHERE DATE >= DATEADD('day', -7, CURRENT_DATE());
-- days_old should be 0-1 (market may be closed on weekends)
```

**2. Check for Gaps in Data:**
```sql
SELECT 
    SYMBOL,
    COUNT(*) as total_days,
    MIN(DATE) as earliest,
    MAX(DATE) as latest,
    DATEDIFF('day', MIN(DATE), MAX(DATE())) as span_days,
    span_days - total_days as missing_days_approx
FROM TIME_SERIES_DAILY_ADJUSTED
GROUP BY SYMBOL
HAVING missing_days_approx > 100  -- Allow for weekends/holidays
ORDER BY missing_days_approx DESC
LIMIT 20;
```

**3. Check for Invalid Prices:**
```sql
SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN CLOSE <= 0 THEN 1 ELSE 0 END) as invalid_close,
    SUM(CASE WHEN HIGH < LOW THEN 1 ELSE 0 END) as high_below_low,
    SUM(CASE WHEN VOLUME < 0 THEN 1 ELSE 0 END) as negative_volume
FROM TIME_SERIES_DAILY_ADJUSTED;
-- All checks should be 0
```

**4. Check Split/Dividend Coverage:**
```sql
SELECT 
    'Splits Recorded' as event_type,
    COUNT(*) as count
FROM TIME_SERIES_DAILY_ADJUSTED
WHERE SPLIT_COEFFICIENT != 1.0
UNION ALL
SELECT 
    'Dividends Recorded',
    COUNT(*)
FROM TIME_SERIES_DAILY_ADJUSTED
WHERE DIVIDEND_AMOUNT > 0;
```

## 🔧 Troubleshooting

### Issue: Compact vs Full Mode Confusion

**Symptoms:**
- Only 100 days of data per symbol
- Missing historical data

**Solution:**
- **Compact mode**: Last 100 trading days (fast, recommended for regular updates)
- **Full mode**: 20+ years of data (slow, use for initial backfill)

### Issue: High API Rate Limit Errors

**Symptoms:**
- Many "rate limit" errors in logs
- Failed extractions

**Solutions:**
- Reduce concurrent processing (batch_size)
- Upgrade to Alpha Vantage premium tier (75 req/min vs 5)
- Increase `skip_recent_hours` to reduce symbols processed

### Issue: Weekend/Holiday Gaps

**Symptoms:**
- "Missing days" in data quality checks
- Irregular date ranges

**Explanation:**
- Markets closed on weekends, holidays
- This is expected behavior
- Use trading day counts, not calendar days

## 📁 File Locations

1. **Python Script:** `scripts/github_actions/fetch_time_series_watermark.py`
2. **SQL Load Script:** `snowflake/load_time_series_from_s3.sql`
3. **Workflow:** `.github/workflows/time_series_watermark_etl.yml`
4. **Watermark Template:** `snowflake/setup/templates/watermark_time_series.sql`
5. **This Guide:** `docs/TIME_SERIES_ETL_GUIDE.md`

## 📚 Related Documentation

- **ETL Development Pattern:** `docs/ETL_DEVELOPMENT_PATTERN.md`
- **Balance Sheet ETL:** `docs/BALANCE_SHEET_ETL_GUIDE.md`
- **Cash Flow ETL:** `docs/CASH_FLOW_ETL_GUIDE.md`
- **Income Statement ETL:** `docs/INCOME_STATEMENT_ETL_GUIDE.md`

## 🔄 Maintenance Schedule

### Weekly (Recommended)
- Run ETL with compact mode
- `skip_recent_hours=168` (7 days)
- Updates recent data for all symbols

### Monthly (Optional)
- Review data quality metrics
- Check for symbols with stale data
- Validate watermark updates

### Quarterly (Initial Backfill Only)
- Run full mode for new symbols
- Backfill historical data as needed

---

**Last Updated:** 2025-10-12  
**ETL Version:** Watermark-based (v2.0)  
**Pattern:** 5-file standard
