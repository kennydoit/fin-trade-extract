# Income Statement ETL - Implementation Summary

## ✅ Complete Implementation

A complete income statement ETL workflow has been created following the **exact same pattern** as balance sheet.

---

## 📁 Files Created

### 1. **Python Extraction Script**
**File:** `scripts/github_actions/fetch_income_statement_watermark.py` (630 lines)

**Key Features:**
- ✅ Uses `INCOME_STATEMENT` API function
- ✅ Watermark-based processing (TABLE_NAME='INCOME_STATEMENT')
- ✅ 135-day staleness logic (90-day quarter + 45-day grace)
- ✅ Bulk watermark updates (single MERGE statement)
- ✅ Rate limiting (75 calls/minute for premium tier)
- ✅ S3 cleanup before extraction
- ✅ Batch processing with progress tracking
- ✅ JSON results output for GitHub Actions artifact
- ✅ Snowflake connection management (close during API extraction)

**API Endpoint:** Alpha Vantage `INCOME_STATEMENT` function  
**S3 Destination:** `s3://fin-trade-craft-landing/income_statement/`

---

### 2. **SQL Loader Script**
**File:** `snowflake/load_income_statement_from_s3.sql` (360 lines)

**Objects Created:**
- ✅ `INCOME_STATEMENT_STAGE` - External stage pointing to S3
- ✅ `INCOME_STATEMENT_STAGING` - Transient table for CSV staging
- ✅ `INCOME_STATEMENT` - Permanent table with PK

**Table Schema (28 Columns):**
| Category | Key Columns |
|----------|-------------|
| **Revenue** | TOTAL_REVENUE, GROSS_PROFIT, COST_OF_REVENUE |
| **Operating** | OPERATING_INCOME, OPERATING_EXPENSES, SG&A, R&D |
| **Interest** | INTEREST_INCOME, INTEREST_EXPENSE, NET_INTEREST_INCOME |
| **Tax** | INCOME_BEFORE_TAX, INCOME_TAX_EXPENSE |
| **Net Income** | NET_INCOME, NET_INCOME_FROM_CONTINUING_OPERATIONS |
| **Metrics** | EBIT, EBITDA |
| **Depreciation** | DEPRECIATION, DEPRECIATION_AND_AMORTIZATION |
| **Metadata** | SYMBOL_ID, LOAD_DATE, PERIOD_TYPE, FISCAL_DATE_ENDING |

**Primary Key:** `(SYMBOL, FISCAL_DATE_ENDING, PERIOD_TYPE)`

**Processing Steps:**
1. Create external stage → S3
2. List files for verification
3. Create permanent table (if not exists)
4. Create transient staging table
5. COPY INTO staging from S3
6. Show staging statistics
7. Data quality checks (missing keys, invalid dates, invalid period types)
8. MERGE into target table (upsert logic)
9. Show merge results
10. Sample final data
11. Drop staging table
12. Completion message

---

### 3. **GitHub Actions Workflow**
**File:** `.github/workflows/income_statement_watermark_etl.yml` (180 lines)

**Workflow Configuration:**
```yaml
name: Income Statement ETL - Watermark Based
trigger: workflow_dispatch + cron schedule
schedule: Sundays at 5 AM UTC ('0 5 * * 0')
runner: ubuntu-22.04
timeout: 360 minutes (6 hours)
python: 3.11
```

**Input Parameters (Match Balance Sheet Exactly):**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `exchange_filter` | choice | '' | NYSE, NASDAQ, AMEX, or blank for all |
| `max_symbols` | number | - | Limit for testing (blank = all) |
| `skip_recent_hours` | number | - | Skip recently processed symbols |
| `batch_size` | number | 50 | Symbols per batch |

**Workflow Steps:**
1. ✅ Checkout code
2. ✅ Setup Python 3.11
3. ✅ Install dependencies (boto3, requests, snowflake-connector-python)
4. ✅ Configure AWS credentials (OIDC)
5. ✅ Fetch income statement data (Python script)
6. ✅ Load data into Snowflake (SQL script via helper)
7. ✅ Upload results artifact
8. ✅ Summary report (efficiency metrics, watermark updates)

**Environment Variables:**
- Secrets: ALPHAVANTAGE_API_KEY, AWS_*, SNOWFLAKE_*
- Hardcoded: S3_BUCKET, S3_INCOME_STATEMENT_PREFIX

---

### 4. **Watermark Initialization Template**
**File:** `snowflake/setup/templates/watermark_income_statement.sql` (80 lines)

**Purpose:** Initialize ETL_WATERMARKS for all symbols with TABLE_NAME='INCOME_STATEMENT'

**Logic:**
```sql
INSERT INTO ETL_WATERMARKS
SELECT 
    'INCOME_STATEMENT' as TABLE_NAME,
    SYMBOL,
    CASE 
        WHEN DELISTING_DATE <= CURRENT_DATE() THEN 'DEL'
        WHEN ASSET_TYPE='Stock' AND STATUS='Active' THEN 'YES'
        ELSE 'NO'
    END as API_ELIGIBLE,
    ...
FROM SYMBOL
WHERE NOT EXISTS (watermark already created)
```

**Includes:**
- Summary statistics (total, eligible, delisted)
- Breakdown by exchange
- Next steps instructions

---

### 5. **Documentation**
**File:** `docs/INCOME_STATEMENT_ETL_GUIDE.md` (420 lines)

**Sections:**
- ✅ Overview & prerequisites
- ✅ GitHub secrets required
- ✅ Setup instructions (step-by-step)
- ✅ Data structure & schema
- ✅ Watermark-based processing explanation
- ✅ Troubleshooting guide
- ✅ Scheduled runs configuration
- ✅ Success criteria checklist
- ✅ Related documentation links

---

## 🎯 Consistency with Balance Sheet

The income statement ETL follows the **exact same pattern** as balance sheet:

| Aspect | Balance Sheet | Income Statement | Status |
|--------|---------------|------------------|--------|
| **Python Script Structure** | fetch_balance_sheet_watermark.py | fetch_income_statement_watermark.py | ✅ Identical |
| **API Function** | BALANCE_SHEET | INCOME_STATEMENT | ✅ Only difference |
| **Watermark Logic** | 135-day staleness | 135-day staleness | ✅ Identical |
| **S3 Prefix** | balance_sheet/ | income_statement/ | ✅ Follows pattern |
| **Stage Naming** | BALANCE_SHEET_STAGE | INCOME_STATEMENT_STAGE | ✅ Consistent |
| **Staging Table** | BALANCE_SHEET_STAGING | INCOME_STATEMENT_STAGING | ✅ Consistent |
| **Target Table** | BALANCE_SHEET | INCOME_STATEMENT | ✅ Consistent |
| **Workflow Inputs** | 4 inputs (choice/number) | 4 inputs (choice/number) | ✅ Identical |
| **Workflow Steps** | 8 steps | 8 steps | ✅ Identical |
| **Logging Format** | Emoji indicators | Emoji indicators | ✅ Identical |
| **Summary Report** | Efficiency metrics | Efficiency metrics | ✅ Identical |
| **Artifact Naming** | watermark-etl-results-{run} | watermark-etl-results-{run} | ✅ Identical |

**Only Differences (Data-Specific):**
- API function: `BALANCE_SHEET` vs `INCOME_STATEMENT`
- Column names: Assets/Liabilities vs Revenue/Expenses
- Scheduled time: 3 AM vs 5 AM UTC (avoid conflicts)

---

## 📊 Data Coverage

### Income Statement Metrics (28 columns)

**Revenue Metrics:**
- TOTAL_REVENUE
- GROSS_PROFIT
- COST_OF_REVENUE
- COST_OF_GOODS_AND_SERVICES_SOLD

**Operating Metrics:**
- OPERATING_INCOME
- OPERATING_EXPENSES
- SELLING_GENERAL_AND_ADMINISTRATIVE
- RESEARCH_AND_DEVELOPMENT
- INVESTMENT_INCOME_NET

**Interest & Other Income:**
- NET_INTEREST_INCOME
- INTEREST_INCOME
- INTEREST_EXPENSE
- NON_INTEREST_INCOME
- OTHER_NON_OPERATING_INCOME

**Depreciation:**
- DEPRECIATION
- DEPRECIATION_AND_AMORTIZATION

**Tax:**
- INCOME_BEFORE_TAX
- INCOME_TAX_EXPENSE
- INTEREST_AND_DEBT_EXPENSE

**Net Income:**
- NET_INCOME (bottom line)
- NET_INCOME_FROM_CONTINUING_OPERATIONS
- COMPREHENSIVE_INCOME_NET_OF_TAX

**Metrics:**
- EBIT
- EBITDA

### Data Characteristics
- **Period Types**: Annual + Quarterly
- **Historical Depth**: 20+ years per symbol
- **Expected Records**: ~600,000+ for all symbols (~75 records/symbol average)
- **Update Frequency**: Quarterly (with 135-day staleness protection)

---

## 🚀 Next Steps

### 1. Initialize Watermarks in Snowflake

Run this in Snowflake:
```sql
snowflake/setup/templates/watermark_income_statement.sql
```

**Expected output:**
- Total records: ~10,000+ (all symbols)
- API eligible: ~8,000+ (active stocks)
- Not eligible: ~2,000+ (ETFs, funds)
- Delisted: varies

### 2. Test with Small Batch

GitHub Actions → Income Statement ETL:
- `max_symbols`: 5
- `exchange_filter`: blank
- `skip_recent_hours`: blank
- `batch_size`: 50

**Expected results:**
- Duration: ~2-3 minutes
- Records loaded: ~400-500 (5 symbols × ~80 reports)
- Watermarks updated: 5 symbols
- Success rate: 100%

### 3. Verify Data

```sql
-- Check loaded data
SELECT COUNT(*), MIN(FISCAL_DATE_ENDING), MAX(FISCAL_DATE_ENDING)
FROM INCOME_STATEMENT;

-- Check watermarks
SELECT * FROM ETL_WATERMARKS 
WHERE TABLE_NAME = 'INCOME_STATEMENT' 
AND LAST_SUCCESSFUL_RUN IS NOT NULL;
```

### 4. Run Full Processing

Remove `max_symbols` limit and let it process all ~8,000 eligible symbols:
- Duration: ~3-5 hours
- Processing rate: ~25-30 symbols/minute
- Total records: ~600,000+

---

## 📈 Expected Costs

### First Full Run (~8,000 symbols)
| Resource | Usage | Cost |
|----------|-------|------|
| **Alpha Vantage API** | 8,000 calls | Included in premium tier (~$50/month) |
| **Snowflake Compute** | ~4 hours X-SMALL | ~$2-3 |
| **S3 Storage** | ~50 MB | ~$0.01 |
| **S3 Requests** | 8,000 PUT + LIST | ~$0.05 |
| **GitHub Actions** | ~4 hours ubuntu-22.04 | Free (included) |
| **TOTAL** | | **~$2-3 per run** |

### Subsequent Runs (Staleness Logic Active)
| Resource | Usage | Cost |
|----------|-------|------|
| **Symbols Processed** | 0-100 (only stale data) | Variable |
| **Duration** | ~5-15 minutes | ~$0.10-0.30 |
| **Total** | | **~$0.10-0.50 per run** |

**Annual Cost Estimate:**
- Weekly runs × 52 weeks = ~$50-150/year (after initial run)
- Plus Alpha Vantage premium: $600/year
- **Total: ~$650-750/year for weekly income statement updates**

---

## ✅ Production Readiness Checklist

- [x] Python script follows balance sheet pattern exactly
- [x] SQL loader follows naming conventions (stage vs staging)
- [x] Workflow matches balance sheet (inputs, steps, logging)
- [x] Watermark template created
- [x] Documentation complete
- [x] All files committed to repository
- [ ] Watermarks initialized in Snowflake
- [ ] Test run with 5 symbols successful
- [ ] Full run completed successfully
- [ ] Scheduled runs enabled

---

## 📚 Documentation References

- **Setup Guide**: `docs/INCOME_STATEMENT_ETL_GUIDE.md`
- **Naming Conventions**: `snowflake/NAMING_CONVENTIONS.md`
- **Workflow Comparison**: `.github/workflows/WORKFLOW_COMPARISON.md`
- **Python Script**: `scripts/github_actions/fetch_income_statement_watermark.py`
- **SQL Loader**: `snowflake/load_income_statement_from_s3.sql`
- **Workflow**: `.github/workflows/income_statement_watermark_etl.yml`
- **Watermark Template**: `snowflake/setup/templates/watermark_income_statement.sql`

---

## 🎉 Summary

You now have **three complete fundamentals ETL workflows**:

1. ✅ **Balance Sheet** - Assets, Liabilities, Equity
2. ✅ **Cash Flow** - Operating, Investing, Financing activities
3. ✅ **Income Statement** - Revenue, Expenses, Net Income

All three follow the **exact same pattern**:
- Watermark-based (135-day staleness)
- Batch processing
- Cost-optimized (close Snowflake during extraction)
- Production-ready (error handling, logging, metrics)
- Consistent naming (stage vs staging tables)
- Comprehensive documentation

**Ready to deploy!** 🚀
