# Workflow Comparison: Balance Sheet vs Cash Flow

## ✅ Consistency Verification

Both workflows now follow the **exact same pattern** for watermark-based ETL processing.

### Key Changes Applied to Cash Flow Workflow

| Component | Before (Inconsistent) | After (Consistent) |
|-----------|----------------------|-------------------|
| **Input Types** | `type: string` | `type: choice/number` with defaults |
| **Python Action** | `setup-python@v4` | `setup-python@v5` |
| **Batch Size** | ❌ Not included | ✅ Input with default: 50 |
| **SQL Execution** | Inline Python code | `snowflake_run_sql_file.py` helper |
| **Logging** | Minimal | Detailed with emoji indicators |
| **Summary Report** | ❌ None | ✅ Efficiency metrics calculated |
| **Artifact Name** | Static `cash-flow-etl-results` | Dynamic `watermark-etl-results-{run_number}` |
| **Job Name** | `cash-flow-etl` | `watermark-cash-flow-etl` |
| **Snowflake Env Vars** | Hardcoded in step | Uses secrets consistently |

### Identical Features (As Designed)

Both workflows now share:

1. **Input Parameters:**
   - `exchange_filter` (choice: '', NYSE, NASDAQ, AMEX)
   - `max_symbols` (number, optional)
   - `skip_recent_hours` (number, optional)
   - `batch_size` (number, default: 50)

2. **Execution Flow:**
   - Checkout → Setup Python → Install deps → AWS OIDC → Fetch data → Load to Snowflake → Upload artifact → Summary

3. **Environment Configuration:**
   - All Snowflake credentials from secrets
   - S3 bucket hardcoded (fin-trade-craft-landing)
   - Python 3.11 on ubuntu-22.04
   - 360-minute timeout
   - OIDC permissions for AWS

4. **Logging Standards:**
   - Emoji indicators (🚀, 🏢, 🌐, 🔒, 📊, ⏭️, 🔄, 📋, 📍)
   - Clear parameter visibility
   - Business logic explanation

5. **Summary Metrics:**
   - Total symbols processed
   - Success/failure counts with percentages
   - Duration in minutes
   - Processing efficiency (symbols/minute)
   - Watermark update confirmation
   - Delisted symbol tracking

### Only Differences (Data-Specific)

| Aspect | Balance Sheet | Cash Flow |
|--------|--------------|-----------|
| **API Function** | `BALANCE_SHEET` | `CASH_FLOW` |
| **S3 Prefix** | `balance_sheet/` | `cash_flow/` |
| **Python Script** | `fetch_balance_sheet_watermark.py` | `fetch_cash_flow_watermark.py` |
| **SQL Loader** | `load_balance_sheet_from_s3.sql` | `load_cash_flow_from_s3.sql` |
| **Table Columns** | Assets, Liabilities, Equity metrics | Operating, Investing, Financing activities |

## 🎯 Production Readiness

Both workflows are now:
- ✅ Fully parameterized for flexible execution
- ✅ Using modern GitHub Actions (v4/v5)
- ✅ Secure AWS authentication (OIDC, no keys)
- ✅ Optimized for batch processing
- ✅ Providing comprehensive observability
- ✅ Following consistent patterns for maintainability

## 📝 Testing Checklist

Before production use, verify:

- [ ] Test with `max_symbols: 5` to validate end-to-end flow
- [ ] Verify summary report displays all metrics
- [ ] Confirm artifact upload includes run number
- [ ] Check that batch_size parameter affects processing
- [ ] Validate watermark updates in ETL_WATERMARKS table
- [ ] Ensure delisted symbols are marked as 'DEL' correctly

## 🔧 Maintenance Notes

When updating either workflow:
1. **Update both workflows** to maintain consistency
2. Only change data-specific elements (API function, column names, table references)
3. Keep all infrastructure, logging, and flow patterns identical
4. Test changes in both workflows before deploying
