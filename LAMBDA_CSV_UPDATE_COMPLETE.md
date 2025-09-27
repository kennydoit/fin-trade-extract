# Lambda Functions Updated for CSV Output

## ✅ Completed Updates:

### 1. S3DataWriter (common/s3_data_writer.py)
- **CONVERTED** from Parquet to CSV format
- Added column order mappings for all Snowflake tables
- Updated S3 key structure to match Snowpipe stages:
  - `overview/` → `s3://fin-trade-craft-landing/overview/`
  - `time-series/` → `s3://fin-trade-craft-landing/time-series/`
  - `income-statement/` → `s3://fin-trade-craft-landing/income-statement/`
  - `balance-sheet/` → `s3://fin-trade-craft-landing/balance-sheet/`
  - `cash-flow/` → `s3://fin-trade-craft-landing/cash-flow/`
  - etc.
- Proper CSV formatting with headers matching Snowflake column names
- File naming: `{table_name}_{timestamp}_{uuid}.csv`

### 2. Overview Extractor (overview-extractor/lambda_function.py)
- **UPDATED** to use CSV output via new S3DataWriter
- Field names converted to uppercase to match Snowflake schema
- Writes to `s3://fin-trade-craft-landing/overview/`
- Column order matches OVERVIEW table exactly

### 3. Time Series Extractor (time-series-extractor/lambda_function.py)
- **CREATED** new extractor for TIME_SERIES_DAILY_ADJUSTED data
- Handles multiple time series records per symbol (one per date)
- Writes to `s3://fin-trade-craft-landing/time-series/`
- Column order matches TIME_SERIES_DAILY_ADJUSTED table exactly
- Includes proper date handling and numeric field conversion

### 4. Financial Statements Extractor (financial-statements-extractor/lambda_function.py)
- **CREATED** unified extractor for Income Statement, Balance Sheet, and Cash Flow
- Handles both quarterly and annual reports
- Single Lambda function with configurable statement type
- Writes to respective S3 paths based on statement type
- Column order matches all financial statement tables exactly
- Event format: `{"symbols": ["AAPL"], "statement_type": "INCOME_STATEMENT"}`

### 5. Requirements.txt
- **UPDATED** to remove pyarrow dependency (no longer needed for Parquet)
- Keeps pandas for CSV processing
- Added csv module documentation (built-in Python)

## 🔄 Data Pipeline Flow:
```
Lambda Function → CSV Files → S3 Bucket → Snowpipe → Snowflake Tables
```

## 📁 S3 Bucket Structure:
```
s3://fin-trade-craft-landing/
├── overview/
│   ├── overview_20250926_143052_abc123.csv
├── time-series/
│   ├── time_series_daily_adjusted_20250926_143053_def456.csv
├── income-statement/
│   ├── income_statement_20250926_143054_ghi789.csv
├── balance-sheet/
│   ├── balance_sheet_20250926_143055_jkl012.csv
├── cash-flow/
│   ├── cash_flow_20250926_143056_mno345.csv
└── landing_data/ (audit trail JSON files)
    ├── overview/
    ├── time_series_daily_adjusted/
    └── ...
```

## 🎯 Snowpipe Integration:
- All Snowpipes are configured to detect CSV files in their respective S3 paths
- Column order in CSV files matches Snowflake table column order exactly
- File format: CSV with headers
- Auto-ingestion when new files appear in S3

## 🚧 Still Needed (Quick Setup):
1. **commodities-extractor** - for COMMODITIES table
2. **economic-indicators-extractor** - for ECONOMIC_INDICATORS table  
3. **insider-transactions-extractor** - for INSIDER_TRANSACTIONS table
4. **listing-status-extractor** - for LISTING_STATUS table

## ⚡ Ready to Test:
The core pipeline is now functional with:
- Overview data (company fundamentals)
- Time series data (daily stock prices)
- Financial statements (income/balance/cash flow)

Run any of these Lambda functions and they will create CSV files that Snowpipe will automatically ingest into Snowflake!

## 🔧 Testing:
```json
{
  "symbols": ["AAPL", "MSFT"],
  "run_id": "test-run-001"
}
```

The analytics views in Snowflake will populate as soon as data flows through the pipeline.